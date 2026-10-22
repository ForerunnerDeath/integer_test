"""Regression tests for the two confirmed High audit findings."""

import asyncio
import os
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta

import httpx
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select
from test_generation import fund
from test_payments import signed_body
from test_provider_flow import generate, signed_provider

from app.db import engine, session_factory
from app.ledger import MAX_BALANCE
from app.main import app
from app.models import OutboxEvent, TaskStatus
from app.worker import claim_outbox, dispatch_attempt

pytestmark = pytest.mark.skipif(
    os.getenv("RUN_DB_TESTS") != "1",
    reason="Requires PostgreSQL: docker compose run --build --rm tests",
)


@pytest_asyncio.fixture(autouse=True)
async def dispose_audit_regression_pool() -> AsyncIterator[None]:
    try:
        yield
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_pending_refund_headroom_blocks_credit_at_bigint_limit() -> None:
    user_id = str(uuid.uuid4())
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as api:
        await fund(api, user_id, MAX_BALANCE)
        task_id = await generate(api, user_id)
        payment: dict[str, object] = {
            "payment_id": uuid.uuid4().hex,
            "user_id": user_id,
            "tokens": 2,
            "status": "completed",
        }
        raw, headers = signed_body(payment)
        rejected = await api.post("/webhooks/payments", content=raw, headers=headers)
        assert rejected.status_code == 409
        assert (await api.get(f"/users/{user_id}/balance")).json()["balance"] == MAX_BALANCE - 2

        first = (await api.get(f"/tasks/{task_id}")).json()["current_leg"]["id"]
        await failed_callback(api, task_id, first, "alpha")
        second = (await api.get(f"/tasks/{task_id}")).json()["current_leg"]["id"]
        await failed_callback(api, task_id, second, "beta")
        task = (await api.get(f"/tasks/{task_id}")).json()
        balance = (await api.get(f"/users/{user_id}/balance")).json()

    assert task["status"] == "failed"
    assert balance["balance"] == MAX_BALANCE
    assert sum(entry["kind"] == "refund" for entry in balance["operations"]) == 1


async def failed_callback(api: AsyncClient, task_id: str, attempt_id: str, provider: str) -> None:
    # Model the accepted POST directly, without a running fake provider.
    async def transport(request: httpx.Request) -> httpx.Response:
        return httpx.Response(202, json={"external_id": attempt_id})

    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as client:
        await dispatch_attempt(uuid.UUID(attempt_id), client)

    payload: dict[str, object] = {
        "task_id": task_id,
        "attempt_id": attempt_id,
        "external_id": attempt_id,
        "status": "failed",
        "error_type": "technical",
    }
    raw, headers = signed_provider(payload)
    result = await api.post(f"/webhooks/providers/{provider}", content=raw, headers=headers)
    assert result.status_code == 200


@pytest.mark.asyncio
async def test_stale_http_timeout_cannot_overrule_newer_accepted_202() -> None:
    user_id = str(uuid.uuid4())
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as api:
        await fund(api, user_id, 20)
        task_id = await generate(api, user_id)
        task = (await api.get(f"/tasks/{task_id}")).json()
        attempt_id = uuid.UUID(task["current_leg"]["id"])
        async with session_factory() as session:
            event = await session.scalar(
                select(OutboxEvent).where(OutboxEvent.event_key == f"dispatch:{attempt_id}")
            )
            assert event is not None
            event_id = event.id
            event.available_at = datetime.now(UTC) - timedelta(days=1)
            await session.commit()

        initial = await claim_outbox()
        assert initial is not None
        assert initial[0] == event_id

        old_started = asyncio.Event()
        old_release = asyncio.Event()

        async def slow_timeout(request: httpx.Request) -> httpx.Response:
            old_started.set()
            await old_release.wait()
            raise httpx.ReadTimeout("late timeout", request=request)

        async with httpx.AsyncClient(transport=httpx.MockTransport(slow_timeout)) as old_client:
            old_request = asyncio.create_task(
                dispatch_attempt(attempt_id, old_client, dispatch_lease=initial)
            )
            await old_started.wait()
            async with session_factory() as session:
                async with session.begin():
                    event = await session.get(OutboxEvent, event_id, with_for_update=True)
                    assert event is not None
                    event.claimed_until = datetime.now(UTC) - timedelta(seconds=1)

            renewed = await claim_outbox()
            assert renewed is not None and renewed[0] == event_id

            async def accepted(request: httpx.Request) -> httpx.Response:
                return httpx.Response(202, json={"external_id": str(attempt_id)})

            async with httpx.AsyncClient(transport=httpx.MockTransport(accepted)) as client:
                await dispatch_attempt(attempt_id, client, dispatch_lease=renewed)

            old_release.set()
            await old_request

        final = (await api.get(f"/tasks/{task_id}")).json()

    assert final["status"] == TaskStatus.processing
    assert final["current_leg"]["status"] == "waiting"
    assert len(final["attempts"]) == 1
