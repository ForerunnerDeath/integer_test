"""Watchdog timeout recovery and outbox lease fencing against real PostgreSQL."""

import asyncio
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select

from app.db import session_factory
from app.main import app
from app.models import LedgerEntry, OutboxEvent, ProviderAttempt, TaskStatus
from app.watchdog import watchdog_once
from app.worker import claim_outbox, finish_outbox
from test_generation import fund
from test_provider_flow import dispatch_and_capture, generate, signed_provider

pytestmark = pytest.mark.usefixtures("dispose_db_pool_after_provider_test")


async def set_attempt_deadline(attempt_id: str, seconds_ago: int = 5) -> None:
    async with session_factory() as session:
        async with session.begin():
            attempt = await session.get(ProviderAttempt, uuid.UUID(attempt_id))
            assert attempt is not None
            attempt.deadline_at = datetime.now(UTC) - timedelta(seconds=seconds_ago)


@pytest.mark.asyncio
async def test_timeout_fallback_then_late_alpha_success_is_ignored() -> None:
    user_id = str(uuid.uuid4())
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        await fund(client, user_id, 50)
        task_id = await generate(client, user_id)
        first_state = (await client.get(f"/tasks/{task_id}")).json()
        first_id = first_state["current_leg"]["id"]
        await dispatch_and_capture(first_id)
        await set_attempt_deadline(first_id)

        assert await watchdog_once() == 1
        state = (await client.get(f"/tasks/{task_id}")).json()
        assert state["status"] == "processing"
        assert len(state["attempts"]) == 2
        assert state["current_leg"]["provider"] == "beta"
        assert state["attempts"][0]["error_type"] == "timeout"

        late = {
            "task_id": task_id,
            "attempt_id": first_id,
            "external_id": first_id,
            "status": "completed",
            "result_url": "https://example.invalid/too-late",
        }
        raw, headers = signed_provider(late)
        ignored = await client.post(
            "/webhooks/providers/alpha", content=raw, headers=headers
        )
        assert ignored.status_code == 200
        assert ignored.json()["outcome"] == "ignored"

        second_id = state["current_leg"]["id"]
        await dispatch_and_capture(second_id)
        completed = {**late, "attempt_id": second_id, "external_id": second_id}
        raw, headers = signed_provider(completed)
        result = await client.post("/webhooks/providers/beta", content=raw, headers=headers)
        assert result.json()["outcome"] == "completed"
        balance = await client.get(f"/users/{user_id}/balance")
    assert balance.json()["balance"] == 48


@pytest.mark.asyncio
async def test_parallel_watchdogs_refund_once_and_reject_late_success() -> None:
    user_id = str(uuid.uuid4())
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        await fund(client, user_id, 30)
        task_id = await generate(client, user_id)
        first_state = (await client.get(f"/tasks/{task_id}")).json()
        first_id = first_state["current_leg"]["id"]
        await dispatch_and_capture(first_id)
        await set_attempt_deadline(first_id)
        await asyncio.gather(*(watchdog_once() for _ in range(8)))

        second = (await client.get(f"/tasks/{task_id}")).json()
        assert len(second["attempts"]) == 2
        second_id = second["current_leg"]["id"]
        await dispatch_and_capture(second_id)
        await set_attempt_deadline(second_id)
        await asyncio.gather(*(watchdog_once() for _ in range(10)))

        final = (await client.get(f"/tasks/{task_id}")).json()
        assert final["status"] == TaskStatus.failed
        assert all(attempt["status"] == "failed" for attempt in final["attempts"])

        late = {
            "task_id": task_id,
            "attempt_id": second_id,
            "external_id": second_id,
            "status": "completed",
            "result_url": "https://example.invalid/after-refund",
        }
        raw, headers = signed_provider(late)
        ignored = await client.post(
            "/webhooks/providers/beta", content=raw, headers=headers
        )
        assert ignored.status_code == 200
        assert ignored.json()["outcome"] == "ignored"
        final = (await client.get(f"/tasks/{task_id}")).json()
        balance = await client.get(f"/users/{user_id}/balance")

    assert final["status"] == "failed"
    assert final["result_url"] is None
    assert balance.json()["balance"] == 30
    async with session_factory() as session:
        entries = (
            await session.scalars(
                select(LedgerEntry).where(
                    LedgerEntry.operation_key == f"refund:{task_id}"
                )
            )
        ).all()
    assert len(entries) == 1
    assert entries[0].delta == 2


@pytest.mark.asyncio
async def test_stale_outbox_lease_cannot_acknowledge_new_lease() -> None:
    user_id = str(uuid.uuid4())
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        await fund(client, user_id, 10)
        task_id = await generate(client, user_id)

    first = await claim_outbox()
    assert first is not None
    event_id, original_lease = first
    async with session_factory() as session:
        async with session.begin():
            event = await session.get(OutboxEvent, event_id, with_for_update=True)
            assert event is not None
            assert event.payload["task_id"] == task_id
            event.claimed_until = datetime.now(UTC) - timedelta(seconds=1)

    second = await claim_outbox()
    assert second is not None
    assert second[0] == event_id
    await finish_outbox(event_id, original_lease, success=True)
    async with session_factory() as session:
        event = await session.get(OutboxEvent, event_id)
        assert event is not None
        assert event.done_at is None
        assert event.claimed_until == second[1]
    await finish_outbox(event_id, second[1], success=True)
    async with session_factory() as session:
        event = await session.get(OutboxEvent, event_id)
        assert event is not None
        assert event.done_at is not None
