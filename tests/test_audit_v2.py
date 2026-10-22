"""Regression tests for the second independent audit findings."""

import asyncio
import os
import uuid
from collections.abc import AsyncIterator

import httpx
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text
from test_generation import fund
from test_provider_flow import generate, signed_provider

from app.db import engine, session_factory
from app.generation import TaskResponse, _read_task_snapshot
from app.main import app
from app.models import AttemptStatus, GenerationTask, TaskStatus
from app.worker import dispatch_attempt

pytestmark = pytest.mark.skipif(
    os.getenv("RUN_DB_TESTS") != "1",
    reason="Requires PostgreSQL: docker compose run --build --rm tests",
)


@pytest_asyncio.fixture(autouse=True)
async def dispose_second_audit_pool() -> AsyncIterator[None]:
    try:
        yield
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [[], None, "unexpected", 1, {}, {"external_id": 12}, {"external_id": "x" * 161}],
)
async def test_malformed_provider_202_immediately_advances(payload: object) -> None:
    user_id = str(uuid.uuid4())
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as api:
        await fund(api, user_id, 10)
        task_id = await generate(api, user_id)
        first_id = (await api.get(f"/tasks/{task_id}")).json()["current_leg"]["id"]

        def invalid_202(request: httpx.Request) -> httpx.Response:
            return httpx.Response(202, json=payload)

        async with httpx.AsyncClient(transport=httpx.MockTransport(invalid_202)) as client:
            await dispatch_attempt(uuid.UUID(first_id), client)

        task = (await api.get(f"/tasks/{task_id}")).json()
        balance = (await api.get(f"/users/{user_id}/balance")).json()

    assert task["status"] == "processing"
    assert len(task["attempts"]) == 2
    assert task["attempts"][0]["status"] == "failed"
    assert task["attempts"][0]["error_type"] == "technical"
    assert task["current_leg"]["provider"] == "beta"
    assert balance["balance"] == 8


@pytest.mark.asyncio
async def test_task_status_returns_consistent_snapshot_during_failover() -> None:
    user_id = str(uuid.uuid4())
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as api:
        await fund(api, user_id, 20)
        task_id = await generate(api, user_id)
        first_id = (await api.get(f"/tasks/{task_id}")).json()["current_leg"]["id"]

        def accepted(request: httpx.Request) -> httpx.Response:
            return httpx.Response(202, json={"external_id": first_id})

        async with httpx.AsyncClient(transport=httpx.MockTransport(accepted)) as client:
            await dispatch_attempt(uuid.UUID(first_id), client)

        first_read = asyncio.Event()
        continue_read = asyncio.Event()

        # This mirrors the endpoint's transaction isolation and interleaves a
        # committed provider transition between task and attempt reads.
        async def snapshot_reader() -> TaskResponse:
            async with session_factory() as session:
                async with session.begin():
                    await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
                    row = await session.get(GenerationTask, uuid.UUID(task_id))
                    assert row is not None
                    first_read.set()
                    await continue_read.wait()
                    result = await _read_task_snapshot(session, uuid.UUID(task_id))
                    return result

        pending = asyncio.create_task(snapshot_reader())
        await first_read.wait()

        raw, headers = signed_provider(
            {
                "task_id": task_id,
                "attempt_id": first_id,
                "external_id": first_id,
                "status": "failed",
                "error_type": "technical",
            }
        )
        response = await api.post("/webhooks/providers/alpha", content=raw, headers=headers)
        assert response.status_code == 200
        assert response.json()["outcome"] == "advanced"
        continue_read.set()
        snapshot = await pending

        assert snapshot.current_leg is not None
        assert snapshot.current_leg.id == uuid.UUID(first_id)
        assert snapshot.current_leg.status == AttemptStatus.waiting
        assert len(snapshot.attempts) == 1
        assert snapshot.status == TaskStatus.processing

        newer = (await api.get(f"/tasks/{task_id}")).json()
        assert len(newer["attempts"]) == 2
        assert newer["current_leg"]["provider"] == "beta"
