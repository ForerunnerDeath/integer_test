"""Integration tests of provider lifecycle with real PostgreSQL."""

import asyncio
import hashlib
import hmac
import json
import os
import uuid
from collections.abc import AsyncIterator

import httpx
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select

from app.config import settings
from app.db import engine, session_factory
from app.main import app
from app.models import GenerationTask, LedgerEntry, LedgerKind, TaskStatus
from app.worker import dispatch_attempt
from test_generation import fund

pytestmark = pytest.mark.skipif(
    os.getenv("RUN_DB_TESTS") != "1",
    reason="Requires PostgreSQL: docker compose run --build --rm tests",
)


@pytest_asyncio.fixture(autouse=True)
async def dispose_db_pool_after_provider_test() -> AsyncIterator[None]:
    try:
        yield
    finally:
        await engine.dispose()


def signed_provider(data: dict[str, object]) -> tuple[bytes, dict[str, str]]:
    raw = json.dumps(data, separators=(",", ":")).encode()
    sig = hmac.new(
        settings.provider_webhook_secret.encode(), raw, hashlib.sha256
    ).hexdigest()
    return raw, {"X-Signature": sig}


async def generate(client: AsyncClient, user_id: str, model: str = "image-fast") -> str:
    created = await client.post(
        "/generate",
        json={"user_id": user_id, "model_slug": model, "prompt": "hello"},
    )
    assert created.status_code == 202, created.text
    return str(created.json()["task_id"])


async def dispatch_and_capture(attempt_id: str) -> dict[str, str]:
    captured: dict[str, str] = {}

    def provider(request: httpx.Request) -> httpx.Response:
        data = json.loads(request.content)
        captured.update(data)
        return httpx.Response(202, json={"external_id": data["attempt_id"]})

    async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as client:
        await dispatch_attempt(uuid.UUID(attempt_id), client)
    return captured


@pytest.mark.asyncio
async def test_provider_success_and_duplicate_callback() -> None:
    user_id = str(uuid.uuid4())
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        await fund(client, user_id, 20)
        task_id = await generate(client, user_id)
        first_state = (await client.get(f"/tasks/{task_id}")).json()
        data = await dispatch_and_capture(first_state["current_leg"]["id"])
        assert data["task_id"] == task_id
        event = {
            "task_id": task_id,
            "attempt_id": data["attempt_id"],
            "external_id": data["attempt_id"],
            "status": "completed",
            "result_url": "https://example.invalid/result",
        }
        raw, headers = signed_provider(event)
        results = await asyncio.gather(
            *(
                client.post("/webhooks/providers/alpha", content=raw, headers=headers)
                for _ in range(8)
            )
        )
        assert all(item.status_code == 200 for item in results)
        assert [r.json()["outcome"] for r in results].count("completed") == 1
        task = await client.get(f"/tasks/{task_id}")
        balance = await client.get(f"/users/{user_id}/balance")
    assert task.json()["status"] == "completed"
    assert task.json()["result_url"] == event["result_url"]
    assert balance.json()["balance"] == 18


@pytest.mark.asyncio
async def test_technical_error_advances_to_beta_and_late_alpha_ignored() -> None:
    user_id = str(uuid.uuid4())
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        await fund(client, user_id, 50)
        task_id = await generate(client, user_id)
        first_state = (await client.get(f"/tasks/{task_id}")).json()
        first = await dispatch_and_capture(first_state["current_leg"]["id"])
        fail = {
            "task_id": task_id,
            "attempt_id": first["attempt_id"],
            "external_id": first["attempt_id"],
            "status": "failed",
            "error_type": "technical",
        }
        raw, headers = signed_provider(fail)
        response = await client.post("/webhooks/providers/alpha", content=raw, headers=headers)
        assert response.status_code == 200
        assert response.json()["outcome"] == "advanced"
        second_state = (await client.get(f"/tasks/{task_id}")).json()
        second = await dispatch_and_capture(second_state["current_leg"]["id"])
        assert second["attempt_id"] != first["attempt_id"]
        done = {
            "task_id": task_id,
            "attempt_id": second["attempt_id"],
            "external_id": second["attempt_id"],
            "status": "completed",
            "result_url": "https://example.invalid/beta",
        }
        raw, headers = signed_provider(done)
        response = await client.post("/webhooks/providers/beta", content=raw, headers=headers)
        assert response.json()["outcome"] == "completed"
        late = {
            **done,
            "attempt_id": first["attempt_id"],
            "external_id": first["attempt_id"],
        }
        raw, headers = signed_provider(late)
        response = await client.post("/webhooks/providers/alpha", content=raw, headers=headers)
        assert response.json()["outcome"] == "ignored"
        task = await client.get(f"/tasks/{task_id}")
    assert task.json()["status"] == "completed"
    assert len(task.json()["attempts"]) == 2
    assert task.json()["result_url"] == done["result_url"]


@pytest.mark.asyncio
async def test_parallel_final_failures_refund_exactly_once() -> None:
    user_id = str(uuid.uuid4())
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        await fund(client, user_id, 30)
        task_id = await generate(client, user_id)
        first = await dispatch_and_capture()
        fail = {
            "task_id": task_id,
            "attempt_id": first["attempt_id"],
            "external_id": first["attempt_id"],
            "status": "failed",
            "error_type": "technical",
        }
        raw, headers = signed_provider(fail)
        result = await client.post("/webhooks/providers/alpha", content=raw, headers=headers)
        assert result.json()["outcome"] == "advanced"
        second = await dispatch_and_capture()
        final = {**fail, "attempt_id": second["attempt_id"], "external_id": second["attempt_id"]}
        raw, headers = signed_provider(final)
        responses = await asyncio.gather(
            *(
                client.post("/webhooks/providers/beta", content=raw, headers=headers)
                for _ in range(10)
            )
        )
        assert all(response.status_code == 200 for response in responses)
        assert sum(r.json()["outcome"] == "failed" for r in responses) == 1
        balance = await client.get(f"/users/{user_id}/balance")
    assert balance.json()["balance"] == 30
    async with session_factory() as session:
        task = await session.get(GenerationTask, uuid.UUID(task_id))
        refunds = (
            await session.scalars(
                select(LedgerEntry).where(
                    LedgerEntry.operation_key == f"refund:{task_id}",
                    LedgerEntry.kind == LedgerKind.refund,
                )
            )
        ).all()
    assert task is not None
    assert task.status == TaskStatus.failed
    assert len(refunds) == 1
