"""Uncovered provider rejection paths against real PostgreSQL."""

import os
import uuid
from collections.abc import AsyncIterator

import httpx
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from test_generation import fund
from test_provider_flow import generate, signed_provider

from app.db import engine
from app.main import app
from app.worker import dispatch_attempt

pytestmark = pytest.mark.skipif(
    os.getenv("RUN_DB_TESTS") != "1",
    reason="Requires PostgreSQL: docker compose run --build --rm tests",
)


@pytest_asyncio.fixture(autouse=True)
async def dispose_provider_error_test_pool() -> AsyncIterator[None]:
    try:
        yield
    finally:
        await engine.dispose()


async def send_to_provider(attempt_id: str, http_status: int | None) -> None:
    """None simulates a POST timeout; 202 is a normal provider submission."""

    def respond(request: httpx.Request) -> httpx.Response:
        if http_status is None:
            raise httpx.ReadTimeout("Simulated provider timeout", request=request)
        if http_status == 202:
            return httpx.Response(202, json={"external_id": str(attempt_id)})
        return httpx.Response(http_status, json={"detail": "Simulated rejection"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        await dispatch_attempt(uuid.UUID(attempt_id), client)


@pytest.mark.asyncio
@pytest.mark.parametrize("http_status", [503, None])
async def test_sync_technical_failure_advances_to_beta(
    http_status: int | None,
) -> None:
    user_id = str(uuid.uuid4())
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        await fund(client, user_id, 20)
        task_id = await generate(client, user_id)
        before = (await client.get(f"/tasks/{task_id}")).json()
        await send_to_provider(before["current_leg"]["id"], http_status)

        state = (await client.get(f"/tasks/{task_id}")).json()
        balance = (await client.get(f"/users/{user_id}/balance")).json()

    assert state["status"] == "processing"
    assert [attempt["provider"] for attempt in state["attempts"]] == ["alpha", "beta"]
    assert state["attempts"][0]["status"] == "failed"
    assert state["attempts"][0]["error_type"] == "technical"
    assert state["current_leg"]["status"] == "pending"
    assert balance["balance"] == 18
    assert len(balance["operations"]) == 2


@pytest.mark.asyncio
async def test_sync_payload_rejection_immediately_refunds() -> None:
    user_id = str(uuid.uuid4())
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        await fund(client, user_id, 10)
        task_id = await generate(client, user_id)
        state = (await client.get(f"/tasks/{task_id}")).json()
        first_id = state["current_leg"]["id"]
        await send_to_provider(first_id, 400)
        # A replay of the same outbox event must not produce another refund.
        await send_to_provider(first_id, 400)

        state = (await client.get(f"/tasks/{task_id}")).json()
        balance = (await client.get(f"/users/{user_id}/balance")).json()

    assert state["status"] == "failed"
    assert state["result_url"] is None
    assert len(state["attempts"]) == 1
    assert state["attempts"][0]["error_type"] == "payload"
    assert balance["balance"] == 10
    assert sum(item["kind"] == "refund" for item in balance["operations"]) == 1


@pytest.mark.asyncio
async def test_video_moderation_skips_alpha_and_refunds() -> None:
    user_id = str(uuid.uuid4())
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        await fund(client, user_id, 30)
        task_id = await generate(client, user_id, model="video-long")
        before = (await client.get(f"/tasks/{task_id}")).json()
        first_id = before["current_leg"]["id"]
        assert before["current_leg"]["provider"] == "beta"
        await send_to_provider(first_id, 202)

        failed: dict[str, object] = {
            "task_id": task_id,
            "attempt_id": first_id,
            "external_id": first_id,
            "status": "failed",
            "error_type": "moderation",
        }
        raw, headers = signed_provider(failed)
        response = await client.post("/webhooks/providers/beta", content=raw, headers=headers)
        assert response.status_code == 200
        assert response.json()["outcome"] == "failed"

        state = (await client.get(f"/tasks/{task_id}")).json()
        balance = (await client.get(f"/users/{user_id}/balance")).json()

    assert state["status"] == "failed"
    assert len(state["attempts"]) == 1
    assert state["attempts"][0]["error_type"] == "moderation"
    assert balance["balance"] == 30
    assert sum(item["kind"] == "refund" for item in balance["operations"]) == 1


@pytest.mark.asyncio
async def test_video_technical_failure_uses_alpha() -> None:
    user_id = str(uuid.uuid4())
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        await fund(client, user_id, 30)
        task_id = await generate(client, user_id, model="video-long")
        before = (await client.get(f"/tasks/{task_id}")).json()
        first_id = before["current_leg"]["id"]
        await send_to_provider(first_id, 202)

        failed: dict[str, object] = {
            "task_id": task_id,
            "attempt_id": first_id,
            "external_id": first_id,
            "status": "failed",
            "error_type": "technical",
        }
        raw, headers = signed_provider(failed)
        result = await client.post("/webhooks/providers/beta", content=raw, headers=headers)
        assert result.status_code == 200
        assert result.json()["outcome"] == "advanced"

        next_state = (await client.get(f"/tasks/{task_id}")).json()
        assert next_state["current_leg"]["provider"] == "alpha"
        second_id = next_state["current_leg"]["id"]
        await send_to_provider(second_id, 202)

        success: dict[str, object] = {
            "task_id": task_id,
            "attempt_id": second_id,
            "external_id": second_id,
            "status": "completed",
            "result_url": "https://example.invalid/video",
        }
        raw, headers = signed_provider(success)
        response = await client.post("/webhooks/providers/alpha", content=raw, headers=headers)
        assert response.status_code == 200
        assert response.json()["outcome"] == "completed"
        completed = (await client.get(f"/tasks/{task_id}")).json()
        balance = (await client.get(f"/users/{user_id}/balance")).json()

    assert completed["status"] == "completed"
    assert [attempt["provider"] for attempt in completed["attempts"]] == ["beta", "alpha"]
    assert balance["balance"] == 5


@pytest.mark.asyncio
async def test_wrong_provider_signature_rejected_without_transition() -> None:
    user_id = str(uuid.uuid4())
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        await fund(client, user_id, 10)
        task_id = await generate(client, user_id)
        before = (await client.get(f"/tasks/{task_id}")).json()
        first_id = before["current_leg"]["id"]
        await send_to_provider(first_id, 202)
        event: dict[str, object] = {
            "task_id": task_id,
            "attempt_id": first_id,
            "external_id": first_id,
            "status": "completed",
            "result_url": "https://example.invalid/not-trusted",
        }
        raw, headers = signed_provider(event)
        headers["X-Signature"] = "0" * 64
        response = await client.post("/webhooks/providers/alpha", content=raw, headers=headers)
        state = (await client.get(f"/tasks/{task_id}")).json()
        balance = (await client.get(f"/users/{user_id}/balance")).json()

    assert response.status_code == 401
    assert state["status"] == "processing"
    assert state["result_url"] is None
    assert balance["balance"] == 8
