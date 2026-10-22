"""Regression coverage for provider-issued IDs and schema evolution."""

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
from app.main import app
from app.worker import dispatch_attempt

pytestmark = pytest.mark.skipif(
    os.getenv("RUN_DB_TESTS") != "1",
    reason="Requires PostgreSQL: docker compose run --build --rm tests",
)


@pytest_asyncio.fixture(autouse=True)
async def dispose_audit_medium_pool() -> AsyncIterator[None]:
    try:
        yield
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_provider_generated_external_id_is_accepted_and_correlated() -> None:
    user_id = str(uuid.uuid4())
    provider_id = f"provider-job-{uuid.uuid4().hex}"
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as api:
        await fund(api, user_id, 20)
        task_id = await generate(api, user_id)
        attempt_id = (await api.get(f"/tasks/{task_id}")).json()["current_leg"]["id"]

        def accepted(request: httpx.Request) -> httpx.Response:
            return httpx.Response(202, json={"external_id": provider_id})

        async with httpx.AsyncClient(transport=httpx.MockTransport(accepted)) as client:
            await dispatch_attempt(uuid.UUID(attempt_id), client)

        waiting = (await api.get(f"/tasks/{task_id}")).json()
        assert waiting["current_leg"]["external_id"] == provider_id
        assert waiting["current_leg"]["status"] == "waiting"

        body, headers = signed_provider(
            {
                "task_id": task_id,
                "attempt_id": attempt_id,
                "external_id": provider_id,
                "status": "completed",
                "result_url": "https://example.invalid/provider-issued",
            }
        )
        callback = await api.post("/webhooks/providers/alpha", content=body, headers=headers)
        assert callback.status_code == 200
        assert callback.json()["outcome"] == "completed"
        assert (await api.get(f"/tasks/{task_id}")).json()["status"] == "completed"


@pytest.mark.asyncio
async def test_early_signed_callback_can_bind_provider_generated_id() -> None:
    user_id = str(uuid.uuid4())
    provider_id = f"early-{uuid.uuid4().hex}"
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as api:
        await fund(api, user_id, 20)
        task_id = await generate(api, user_id)
        attempt_id = (await api.get(f"/tasks/{task_id}")).json()["current_leg"]["id"]

        async def response_after_callback(request: httpx.Request) -> httpx.Response:
            raw, headers = signed_provider(
                {
                    "task_id": task_id,
                    "attempt_id": attempt_id,
                    "external_id": provider_id,
                    "status": "completed",
                    "result_url": "https://example.invalid/early",
                }
            )
            result = await api.post("/webhooks/providers/alpha", content=raw, headers=headers)
            assert result.status_code == 200, result.text
            return httpx.Response(202, json={"external_id": provider_id})

        async with httpx.AsyncClient(transport=httpx.MockTransport(response_after_callback)) as client:
            await dispatch_attempt(uuid.UUID(attempt_id), client)

        task = (await api.get(f"/tasks/{task_id}")).json()
        assert task["status"] == "completed"
        assert task["current_leg"]["external_id"] == provider_id


@pytest.mark.asyncio
async def test_seeded_sequences_accept_followup_model_and_leg() -> None:
    async with session_factory() as session:
        async with session.begin():
            model_id = (
                await session.execute(
                    text("INSERT INTO models(slug, price) VALUES (:slug, 5) RETURNING id"),
                    {"slug": f"probe-{uuid.uuid4().hex}"},
                )
            ).scalar_one()
            leg_id = (
                await session.execute(
                    text(
                        "INSERT INTO model_legs("
                        "model_id, provider, priority, max_ref_images, skip_on_moderation"
                        ") VALUES (:model_id, 'alpha', 1, 0, false) RETURNING id"
                    ),
                    {"model_id": model_id},
                )
            ).scalar_one()
    assert model_id > 2
    assert leg_id > 4
