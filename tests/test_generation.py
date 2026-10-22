"""Generation admission and financial invariants on real PostgreSQL."""

import asyncio
import os
import uuid
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy import func, select

from app.db import engine, session_factory
from app.main import app
from app.models import GenerationTask, LedgerEntry, OutboxEvent, ProviderAttempt, User
from test_payments import payment, signed_body

pytestmark = pytest.mark.skipif(
    os.getenv("RUN_DB_TESTS") != "1",
    reason="Requires PostgreSQL: docker compose run --build --rm tests",
)


@pytest_asyncio.fixture(autouse=True)
async def dispose_db_pool_after_generation_test() -> AsyncIterator[None]:
    try:
        yield
    finally:
        await engine.dispose()


async def fund(client: AsyncClient, user_id: str, tokens: int) -> None:
    body, headers = signed_body(payment(user_id, tokens=tokens))
    response = await client.post("/webhooks/payments", content=body, headers=headers)
    assert response.status_code == 200, response.text


@pytest.mark.asyncio
async def test_generation_debits_and_creates_outbox_atomically() -> None:
    user_id = str(uuid.uuid4())
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        await fund(client, user_id, 30)
        created = await client.post(
            "/generate",
            json={
                "user_id": user_id, "model_slug": "image-fast",
                "prompt": "cat on the moon", "ref_images": [],
            },
        )
        assert created.status_code == 202, created.text
        data = created.json()
        assert data["provider"] == "alpha"
        assert data["price"] == 2
        assert data["balance"] == 28
        task = await client.get(f"/tasks/{data['task_id']}")
        balance = await client.get(f"/users/{user_id}/balance")
    assert task.status_code == 200
    assert task.json()["current_leg"]["provider"] == "alpha"
    assert task.json()["attempts"][0]["status"] == "pending"
    assert balance.json()["balance"] == 28

    async with session_factory() as session:
        task_id = uuid.UUID(data["task_id"])
        task_row = await session.get(GenerationTask, task_id)
        events = (
            await session.scalars(
                select(OutboxEvent).where(
                    OutboxEvent.event_key == f"dispatch:{task_row.current_attempt_id}"
                )
            )
        ).all()
        attempts = (
            await session.scalars(
                select(ProviderAttempt).where(ProviderAttempt.task_id == task_id)
            )
        ).all()
    assert task_row is not None
    assert len(events) == 1
    assert events[0].payload["task_id"] == str(task_id)
    assert len(attempts) == 1


@pytest.mark.asyncio
async def test_routing_limits_and_validation_before_debit() -> None:
    user_id = str(uuid.uuid4())
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        await fund(client, user_id, 100)
        beta = await client.post(
            "/generate",
            json={
                "user_id": user_id, "model_slug": "image-fast",
                "prompt": "sea", "ref_images": ["a", "b", "c", "d", "e"],
            },
        )
        impossible = await client.post(
            "/generate",
            json={
                "user_id": user_id, "model_slug": "video-long",
                "prompt": "sea", "ref_images": ["a", "b", "c"],
            },
        )
        unknown = await client.post(
            "/generate",
            json={
                "user_id": user_id, "model_slug": "missing",
                "prompt": "sea",
            },
        )
        balance = await client.get(f"/users/{user_id}/balance")
    assert beta.status_code == 202
    assert beta.json()["provider"] == "beta"
    assert impossible.status_code == 400
    assert unknown.status_code == 400
    assert balance.json()["balance"] == 98


@pytest.mark.asyncio
async def test_concurrent_admissions_never_exceed_three_tasks() -> None:
    user_id = str(uuid.uuid4())
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        await fund(client, user_id, 100)
        responses = await asyncio.gather(
            *(
                client.post(
                    "/generate",
                    json={"user_id": user_id, "model_slug": "image-fast", "prompt": "cloud"},
                )
                for _ in range(10)
            )
        )
        balance = await client.get(f"/users/{user_id}/balance")
    assert [response.status_code for response in responses].count(202) == 3
    assert [response.status_code for response in responses].count(429) == 7
    assert balance.json()["balance"] == 94

    async with session_factory() as session:
        count = await session.scalar(
            select(func.count()).select_from(GenerationTask).where(
                GenerationTask.user_id == uuid.UUID(user_id)
            )
        )
    assert count == 3


@pytest.mark.asyncio
async def test_concurrent_admissions_never_overdraw_balance() -> None:
    user_id = str(uuid.uuid4())
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        await fund(client, user_id, 3)
        responses = await asyncio.gather(
            *(
                client.post(
                    "/generate",
                    json={"user_id": user_id, "model_slug": "image-fast", "prompt": "rain"},
                )
                for _ in range(5)
            )
        )
        balance = await client.get(f"/users/{user_id}/balance")
    assert [response.status_code for response in responses].count(202) == 1
    assert [response.status_code for response in responses].count(402) == 4
    assert balance.json()["balance"] == 1


@pytest.mark.asyncio
async def test_failed_generation_leaves_balance_and_ledger_untouched() -> None:
    user_id = str(uuid.uuid4())
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        await fund(client, user_id, 5)
        no_money = await client.post(
            "/generate",
            json={"user_id": user_id, "model_slug": "video-long", "prompt": "rocket"},
        )
        balance = await client.get(f"/users/{user_id}/balance")
    assert no_money.status_code == 402
    assert balance.json()["balance"] == 5

    async with session_factory() as session:
        db_user = await session.get(User, uuid.UUID(user_id))
        total = await session.scalar(
            select(func.coalesce(func.sum(LedgerEntry.delta), 0)).where(
                LedgerEntry.user_id == uuid.UUID(user_id)
            )
        )
        active = await session.scalar(
            select(func.count()).select_from(GenerationTask).where(
                GenerationTask.user_id == uuid.UUID(user_id)
            )
        )
    assert db_user is not None
    assert db_user.balance == total == 5
    assert active == 0
