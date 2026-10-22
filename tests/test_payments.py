"""Real PostgreSQL integration tests (not mock database concurrency tests)."""

import asyncio
import hashlib
import hmac
import json
import os
import uuid
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from app.config import settings
from app.db import engine
from app.main import app

pytestmark = pytest.mark.skipif(
    os.getenv("RUN_DB_TESTS") != "1",
    reason="Requires PostgreSQL: docker compose run --build --rm tests",
)


@pytest_asyncio.fixture(autouse=True)
async def dispose_db_pool_after_test() -> AsyncIterator[None]:
    """An asyncpg connection belongs to its originating event loop.

    pytest-asyncio starts a new loop for each test. SQLAlchemy's module-level
    engine otherwise pools connections that were created in the previous loop.
    Dispose the test pool before that loop is closed, without changing the
    production app's pooling strategy.
    """
    try:
        yield
    finally:
        await engine.dispose()


def signed_body(payload: dict[str, object]) -> tuple[bytes, dict[str, str]]:
    body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    signature = hmac.new(
        settings.payment_webhook_secret.encode("utf-8"),
        body,
        hashlib.sha256,
    ).hexdigest()
    return body, {"X-Signature": signature, "Content-Type": "application/json"}


def payment(user_id: str, *, tokens: int = 10, status: str = "completed") -> dict[str, object]:
    return {
        "payment_id": uuid.uuid4().hex,
        "user_id": user_id,
        "tokens": tokens,
        "status": status,
    }


@pytest.mark.asyncio
async def test_twenty_simultaneous_payment_webhooks_credit_once() -> None:
    user_id = str(uuid.uuid4())
    body, headers = signed_body(payment(user_id, tokens=40))
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        responses = await asyncio.gather(
            *(client.post("/webhooks/payments", content=body, headers=headers) for _ in range(20))
        )
        assert all(response.status_code == 200 for response in responses), [
            (response.status_code, response.text) for response in responses
        ]
        outcomes = [response.json()["status"] for response in responses]
        assert outcomes.count("credited") == 1
        assert outcomes.count("duplicate") == 19

        balance = await client.get(f"/users/{user_id}/balance")
    assert balance.status_code == 200
    data = balance.json()
    assert data["balance"] == 40
    assert len(data["operations"]) == 1
    assert data["operations"][0]["delta"] == 40
    assert data["operations"][0]["balance_after"] == 40


@pytest.mark.asyncio
async def test_different_payments_do_not_lose_concurrent_credits() -> None:
    user_id = str(uuid.uuid4())
    requests = [signed_body(payment(user_id, tokens=7)) for _ in range(12)]
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        responses = await asyncio.gather(
            *(client.post("/webhooks/payments", content=body, headers=headers) for body, headers in requests)
        )
        assert all(response.status_code == 200 for response in responses), [
            (response.status_code, response.text) for response in responses
        ]
        assert all(response.json()["status"] == "credited" for response in responses)
        balance = await client.get(f"/users/{user_id}/balance")
    assert balance.status_code == 200
    data = balance.json()
    assert data["balance"] == 84
    assert sum(item["delta"] for item in data["operations"]) == 84
    assert len(data["operations"]) == 12


@pytest.mark.asyncio
async def test_duplicate_id_with_different_amount_returns_conflict() -> None:
    user_id = str(uuid.uuid4())
    original = payment(user_id, tokens=8)
    altered = {**original, "tokens": 1000}
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        body, headers = signed_body(original)
        first = await client.post("/webhooks/payments", content=body, headers=headers)
        body, headers = signed_body(altered)
        second = await client.post("/webhooks/payments", content=body, headers=headers)
        balance = await client.get(f"/users/{user_id}/balance")
    assert first.status_code == 200
    assert second.status_code == 409
    assert balance.json()["balance"] == 8
    assert len(balance.json()["operations"]) == 1


@pytest.mark.asyncio
async def test_invalid_signature_does_not_credit_tokens() -> None:
    user_id = str(uuid.uuid4())
    body, headers = signed_body(payment(user_id))
    headers["X-Signature"] = "0" * 64
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        invalid = await client.post("/webhooks/payments", content=body, headers=headers)
        balance = await client.get(f"/users/{user_id}/balance")
    assert invalid.status_code == 401
    assert balance.status_code == 404


@pytest.mark.asyncio
async def test_pending_then_completed_credits_once() -> None:
    user_id = str(uuid.uuid4())
    event = payment(user_id, tokens=11, status="pending")
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        body, headers = signed_body(event)
        pending = await client.post("/webhooks/payments", content=body, headers=headers)
        no_balance = await client.get(f"/users/{user_id}/balance")
        body, headers = signed_body({**event, "status": "completed"})
        completed = await client.post("/webhooks/payments", content=body, headers=headers)
        repeated = await client.post("/webhooks/payments", content=body, headers=headers)
        balance = await client.get(f"/users/{user_id}/balance")
    assert pending.status_code == 200 and pending.json()["status"] == "ignored"
    assert no_balance.status_code == 404
    assert completed.json()["status"] == "credited"
    assert repeated.json()["status"] == "duplicate"
    assert balance.json()["balance"] == 11
