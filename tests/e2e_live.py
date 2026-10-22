"""Real-network tests with four API workers, providers and restartable outbox worker.

Run from the host using: python tests/e2e_live.py
Requires: docker compose -f compose.e2e.yaml -p integer-e2e up --build -d
"""

import asyncio
import hashlib
import hmac
import json
import subprocess
import time
import uuid
from collections.abc import Awaitable, Callable
from typing import TypedDict

import httpx
from pydantic import TypeAdapter

API = "http://127.0.0.1:18000"
ALPHA = "http://127.0.0.1:18101"
BETA = "http://127.0.0.1:18102"
COMPOSE = ["docker", "compose", "-p", "integer-e2e", "-f", "compose.e2e.yaml"]


class E2EAttempt(TypedDict):
    id: str
    provider: str


class E2ETask(TypedDict):
    status: str
    attempts: list[E2EAttempt]


class E2EBalance(TypedDict):
    balance: int


TASK_ADAPTER = TypeAdapter(E2ETask)
BALANCE_ADAPTER = TypeAdapter(E2EBalance)


def docker(*args: str) -> None:
    subprocess.run([*COMPOSE, *args], check=True)


def signed(payload: dict[str, object], secret: str) -> tuple[bytes, dict[str, str]]:
    body = json.dumps(payload, separators=(",", ":")).encode()
    signature = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return body, {"X-Signature": signature, "Content-Type": "application/json"}


async def until(
    operation: Callable[[], Awaitable[bool]],
    *,
    limit: float = 15,
    message: str,
) -> None:
    deadline = time.monotonic() + limit
    while time.monotonic() < deadline:
        try:
            if await operation():
                return
        except (httpx.RequestError, AssertionError):
            pass
        await asyncio.sleep(0.15)
    raise AssertionError(message)


async def wait_endpoint(client: httpx.AsyncClient, url: str) -> bool:
    r = await client.get(url)
    return r.status_code == 200


async def configure(client: httpx.AsyncClient, provider: str, mode: str) -> None:
    url = ALPHA if provider == "alpha" else BETA
    response = await client.post(f"{url}/_test/config", json={"mode": mode, "delay": 0.1})
    assert response.status_code == 200, response.text


async def balance(client: httpx.AsyncClient, user: str) -> E2EBalance:
    r = await client.get(f"{API}/users/{user}/balance")
    r.raise_for_status()
    return BALANCE_ADAPTER.validate_python(r.json())


async def task(client: httpx.AsyncClient, task_id: str) -> E2ETask:
    r = await client.get(f"{API}/tasks/{task_id}")
    r.raise_for_status()
    return TASK_ADAPTER.validate_python(r.json())


async def main() -> None:
    async with httpx.AsyncClient(timeout=10) as client:
        for url in (API, ALPHA, BETA):
            await until(
                lambda url=url: wait_endpoint(client, f"{url}/health"),
                limit=30,
                message=f"{url} did not become healthy",
            )

        user = str(uuid.uuid4())
        payment: dict[str, object] = {
            "payment_id": uuid.uuid4().hex,
            "user_id": user,
            "tokens": 100,
            "status": "completed",
        }
        raw, headers = signed(payment, "e2e-payment-secret")
        replies = await asyncio.gather(
            *(
                client.post(f"{API}/webhooks/payments", content=raw, headers=headers)
                for _ in range(20)
            )
        )
        assert all(r.status_code == 200 for r in replies), [r.text for r in replies]
        assert sum(r.json()["status"] == "credited" for r in replies) == 1
        assert (await balance(client, user))["balance"] == 100
        print("[OK] 20 payments across four API processes credit once")

        await configure(client, "alpha", "success")
        await configure(client, "beta", "success")

        async def generate(model: str = "image-fast") -> str:
            response = await client.post(
                f"{API}/generate",
                json={"user_id": user, "model_slug": model, "prompt": "live e2e"},
            )
            assert response.status_code == 202, response.text
            return response.json()["task_id"]

        first = await generate()
        await until(
            lambda: is_status(client, first, "completed"),
            message="Alpha success did not finish",
        )
        assert len((await task(client, first))["attempts"]) == 1
        print("[OK] Real Alpha request and signed callback complete")

        await configure(client, "alpha", "technical")
        second = await generate()
        await until(
            lambda: is_status(client, second, "completed"),
            message="Alpha failure did not fall back to Beta",
        )
        attempts = (await task(client, second))["attempts"]
        assert [a["provider"] for a in attempts] == ["alpha", "beta"]
        print("[OK] Real Alpha failure falls back to Beta")

        await configure(client, "alpha", "no_webhook")
        await configure(client, "beta", "no_webhook")
        timed = await generate()
        await until(
            lambda: is_status(client, timed, "failed"),
            limit=20,
            message="Watchdog did not exhaust both providers",
        )
        assert (await balance(client, user))["balance"] == 96
        print("[OK] Provider silence produces one refund")

        await configure(client, "alpha", "success")
        # Crash the worker before another task is even enqueued. Outbox must
        # survive because the API writes it transactionally to PostgreSQL.
        docker("stop", "-t", "1", "worker")
        offline = await generate()
        await asyncio.sleep(0.4)
        assert (await task(client, offline))["status"] == "pending"
        docker("start", "worker")
        await until(
            lambda: is_status(client, offline, "completed"),
            message="Restarted worker did not recover saved outbox event",
        )
        print("[OK] Worker process restart recovers durable queued task")

        # Two worker processes share one outbox; provider idempotency prevents
        # duplicate remote tasks if a claim is retried.
        docker("up", "-d", "--scale", "worker=2", "worker")
        await configure(client, "alpha", "duplicate")
        simultaneous = await asyncio.gather(*(generate() for _ in range(3)))
        await until(
            lambda: all_status(client, simultaneous, "completed"),
            message="Concurrent workers did not finish three tasks",
        )
        for tid in simultaneous:
            attempt = (await task(client, tid))["attempts"][0]["id"]
            r = await client.get(f"{ALPHA}/_test/jobs/{attempt}")
            assert r.status_code == 200
            assert r.json()["callback_deliveries"] == 2
        print("[OK] Concurrent workers and duplicate callbacks finish once")

        final_balance = await balance(client, user)
        # 100 - 2 (success) - 2 (failover success) - 0 (refunded timeout)
        # - 2 (restart) - 6 (three final successes) = 88
        assert final_balance["balance"] == 88, final_balance
        print("[OK] Final balance 88, no duplicate debit or refund")


async def is_status(client: httpx.AsyncClient, tid: str, expected: str) -> bool:
    return (await task(client, tid))["status"] == expected


async def all_status(client: httpx.AsyncClient, tasks: list[str], status: str) -> bool:
    return all(await asyncio.gather(*(is_status(client, tid, status) for tid in tasks)))


if __name__ == "__main__":
    asyncio.run(main())
