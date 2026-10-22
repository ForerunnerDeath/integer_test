"""Configurable alpha/beta simulator; it does not coordinate generation state."""

import asyncio
import hashlib
import hmac
import json
import os
import uuid

import httpx
from fastapi import BackgroundTasks, FastAPI, HTTPException, status
from pydantic import BaseModel

from app.config import settings

PROVIDER_NAME = os.getenv("PROVIDER_NAME", "alpha")
MODE = os.getenv("FAKE_PROVIDER_MODE", "success")
DELAY = float(os.getenv("FAKE_PROVIDER_DELAY", "2"))
CALLBACK_BASE = os.getenv("PROVIDER_CALLBACK_BASE", "http://api:8000")

app = FastAPI(title=f"Fake provider {PROVIDER_NAME}")


class FakeTaskRequest(BaseModel):
    task_id: uuid.UUID
    attempt_id: uuid.UUID
    model_slug: str
    prompt: str
    ref_images: list[str]


async def send_webhook(data: FakeTaskRequest, external_id: str, mode: str) -> None:
    delay = (
        max(DELAY, float(settings.provider_timeout) + 5)
        if mode == "late_success"
        else max(0, DELAY)
    )
    await asyncio.sleep(delay)
    if mode == "no_webhook":
        return

    message: dict[str, str] = {
        "task_id": str(data.task_id),
        "attempt_id": str(data.attempt_id),
        "external_id": external_id,
        "status": "completed",
        "result_url": f"https://example.invalid/{PROVIDER_NAME}/{external_id}",
    }
    if mode in ("technical", "moderation", "payload"):
        message["status"] = "failed"
        message["error_type"] = mode
        message.pop("result_url", None)
    raw = json.dumps(message, separators=(",", ":")).encode()
    signature = hmac.new(
        settings.provider_webhook_secret.encode(), raw, hashlib.sha256
    ).hexdigest()
    url = f"{CALLBACK_BASE}/webhooks/providers/{PROVIDER_NAME}"
    async with httpx.AsyncClient(timeout=10) as client:
        requests = 2 if mode == "duplicate" else 1
        await asyncio.gather(
            *(
                client.post(url, content=raw, headers={"X-Signature": signature})
                for _ in range(requests)
            )
        )


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/tasks", status_code=status.HTTP_202_ACCEPTED)
async def create_task(
    request: FakeTaskRequest,
    background_tasks: BackgroundTasks,
) -> dict[str, str]:
    if MODE in ("http_500", "http_503"):
        raise HTTPException(status_code=int(MODE.split("_")[1]), detail="Simulated failure")
    if MODE == "http_400":
        raise HTTPException(status_code=400, detail="Simulated invalid payload")
    if MODE == "http_403":
        raise HTTPException(status_code=403, detail="Simulated moderation rejection")
    if MODE == "timeout":
        await asyncio.sleep(30)

    # Deterministic external ID also serves as the provider idempotency key.
    external_id = str(request.attempt_id)
    background_tasks.add_task(send_webhook, request, external_id, MODE)
    return {"external_id": external_id}
