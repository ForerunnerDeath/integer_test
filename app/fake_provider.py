"""Configurable in-memory Alpha/Beta simulator used for integration and E2E tests.

Attempts are identified by a stable UUID. A repeated POST with the same payload
is accepted without scheduling a second callback in this simulator process.
"""

import asyncio
import hashlib
import hmac
import json
import os
import random
import uuid
from dataclasses import dataclass

import httpx
from fastapi import BackgroundTasks, FastAPI, HTTPException, status
from pydantic import BaseModel, Field

from app.config import settings

PROVIDER_NAME = os.getenv("PROVIDER_NAME", "alpha")
CALLBACK_BASE = os.getenv("PROVIDER_CALLBACK_BASE", "http://api:8000")
CONTROL_ENABLED = os.getenv("FAKE_PROVIDER_TEST_CONTROL", "0") == "1"
VALID_MODES = {
    "success", "technical", "moderation", "payload", "duplicate",
    "no_webhook", "late_success", "http_500", "http_503",
    "http_400", "http_403", "timeout", "random",
}

app = FastAPI(title=f"Fake provider {PROVIDER_NAME}")


class FakeTaskRequest(BaseModel):
    task_id: uuid.UUID
    attempt_id: uuid.UUID
    model_slug: str
    prompt: str
    ref_images: list[str]


class ControlRequest(BaseModel):
    mode: str
    delay: float = Field(ge=0, le=40)


@dataclass
class Job:
    fingerprint: str
    mode: str
    requests: int = 1
    callback_deliveries: int = 0


jobs: dict[str, Job] = {}
jobs_lock = asyncio.Lock()
runtime_mode = os.getenv("FAKE_PROVIDER_MODE", "success")
runtime_delay = float(os.getenv("FAKE_PROVIDER_DELAY", "2"))
failure_rate = float(os.getenv("FAKE_PROVIDER_FAILURE_RATE", "0"))
failure_type = os.getenv("FAKE_PROVIDER_FAILURE_TYPE", "technical")


def reference_limit(model_slug: str) -> int | None:
    return {
        ("alpha", "image-fast"): 4,
        ("beta", "image-fast"): 10,
        ("alpha", "video-long"): 1,
        ("beta", "video-long"): 2,
    }.get((PROVIDER_NAME, model_slug))


async def send_webhook(
    data: FakeTaskRequest, external_id: str, mode: str, base_delay: float
) -> None:
    delay = (
        max(base_delay, float(settings.provider_timeout) + 5)
        if mode == "late_success"
        else max(0, base_delay)
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
    signature = hmac.new(settings.provider_webhook_secret.encode(), raw, hashlib.sha256)
    url = f"{CALLBACK_BASE}/webhooks/providers/{PROVIDER_NAME}"
    count = 2 if mode == "duplicate" else 1
    async with httpx.AsyncClient(timeout=10) as client:
        results = await asyncio.gather(
            *(
                client.post(url, content=raw, headers={"X-Signature": signature.hexdigest()})
                for _ in range(count)
            ),
            return_exceptions=True,
        )
    async with jobs_lock:
        existing = jobs.get(external_id)
        if existing is not None:
            existing.callback_deliveries += len(results)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/tasks", status_code=status.HTTP_202_ACCEPTED)
async def create_task(
    request: FakeTaskRequest, background_tasks: BackgroundTasks
) -> dict[str, str]:
    limit = reference_limit(request.model_slug)
    if limit is None or len(request.ref_images) > limit:
        raise HTTPException(status_code=400, detail="Unsupported model or reference count")

    key = str(request.attempt_id)
    fingerprint = hashlib.sha256(
        request.model_dump_json().encode("utf-8")
    ).hexdigest()
    async with jobs_lock:
        existing = jobs.get(key)
        if existing is not None:
            if existing.fingerprint != fingerprint:
                raise HTTPException(status_code=409, detail="Attempt ID used for different payload")
            existing.requests += 1
            return {"external_id": key}

        mode = runtime_mode
        if mode == "random":
            mode = failure_type if random.random() < failure_rate else "success"
        if mode in ("http_500", "http_503"):
            raise HTTPException(status_code=int(mode.split("_")[1]), detail="Simulated error")
        if mode == "http_400":
            raise HTTPException(status_code=400, detail="Simulated invalid payload")
        if mode == "http_403":
            raise HTTPException(status_code=403, detail="Simulated moderation")
        if mode not in VALID_MODES:
            raise HTTPException(status_code=500, detail="Invalid simulator mode")

        jobs[key] = Job(fingerprint=fingerprint, mode=mode)
        base_delay = runtime_delay

    if mode == "timeout":
        await asyncio.sleep(30)
        return {"external_id": key}

    background_tasks.add_task(send_webhook, request, key, mode, base_delay)
    return {"external_id": key}


@app.post("/_test/config")
async def test_config(request: ControlRequest) -> dict[str, str | float]:
    if not CONTROL_ENABLED:
        raise HTTPException(status_code=404, detail="Test controls disabled")
    if request.mode not in VALID_MODES:
        raise HTTPException(status_code=422, detail="Unknown simulator mode")
    global runtime_mode, runtime_delay
    async with jobs_lock:
        runtime_mode = request.mode
        runtime_delay = request.delay
    return {"mode": runtime_mode, "delay": runtime_delay}


@app.get("/_test/jobs/{attempt_id}")
async def job_status(attempt_id: uuid.UUID) -> dict[str, str | int]:
    if not CONTROL_ENABLED:
        raise HTTPException(status_code=404, detail="Test controls disabled")
    async with jobs_lock:
        job = jobs.get(str(attempt_id))
        if job is None:
            raise HTTPException(status_code=404, detail="Job not found")
        return {
            "mode": job.mode,
            "requests": job.requests,
            "callback_deliveries": job.callback_deliveries,
        }
