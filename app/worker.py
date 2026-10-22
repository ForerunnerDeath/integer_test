"""Outbox dispatcher; database lease protects competing worker processes."""

import asyncio
import logging
import uuid
from datetime import datetime, timedelta, timezone

import httpx
from sqlalchemy import func, or_, select

from app.config import settings
from app.db import session_factory
from app.models import (
    AttemptStatus,
    GenerationTask,
    Model,
    OutboxEvent,
    ProviderAttempt,
    TaskStatus,
)
from app.provider_flow import ProviderWebhook, transition_attempt

log = logging.getLogger(__name__)
LEASE_SECONDS = 30


async def claim_outbox() -> uuid.UUID | None:
    async with session_factory() as session:
        async with session.begin():
            event = await session.scalar(
                select(OutboxEvent)
                .where(
                    OutboxEvent.done_at.is_(None),
                    OutboxEvent.available_at <= func.now(),
                    or_(
                        OutboxEvent.claimed_until.is_(None),
                        OutboxEvent.claimed_until < func.now(),
                    ),
                )
                .order_by(OutboxEvent.available_at, OutboxEvent.id)
                .limit(1)
                .with_for_update(skip_locked=True)
            )
            if event is None:
                return None
            event.claimed_until = datetime.now(timezone.utc) + timedelta(
                seconds=LEASE_SECONDS
            )
            event.retries += 1
            return event.id


async def dispatch_attempt(attempt_id: uuid.UUID, client: httpx.AsyncClient) -> None:
    """Persist the stable provider id before HTTP; callbacks may arrive early."""
    async with session_factory() as session:
        async with session.begin():
            attempt = await session.scalar(
                select(ProviderAttempt)
                .where(ProviderAttempt.id == attempt_id)
                .with_for_update()
            )
            if attempt is None:
                raise ValueError("Dispatch refers to a missing attempt")
            task = await session.get(GenerationTask, attempt.task_id)
            if task is None:
                raise ValueError("Dispatch refers to a missing task")
            if (
                task.current_attempt_id != attempt.id
                or task.status in (TaskStatus.completed, TaskStatus.failed)
                or attempt.status in (AttemptStatus.completed, AttemptStatus.failed)
            ):
                return
            model = await session.get(Model, task.model_id)
            if model is None:
                raise ValueError("Dispatch refers to a missing model")
            attempt.status = AttemptStatus.sending
            attempt.external_id = str(attempt.id)
            attempt.deadline_at = datetime.now(timezone.utc) + timedelta(
                seconds=settings.provider_timeout
            )
            task.status = TaskStatus.processing
            url = f"{settings.alpha_url if attempt.provider == 'alpha' else settings.beta_url}/tasks"
            body = {
                "task_id": str(task.id),
                "attempt_id": str(attempt.id),
                "model_slug": model.slug,
                "prompt": task.prompt,
                "ref_images": task.ref_images,
            }
            provider = attempt.provider
            external_id = attempt.external_id

    failure: str | None = None
    try:
        response = await client.post(url, json=body, timeout=10)
        if response.status_code == 400:
            failure = "payload"
        elif response.status_code == 403:
            failure = "moderation"
        elif response.status_code != 202:
            failure = "technical"
        elif response.json().get("external_id") != external_id:
            failure = "technical"
    except (httpx.RequestError, ValueError, TypeError):
        failure = "technical"

    if failure is not None:
        await transition_attempt(
            ProviderWebhook(
                task_id=uuid.UUID(body["task_id"]),
                attempt_id=attempt_id,
                external_id=external_id,
                status="failed",
                error_type=failure,
            ),
            provider,
        )
        return

    async with session_factory() as session:
        async with session.begin():
            attempt = await session.scalar(
                select(ProviderAttempt)
                .where(ProviderAttempt.id == attempt_id)
                .with_for_update()
            )
            if attempt is not None and attempt.status == AttemptStatus.sending:
                attempt.status = AttemptStatus.waiting


async def finish_outbox(event_id: uuid.UUID, *, success: bool) -> None:
    async with session_factory() as session:
        async with session.begin():
            event = await session.get(OutboxEvent, event_id, with_for_update=True)
            if event is None:
                return
            event.claimed_until = None
            if success:
                event.done_at = datetime.now(timezone.utc)
            else:
                event.available_at = datetime.now(timezone.utc) + timedelta(seconds=3)


async def process_one(client: httpx.AsyncClient) -> bool:
    event_id = await claim_outbox()
    if event_id is None:
        return False
    try:
        async with session_factory() as session:
            event = await session.get(OutboxEvent, event_id)
            if event is None:
                raise ValueError("Claimed outbox event disappeared")
            if event.topic != "dispatch_attempt":
                raise ValueError(f"Unsupported outbox topic: {event.topic}")
            attempt_id = uuid.UUID(str(event.payload["attempt_id"]))
        await dispatch_attempt(attempt_id, client)
    except Exception:
        log.exception("Outbox dispatch failed for %s", event_id)
        await finish_outbox(event_id, success=False)
    else:
        await finish_outbox(event_id, success=True)
    return True


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    async with httpx.AsyncClient() as client:
        while True:
            if not await process_one(client):
                await asyncio.sleep(0.5)


if __name__ == "__main__":
    asyncio.run(main())
