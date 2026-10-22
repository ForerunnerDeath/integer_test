"""Outbox dispatcher; database lease protects competing worker processes."""

import asyncio
import logging
import uuid
from datetime import UTC, datetime, timedelta

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
from app.watchdog import watchdog_once

log = logging.getLogger(__name__)
LEASE_SECONDS = 30


async def claim_outbox() -> tuple[uuid.UUID, datetime] | None:
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
            leased_until = datetime.now(UTC) + timedelta(seconds=LEASE_SECONDS)
            event.claimed_until = leased_until
            event.retries += 1
            return event.id, leased_until


async def dispatch_attempt(attempt_id: uuid.UUID, client: httpx.AsyncClient) -> None:
    """Persist the stable provider id before HTTP; callbacks may arrive early."""
    async with session_factory() as session:
        async with session.begin():
            relation = await session.scalar(
                select(ProviderAttempt.task_id).where(ProviderAttempt.id == attempt_id)
            )
            if relation is None:
                raise ValueError("Dispatch refers to a missing attempt")
            task = await session.scalar(
                select(GenerationTask).where(GenerationTask.id == relation).with_for_update()
            )
            if task is None:
                raise ValueError("Dispatch refers to a missing task")
            attempt = await session.scalar(
                select(ProviderAttempt).where(ProviderAttempt.id == attempt_id).with_for_update()
            )
            if attempt is None:
                raise ValueError("Dispatch refers to a missing attempt")
            if (
                task.current_attempt_id != attempt.id
                or task.status in (TaskStatus.completed, TaskStatus.failed)
                or attempt.status in (
                    AttemptStatus.waiting,
                    AttemptStatus.completed,
                    AttemptStatus.failed,
                )
            ):
                return
            model = await session.get(Model, task.model_id)
            if model is None:
                raise ValueError("Dispatch refers to a missing model")
            attempt.status = AttemptStatus.sending
            attempt.external_id = str(attempt.id)
            if attempt.deadline_at is None:
                attempt.deadline_at = datetime.now(UTC) + timedelta(
                    seconds=settings.provider_timeout
                )
            task.status = TaskStatus.processing
            base_url = settings.alpha_url if attempt.provider == "alpha" else settings.beta_url
            url = f"{base_url}/tasks"
            body = {
                "task_id": str(task.id),
                "attempt_id": str(attempt.id),
                "model_slug": model.slug,
                "prompt": task.prompt,
                "ref_images": task.ref_images,
            }
            provider = attempt.provider
            external_id = str(attempt.id)
            task_id = task.id

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
                task_id=task_id,
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
            relation = await session.scalar(
                select(ProviderAttempt.task_id).where(ProviderAttempt.id == attempt_id)
            )
            if relation is None:
                return
            await session.scalar(
                select(GenerationTask).where(GenerationTask.id == relation).with_for_update()
            )
            attempt = await session.scalar(
                select(ProviderAttempt).where(ProviderAttempt.id == attempt_id).with_for_update()
            )
            if attempt is not None and attempt.status == AttemptStatus.sending:
                attempt.status = AttemptStatus.waiting


async def finish_outbox(
    event_id: uuid.UUID,
    lease_until: datetime,
    *,
    success: bool,
) -> None:
    """Only the current lease holder may acknowledge or retry an event."""
    async with session_factory() as session:
        async with session.begin():
            event = await session.get(OutboxEvent, event_id, with_for_update=True)
            if (
                event is None
                or event.done_at is not None
                or event.claimed_until != lease_until
            ):
                return
            event.claimed_until = None
            if success:
                event.done_at = datetime.now(UTC)
            else:
                event.available_at = datetime.now(UTC) + timedelta(seconds=3)


async def process_one(client: httpx.AsyncClient) -> bool:
    claim = await claim_outbox()
    if claim is None:
        return False
    event_id, lease_until = claim
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
        await finish_outbox(event_id, lease_until, success=False)
    else:
        await finish_outbox(event_id, lease_until, success=True)
    return True


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    loop = asyncio.get_running_loop()
    next_watchdog_at = 0.0
    async with httpx.AsyncClient() as client:
        while True:
            if loop.time() >= next_watchdog_at:
                try:
                    await watchdog_once()
                except Exception:
                    log.exception("Watchdog scan failed")
                next_watchdog_at = loop.time() + max(0.1, settings.watchdog_interval_seconds)
            if not await process_one(client):
                await asyncio.sleep(0.5)


if __name__ == "__main__":
    asyncio.run(main())
