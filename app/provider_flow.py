"""Signed provider callbacks, failover and once-only final refunds."""

import uuid
from datetime import UTC, datetime
from typing import Literal

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, ValidationError, model_validator
from sqlalchemy import select

from app.config import settings
from app.db import session_factory
from app.ledger import locked_user, record_movement
from app.models import (
    AttemptStatus,
    GenerationTask,
    LedgerKind,
    ModelLeg,
    OutboxEvent,
    ProviderAttempt,
    TaskStatus,
)
from app.security import verify_webhook_signature

router = APIRouter(tags=["provider-webhooks"])
ErrorType = Literal["technical", "moderation", "payload"]
ResultStatus = Literal["completed", "failed"]
TransitionResult = Literal["completed", "advanced", "failed", "ignored"]


class ProviderWebhook(BaseModel):
    task_id: uuid.UUID
    attempt_id: uuid.UUID
    external_id: str
    status: ResultStatus
    error_type: ErrorType | None = None
    result_url: str | None = None

    @model_validator(mode="after")
    def check_fields(self) -> "ProviderWebhook":
        if self.status == "completed" and not self.result_url:
            raise ValueError("A completed callback requires result_url")
        if self.status == "failed" and self.error_type is None:
            raise ValueError("A failed callback requires error_type")
        return self


class ProviderWebhookResponse(BaseModel):
    outcome: TransitionResult


async def transition_attempt(
    callback: ProviderWebhook,
    provider: str,
) -> TransitionResult:
    """Resolve a provider attempt under database locks across API processes.

    Always lock user before task: admissions and refunds follow the same order.
    A late, duplicate or superseded attempt never changes task state or tokens.
    """
    async with session_factory() as session:
        async with session.begin():
            # First read only immutable relation fields, without loading mutable
            # ORM rows that could be stale while another callback holds the lock.
            origin = await session.execute(
                select(ProviderAttempt.task_id, GenerationTask.user_id)
                .join(GenerationTask, GenerationTask.id == ProviderAttempt.task_id)
                .where(ProviderAttempt.id == callback.attempt_id)
            )
            relation = origin.one_or_none()
            if relation is None or relation.task_id != callback.task_id:
                raise HTTPException(status_code=404, detail="Attempt not found")

            await locked_user(session, relation.user_id)
            task = await session.scalar(
                select(GenerationTask)
                .where(GenerationTask.id == callback.task_id)
                .with_for_update()
            )
            attempt = await session.scalar(
                select(ProviderAttempt)
                .where(ProviderAttempt.id == callback.attempt_id)
                .with_for_update()
            )
            if task is None or attempt is None:
                raise HTTPException(status_code=404, detail="Attempt not found")
            if attempt.provider != provider:
                raise HTTPException(status_code=400, detail="Wrong callback provider")

            if (
                task.current_attempt_id != attempt.id
                or task.status not in (TaskStatus.pending, TaskStatus.processing)
                or attempt.status not in (AttemptStatus.sending, AttemptStatus.waiting)
            ):
                return "ignored"
            if attempt.external_id != callback.external_id:
                raise HTTPException(status_code=409, detail="Provider task ID mismatch")

            attempt.finished_at = datetime.now(UTC)
            if callback.status == "completed":
                attempt.status = AttemptStatus.completed
                task.status = TaskStatus.completed
                task.result_url = callback.result_url
                return "completed"

            attempt.status = AttemptStatus.failed
            attempt.error_type = callback.error_type

            if callback.error_type != "payload":
                leg = await session.get(ModelLeg, attempt.leg_id)
                if leg is None:
                    raise RuntimeError("Provider leg is missing")
                criteria = [
                    ModelLeg.model_id == task.model_id,
                    ModelLeg.priority > leg.priority,
                    ModelLeg.max_ref_images >= len(task.ref_images),
                ]
                if callback.error_type == "moderation":
                    criteria.append(ModelLeg.skip_on_moderation.is_(False))
                next_leg = await session.scalar(
                    select(ModelLeg)
                    .where(*criteria)
                    .order_by(ModelLeg.priority, ModelLeg.id)
                    .limit(1)
                )
                if next_leg is not None:
                    next_id = uuid.uuid4()
                    session.add(
                        ProviderAttempt(
                            id=next_id,
                            task_id=task.id,
                            leg_id=next_leg.id,
                            ordinal=attempt.ordinal + 1,
                            provider=next_leg.provider,
                            status=AttemptStatus.pending,
                        )
                    )
                    session.add(
                        OutboxEvent(
                            event_key=f"dispatch:{next_id}",
                            topic="dispatch_attempt",
                            payload={
                                "task_id": str(task.id),
                                "attempt_id": str(next_id),
                            },
                        )
                    )
                    task.current_attempt_id = next_id
                    task.status = TaskStatus.processing
                    return "advanced"

            # All routes exhausted or invalid payload: refund atomically with
            # the terminal transition, so retrying an identical callback is safe.
            user = await locked_user(session, task.user_id)
            await record_movement(
                session,
                user,
                operation_key=f"refund:{task.id}",
                kind=LedgerKind.refund,
                amount=task.price,
            )
            task.status = TaskStatus.failed
            return "failed"


@router.post("/webhooks/providers/{name}", response_model=ProviderWebhookResponse)
async def provider_webhook(name: str, request: Request) -> ProviderWebhookResponse:
    if name not in ("alpha", "beta"):
        raise HTTPException(status_code=404, detail="Unknown provider")
    body = await request.body()
    verify_webhook_signature(
        body,
        request.headers.get("X-Signature"),
        settings.provider_webhook_secret,
    )
    try:
        event = ProviderWebhook.model_validate_json(body)
    except ValidationError as exc:
        raise HTTPException(status_code=422, detail="Invalid provider callback") from exc
    return ProviderWebhookResponse(outcome=await transition_attempt(event, name))
