"""Generation submission with atomic debit, admission limit and durable outbox."""

import uuid
from datetime import datetime

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, Field, field_validator
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import session_factory
from app.ledger import InsufficientTokens, locked_user, record_movement
from app.models import (
    AttemptStatus,
    GenerationTask,
    LedgerKind,
    Model,
    ModelLeg,
    OutboxEvent,
    ProviderAttempt,
    TaskStatus,
)

router = APIRouter(tags=["generations"])
ACTIVE_STATUSES = (TaskStatus.pending, TaskStatus.processing)
MAX_ACTIVE_TASKS = 3


class GenerateRequest(BaseModel):
    user_id: uuid.UUID
    model_slug: str = Field(min_length=1, max_length=80)
    prompt: str = Field(min_length=1)
    ref_images: list[str] = Field(default_factory=list)

    @field_validator("prompt")
    @classmethod
    def nonempty_prompt(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("Prompt must not be blank")
        return value


class GenerateResponse(BaseModel):
    task_id: uuid.UUID
    status: TaskStatus
    model_slug: str
    price: int
    provider: str
    balance: int


class AttemptResponse(BaseModel):
    id: uuid.UUID
    leg_id: int
    provider: str
    ordinal: int
    status: AttemptStatus
    external_id: str | None
    error_type: str | None
    deadline_at: datetime | None
    finished_at: datetime | None


class TaskResponse(BaseModel):
    task_id: uuid.UUID
    user_id: uuid.UUID
    model_slug: str
    status: TaskStatus
    price: int
    result_url: str | None
    current_leg: AttemptResponse | None
    attempts: list[AttemptResponse]
    created_at: datetime


@router.post(
    "/generate",
    response_model=GenerateResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def generate(request: GenerateRequest) -> GenerateResponse:
    """Admit and enqueue a generation, never calling a provider under a DB lock."""

    async with session_factory() as session:
        async with session.begin():
            model = await session.scalar(select(Model).where(Model.slug == request.model_slug))
            if model is None:
                raise HTTPException(status_code=400, detail="Unknown model")

            eligible_legs = await session.scalars(
                select(ModelLeg)
                .where(
                    ModelLeg.model_id == model.id,
                    ModelLeg.max_ref_images >= len(request.ref_images),
                )
                .order_by(ModelLeg.priority, ModelLeg.id)
                .limit(1)
            )
            first_leg = eligible_legs.first()
            if first_leg is None:
                raise HTTPException(
                    status_code=400,
                    detail="No provider supports this model and number of reference images",
                )

            # Every debit, credit and refund takes the user's row lock first.
            # Competing generate requests then see the previous committed tasks
            # and cannot jointly cross the per-user limit.
            try:
                user = await locked_user(session, request.user_id)
            except LookupError as exc:
                raise HTTPException(status_code=402, detail="Insufficient tokens") from exc

            active = await session.scalar(
                select(func.count())
                .select_from(GenerationTask)
                .where(
                    GenerationTask.user_id == user.id,
                    GenerationTask.status.in_(ACTIVE_STATUSES),
                )
            )
            if active is not None and active >= MAX_ACTIVE_TASKS:
                raise HTTPException(
                    status_code=429,
                    detail="Maximum of three active generation tasks reached",
                )

            task_id = uuid.uuid4()
            attempt_id = uuid.uuid4()
            try:
                await record_movement(
                    session,
                    user,
                    operation_key=f"generate:{task_id}",
                    kind=LedgerKind.debit,
                    amount=model.price,
                )
            except InsufficientTokens as exc:
                raise HTTPException(status_code=402, detail="Insufficient tokens") from exc

            task = GenerationTask(
                id=task_id,
                user_id=user.id,
                model_id=model.id,
                prompt=request.prompt,
                ref_images=request.ref_images,
                price=model.price,
                status=TaskStatus.pending,
                current_attempt_id=attempt_id,
            )
            attempt = ProviderAttempt(
                id=attempt_id,
                task_id=task_id,
                leg_id=first_leg.id,
                ordinal=1,
                provider=first_leg.provider,
                status=AttemptStatus.pending,
            )
            event = OutboxEvent(
                event_key=f"dispatch:{attempt_id}",
                topic="dispatch_attempt",
                payload={"task_id": str(task_id), "attempt_id": str(attempt_id)},
            )
            session.add_all((task, attempt, event))

            # The ledger, task, first attempt and outbox commit together.
            # A worker will dispatch the event in a later stage.
            response = GenerateResponse(
                task_id=task_id,
                status=TaskStatus.pending,
                model_slug=model.slug,
                price=model.price,
                provider=first_leg.provider,
                balance=user.balance,
            )
    return response


@router.get("/tasks/{task_id}", response_model=TaskResponse)
async def task_status(task_id: uuid.UUID) -> TaskResponse:
    """Return the current provider leg and ordered attempt history."""

    async with session_factory() as session:
        async with session.begin():
            # Task, model, current leg and attempt history form one logical
            # response. READ COMMITTED can mix versions across these queries.
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
            return await _read_task_snapshot(session, task_id)


async def _read_task_snapshot(session: AsyncSession, task_id: uuid.UUID) -> TaskResponse:
    task = await session.get(GenerationTask, task_id)
    if task is None:
        raise HTTPException(status_code=404, detail="Task not found")
    model = await session.get(Model, task.model_id)
    if model is None:
        raise RuntimeError("Generation task refers to an unknown model")

    attempts = (
        await session.scalars(
            select(ProviderAttempt)
            .where(ProviderAttempt.task_id == task_id)
            .order_by(ProviderAttempt.ordinal)
        )
    ).all()
    history = [
        AttemptResponse(
            id=attempt.id,
            leg_id=attempt.leg_id,
            provider=attempt.provider,
            ordinal=attempt.ordinal,
            status=attempt.status,
            external_id=attempt.external_id,
            error_type=attempt.error_type,
            deadline_at=attempt.deadline_at,
            finished_at=attempt.finished_at,
        )
        for attempt in attempts
    ]
    current = next(
        (attempt for attempt in history if attempt.id == task.current_attempt_id),
        None,
    )
    return TaskResponse(
        task_id=task.id,
        user_id=task.user_id,
        model_slug=model.slug,
        status=task.status,
        price=task.price,
        result_url=task.result_url,
        current_leg=current,
        attempts=history,
        created_at=task.created_at,
    )
