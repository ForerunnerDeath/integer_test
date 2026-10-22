import enum
import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.db import Base


def uid() -> uuid.UUID:
    return uuid.uuid4()


class TaskStatus(str, enum.Enum):
    pending = "pending"
    processing = "processing"
    completed = "completed"
    failed = "failed"


class AttemptStatus(str, enum.Enum):
    pending = "pending"
    sending = "sending"
    waiting = "waiting"
    completed = "completed"
    failed = "failed"


class LedgerKind(str, enum.Enum):
    credit = "credit"
    debit = "debit"
    refund = "refund"


class User(Base):
    __tablename__ = "users"
    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uid
    )
    balance: Mapped[int] = mapped_column(
        BigInteger, nullable=False, default=0, server_default="0"
    )
    __table_args__ = (
        CheckConstraint("balance >= 0", name="ck_users_nonnegative_balance"),
    )


class LedgerEntry(Base):
    __tablename__ = "ledger_entries"
    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uid
    )
    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id"), nullable=False)
    operation_key: Mapped[str] = mapped_column(String(160), nullable=False, unique=True)
    kind: Mapped[LedgerKind] = mapped_column(Enum(LedgerKind), nullable=False)
    delta: Mapped[int] = mapped_column(BigInteger, nullable=False)
    balance_after: Mapped[int] = mapped_column(BigInteger, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    __table_args__ = (
        CheckConstraint("delta <> 0", name="ck_ledger_nonzero_delta"),
        Index("ix_ledger_user_created", "user_id", "created_at"),
    )


class Model(Base):
    __tablename__ = "models"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    slug: Mapped[str] = mapped_column(String(80), unique=True, nullable=False)
    price: Mapped[int] = mapped_column(Integer, nullable=False)
    __table_args__ = (CheckConstraint("price > 0", name="ck_models_positive_price"),)


class ModelLeg(Base):
    __tablename__ = "model_legs"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    model_id: Mapped[int] = mapped_column(ForeignKey("models.id"), nullable=False)
    provider: Mapped[str] = mapped_column(String(30), nullable=False)
    priority: Mapped[int] = mapped_column(Integer, nullable=False)
    max_ref_images: Mapped[int] = mapped_column(Integer, nullable=False)
    skip_on_moderation: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False
    )
    __table_args__ = (
        UniqueConstraint("model_id", "priority", name="uq_model_leg_priority"),
        CheckConstraint("max_ref_images >= 0", name="ck_leg_ref_limit"),
    )


class GenerationTask(Base):
    __tablename__ = "generation_tasks"
    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uid
    )
    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id"), nullable=False)
    model_id: Mapped[int] = mapped_column(ForeignKey("models.id"), nullable=False)
    prompt: Mapped[str] = mapped_column(Text, nullable=False)
    ref_images: Mapped[list[str]] = mapped_column(JSON, nullable=False, default=list)
    price: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[TaskStatus] = mapped_column(
        Enum(TaskStatus), nullable=False, default=TaskStatus.pending
    )
    current_attempt_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))
    result_url: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    __table_args__ = (Index("ix_tasks_user_status", "user_id", "status"),)


class ProviderAttempt(Base):
    __tablename__ = "provider_attempts"
    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uid
    )
    task_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("generation_tasks.id"), nullable=False
    )
    leg_id: Mapped[int] = mapped_column(ForeignKey("model_legs.id"), nullable=False)
    ordinal: Mapped[int] = mapped_column(Integer, nullable=False)
    provider: Mapped[str] = mapped_column(String(30), nullable=False)
    status: Mapped[AttemptStatus] = mapped_column(
        Enum(AttemptStatus), nullable=False, default=AttemptStatus.pending
    )
    external_id: Mapped[str | None] = mapped_column(String(160))
    error_type: Mapped[str | None] = mapped_column(String(30))
    deadline_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    __table_args__ = (
        UniqueConstraint("task_id", "ordinal", name="uq_attempt_task_ordinal"),
        Index("ix_attempt_status_deadline", "status", "deadline_at"),
    )


class Payment(Base):
    __tablename__ = "payments"
    payment_id: Mapped[str] = mapped_column(String(160), primary_key=True)
    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id"), nullable=False)
    tokens: Mapped[int] = mapped_column(BigInteger, nullable=False)
    status: Mapped[str] = mapped_column(String(30), nullable=False)
    __table_args__ = (
        CheckConstraint("tokens > 0", name="ck_payments_positive_tokens"),
    )


class OutboxEvent(Base):
    __tablename__ = "outbox_events"
    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uid
    )
    event_key: Mapped[str] = mapped_column(String(160), unique=True, nullable=False)
    topic: Mapped[str] = mapped_column(String(80), nullable=False)
    payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    available_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    claimed_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    done_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    retries: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    __table_args__ = (Index("ix_outbox_available", "done_at", "available_at"),)
