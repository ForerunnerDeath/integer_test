"""Freeze the initial schema independently of later ORM model changes."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0001_initial"
down_revision = None
branch_labels = None
depends_on = None

# Explicit PostgreSQL enums: create once before tables, drop once after them.
ledger_kind = postgresql.ENUM("credit", "debit", "refund", name="ledgerkind", create_type=False)
task_status = postgresql.ENUM(
    "pending", "processing", "completed", "failed", name="taskstatus", create_type=False
)
attempt_status = postgresql.ENUM(
    "pending", "sending", "waiting", "completed", "failed",
    name="attemptstatus", create_type=False
)


def upgrade() -> None:
    bind = op.get_bind()
    ledger_kind.create(bind, checkfirst=True)
    task_status.create(bind, checkfirst=True)
    attempt_status.create(bind, checkfirst=True)

    op.create_table(
        "users",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("balance", sa.BigInteger(), nullable=False, server_default="0"),
        sa.CheckConstraint("balance >= 0", name="ck_users_nonnegative_balance"),
    )
    op.create_table(
        "models",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("slug", sa.String(80), nullable=False, unique=True),
        sa.Column("price", sa.Integer(), nullable=False),
        sa.CheckConstraint("price > 0", name="ck_models_positive_price"),
    )
    op.create_table(
        "model_legs",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("model_id", sa.Integer(), sa.ForeignKey("models.id"), nullable=False),
        sa.Column("provider", sa.String(30), nullable=False),
        sa.Column("priority", sa.Integer(), nullable=False),
        sa.Column("max_ref_images", sa.Integer(), nullable=False),
        sa.Column("skip_on_moderation", sa.Boolean(), nullable=False),
        sa.UniqueConstraint("model_id", "priority", name="uq_model_leg_priority"),
        sa.CheckConstraint("max_ref_images >= 0", name="ck_leg_ref_limit"),
    )
    op.create_table(
        "ledger_entries",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("user_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("users.id"), nullable=False),
        sa.Column("operation_key", sa.String(160), nullable=False, unique=True),
        sa.Column("kind", ledger_kind, nullable=False),
        sa.Column("delta", sa.BigInteger(), nullable=False),
        sa.Column("balance_after", sa.BigInteger(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.CheckConstraint("delta <> 0", name="ck_ledger_nonzero_delta"),
    )
    op.create_index("ix_ledger_user_created", "ledger_entries", ["user_id", "created_at"])
    op.create_table(
        "generation_tasks",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("user_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("users.id"), nullable=False),
        sa.Column("model_id", sa.Integer(), sa.ForeignKey("models.id"), nullable=False),
        sa.Column("prompt", sa.Text(), nullable=False),
        sa.Column("ref_images", sa.JSON(), nullable=False),
        sa.Column("price", sa.Integer(), nullable=False),
        sa.Column("status", task_status, nullable=False),
        sa.Column("current_attempt_id", postgresql.UUID(as_uuid=True)),
        sa.Column("result_url", sa.Text()),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    op.create_index("ix_tasks_user_status", "generation_tasks", ["user_id", "status"])
    op.create_table(
        "provider_attempts",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "task_id", postgresql.UUID(as_uuid=True),
            sa.ForeignKey("generation_tasks.id"), nullable=False,
        ),
        sa.Column("leg_id", sa.Integer(), sa.ForeignKey("model_legs.id"), nullable=False),
        sa.Column("ordinal", sa.Integer(), nullable=False),
        sa.Column("provider", sa.String(30), nullable=False),
        sa.Column("status", attempt_status, nullable=False),
        sa.Column("external_id", sa.String(160)),
        sa.Column("error_type", sa.String(30)),
        sa.Column("deadline_at", sa.DateTime(timezone=True)),
        sa.Column("finished_at", sa.DateTime(timezone=True)),
        sa.UniqueConstraint("task_id", "ordinal", name="uq_attempt_task_ordinal"),
    )
    op.create_index(
        "ix_attempt_status_deadline", "provider_attempts", ["status", "deadline_at"]
    )
    op.create_table(
        "payments",
        sa.Column("payment_id", sa.String(160), primary_key=True),
        sa.Column("user_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("users.id"), nullable=False),
        sa.Column("tokens", sa.BigInteger(), nullable=False),
        sa.Column("status", sa.String(30), nullable=False),
        sa.CheckConstraint("tokens > 0", name="ck_payments_positive_tokens"),
    )
    op.create_table(
        "outbox_events",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("event_key", sa.String(160), nullable=False, unique=True),
        sa.Column("topic", sa.String(80), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("available_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("claimed_until", sa.DateTime(timezone=True)),
        sa.Column("done_at", sa.DateTime(timezone=True)),
        sa.Column("retries", sa.Integer(), nullable=False),
    )
    op.create_index(
        "ix_outbox_available", "outbox_events", ["done_at", "available_at"]
    )

    model_table = sa.table(
        "models",
        sa.column("id", sa.Integer()),
        sa.column("slug", sa.String()),
        sa.column("price", sa.Integer()),
    )
    leg_table = sa.table(
        "model_legs",
        sa.column("id", sa.Integer()),
        sa.column("model_id", sa.Integer()),
        sa.column("provider", sa.String()),
        sa.column("priority", sa.Integer()),
        sa.column("max_ref_images", sa.Integer()),
        sa.column("skip_on_moderation", sa.Boolean()),
    )
    op.bulk_insert(
        model_table,
        [
            {"id": 1, "slug": "image-fast", "price": 2},
            {"id": 2, "slug": "video-long", "price": 25},
        ],
    )
    op.bulk_insert(
        leg_table,
        [
            {
                "id": 1, "model_id": 1, "provider": "alpha",
                "priority": 1, "max_ref_images": 4, "skip_on_moderation": False,
            },
            {
                "id": 2, "model_id": 1, "provider": "beta",
                "priority": 2, "max_ref_images": 10, "skip_on_moderation": False,
            },
            {
                "id": 3, "model_id": 2, "provider": "beta",
                "priority": 1, "max_ref_images": 2, "skip_on_moderation": False,
            },
            {
                "id": 4, "model_id": 2, "provider": "alpha",
                "priority": 2, "max_ref_images": 1, "skip_on_moderation": True,
            },
        ],
    )


def downgrade() -> None:
    op.drop_index("ix_outbox_available", table_name="outbox_events")
    op.drop_table("outbox_events")
    op.drop_table("payments")
    op.drop_index("ix_attempt_status_deadline", table_name="provider_attempts")
    op.drop_table("provider_attempts")
    op.drop_index("ix_tasks_user_status", table_name="generation_tasks")
    op.drop_table("generation_tasks")
    op.drop_index("ix_ledger_user_created", table_name="ledger_entries")
    op.drop_table("ledger_entries")
    op.drop_table("model_legs")
    op.drop_table("models")
    op.drop_table("users")

    attempt_status.drop(op.get_bind(), checkfirst=True)
    task_status.drop(op.get_bind(), checkfirst=True)
    ledger_kind.drop(op.get_bind(), checkfirst=True)
