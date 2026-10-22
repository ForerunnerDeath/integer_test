"""Repair seeded ID sequences and match ORM non-null timestamp metadata.

Revision ID: 0002_audit_schema
Revises: 0001_initial
"""

import sqlalchemy as sa
from alembic import op

revision = "0002_audit_schema"
down_revision = "0001_initial"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Explicit IDs inserted in 0001 do not advance PostgreSQL SERIAL sequences.
    for table in ("models", "model_legs"):
        op.execute(
            sa.text(
                f"SELECT setval(pg_get_serial_sequence('{table}', 'id'), "
                f"(SELECT MAX(id) FROM {table}), true)"
            )
        )

    for table, column in (
        ("ledger_entries", "created_at"),
        ("generation_tasks", "created_at"),
        ("outbox_events", "available_at"),
    ):
        op.alter_column(
            table, column, existing_type=sa.DateTime(timezone=True),
            nullable=False,
        )


def downgrade() -> None:
    for table, column in (
        ("ledger_entries", "created_at"),
        ("generation_tasks", "created_at"),
        ("outbox_events", "available_at"),
    ):
        op.alter_column(
            table, column, existing_type=sa.DateTime(timezone=True),
            nullable=True,
        )
    # Sequence values intentionally stay monotonic on downgrade.
