"""Create initial schema and seed provider catalogue."""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from app.db import Base
from app import models  # noqa: F401
revision = "0001_initial"
down_revision = None
branch_labels = None
depends_on = None

def upgrade():
    Base.metadata.create_all(bind=op.get_bind(), checkfirst=False)
    model_table = sa.table("models", sa.column("id", sa.Integer), sa.column("slug", sa.String), sa.column("price", sa.Integer))
    leg_table = sa.table("model_legs", sa.column("id", sa.Integer), sa.column("model_id", sa.Integer), sa.column("provider", sa.String), sa.column("priority", sa.Integer), sa.column("max_ref_images", sa.Integer), sa.column("skip_on_moderation", sa.Boolean))
    op.bulk_insert(model_table, [{"id": 1, "slug": "image-fast", "price": 2}, {"id": 2, "slug": "video-long", "price": 25}])
    op.bulk_insert(leg_table, [
        {"id": 1, "model_id": 1, "provider": "alpha", "priority": 1, "max_ref_images": 4, "skip_on_moderation": False},
        {"id": 2, "model_id": 1, "provider": "beta", "priority": 2, "max_ref_images": 10, "skip_on_moderation": False},
        {"id": 3, "model_id": 2, "provider": "beta", "priority": 1, "max_ref_images": 2, "skip_on_moderation": False},
        {"id": 4, "model_id": 2, "provider": "alpha", "priority": 2, "max_ref_images": 1, "skip_on_moderation": True},
    ])

def downgrade():
    Base.metadata.drop_all(bind=op.get_bind())
    for name in ("ledgerkind", "taskstatus", "attemptstatus"):
        postgresql.ENUM(name=name).drop(op.get_bind(), checkfirst=True)
