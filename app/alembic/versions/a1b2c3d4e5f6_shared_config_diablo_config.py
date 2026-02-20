"""shared config diablo_config table

Revision ID: a1b2c3d4e5f6
Revises: 9c38f4f15a29
Create Date: 2025-02-19

Shared configuration: singleton table for multi-server config storage.
"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "a1b2c3d4e5f6"
down_revision = "9c38f4f15a29"
branch_labels = None
depends_on = None


def upgrade():
    connection = op.get_bind()
    dialect = connection.dialect
    if dialect.name == "sqlite":
        jsonb = sa.Text  # type: ignore
    else:
        jsonb = postgresql.JSONB  # type: ignore

    op.create_table(
        "diablo_config",
        sa.Column("id", sa.Integer(), primary_key=True, server_default="1"),
        sa.Column("config_data", jsonb, nullable=False),
        sa.Column("version", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_by", sa.String(255), server_default=""),
        sa.CheckConstraint("id = 1", name="singleton"),
    )


def downgrade():
    op.drop_table("diablo_config")
