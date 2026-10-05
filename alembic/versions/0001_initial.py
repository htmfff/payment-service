"""Create the payments and outbox_events tables

Revision ID: 0001_initial
Revises:
Create Date: 2026-01-15 10:00:00.000000
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001_initial"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

payment_currency = postgresql.ENUM(
    "RUB",
    "USD",
    "EUR",
    name="payment_currency",
    create_type=False,
)
payment_status = postgresql.ENUM(
    "pending",
    "succeeded",
    "failed",
    name="payment_status",
    create_type=False,
)
outbox_status = postgresql.ENUM(
    "pending",
    "published",
    "dead",
    name="outbox_status",
    create_type=False,
)


def upgrade() -> None:
    bind = op.get_bind()
    payment_currency.create(bind, checkfirst=True)
    payment_status.create(bind, checkfirst=True)
    outbox_status.create(bind, checkfirst=True)

    op.create_table(
        "payments",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("idempotency_key", sa.String(length=128), nullable=False),
        sa.Column("request_fingerprint", sa.String(length=64), nullable=False),
        sa.Column("amount", sa.Numeric(precision=18, scale=2), nullable=False),
        sa.Column("currency", payment_currency, nullable=False),
        sa.Column("description", sa.String(length=255), nullable=False),
        sa.Column(
            "metadata",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column("webhook_url", sa.String(length=2048), nullable=False),
        sa.Column("status", payment_status, server_default="pending", nullable=False),
        sa.Column("gateway_reference", sa.String(length=64), nullable=True),
        sa.Column("failure_reason", sa.Text(), nullable=True),
        sa.Column("processing_attempts", sa.Integer(), server_default="0", nullable=False),
        sa.Column("webhook_attempts", sa.Integer(), server_default="0", nullable=False),
        sa.Column("webhook_last_error", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint("amount > 0", name="ck_payments_amount_positive"),
        sa.PrimaryKeyConstraint("id", name="pk_payments"),
        sa.UniqueConstraint("idempotency_key", name="uq_payments_idempotency_key"),
    )
    op.create_index("ix_payments_status_created_at", "payments", ["status", "created_at"])

    op.create_table(
        "outbox_events",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "aggregate_type",
            sa.String(length=64),
            server_default="payment",
            nullable=False,
        ),
        sa.Column("aggregate_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("event_type", sa.String(length=64), nullable=False),
        sa.Column("routing_key", sa.String(length=128), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("status", outbox_status, server_default="pending", nullable=False),
        sa.Column("attempts", sa.Integer(), server_default="0", nullable=False),
        sa.Column(
            "available_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(
            ["aggregate_id"],
            ["payments.id"],
            name="fk_outbox_events_aggregate_id_payments",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_outbox_events"),
    )
    op.create_index(
        "ix_outbox_events_dispatch",
        "outbox_events",
        ["available_at"],
        postgresql_where=sa.text("status = 'pending'"),
    )


def downgrade() -> None:
    op.drop_index("ix_outbox_events_dispatch", table_name="outbox_events")
    op.drop_table("outbox_events")
    op.drop_index("ix_payments_status_created_at", table_name="payments")
    op.drop_table("payments")

    bind = op.get_bind()
    outbox_status.drop(bind, checkfirst=True)
    payment_status.drop(bind, checkfirst=True)
    payment_currency.drop(bind, checkfirst=True)
