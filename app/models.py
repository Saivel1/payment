import uuid
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    Identity,
    Index,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


def utcnow() -> datetime:
    return datetime.now(UTC)


class Currency(StrEnum):
    RUB = "RUB"
    USD = "USD"
    EUR = "EUR"


class PaymentStatus(StrEnum):
    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class Base(DeclarativeBase):
    type_annotation_map = {dict[str, Any]: JSONB}


class Payment(Base):
    __tablename__ = "payments"
    __table_args__ = (
        UniqueConstraint("idempotency_key", name="uq_payments_idempotency_key"),
        CheckConstraint("amount > 0", name="ck_payments_amount_positive"),
        CheckConstraint("currency IN ('RUB', 'USD', 'EUR')", name="ck_payments_currency"),
        CheckConstraint("status IN ('pending', 'succeeded', 'failed')", name="ck_payments_status"),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    amount: Mapped[Decimal] = mapped_column(Numeric(18, 2))
    currency: Mapped[str] = mapped_column(String(3))
    description: Mapped[str] = mapped_column(String(1024))
    # `metadata` is reserved by SQLAlchemy's declarative base, hence the attribute name.
    meta: Mapped[dict[str, Any]] = mapped_column("metadata", default=dict, server_default=text("'{}'::jsonb"))
    status: Mapped[str] = mapped_column(String(16), default=PaymentStatus.PENDING.value)
    idempotency_key: Mapped[str] = mapped_column(String(255))
    request_hash: Mapped[str] = mapped_column(String(64))
    webhook_url: Mapped[str] = mapped_column(Text)
    # Python-side defaults so values are available right after commit without a lazy refresh.
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, server_default=func.now())
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    webhook_delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class OutboxEvent(Base):
    __tablename__ = "outbox"
    __table_args__ = (Index("ix_outbox_unpublished", "id", postgresql_where=text("published_at IS NULL")),)

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    aggregate_id: Mapped[uuid.UUID]
    event_type: Mapped[str] = mapped_column(String(64))
    routing_key: Mapped[str] = mapped_column(String(255))
    payload: Mapped[dict[str, Any]]
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, server_default=func.now())
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
