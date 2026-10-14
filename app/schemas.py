import json
import uuid
from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator

from app.config import get_settings
from app.models import Currency, Payment, PaymentStatus
from app.url_safety import validate_webhook_url

MAX_METADATA_BYTES = 16 * 1024


class PaymentCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    amount: Annotated[Decimal, Field(gt=0, max_digits=18, decimal_places=2)]
    currency: Currency
    description: Annotated[str, Field(min_length=1, max_length=1024)]
    metadata: dict[str, Any] = Field(default_factory=dict)
    webhook_url: HttpUrl

    @field_validator("amount")
    @classmethod
    def normalize_amount(cls, value: Decimal) -> Decimal:
        # 100, 100.0 and 100.00 are the same payment; matters for the idempotency fingerprint.
        return value.quantize(Decimal("0.01"))

    @field_validator("metadata")
    @classmethod
    def limit_metadata_size(cls, value: dict[str, Any]) -> dict[str, Any]:
        if len(json.dumps(value, ensure_ascii=False).encode()) > MAX_METADATA_BYTES:
            raise ValueError(f"metadata must not exceed {MAX_METADATA_BYTES} bytes")
        return value

    @field_validator("webhook_url")
    @classmethod
    def check_webhook_url(cls, value: HttpUrl) -> HttpUrl:
        validate_webhook_url(str(value), allow_private=get_settings().webhook_allow_private_networks)
        return value


class PaymentAccepted(BaseModel):
    payment_id: uuid.UUID
    status: PaymentStatus
    created_at: datetime

    @classmethod
    def from_model(cls, payment: Payment) -> "PaymentAccepted":
        return cls(payment_id=payment.id, status=PaymentStatus(payment.status), created_at=payment.created_at)


class PaymentRead(BaseModel):
    payment_id: uuid.UUID
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any]
    status: PaymentStatus
    idempotency_key: str
    webhook_url: str
    created_at: datetime
    processed_at: datetime | None
    webhook_delivered_at: datetime | None

    @classmethod
    def from_model(cls, payment: Payment) -> "PaymentRead":
        return cls(
            payment_id=payment.id,
            amount=payment.amount,
            currency=Currency(payment.currency),
            description=payment.description,
            metadata=payment.meta,
            status=PaymentStatus(payment.status),
            idempotency_key=payment.idempotency_key,
            webhook_url=payment.webhook_url,
            created_at=payment.created_at,
            processed_at=payment.processed_at,
            webhook_delivered_at=payment.webhook_delivered_at,
        )


class PaymentCreatedEvent(BaseModel):
    """Message published to `payments.new`."""

    event_id: uuid.UUID
    payment_id: uuid.UUID


class WebhookPayload(BaseModel):
    payment_id: uuid.UUID
    status: PaymentStatus
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any]
    created_at: datetime
    processed_at: datetime | None

    @classmethod
    def from_model(cls, payment: Payment) -> "WebhookPayload":
        return cls(
            payment_id=payment.id,
            status=PaymentStatus(payment.status),
            amount=payment.amount,
            currency=Currency(payment.currency),
            description=payment.description,
            metadata=payment.meta,
            created_at=payment.created_at,
            processed_at=payment.processed_at,
        )
