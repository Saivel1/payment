import hashlib
import json
import uuid

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import OutboxEvent, Payment, PaymentStatus
from app.schemas import PaymentCreate, PaymentCreatedEvent
from app.topology import PAYMENTS_NEW


class IdempotencyConflict(Exception):
    """The Idempotency-Key was already used with a different request body."""


def request_fingerprint(data: PaymentCreate) -> str:
    canonical = json.dumps(data.model_dump(mode="json"), sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode()).hexdigest()


async def get_payment(session: AsyncSession, payment_id: uuid.UUID) -> Payment | None:
    return await session.get(Payment, payment_id)


async def _get_by_idempotency_key(session: AsyncSession, key: str) -> Payment | None:
    return await session.scalar(select(Payment).where(Payment.idempotency_key == key))


def _replay(existing: Payment, fingerprint: str) -> Payment:
    if existing.request_hash != fingerprint:
        raise IdempotencyConflict
    return existing


async def create_payment(session: AsyncSession, data: PaymentCreate, idempotency_key: str) -> Payment:
    """Create a payment and its outbox event in one transaction.

    A repeated request with the same key and body returns the original payment;
    the same key with a different body raises IdempotencyConflict. Concurrent duplicates
    are resolved by the unique constraint on idempotency_key.
    """
    fingerprint = request_fingerprint(data)

    existing = await _get_by_idempotency_key(session, idempotency_key)
    if existing is not None:
        return _replay(existing, fingerprint)

    payment = Payment(
        id=uuid.uuid4(),
        amount=data.amount,
        currency=data.currency.value,
        description=data.description,
        meta=data.metadata,
        status=PaymentStatus.PENDING.value,
        idempotency_key=idempotency_key,
        request_hash=fingerprint,
        webhook_url=str(data.webhook_url),
    )
    event = PaymentCreatedEvent(event_id=uuid.uuid4(), payment_id=payment.id)
    session.add(payment)
    session.add(
        OutboxEvent(
            aggregate_id=payment.id,
            event_type="payment.created",
            routing_key=PAYMENTS_NEW,
            payload=event.model_dump(mode="json"),
        )
    )
    try:
        await session.commit()
    except IntegrityError:
        await session.rollback()
        existing = await _get_by_idempotency_key(session, idempotency_key)
        if existing is None:
            raise
        return _replay(existing, fingerprint)
    return payment
