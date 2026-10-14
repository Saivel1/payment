import asyncio
import random
import uuid

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import Settings
from app.models import Payment, PaymentStatus, utcnow
from app.schemas import WebhookPayload


class PaymentNotFound(Exception):
    pass


async def emulate_gateway(settings: Settings) -> PaymentStatus:
    await asyncio.sleep(random.uniform(settings.processing_min_seconds, settings.processing_max_seconds))
    return PaymentStatus.SUCCEEDED if random.random() < settings.processing_success_rate else PaymentStatus.FAILED


async def process_payment(
    session_factory: async_sessionmaker[AsyncSession], payment_id: uuid.UUID, settings: Settings
) -> tuple[str, WebhookPayload] | None:
    """Charge the payment if it is still pending and return (webhook_url, payload) to notify,
    or None if the webhook was already delivered.

    Idempotent: the row lock serializes duplicate deliveries of the same message, and a payment
    is charged only while it is `pending`, so redeliveries and retries only resend the webhook.
    """
    async with session_factory() as session, session.begin():
        payment = await session.scalar(select(Payment).where(Payment.id == payment_id).with_for_update())
        if payment is None:
            raise PaymentNotFound(str(payment_id))
        if payment.status == PaymentStatus.PENDING:
            payment.status = (await emulate_gateway(settings)).value
            payment.processed_at = utcnow()
        if payment.webhook_delivered_at is not None:
            return None
        return payment.webhook_url, WebhookPayload.from_model(payment)


async def mark_webhook_delivered(session_factory: async_sessionmaker[AsyncSession], payment_id: uuid.UUID) -> None:
    async with session_factory() as session, session.begin():
        await session.execute(
            update(Payment)
            .where(Payment.id == payment_id, Payment.webhook_delivered_at.is_(None))
            .values(webhook_delivered_at=utcnow())
        )
