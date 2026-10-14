"""Consumer of `payments.new`: charges the payment, stores the result, sends the webhook.

Run: faststream run app.consumer.main:app

Acknowledgement is manual so that every outcome is explicit:
  * success                    -> ack
  * error, attempts left       -> republish to the delay queue for this attempt, then ack
  * error, no attempts left    -> publish to the DLQ, then ack
  * malformed message          -> reject (dead-lettered to the DLQ by the queue's DLX)
If the process dies before acking, RabbitMQ redelivers the message; processing is idempotent.
"""

from typing import Any

from faststream import AckPolicy, FastStream, Logger
from faststream.rabbit import Channel, RabbitBroker
from faststream.rabbit.annotations import RabbitMessage
from pydantic import ValidationError

from app.config import get_settings, require_secret, require_value
from app.consumer.processing import PaymentNotFound, mark_webhook_delivered, process_payment
from app.consumer.webhook import WebhookSender
from app.db import SessionFactory, engine
from app.schemas import PaymentCreatedEvent
from app.topology import ATTEMPT_HEADER, ERROR_HEADER, build_topology, declare_topology

settings = get_settings()
topology = build_topology(settings)

broker = RabbitBroker(
    require_value(settings.rabbitmq_url, "RABBITMQ_URL"),
    default_channel=Channel(prefetch_count=settings.consumer_prefetch),
)
app = FastStream(broker)

webhooks = WebhookSender(
    secret=require_secret(settings.webhook_secret, "WEBHOOK_SECRET"),
    timeout=settings.webhook_timeout_seconds,
    allow_private_networks=settings.webhook_allow_private_networks,
)


@app.on_startup
async def on_startup() -> None:
    # Declare retry/DLQ routes before the subscriber starts consuming.
    await broker.connect()
    await declare_topology(broker, topology)
    await webhooks.start()


@app.after_shutdown
async def after_shutdown() -> None:
    await webhooks.close()
    await engine.dispose()


def _attempt(msg: RabbitMessage) -> int:
    try:
        return max(1, int(msg.headers.get(ATTEMPT_HEADER, 1)))
    except (TypeError, ValueError):
        return 1


async def _handle(event: PaymentCreatedEvent) -> None:
    result = await process_payment(SessionFactory, event.payment_id, settings)
    if result is None:
        return
    url, payload = result
    await webhooks.send(url, payload)
    await mark_webhook_delivered(SessionFactory, event.payment_id)


async def _to_dead_letter(body: Any, msg: RabbitMessage, attempt: int, error: str) -> None:
    await broker.publish(
        body,
        exchange=topology.dead_letter_exchange,
        routing_key=topology.dead_letter_queue.name,
        headers={ATTEMPT_HEADER: attempt, ERROR_HEADER: error},
        message_id=msg.message_id,
        persist=True,
    )


async def _to_retry(body: Any, msg: RabbitMessage, attempt: int, error: str) -> None:
    await broker.publish(
        body,
        exchange=topology.retry_exchange,
        routing_key=topology.retry_queue_for(attempt).name,
        headers={ATTEMPT_HEADER: attempt + 1, ERROR_HEADER: error},
        message_id=msg.message_id,
        persist=True,
    )


@broker.subscriber(topology.queue, topology.exchange, ack_policy=AckPolicy.MANUAL)
async def handle_payment(body: Any, msg: RabbitMessage, logger: Logger) -> None:
    attempt = _attempt(msg)

    try:
        event = PaymentCreatedEvent.model_validate(body)
    except ValidationError:
        logger.error("Malformed message %s, rejecting to DLQ", msg.message_id)
        await msg.reject(requeue=False)
        return

    try:
        await _handle(event)
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"[:500]
        try:
            if isinstance(exc, PaymentNotFound) or attempt >= settings.max_attempts:
                logger.error("Payment %s failed on attempt %d, moving to DLQ: %s", event.payment_id, attempt, error)
                await _to_dead_letter(body, msg, attempt, error)
            else:
                logger.warning("Payment %s failed on attempt %d, scheduling retry: %s", event.payment_id, attempt, error)
                await _to_retry(body, msg, attempt, error)
        except Exception:
            logger.exception("Could not reroute message %s, requeueing", msg.message_id)
            await msg.nack(requeue=True)
            return
        await msg.ack()
        return

    logger.info("Payment %s processed and webhook delivered", event.payment_id)
    await msg.ack()
