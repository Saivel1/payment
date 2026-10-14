"""Outbox relay: moves events from the `outbox` table to RabbitMQ.

Run: python -m app.outbox_relay

Rows are locked with FOR UPDATE SKIP LOCKED, so several relay instances can run in parallel.
A row is marked published only after the broker confirmed the message (publisher confirms),
in the same transaction that holds the lock. Delivery is at-least-once: if the process dies
between the confirm and the commit, the event is published again, which the consumer tolerates.
"""

import asyncio
import contextlib
import logging
import signal

from faststream.rabbit import RabbitBroker
from sqlalchemy import select

from app.config import Settings, get_settings, require_value
from app.db import SessionFactory, engine
from app.models import OutboxEvent, utcnow
from app.topology import Topology, build_topology, declare_topology

logger = logging.getLogger("outbox_relay")


async def publish_batch(broker: RabbitBroker, topology: Topology, batch_size: int) -> int:
    published = 0
    async with SessionFactory() as session, session.begin():
        events = (
            await session.scalars(
                select(OutboxEvent)
                .where(OutboxEvent.published_at.is_(None))
                .order_by(OutboxEvent.id)
                .limit(batch_size)
                .with_for_update(skip_locked=True)
            )
        ).all()
        for event in events:
            try:
                await broker.publish(
                    event.payload,
                    exchange=topology.exchange,
                    routing_key=event.routing_key,
                    message_id=str(event.payload.get("event_id", event.id)),
                    message_type=event.event_type,
                    persist=True,
                )
            except Exception:
                # Keep what was already confirmed; the rest is retried on the next iteration.
                logger.exception("Failed to publish outbox event %s", event.id)
                break
            event.published_at = utcnow()
            published += 1
    return published


async def run(settings: Settings) -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)

    topology = build_topology(settings)
    broker = RabbitBroker(require_value(settings.rabbitmq_url, "RABBITMQ_URL"))
    try:
        async with broker:
            await broker.start()  # no subscribers; sets up the producer used by publish()
            await declare_topology(broker, topology)
            logger.info("Outbox relay started")
            while not stop.is_set():
                try:
                    published = await publish_batch(broker, topology, settings.outbox_batch_size)
                except Exception:
                    logger.exception("Outbox iteration failed")
                    published = 0
                if published:
                    logger.info("Published %d outbox events", published)
                if published < settings.outbox_batch_size:
                    with contextlib.suppress(TimeoutError):
                        await asyncio.wait_for(stop.wait(), timeout=settings.outbox_poll_interval)
    finally:
        await engine.dispose()
        logger.info("Outbox relay stopped")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    asyncio.run(run(get_settings()))


if __name__ == "__main__":
    main()
