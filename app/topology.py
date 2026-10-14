"""RabbitMQ topology shared by the outbox relay (publisher) and the consumer.

    [payments] --payments.new--> payments.new ──(consumer)
         ^                           │ reject (malformed message)
         │ TTL expired               v
    payments.new.retry.N <--  [payments.retry]     [payments.dlx] --> payments.new.dlq
         (x-message-ttl = base * 2**(N-1))              ^
                                                        └── consumer, after the last attempt

Retries go through per-attempt delay queues instead of sleeping in the consumer,
so a slow webhook target doesn't block other messages.
"""

from dataclasses import dataclass

from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

from app.config import Settings

PAYMENTS_NEW = "payments.new"
PAYMENTS_NEW_DLQ = "payments.new.dlq"

ATTEMPT_HEADER = "x-attempt"
ERROR_HEADER = "x-last-error"


@dataclass(frozen=True)
class Topology:
    exchange: RabbitExchange
    retry_exchange: RabbitExchange
    dead_letter_exchange: RabbitExchange
    queue: RabbitQueue
    retry_queues: tuple[RabbitQueue, ...]
    dead_letter_queue: RabbitQueue

    def retry_queue_for(self, failed_attempt: int) -> RabbitQueue:
        return self.retry_queues[failed_attempt - 1]


def build_topology(settings: Settings) -> Topology:
    exchange = RabbitExchange("payments", type=ExchangeType.DIRECT, durable=True)
    retry_exchange = RabbitExchange("payments.retry", type=ExchangeType.DIRECT, durable=True)
    dead_letter_exchange = RabbitExchange("payments.dlx", type=ExchangeType.DIRECT, durable=True)

    queue = RabbitQueue(
        PAYMENTS_NEW,
        durable=True,
        routing_key=PAYMENTS_NEW,
        arguments={
            "x-dead-letter-exchange": dead_letter_exchange.name,
            "x-dead-letter-routing-key": PAYMENTS_NEW_DLQ,
        },
    )
    retry_queues = tuple(
        RabbitQueue(
            f"{PAYMENTS_NEW}.retry.{attempt}",
            durable=True,
            routing_key=f"{PAYMENTS_NEW}.retry.{attempt}",
            arguments={
                "x-message-ttl": settings.retry_base_delay_ms * 2 ** (attempt - 1),
                "x-dead-letter-exchange": exchange.name,
                "x-dead-letter-routing-key": PAYMENTS_NEW,
            },
        )
        for attempt in range(1, settings.max_attempts)
    )
    dead_letter_queue = RabbitQueue(PAYMENTS_NEW_DLQ, durable=True, routing_key=PAYMENTS_NEW_DLQ)

    return Topology(exchange, retry_exchange, dead_letter_exchange, queue, retry_queues, dead_letter_queue)


async def declare_topology(broker: RabbitBroker, topology: Topology) -> None:
    """Idempotently declare exchanges, queues and bindings. Called on startup by every process
    that publishes, so a message is never published before its route exists."""
    pairs = [
        (topology.exchange, topology.queue),
        *((topology.retry_exchange, q) for q in topology.retry_queues),
        (topology.dead_letter_exchange, topology.dead_letter_queue),
    ]
    exchanges = {}
    for rabbit_exchange in (topology.exchange, topology.retry_exchange, topology.dead_letter_exchange):
        exchanges[rabbit_exchange.name] = await broker.declare_exchange(rabbit_exchange)
    for rabbit_exchange, rabbit_queue in pairs:
        declared = await broker.declare_queue(rabbit_queue)
        # Every queue is bound by its own name (see routing_key= above).
        await declared.bind(exchanges[rabbit_exchange.name], routing_key=rabbit_queue.name)
