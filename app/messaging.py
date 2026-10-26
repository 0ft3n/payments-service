"""RabbitMQ topology and message contracts shared by the relay and the consumer.

outbox relay ──> [payments] ──payments.new──> (payments.new) ──> consumer
                     ^                              │ reject after the last attempt
                     │ TTL expired                  v
           (payments.new.retry.<delay>ms)    [payments.dlx] ──> (payments.new.dlq)
                     ^
                     └──── consumer republishes a failed message
"""

import uuid

from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue
from pydantic import BaseModel

PAYMENT_CREATED_EVENT = "payment.created"
NEW_PAYMENTS_ROUTING_KEY = "payments.new"
DLQ_ROUTING_KEY = "payments.new.dlq"
ATTEMPT_HEADER = "x-attempt"
LAST_ERROR_HEADER = "x-last-error"

PAYMENTS_EXCHANGE = RabbitExchange("payments", type=ExchangeType.DIRECT, durable=True)
DEAD_LETTER_EXCHANGE = RabbitExchange("payments.dlx", type=ExchangeType.DIRECT, durable=True)

NEW_PAYMENTS_QUEUE = RabbitQueue(
    "payments.new",
    durable=True,
    routing_key=NEW_PAYMENTS_ROUTING_KEY,
    arguments={
        "x-dead-letter-exchange": DEAD_LETTER_EXCHANGE.name,
        "x-dead-letter-routing-key": DLQ_ROUTING_KEY,
    },
)

DEAD_LETTER_QUEUE = RabbitQueue("payments.new.dlq", durable=True, routing_key=DLQ_ROUTING_KEY)


class PaymentCreatedEvent(BaseModel):
    payment_id: uuid.UUID


def retry_delay(attempt: int, base_delay: float) -> float:
    """Delay in seconds before the attempt that follows `attempt` (1-based)."""
    return base_delay * 2 ** (attempt - 1)


def retry_queue(delay: float) -> RabbitQueue:
    """A holding queue without consumers: messages wait out the TTL, then go back to work."""
    delay_ms = int(delay * 1000)
    return RabbitQueue(
        f"payments.new.retry.{delay_ms}ms",
        durable=True,
        arguments={
            "x-message-ttl": delay_ms,
            "x-dead-letter-exchange": PAYMENTS_EXCHANGE.name,
            "x-dead-letter-routing-key": NEW_PAYMENTS_ROUTING_KEY,
        },
    )


def retry_queues(max_attempts: int, base_delay: float) -> list[RabbitQueue]:
    return [retry_queue(retry_delay(a, base_delay)) for a in range(1, max_attempts)]


async def declare_topology(broker: RabbitBroker, max_attempts: int, base_delay: float) -> None:
    """Idempotently declare every exchange, queue and binding the service relies on."""
    payments = await broker.declare_exchange(PAYMENTS_EXCHANGE)
    dlx = await broker.declare_exchange(DEAD_LETTER_EXCHANGE)

    new_payments = await broker.declare_queue(NEW_PAYMENTS_QUEUE)
    await new_payments.bind(payments, routing_key=NEW_PAYMENTS_ROUTING_KEY)

    dlq = await broker.declare_queue(DEAD_LETTER_QUEUE)
    await dlq.bind(dlx, routing_key=DLQ_ROUTING_KEY)

    for queue in retry_queues(max_attempts, base_delay):
        await broker.declare_queue(queue)
