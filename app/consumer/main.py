import logging

import httpx
from faststream import FastStream
from faststream.middlewares import AckPolicy
from faststream.rabbit import Channel, RabbitBroker, RabbitMessage

from app.config import get_settings
from app.db import create_engine, create_session_factory
from app.log import setup_logging
from app.messaging import (
    ATTEMPT_HEADER,
    LAST_ERROR_HEADER,
    NEW_PAYMENTS_QUEUE,
    PAYMENTS_EXCHANGE,
    PaymentCreatedEvent,
    declare_topology,
    retry_delay,
    retry_queue,
)
from app.services.gateway import EmulatedGateway
from app.services.processing import PaymentProcessor
from app.services.webhooks import WebhookSender

logger = logging.getLogger(__name__)

settings = get_settings()
setup_logging(settings.log_level)

broker = RabbitBroker(
    settings.rabbitmq_url,
    default_channel=Channel(prefetch_count=settings.consumer_prefetch),
)
app = FastStream(broker)

engine = create_engine(settings.database_url)
http_client = httpx.AsyncClient(timeout=settings.webhook_timeout)
processor = PaymentProcessor(
    session_factory=create_session_factory(engine),
    gateway=EmulatedGateway(
        min_delay=settings.gateway_min_delay,
        max_delay=settings.gateway_max_delay,
        success_rate=settings.gateway_success_rate,
    ),
    notifier=WebhookSender(http_client),
)


@app.on_startup
async def setup_topology() -> None:
    await broker.connect()
    await declare_topology(broker, settings.max_attempts, settings.retry_base_delay)


@app.after_shutdown
async def close_resources() -> None:
    await http_client.aclose()
    await engine.dispose()


# REJECT_ON_ERROR: an exception that escapes the handler rejects the message without
# requeue, and the queue's dead-letter exchange routes it to payments.new.dlq.
@broker.subscriber(NEW_PAYMENTS_QUEUE, PAYMENTS_EXCHANGE, ack_policy=AckPolicy.REJECT_ON_ERROR)
async def handle_new_payment(event: PaymentCreatedEvent, message: RabbitMessage) -> None:
    attempt = int(message.headers.get(ATTEMPT_HEADER, 1))
    try:
        await processor.process(event.payment_id)
    except Exception as exc:
        if attempt >= settings.max_attempts:
            logger.error(
                "Payment %s failed on attempt %d/%d, sending to DLQ: %s",
                event.payment_id,
                attempt,
                settings.max_attempts,
                exc,
            )
            raise

        delay = retry_delay(attempt, settings.retry_base_delay)
        logger.warning(
            "Payment %s failed on attempt %d/%d, retrying in %.1fs: %s",
            event.payment_id,
            attempt,
            settings.max_attempts,
            delay,
            exc,
        )
        await schedule_retry(event, message.message_id, attempt + 1, delay, exc)


async def schedule_retry(
    event: PaymentCreatedEvent, message_id: str, next_attempt: int, delay: float, exc: Exception
) -> None:
    # The copy waits in a TTL queue and then returns to payments.new; the original
    # is acked when the handler returns.
    await broker.publish(
        event.model_dump(mode="json"),
        queue=retry_queue(delay),
        message_id=message_id,
        headers={
            ATTEMPT_HEADER: next_attempt,
            LAST_ERROR_HEADER: f"{type(exc).__name__}: {exc}"[:500],
        },
        persist=True,
    )
