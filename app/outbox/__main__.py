import asyncio

from faststream.rabbit import Channel, RabbitBroker

from app.config import get_settings
from app.db import create_engine, create_session_factory
from app.log import setup_logging
from app.messaging import declare_topology
from app.outbox.relay import OutboxRelay


async def main() -> None:
    settings = get_settings()
    setup_logging(settings.log_level)

    engine = create_engine(settings.database_url)
    # on_return_raises: an unroutable message must fail the publish, not vanish.
    broker = RabbitBroker(
        settings.rabbitmq_url,
        default_channel=Channel(publisher_confirms=True, on_return_raises=True),
    )
    await broker.connect()
    await declare_topology(broker, settings.max_attempts, settings.retry_base_delay)

    relay = OutboxRelay(
        create_session_factory(engine),
        broker,
        batch_size=settings.outbox_batch_size,
        poll_interval=settings.outbox_poll_interval,
    )
    try:
        await relay.run()
    finally:
        await broker.stop()
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
