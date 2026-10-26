import asyncio
import logging
from datetime import UTC, datetime

from faststream.rabbit import RabbitBroker
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.messaging import PAYMENTS_EXCHANGE
from app.models import OutboxEvent

logger = logging.getLogger(__name__)


class OutboxRelay:
    """Moves committed outbox rows to RabbitMQ.

    A row is marked as published only after the broker confirms the message, in the
    same transaction that locked it. A crash in between leads to a re-publish, never to
    a lost event, so delivery is at-least-once and the consumer is idempotent.
    FOR UPDATE SKIP LOCKED lets several relay instances share the table safely.
    """

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        broker: RabbitBroker,
        batch_size: int,
        poll_interval: float,
    ) -> None:
        self._session_factory = session_factory
        self._broker = broker
        self._batch_size = batch_size
        self._poll_interval = poll_interval

    async def run(self) -> None:
        logger.info("Outbox relay started")
        while True:
            try:
                published = await self.publish_batch()
            except Exception:
                logger.exception("Outbox relay iteration failed")
                published = 0
            if published < self._batch_size:
                await asyncio.sleep(self._poll_interval)

    async def publish_batch(self) -> int:
        async with self._session_factory() as session, session.begin():
            events = (
                await session.scalars(
                    select(OutboxEvent)
                    .where(OutboxEvent.published_at.is_(None))
                    .order_by(OutboxEvent.created_at)
                    .limit(self._batch_size)
                    .with_for_update(skip_locked=True)
                )
            ).all()

            published = 0
            for event in events:
                event.attempts += 1
                try:
                    await self._broker.publish(
                        event.payload,
                        exchange=PAYMENTS_EXCHANGE,
                        routing_key=event.routing_key,
                        message_id=str(event.id),
                        message_type=event.event_type,
                        persist=True,
                        mandatory=True,
                    )
                except Exception as exc:
                    # Keep order: stop the batch and retry this event on the next poll.
                    event.last_error = f"{type(exc).__name__}: {exc}"
                    logger.warning("Failed to publish outbox event %s: %s", event.id, exc)
                    break
                event.published_at = datetime.now(UTC)
                event.last_error = None
                published += 1

        if published:
            logger.info("Published %d outbox event(s)", published)
        return published
