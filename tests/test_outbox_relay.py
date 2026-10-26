from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.messaging import PAYMENTS_EXCHANGE
from app.models import OutboxEvent
from app.outbox.relay import OutboxRelay
from app.schemas import PaymentCreate
from app.services.payments import create_payment
from tests.factories import payment_body


class FakeBroker:
    def __init__(self, fail: bool = False) -> None:
        self.fail = fail
        self.published: list[dict[str, Any]] = []

    async def publish(self, message: Any, **kwargs: Any) -> None:
        if self.fail:
            raise ConnectionError("broker is down")
        self.published.append({"message": message, **kwargs})


async def add_payments(session_factory: async_sessionmaker[AsyncSession], n: int) -> None:
    for i in range(n):
        async with session_factory() as session:
            await create_payment(session, PaymentCreate(**payment_body()), f"key-{i}")


async def outbox(session_factory: async_sessionmaker[AsyncSession]) -> list[OutboxEvent]:
    async with session_factory() as session:
        return list(await session.scalars(select(OutboxEvent).order_by(OutboxEvent.created_at)))


async def test_publishes_pending_events_once(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    await add_payments(session_factory, 3)
    broker = FakeBroker()
    relay = OutboxRelay(session_factory, broker, batch_size=10, poll_interval=0)  # type: ignore[arg-type]

    assert await relay.publish_batch() == 3
    assert await relay.publish_batch() == 0

    events = await outbox(session_factory)
    assert all(e.published_at is not None for e in events)
    assert [p["message"] for p in broker.published] == [e.payload for e in events]
    assert [p["message_id"] for p in broker.published] == [str(e.id) for e in events]
    assert all(p["exchange"] == PAYMENTS_EXCHANGE and p["persist"] for p in broker.published)


async def test_respects_batch_size(session_factory: async_sessionmaker[AsyncSession]) -> None:
    await add_payments(session_factory, 3)
    relay = OutboxRelay(session_factory, FakeBroker(), batch_size=2, poll_interval=0)  # type: ignore[arg-type]

    assert await relay.publish_batch() == 2
    assert await relay.publish_batch() == 1


async def test_broker_failure_keeps_event_for_next_poll(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    await add_payments(session_factory, 2)
    broker = FakeBroker(fail=True)
    relay = OutboxRelay(session_factory, broker, batch_size=10, poll_interval=0)  # type: ignore[arg-type]

    assert await relay.publish_batch() == 0
    first, second = await outbox(session_factory)
    assert first.published_at is None
    assert first.attempts == 1
    assert first.last_error == "ConnectionError: broker is down"
    assert second.attempts == 0

    broker.fail = False
    assert await relay.publish_batch() == 2
    assert all(
        e.published_at is not None and e.last_error is None for e in await outbox(session_factory)
    )
