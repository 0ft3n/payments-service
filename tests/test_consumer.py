import uuid
from typing import Any

import pytest
from faststream.rabbit import TestRabbitBroker

from app.consumer import main as consumer
from app.messaging import ATTEMPT_HEADER, NEW_PAYMENTS_ROUTING_KEY, PAYMENTS_EXCHANGE, retry_delay


class FakeProcessor:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.calls: list[uuid.UUID] = []

    async def process(self, payment_id: uuid.UUID) -> None:
        self.calls.append(payment_id)
        if self.error:
            raise self.error


@pytest.fixture
def retries(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    scheduled: list[dict[str, Any]] = []

    async def fake_schedule_retry(
        event: Any, message_id: str, next_attempt: int, delay: float, exc: Exception
    ) -> None:
        scheduled.append({"event": event, "next_attempt": next_attempt, "delay": delay, "exc": exc})

    monkeypatch.setattr(consumer, "schedule_retry", fake_schedule_retry)
    return scheduled


async def deliver(payment_id: uuid.UUID, attempt: int | None = None) -> None:
    headers = {ATTEMPT_HEADER: attempt} if attempt is not None else None
    async with TestRabbitBroker(consumer.broker, connect_only=False) as broker:
        await broker.publish(
            {"payment_id": str(payment_id)},
            exchange=PAYMENTS_EXCHANGE,
            routing_key=NEW_PAYMENTS_ROUTING_KEY,
            headers=headers,
        )


def test_retry_delay_is_exponential() -> None:
    assert [retry_delay(a, 2.0) for a in (1, 2, 3)] == [2.0, 4.0, 8.0]


async def test_message_is_processed(
    monkeypatch: pytest.MonkeyPatch, retries: list[dict[str, Any]]
) -> None:
    processor = FakeProcessor()
    monkeypatch.setattr(consumer, "processor", processor)
    payment_id = uuid.uuid4()

    await deliver(payment_id)

    assert processor.calls == [payment_id]
    assert retries == []


@pytest.mark.parametrize("attempt", [None, 1, 2])
async def test_failed_attempt_is_scheduled_for_retry_with_backoff(
    monkeypatch: pytest.MonkeyPatch, retries: list[dict[str, Any]], attempt: int | None
) -> None:
    error = RuntimeError("webhook is down")
    monkeypatch.setattr(consumer, "processor", FakeProcessor(error))
    current = attempt or 1

    await deliver(uuid.uuid4(), attempt)

    assert len(retries) == 1
    assert retries[0]["next_attempt"] == current + 1
    assert retries[0]["delay"] == retry_delay(current, consumer.settings.retry_base_delay)
    assert retries[0]["exc"] is error


async def test_last_attempt_is_rejected_to_dead_letter_queue(
    monkeypatch: pytest.MonkeyPatch, retries: list[dict[str, Any]]
) -> None:
    monkeypatch.setattr(consumer, "processor", FakeProcessor(RuntimeError("webhook is down")))

    with pytest.raises(RuntimeError):
        await deliver(uuid.uuid4(), consumer.settings.max_attempts)

    assert retries == []
