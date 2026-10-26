import uuid

import httpx
import pytest
import respx
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models import Payment, PaymentStatus
from app.services.processing import PaymentNotFoundError, PaymentProcessor
from app.services.webhooks import WebhookDeliveryError, WebhookSender
from tests.factories import create_payment

WEBHOOK_URL = "https://merchant.example/webhook"


class FakeGateway:
    def __init__(self, result: PaymentStatus = PaymentStatus.SUCCEEDED) -> None:
        self.result = result
        self.calls = 0

    async def charge(self, payment: Payment) -> PaymentStatus:
        self.calls += 1
        return self.result


@pytest.fixture
async def http_client() -> httpx.AsyncClient:
    async with httpx.AsyncClient() as client:
        yield client


def make_processor(
    session_factory: async_sessionmaker[AsyncSession],
    gateway: FakeGateway,
    http_client: httpx.AsyncClient,
) -> PaymentProcessor:
    return PaymentProcessor(session_factory, gateway, WebhookSender(http_client))


async def reload(
    session_factory: async_sessionmaker[AsyncSession], payment_id: uuid.UUID
) -> Payment:
    async with session_factory() as session:
        payment = await session.get(Payment, payment_id)
    assert payment is not None
    return payment


@pytest.mark.parametrize("result", [PaymentStatus.SUCCEEDED, PaymentStatus.FAILED])
@respx.mock
async def test_processes_payment_and_sends_webhook(
    session_factory: async_sessionmaker[AsyncSession],
    http_client: httpx.AsyncClient,
    result: PaymentStatus,
) -> None:
    route = respx.post(WEBHOOK_URL).respond(200)
    payment = await create_payment(session_factory)

    await make_processor(session_factory, FakeGateway(result), http_client).process(payment.id)

    stored = await reload(session_factory, payment.id)
    assert stored.status is result
    assert stored.processed_at is not None
    assert stored.webhook_sent_at is not None
    assert route.call_count == 1
    request = route.calls.last.request
    assert request.headers["X-Webhook-Id"] == f"{payment.id}:{result.value}"
    body = httpx.Response(200, content=request.content).json()
    assert body["payment_id"] == str(payment.id)
    assert body["status"] == result.value
    assert body["event"] == f"payment.{result.value}"


@respx.mock
async def test_webhook_failure_keeps_result_and_retry_only_resends_webhook(
    session_factory: async_sessionmaker[AsyncSession], http_client: httpx.AsyncClient
) -> None:
    route = respx.post(WEBHOOK_URL)
    route.side_effect = [httpx.Response(503), httpx.Response(200)]
    gateway = FakeGateway()
    processor = make_processor(session_factory, gateway, http_client)
    payment = await create_payment(session_factory)

    with pytest.raises(WebhookDeliveryError):
        await processor.process(payment.id)

    stored = await reload(session_factory, payment.id)
    assert stored.status is PaymentStatus.SUCCEEDED
    assert stored.webhook_sent_at is None

    await processor.process(payment.id)

    stored = await reload(session_factory, payment.id)
    assert stored.webhook_sent_at is not None
    assert gateway.calls == 1
    assert route.call_count == 2


@respx.mock
async def test_network_error_is_a_delivery_error(
    session_factory: async_sessionmaker[AsyncSession], http_client: httpx.AsyncClient
) -> None:
    respx.post(WEBHOOK_URL).mock(side_effect=httpx.ConnectError("refused"))
    payment = await create_payment(session_factory)

    with pytest.raises(WebhookDeliveryError):
        await make_processor(session_factory, FakeGateway(), http_client).process(payment.id)


@respx.mock
async def test_redelivered_message_for_finished_payment_does_nothing(
    session_factory: async_sessionmaker[AsyncSession], http_client: httpx.AsyncClient
) -> None:
    route = respx.post(WEBHOOK_URL).respond(200)
    gateway = FakeGateway()
    processor = make_processor(session_factory, gateway, http_client)
    payment = await create_payment(session_factory)

    await processor.process(payment.id)
    await processor.process(payment.id)

    assert gateway.calls == 1
    assert route.call_count == 1


async def test_unknown_payment_raises(
    session_factory: async_sessionmaker[AsyncSession], http_client: httpx.AsyncClient
) -> None:
    with pytest.raises(PaymentNotFoundError):
        await make_processor(session_factory, FakeGateway(), http_client).process(uuid.uuid4())
