import asyncio
import uuid

from httpx import AsyncClient
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.messaging import NEW_PAYMENTS_ROUTING_KEY, PAYMENT_CREATED_EVENT
from app.models import OutboxEvent, Payment
from tests.factories import payment_body

URL = "/api/v1/payments"


async def count(session_factory: async_sessionmaker[AsyncSession], model: type) -> int:
    async with session_factory() as session:
        return await session.scalar(select(func.count()).select_from(model))


async def test_requests_without_valid_api_key_are_rejected(client: AsyncClient) -> None:
    headers = {"Idempotency-Key": "key-1"}
    for api_key in (None, "wrong"):
        client.headers.pop("X-API-Key", None)
        if api_key:
            client.headers["X-API-Key"] = api_key
        assert (await client.post(URL, json=payment_body(), headers=headers)).status_code == 401
        assert (await client.get(f"{URL}/{uuid.uuid4()}")).status_code == 401


async def test_create_payment_stores_payment_and_outbox_event(
    client: AsyncClient, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    response = await client.post(URL, json=payment_body(), headers={"Idempotency-Key": "key-1"})

    assert response.status_code == 202
    data = response.json()
    assert data["status"] == "pending"
    assert set(data) == {"payment_id", "status", "created_at"}

    async with session_factory() as session:
        event = await session.scalar(select(OutboxEvent))
    assert event is not None
    assert event.event_type == PAYMENT_CREATED_EVENT
    assert event.routing_key == NEW_PAYMENTS_ROUTING_KEY
    assert event.payload == {"payment_id": data["payment_id"]}
    assert event.published_at is None


async def test_repeated_request_returns_the_same_payment(
    client: AsyncClient, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    headers = {"Idempotency-Key": "key-1"}
    first = await client.post(URL, json=payment_body(), headers=headers)
    second = await client.post(URL, json=payment_body(), headers=headers)

    assert second.status_code == 202
    assert second.json() == first.json()
    assert second.headers["Idempotent-Replayed"] == "true"
    assert await count(session_factory, Payment) == 1
    assert await count(session_factory, OutboxEvent) == 1


async def test_metadata_key_order_does_not_break_idempotency(client: AsyncClient) -> None:
    headers = {"Idempotency-Key": "key-1"}
    first = await client.post(URL, json=payment_body(metadata={"a": 1, "b": 2}), headers=headers)
    second = await client.post(URL, json=payment_body(metadata={"b": 2, "a": 1}), headers=headers)

    assert second.json()["payment_id"] == first.json()["payment_id"]


async def test_same_key_with_different_body_is_a_conflict(client: AsyncClient) -> None:
    headers = {"Idempotency-Key": "key-1"}
    await client.post(URL, json=payment_body(), headers=headers)
    response = await client.post(URL, json=payment_body(amount="1.00"), headers=headers)

    assert response.status_code == 409


async def test_concurrent_duplicates_create_one_payment(
    client: AsyncClient, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    headers = {"Idempotency-Key": "key-1"}
    responses = await asyncio.gather(
        *(client.post(URL, json=payment_body(), headers=headers) for _ in range(10))
    )

    assert {r.status_code for r in responses} == {202}
    assert len({r.json()["payment_id"] for r in responses}) == 1
    assert await count(session_factory, Payment) == 1
    assert await count(session_factory, OutboxEvent) == 1


async def test_get_payment_returns_details(client: AsyncClient) -> None:
    created = await client.post(URL, json=payment_body(), headers={"Idempotency-Key": "key-1"})
    payment_id = created.json()["payment_id"]

    response = await client.get(f"{URL}/{payment_id}")

    assert response.status_code == 200
    assert response.json() == {
        "payment_id": payment_id,
        "amount": "100.50",
        "currency": "RUB",
        "description": "Order #42",
        "metadata": {"order_id": 42},
        "status": "pending",
        "idempotency_key": "key-1",
        "webhook_url": "https://merchant.example/webhook",
        "created_at": created.json()["created_at"],
        "processed_at": None,
        "webhook_sent_at": None,
    }


async def test_get_unknown_payment_returns_404(client: AsyncClient) -> None:
    response = await client.get(f"{URL}/{uuid.uuid4()}")

    assert response.status_code == 404


async def test_idempotency_key_header_is_required(client: AsyncClient) -> None:
    response = await client.post(URL, json=payment_body())

    assert response.status_code == 422


async def test_invalid_body_is_rejected(client: AsyncClient) -> None:
    for body in (
        payment_body(amount="0"),
        payment_body(amount="1.001"),
        payment_body(currency="GBP"),
        payment_body(webhook_url="not-a-url"),
        payment_body(unknown="field"),
    ):
        response = await client.post(URL, json=body, headers={"Idempotency-Key": "key-1"})
        assert response.status_code == 422, body
