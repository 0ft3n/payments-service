import uuid
from decimal import Decimal

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models import Currency, Payment, PaymentStatus


def payment_body(**overrides: object) -> dict[str, object]:
    body: dict[str, object] = {
        "amount": "100.50",
        "currency": "RUB",
        "description": "Order #42",
        "metadata": {"order_id": 42},
        "webhook_url": "https://merchant.example/webhook",
    }
    body.update(overrides)
    return body


async def create_payment(
    session_factory: async_sessionmaker[AsyncSession], **overrides: object
) -> Payment:
    values: dict[str, object] = {
        "id": uuid.uuid4(),
        "amount": Decimal("10.00"),
        "currency": Currency.USD,
        "description": "test",
        "metadata_": {},
        "status": PaymentStatus.PENDING,
        "idempotency_key": str(uuid.uuid4()),
        "request_hash": "0" * 64,
        "webhook_url": "https://merchant.example/webhook",
    }
    values.update(overrides)
    async with session_factory() as session, session.begin():
        payment = Payment(**values)
        session.add(payment)
    return payment
