import hashlib
import json
import uuid

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.messaging import NEW_PAYMENTS_ROUTING_KEY, PAYMENT_CREATED_EVENT, PaymentCreatedEvent
from app.models import OutboxEvent, Payment, PaymentStatus
from app.schemas import PaymentCreate


class IdempotencyConflictError(Exception):
    """The idempotency key was already used for a request with a different body."""


def request_fingerprint(data: PaymentCreate) -> str:
    canonical = json.dumps(data.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


async def create_payment(
    session: AsyncSession, data: PaymentCreate, idempotency_key: str
) -> tuple[Payment, bool]:
    """Create a payment and its outbox event in one transaction.

    Returns the payment and whether it was created by this call. A repeated request
    with the same key returns the original payment and does not emit a second event.
    """
    fingerprint = request_fingerprint(data)
    payment_id = uuid.uuid4()

    # ON CONFLICT DO NOTHING makes concurrent duplicates safe: the losing transaction
    # waits for the winner to commit and then reads its row below.
    stmt = (
        insert(Payment)
        .values(
            id=payment_id,
            amount=data.amount,
            currency=data.currency,
            description=data.description,
            metadata_=data.metadata,
            status=PaymentStatus.PENDING,
            idempotency_key=idempotency_key,
            request_hash=fingerprint,
            webhook_url=str(data.webhook_url),
        )
        .on_conflict_do_nothing(index_elements=[Payment.idempotency_key])
        .returning(Payment)
    )
    payment = (await session.scalars(stmt)).one_or_none()

    if payment is None:
        existing = await session.scalar(
            select(Payment).where(Payment.idempotency_key == idempotency_key)
        )
        assert existing is not None
        if existing.request_hash != fingerprint:
            raise IdempotencyConflictError
        return existing, False

    session.add(
        OutboxEvent(
            aggregate_id=payment.id,
            event_type=PAYMENT_CREATED_EVENT,
            routing_key=NEW_PAYMENTS_ROUTING_KEY,
            payload=PaymentCreatedEvent(payment_id=payment.id).model_dump(mode="json"),
        )
    )
    await session.commit()
    return payment, True


async def get_payment(session: AsyncSession, payment_id: uuid.UUID) -> Payment | None:
    return await session.get(Payment, payment_id)
