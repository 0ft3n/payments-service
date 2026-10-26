import logging
import uuid
from datetime import UTC, datetime
from typing import Protocol

from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models import Payment, PaymentStatus

logger = logging.getLogger(__name__)


class PaymentNotFoundError(Exception):
    pass


class Gateway(Protocol):
    async def charge(self, payment: Payment) -> PaymentStatus: ...


class Notifier(Protocol):
    async def send(self, payment: Payment) -> None: ...


class PaymentProcessor:
    """Charges a payment and notifies the client. Safe to run more than once per payment.

    The message may be redelivered at any step (retry, consumer crash), so every step
    checks the stored state first: a processed payment is never charged again, and a
    webhook that was already delivered is not sent again.
    """

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        gateway: Gateway,
        notifier: Notifier,
    ) -> None:
        self._session_factory = session_factory
        self._gateway = gateway
        self._notifier = notifier

    async def process(self, payment_id: uuid.UUID) -> Payment:
        payment = await self._load(payment_id)

        if payment.status is PaymentStatus.PENDING:
            status = await self._gateway.charge(payment)
            payment = await self._finish(payment_id, status)
            logger.info("Payment %s processed: %s", payment_id, payment.status.value)

        if payment.webhook_sent_at is None:
            await self._notifier.send(payment)
            await self._mark_webhook_sent(payment_id)
            logger.info("Webhook for payment %s delivered to %s", payment_id, payment.webhook_url)

        return payment

    async def _load(self, payment_id: uuid.UUID) -> Payment:
        async with self._session_factory() as session:
            payment = await session.get(Payment, payment_id)
        if payment is None:
            raise PaymentNotFoundError(f"Payment {payment_id} not found")
        return payment

    async def _finish(self, payment_id: uuid.UUID, status: PaymentStatus) -> Payment:
        # The status guard keeps the first result if two deliveries raced through the gateway.
        async with self._session_factory() as session, session.begin():
            updated = await session.scalar(
                update(Payment)
                .where(Payment.id == payment_id, Payment.status == PaymentStatus.PENDING)
                .values(status=status, processed_at=datetime.now(UTC))
                .returning(Payment)
            )
        return updated if updated is not None else await self._load(payment_id)

    async def _mark_webhook_sent(self, payment_id: uuid.UUID) -> None:
        async with self._session_factory() as session, session.begin():
            await session.execute(
                update(Payment)
                .where(Payment.id == payment_id)
                .values(webhook_sent_at=datetime.now(UTC))
            )
