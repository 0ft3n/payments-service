import asyncio
import random

from app.models import Payment, PaymentStatus


class EmulatedGateway:
    """Stand-in for an external payment provider: slow and occasionally declining."""

    def __init__(self, min_delay: float, max_delay: float, success_rate: float) -> None:
        self._min_delay = min_delay
        self._max_delay = max_delay
        self._success_rate = success_rate

    async def charge(self, payment: Payment) -> PaymentStatus:
        await asyncio.sleep(random.uniform(self._min_delay, self._max_delay))
        if random.random() < self._success_rate:
            return PaymentStatus.SUCCEEDED
        return PaymentStatus.FAILED
