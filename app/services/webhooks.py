from typing import Any

import httpx

from app.models import Payment


class WebhookDeliveryError(Exception):
    pass


def build_payload(payment: Payment) -> dict[str, Any]:
    return {
        "event": f"payment.{payment.status.value}",
        "payment_id": str(payment.id),
        "status": payment.status.value,
        "amount": str(payment.amount),
        "currency": payment.currency.value,
        "description": payment.description,
        "metadata": payment.metadata_,
        "created_at": payment.created_at.isoformat(),
        "processed_at": payment.processed_at.isoformat() if payment.processed_at else None,
    }


class WebhookSender:
    def __init__(self, client: httpx.AsyncClient) -> None:
        self._client = client

    async def send(self, payment: Payment) -> None:
        try:
            response = await self._client.post(
                payment.webhook_url,
                json=build_payload(payment),
                # Lets the receiver deduplicate: delivery is at-least-once.
                headers={"X-Webhook-Id": f"{payment.id}:{payment.status.value}"},
            )
        except httpx.HTTPError as exc:
            raise WebhookDeliveryError(f"{type(exc).__name__}: {exc}") from exc
        if not response.is_success:
            raise WebhookDeliveryError(f"Webhook endpoint answered {response.status_code}")
