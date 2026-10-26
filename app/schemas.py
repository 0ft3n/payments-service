import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, HttpUrl

from app.models import Currency, PaymentStatus


class PaymentCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    amount: Decimal = Field(gt=0, max_digits=18, decimal_places=2)
    currency: Currency
    description: str = Field(default="", max_length=500)
    metadata: dict[str, Any] = Field(default_factory=dict)
    webhook_url: HttpUrl = Field(max_length=2048)


class PaymentAccepted(BaseModel):
    payment_id: uuid.UUID
    status: PaymentStatus
    created_at: datetime


class PaymentRead(BaseModel):
    payment_id: uuid.UUID
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any]
    status: PaymentStatus
    idempotency_key: str
    webhook_url: str
    created_at: datetime
    processed_at: datetime | None
    webhook_sent_at: datetime | None


class ErrorResponse(BaseModel):
    detail: str
