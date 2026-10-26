import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, Response, status

from app.api.deps import SessionDep, verify_api_key
from app.models import Payment
from app.schemas import ErrorResponse, PaymentAccepted, PaymentCreate, PaymentRead
from app.services import payments as payment_service

router = APIRouter(
    prefix="/api/v1/payments",
    tags=["payments"],
    dependencies=[Depends(verify_api_key)],
    responses={401: {"model": ErrorResponse}},
)


def to_read_model(payment: Payment) -> PaymentRead:
    return PaymentRead(
        payment_id=payment.id,
        amount=payment.amount,
        currency=payment.currency,
        description=payment.description,
        metadata=payment.metadata_,
        status=payment.status,
        idempotency_key=payment.idempotency_key,
        webhook_url=payment.webhook_url,
        created_at=payment.created_at,
        processed_at=payment.processed_at,
        webhook_sent_at=payment.webhook_sent_at,
    )


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=PaymentAccepted,
    responses={409: {"model": ErrorResponse}},
)
async def create_payment(
    data: PaymentCreate,
    session: SessionDep,
    response: Response,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key", min_length=1, max_length=255)],
) -> PaymentAccepted:
    try:
        payment, created = await payment_service.create_payment(session, data, idempotency_key)
    except payment_service.IdempotencyConflictError:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Idempotency-Key was already used with a different request body",
        ) from None

    if not created:
        response.headers["Idempotent-Replayed"] = "true"
    return PaymentAccepted(
        payment_id=payment.id, status=payment.status, created_at=payment.created_at
    )


@router.get(
    "/{payment_id}",
    response_model=PaymentRead,
    responses={404: {"model": ErrorResponse}},
)
async def get_payment(payment_id: uuid.UUID, session: SessionDep) -> PaymentRead:
    payment = await payment_service.get_payment(session, payment_id)
    if payment is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Payment not found")
    return to_read_model(payment)
