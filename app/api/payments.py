import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, status

from app import payments
from app.api.deps import SessionDep, require_api_key
from app.schemas import PaymentAccepted, PaymentCreate, PaymentRead

router = APIRouter(prefix="/api/v1/payments", tags=["payments"], dependencies=[Depends(require_api_key)])

IdempotencyKey = Annotated[str, Header(alias="Idempotency-Key", min_length=1, max_length=255)]


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=PaymentAccepted,
    responses={409: {"description": "Idempotency-Key was already used with a different request body"}},
)
async def create_payment(body: PaymentCreate, idempotency_key: IdempotencyKey, session: SessionDep) -> PaymentAccepted:
    try:
        payment = await payments.create_payment(session, body, idempotency_key)
    except payments.IdempotencyConflict:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Idempotency-Key was already used with a different request body",
        ) from None
    return PaymentAccepted.from_model(payment)


@router.get("/{payment_id}", response_model=PaymentRead, responses={404: {"description": "Payment not found"}})
async def get_payment(payment_id: uuid.UUID, session: SessionDep) -> PaymentRead:
    payment = await payments.get_payment(session, payment_id)
    if payment is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Payment not found")
    return PaymentRead.from_model(payment)
