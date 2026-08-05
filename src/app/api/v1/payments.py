from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends, Header, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.dependencies import get_db_session, verify_api_key
from app.repositories.payment_repository import PaymentRepository
from app.schemas.payment import PaymentAccepted, PaymentCreate, PaymentDetail
from app.services.exceptions import IdempotencyConflictError
from app.services.payment_service import create_payment

router = APIRouter(
    prefix="/api/v1/payments",
    tags=["payments"],
    dependencies=[Depends(verify_api_key)],
)


@router.post("", response_model=PaymentAccepted, status_code=status.HTTP_202_ACCEPTED)
async def create_payment_endpoint(
    data: PaymentCreate,
    # без default: отсутствие заголовка должно быть 422, а не "как будто без идемпотентности"
    idempotency_key: str = Header(..., alias="Idempotency-Key"),
    session: AsyncSession = Depends(get_db_session),
) -> PaymentAccepted:
    try:
        payment = await create_payment(session, idempotency_key, data)
    except IdempotencyConflictError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Idempotency-Key already used with a different request body",
        ) from exc
    return PaymentAccepted.model_validate(payment)


@router.get("/{payment_id}", response_model=PaymentDetail)
async def get_payment(
    payment_id: uuid.UUID,
    session: AsyncSession = Depends(get_db_session),
) -> PaymentDetail:
    payment = await PaymentRepository(session).get_by_id(payment_id)
    if payment is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Payment not found")
    return PaymentDetail.model_validate(payment)
