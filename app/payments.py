"""Idempotent payment callback processing with an atomic balance ledger."""

import hashlib
import uuid
from typing import Literal

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field, ValidationError
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.config import settings
from app.db import session_factory
from app.ledger import BalanceOverflow, locked_user, record_movement
from app.models import LedgerKind, Payment, User
from app.security import verify_webhook_signature

router = APIRouter(tags=["payments"])

# Only final successful payments credit tokens. Non-final events can later be
# followed by a successful callback with the same payment ID.
SUCCESS_STATUSES = frozenset({"completed", "paid", "success", "succeeded"})
PaymentOutcome = Literal["credited", "duplicate", "ignored"]


class PaymentWebhook(BaseModel):
    payment_id: str = Field(min_length=1, max_length=152)
    user_id: uuid.UUID
    tokens: int = Field(gt=0, lt=2**63)
    status: str = Field(min_length=1, max_length=30)


class PaymentWebhookResponse(BaseModel):
    status: PaymentOutcome


async def credit_payment(event: PaymentWebhook) -> PaymentOutcome:
    if event.status.lower() not in SUCCESS_STATUSES:
        return "ignored"

    # A SHA256 digest keeps the unique ledger key bounded even for long IDs.
    operation_key = f"payment:{hashlib.sha256(event.payment_id.encode()).hexdigest()}"

    async with session_factory() as session:
        async with session.begin():
            # No registration endpoint is required for this assignment.
            # INSERT ... ON CONFLICT lets several concurrent callbacks create
            # the same user without throwing unique-constraint exceptions.
            await session.execute(
                pg_insert(User)
                .values(id=event.user_id, balance=0)
                .on_conflict_do_nothing(index_elements=[User.id])
            )

            # Serialize *all* movements of the same user's balance before
            # changing the cached balance and appending its ledger entry.
            user = await locked_user(session, event.user_id)

            # The unique payment ID is the idempotency guard, even when
            # callbacks race from different API worker processes.
            inserted_id = await session.scalar(
                pg_insert(Payment)
                .values(
                    payment_id=event.payment_id,
                    user_id=event.user_id,
                    tokens=event.tokens,
                    status=event.status.lower(),
                )
                .on_conflict_do_nothing(index_elements=[Payment.payment_id])
                .returning(Payment.payment_id)
            )
            if inserted_id is None:
                previous = await session.get(Payment, event.payment_id)
                if previous is None:
                    raise RuntimeError("Duplicate payment could not be loaded")
                if previous.user_id != event.user_id or previous.tokens != event.tokens:
                    raise HTTPException(
                        status_code=409,
                        detail="Payment ID was previously used for different details",
                    )
                return "duplicate"

            try:
                await record_movement(
                    session, user,
                    operation_key=operation_key,
                    kind=LedgerKind.credit,
                    amount=event.tokens,
                )
            except BalanceOverflow as exc:
                raise HTTPException(status_code=409, detail=str(exc)) from exc

    return "credited"


@router.post("/webhooks/payments", response_model=PaymentWebhookResponse)
async def payment_webhook(request: Request) -> PaymentWebhookResponse:
    raw_body = await request.body()
    verify_webhook_signature(
        raw_body,
        request.headers.get("X-Signature"),
        settings.payment_webhook_secret,
    )

    try:
        payment = PaymentWebhook.model_validate_json(raw_body)
    except ValidationError as exc:
        raise HTTPException(status_code=422, detail="Invalid payment webhook payload") from exc

    return PaymentWebhookResponse(status=await credit_payment(payment))
