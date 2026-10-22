"""Read-only user balance and recent journal entries."""

import uuid
from datetime import datetime

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from sqlalchemy import select

from app.db import session_factory
from app.models import LedgerEntry, LedgerKind, User

router = APIRouter(tags=["balances"])


class LedgerEntryResponse(BaseModel):
    operation_key: str
    kind: LedgerKind
    delta: int
    balance_after: int
    created_at: datetime


class BalanceResponse(BaseModel):
    user_id: uuid.UUID
    balance: int
    operations: list[LedgerEntryResponse]


@router.get("/users/{user_id}/balance", response_model=BalanceResponse)
async def user_balance(user_id: uuid.UUID) -> BalanceResponse:
    async with session_factory() as session:
        user = await session.get(User, user_id)
        if user is None:
            raise HTTPException(status_code=404, detail="User not found")

        result = await session.scalars(
            select(LedgerEntry)
            .where(LedgerEntry.user_id == user_id)
            .order_by(LedgerEntry.created_at.desc(), LedgerEntry.id.desc())
            .limit(20)
        )
        operations = [
            LedgerEntryResponse(
                operation_key=entry.operation_key,
                kind=entry.kind,
                delta=entry.delta,
                balance_after=entry.balance_after,
                created_at=entry.created_at,
            )
            for entry in result
        ]
        return BalanceResponse(user_id=user_id, balance=user.balance, operations=operations)
