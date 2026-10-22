"""Shared atomic token ledger operations.

Call inside session.begin(); all callers use the same user-row locking order.
"""

import uuid

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import LedgerEntry, LedgerKind, User

MAX_BALANCE = 2**63 - 1


class InsufficientTokens(Exception):
    pass


class BalanceOverflow(Exception):
    pass


async def locked_user(session: AsyncSession, user_id: uuid.UUID) -> User:
    user = await session.scalar(select(User).where(User.id == user_id).with_for_update())
    if user is None:
        raise LookupError("User does not exist")
    return user


async def record_movement(
    session: AsyncSession,
    user: User,
    *,
    operation_key: str,
    kind: LedgerKind,
    amount: int,
) -> LedgerEntry:
    """Update cached balance and append ledger in the caller's transaction.

    The caller must have locked the user's row using locked_user() first.
    A conflicting operation key raises an integrity error, rolling back the
    enclosing transaction; an earlier operation must never be counted twice.
    """
    if amount <= 0:
        raise ValueError("Amount must be positive")
    if kind == LedgerKind.debit:
        delta = -amount
    elif kind in (LedgerKind.credit, LedgerKind.refund):
        delta = amount
    else:
        raise ValueError("Unsupported ledger operation kind")
    new_balance = user.balance + delta
    if new_balance < 0:
        raise InsufficientTokens("Insufficient tokens")
    if new_balance > MAX_BALANCE:
        raise BalanceOverflow("Token balance exceeds PostgreSQL BIGINT")
    user.balance = new_balance
    entry = LedgerEntry(
        user_id=user.id,
        operation_key=operation_key,
        kind=kind,
        delta=delta,
        balance_after=new_balance,
    )
    session.add(entry)
    await session.flush()
    return entry
