"""Database-driven timeout recovery for provider attempts.

Only the current attempt of a nonterminal generation is eligible. State changes
are serialized in transition_attempt() alongside real provider callbacks.
"""

import logging

from sqlalchemy import func, select

from app.db import session_factory
from app.models import AttemptStatus, GenerationTask, ProviderAttempt, TaskStatus
from app.provider_flow import ProviderWebhook, transition_attempt

log = logging.getLogger(__name__)


async def watchdog_once(batch_size: int = 50) -> int:
    """Expire overdue attempts and return the number of applied transitions."""
    if batch_size < 1:
        raise ValueError("batch_size must be positive")

    async with session_factory() as session:
        overdue = (
            await session.execute(
                select(
                    ProviderAttempt.id,
                    ProviderAttempt.task_id,
                    ProviderAttempt.provider,
                    ProviderAttempt.external_id,
                )
                .join(GenerationTask, GenerationTask.id == ProviderAttempt.task_id)
                .where(
                    GenerationTask.current_attempt_id == ProviderAttempt.id,
                    GenerationTask.status.in_((TaskStatus.pending, TaskStatus.processing)),
                    ProviderAttempt.status.in_((AttemptStatus.sending, AttemptStatus.waiting)),
                    ProviderAttempt.deadline_at <= func.now(),
                )
                .order_by(ProviderAttempt.deadline_at, ProviderAttempt.id)
                .limit(batch_size)
            )
        ).all()

    changed = 0
    for attempt_id, task_id, provider, external_id in overdue:
        try:
            outcome = await transition_attempt(
                ProviderWebhook(
                    task_id=task_id,
                    attempt_id=attempt_id,
                    external_id=external_id or str(attempt_id),
                    status="failed",
                    error_type="timeout",
                ),
                provider,
                timeout_only=True,
            )
        except Exception:
            log.exception("Could not expire provider attempt %s", attempt_id)
        else:
            if outcome != "ignored":
                changed += 1

    return changed
