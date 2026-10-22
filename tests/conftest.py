"""Isolate integration-test data while preserving the migrated model catalog.

The Compose test-db is shared by the suite and survives multiple test runs.
Clean only mutable application tables; never truncate the model catalog or
run this fixture against the normal application database.
"""

import os
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
from sqlalchemy import text

from app.db import engine

_TABLES = (
    "outbox_events",
    "provider_attempts",
    "generation_tasks",
    "ledger_entries",
    "payments",
    "users",
)


@pytest_asyncio.fixture(autouse=True)
async def isolate_postgres_test_data() -> AsyncIterator[None]:
    if os.getenv("RUN_DB_TESTS") != "1":
        yield
        return

    database_url = str(engine.url)
    if "/integer_test" not in database_url:
        pytest.fail("Refusing to truncate non-test PostgreSQL database")

    try:
        async with engine.begin() as connection:
            await connection.execute(
                text(f"TRUNCATE TABLE {', '.join(_TABLES)} RESTART IDENTITY CASCADE")
            )
        yield
    finally:
        # pytest-asyncio may create a new event loop for every test, including
        # synchronous tests that only execute this autouse fixture. Pooled
        # asyncpg connections belong to their original loop and must be
        # disposed before that loop closes.
        await engine.dispose()
