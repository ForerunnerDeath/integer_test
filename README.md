# Integer generation service

**Status: stage 1 only (infrastructure and schema). Not ready for submission.**

## Launch

```bash
cp .env.example .env
docker compose up --build
```

In PowerShell replace copy command with `Copy-Item .env.example .env`.

Health endpoint: http://localhost:8000/health
Docs: http://localhost:8000/docs
Postgres: localhost:55432 (user/password/database: integer).

Compose starts PostgreSQL, Alembic migration, API with **four Uvicorn processes**, background worker scaffold and two provider scaffolds. API generation, callback handling and actual worker behavior are not yet implemented.

## Migrations

`docker compose run --rm migrate alembic upgrade head` applies the initial migration and seeds:
- `image-fast`, price 2, alpha(max 4) → beta(max 10).
- `video-long`, price 25, beta(max 2) → alpha(max 1, skip_on_moderation).

For subsequent revisions use `alembic revision -m "describe change"` and implement `upgrade()` / `downgrade()` before applying. The current first migration deliberately constructs the declarative schema. Future revisions should use explicit Alembic operations.

## Planned guarantees

Token movements will be serialized in PostgreSQL and represented by unique ledger operation keys, with balance update and ledger insert in the same transaction. Provider callbacks and timeout worker will serialize changes by task-row locks. Late results for expired attempts will be ignored. Outbox dispatch is designed to survive worker restarts. **These guarantees are not implemented yet.**

## Stages

1. Scaffold/schema (current).
2. Payment webhook and ledger.
3. Generation and balance API.
4. Provider routing and callbacks.
5. Watchdog and recovery.
6. Concurrency integration tests.
7. Review and documentation.

Run local tests after `pip install -e ".[dev]"` with `pytest`.
