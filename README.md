# Integer generation service

**Status: stage 2 — payment ledger and webhook completed in code; generation and provider orchestration are not yet implemented. Not ready for submission.**

## Quick start

```bash
cp .env.example .env
docker compose up --build -d
docker compose run --build --rm tests
```

In PowerShell use `Copy-Item .env.example .env` instead of `cp`.
The Docker test service runs integration tests against **real PostgreSQL**, not a fake store, and sets `RUN_DB_TESTS=1`.
Without that flag, host-side `pytest` skips DB-dependent integration tests; pure API and signature tests still run.

- API: http://localhost:8000/docs
- Health: http://localhost:8000/health
- PostgreSQL from host: `localhost:55432` (user/password/database: `integer`)
- Four API worker processes are configured via Uvicorn.
- The separate background worker and Alpha/Beta services are still stage-1 scaffolds.

## Payment webhook — implemented

`POST /webhooks/payments` accepts a JSON body of the form:

```json
{"payment_id": "external-123", "user_id": "11111111-1111-1111-1111-111111111111", "tokens": 25, "status": "completed"}
```

`X-Signature` must be a **hex-encoded HMAC-SHA256 of the exact raw request body**, using `PAYMENT_WEBHOOK_SECRET` (the development default is in `.env.example`). Never sign a re-serialized JSON object instead of its original bytes.

Accepted successful statuses: `completed`, `paid`, `success`, `succeeded` (case-insensitive). Other statuses return `{"status":"ignored"}` without recording a payment, so e.g. a `pending` event can later become `completed`. This is an explicit convention because the assignment did not define the payment status vocabulary.

Successful callbacks respond `{"status":"credited"}`; exact replays respond `{"status":"duplicate"}`. Reusing a payment ID with a different user or token amount returns HTTP 409. Missing/bad signatures return 401; malformed payloads return 422.

For this exercise the first successful payment **creates the user** if missing, since registration is out of scope. `GET /users/{uuid}/balance` returns the balance and 20 most recent journal operations; an unknown user returns 404.

### Why one credit happens even with 20 identical concurrent callbacks

The API verifies the signature before interpreting the message. A single PostgreSQL transaction:

1. Inserts the user if missing (`ON CONFLICT DO NOTHING`).
2. Locks their row with `SELECT FOR UPDATE`, serializing competing changes of this user's balance.
3. Inserts the external payment with unique `payment_id` (`ON CONFLICT DO NOTHING RETURNING`).
4. **Only for the winner:** increases the user's cached balance and inserts one ledger entry with a deterministic unique operation key.

The payment, balance and ledger commit or roll back **together**. If a process crashes before commit, PostgreSQL rolls back all changes; a retry may complete. If it crashes after commit but before replying, a retry sees the existing payment and does not credit again. Different simultaneous payments for the same user are serialized, so none can overwrite another's balance. The full set of future balance debit/refund flows must use the same locking convention.

## Migrations and seed data

`docker compose run --rm migrate alembic upgrade head` applies the initial migration, including:
- `image-fast`, price 2, alpha(max 4) → beta(max 10).
- `video-long`, price 25, beta(max 2) → alpha(max 1, skip_on_moderation).

For a new schema change, create `alembic revision -m "reason"`, implement explicit `upgrade()` and `downgrade()` operations, and run `alembic upgrade head`. The first migration uses SQLAlchemy metadata schema creation; later migrations should use explicit Alembic operations.

## Tests and developer checks

```powershell
docker compose run --build --rm tests
.\.venv\Scripts\python.exe -m ruff check .
.\.venv\Scripts\python.exe -m ruff format --check .
.\.venv\Scripts\python.exe -m pyright
.\.venv\Scripts\python.exe -m pytest -q
```

The Docker integration tests cover 20 simultaneous identical payments, 12 different simultaneous payments to one user, duplicate-ID conflicts, invalid signatures, and `pending → completed` status transitions. The 10 simultaneous provider failure/refund test belongs to stage 5.

## Development stages

1. Scaffold/schema (**done**).
2. Signed payment webhook and concurrency-safe ledger (**implemented; awaiting local PostgreSQL verification**).
3. Generation API and transactional debit.
4. Provider routing and callbacks.
5. Watchdog, timeout recovery, and once-only refunds.
6. Comprehensive multi-worker race/crash tests.
7. Code review and documentation.

Provider result ingestion, timeout recovery and refunds are not implemented yet. Their guarantees will be documented after implementation.
