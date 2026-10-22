# Integer generation service

**Status: stage 4 — provider simulator, signed callbacks, failover, and outbox worker added. Watchdog/reconciliation remain unfinished. Not ready for submission.**

## Quick start

```bash
cp .env.example .env
docker compose up --build -d
docker compose run --build --rm tests
```

In PowerShell use `Copy-Item .env.example .env` instead of `cp`.
The Docker test service runs integration tests against an **isolated ephemeral PostgreSQL container** (`test-db`), not the development database, and sets `RUN_DB_TESTS=1`. The `test-migrate` service applies the complete migration chain to this fresh database. The test database is discarded when the test container is removed; it is not persistent.
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

The shared `app/ledger.py` module enforces signed movement direction, non-negative balance, and PostgreSQL BIGINT bounds. Debit/refund flows must call this service inside their own transaction with a locked user. The payment, balance and ledger commit or roll back **together**. If a process crashes before commit, PostgreSQL rolls back all changes; a retry may complete. If it crashes after commit but before replying, a retry sees the existing payment and does not credit again. Different simultaneous payments for the same user are serialized, so none can overwrite another's balance. The full set of future balance debit/refund flows must use the same locking convention.

## Migrations and seed data

`docker compose run --rm migrate alembic upgrade head` applies the initial migration, including:
- `image-fast`, price 2, alpha(max 4) → beta(max 10).
- `video-long`, price 25, beta(max 2) → alpha(max 1, skip_on_moderation).

For a new schema change, create `alembic revision -m "reason"`, implement explicit `upgrade()` and `downgrade()` operations, and run `alembic upgrade head`. The first migration uses explicit Alembic operations so later ORM edits cannot change historical migrations. It preserves the initial schema and revision ID; databases already migrated at that revision need no schema changes. Subsequent schema changes must be new revisions; never edit historical migrations after the assignment is deployed.

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
2. Signed payment webhook and concurrency-safe ledger (**done**).\n2.5. Frozen migration, shared ledger and isolated DB tests (**done; 10 integration tests passed**).
3. Generation API and transactional debit (**implemented; awaiting integration verification**).
4. Provider routing and callbacks (**implemented; local verification required**).
5. Watchdog, timeout recovery, and once-only refunds.
6. Comprehensive multi-worker race/crash tests.
7. Code review and documentation.

Provider result ingestion, timeout recovery and refunds are not implemented yet. Their guarantees will be documented after implementation.

### Stage 2.5 verification

Run `docker compose run --build --rm tests` to recreate the test database, run migrations from zero and execute integration tests. Validate with Ruff and Pyright before treating this stage as finished. The read endpoint currently performs separate balance/ledger queries; consistent read snapshots and true process-kill tests remain future hardening items.

## Generation admission (stage 3)

`POST /generate` accepts `{"user_id":"<UUID>","model_slug":"image-fast","prompt":"a cat","ref_images":[]}` and returns HTTP 202 with task ID, initial status, selected provider, price and resulting balance. `GET /tasks/{task_id}` returns status, current leg and ordered attempt history.

The service first validates the model and finds the first eligible provider leg in priority order; no matching leg returns 400 without debiting. It then locks the user's row, counts active (pending/processing) tasks and enforces a maximum of three (429), then debits the model price (402 when insufficient funds). In **one PostgreSQL transaction** it commits the debit journal entry, generation task, first provider attempt and outbox dispatch event. Nothing is sent over the network while holding the transaction. Concurrent submissions for one user serialize on that user's row lock. A missing user is treated as insufficient funds (402), since no registration endpoint is in scope.

**Important:** the outbox worker, fake provider HTTP requests, provider webhooks and terminal refunds are now implemented. The watchdog, stuck-event recovery policies and crash/retry verification are not yet complete. `POST /generate` has no client-provided idempotency key in the assignment; retries intentionally create new tasks and spend again if otherwise eligible.

New PostgreSQL tests validate initial task/attempt/outbox creation, eligible leg selection and rejection, 10 simultaneous submissions respecting the active-task limit, five concurrent submissions with funds for just one, and no debit on rejected generation.


## Provider lifecycle (stage 4; pending local integration verification)

The worker claims ready outbox events with PostgreSQL `FOR UPDATE SKIP LOCKED` and a lease, then submits `POST /tasks` to Alpha or Beta. A stable attempt UUID is sent as the provider's external task ID; the simulator treats this identifier as its idempotency key. The worker **does not hold task-row locks while making HTTP requests**.

Fake provider services read these optional environment variables (configure under each provider's Compose `environment`): `FAKE_PROVIDER_MODE` (`success`, `technical`, `moderation`, `payload`, `duplicate`, `no_webhook`, `late_success`, `http_500`, `http_503`, `http_400`, `http_403`, or `timeout`) and `FAKE_PROVIDER_DELAY` (default two seconds). They accept `POST /tasks`, return HTTP 202 with the stable `external_id`, and send a signed callback to `/webhooks/providers/{name}`.

Provider callbacks sign the **unmodified JSON bytes** with `PROVIDER_WEBHOOK_SECRET` and supply a hex HMAC in `X-Signature`. Completed callbacks persist their result URL. Technical failures advance to the next suitable leg; moderation failures skip legs marked `skip_on_moderation`; payload errors terminate immediately. Once all available legs fail, the task transitions to failed and the shared ledger refunds its price within the same transaction. Duplicate, superseded and late callbacks cannot change a terminal task or grant a second refund. The provider simulator and integration tests use this agreed callback schema; it is not a public external API standard.

The new integration tests cover successful delivery with simultaneous duplicate callbacks, technical failover to the next provider with ignored late results, and ten simultaneous final-failure callbacks resulting in exactly one refund. **These new tests and the linters still need to run in the user's Docker/local environment.**

Caveats: the worker uses a leased outbox event, but dispatch-after-crash may be attempted again, requiring provider-side idempotency. The fake provider uses FastAPI BackgroundTasks, not durable job storage, and its scheduled callback can disappear on a provider process restart. The stage-five watchdog will resolve attempts that have received no callback, along with late results after timeout. The initial lease timeout and dispatch retry behavior will also be hardened and tested there.
