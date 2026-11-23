# Payment Processing System

[![CI](https://github.com/vbloher/payment-processing-system/actions/workflows/ci.yml/badge.svg)](https://github.com/vbloher/payment-processing-system/actions/workflows/ci.yml)
![Go](https://img.shields.io/badge/go-1.27-00ADD8?logo=go)

A small payment system in Go, built to be defended at a whiteboard: one money flow, an outbound payment from a
customer's balance through a partner bank, done in depth with every failure branch. The payment runs as a Temporal
saga across a double-entry ledger and a mock bank, facts flow through a transactional outbox to NATS JetStream and an
inbox-backed notifier, and a scheduled reconciliation finds drift and resolves stuck payments. Every pattern here has
a test that shows the failure it protects against.

```
client ─HTTP─▶ payment ══ Temporal saga ══▶ ledger   (gRPC: hold / capture / release)
                  │                    ╚══▶ mockbank (HTTP: transfer / cancel / statement)
                  │ outbox (same transaction as the status change)
                  ▼
           NATS JetStream ──▶ notification (inbox) ──▶ webhook / log
```

| Service        | Does                                             | Input                                    | Storage               |
|----------------|--------------------------------------------------|------------------------------------------|-----------------------|
| `payment`      | payment API, saga, outbox, reconciliation        | HTTP; Temporal worker; outbox worker     | database `payment`    |
| `ledger`       | double entry, holds, balances                    | gRPC                                     | database `ledger`     |
| `notification` | consumes events, delivers notifications          | JetStream consumer                       | database `notification` |
| `mockbank`     | a bank with deterministic, controllable failures | HTTP                                     | process memory        |

Database per service: three databases in one Postgres instance; no service reads another's database.

## Who does what

| Technology     | Its job                                                                                                                           | What it does not do                 |
|----------------|-----------------------------------------------------------------------------------------------------------------------------------|-------------------------------------|
| Temporal       | Commands the payment's outcome depends on: saga steps, retries, compensation, waiting for reconciliation. The reconciliation schedule. | Send notifications.                 |
| NATS JetStream | Facts after commit, for consumers the payment does not wait for.                                                                  | Start or advance a payment step.    |
| Outbox         | Atomic "status changed + event recorded" in `payment`.                                                                            |                                     |
| Inbox          | An exactly-once effect in `notification` under at-least-once delivery.                                                            |                                     |
| Reconciliation | The last line of defence: finds discrepancies between payments, the ledger and the bank, and resolves stuck payments.            |                                     |

The test for any new link between components: **would the payment change if the receiver were down?** Yes: it is a
Temporal step. No: it is an event on NATS.

## The payment saga

```
1. StartProcessing     created → processing
2. ledger.Hold         ── refused ─────────────────────────────▶ Fail      (no bank call)
3. bank.Transfer       ── declined ──▶ ledger.Release ─────────▶ Fail      (compensation)
      │ succeeded                          ▲
      │                                    │ declined / cancelled
      ├─ unknown ─▶ bank.Cancel ───────────┤        (succeeded → step 4)
      │                │ still unknown     │
      │                ▼                   │
      │           MarkReview ─▶ signal ────┘        (sent by reconciliation)
      ▼
4. ledger.Capture      unlimited retries: the pivot is behind us
5. Complete            → succeeded + outbox(payment.succeeded)

Fail                   → failed + outbox(payment.failed), with a reason
MarkReview             → review + outbox(payment.review_required)
```

The workflow is [`payment_workflow.go`](services/payment/internal/saga/payment_workflow.go); every step is idempotent,
and the operation key in the ledger and in the bank is the payment ID.

| From         | To                     | When                                                                                                                |
|--------------|------------------------|---------------------------------------------------------------------------------------------------------------------|
| `created`    | `processing`           | the workflow started                                                                                                |
| `processing` | `succeeded`            | the hold was captured                                                                                               |
| `processing` | `failed`               | the ledger refused the hold (`insufficient_funds`, `account_not_found`, `ledger_rejected` for another currency); the bank declined (`bank_declined`) or cancelled (`bank_cancelled`) the transfer |
| `processing` | `review`               | the bank's outcome is unknown                                                                                       |
| `review`     | `succeeded` / `failed` | the reconciliation signal                                                                                           |

A transition to the current status is a no-op that emits no second event (an activity retry). Any other transition
is a non-retryable error. The state machine lives in [`domain/status.go`](services/payment/internal/domain/status.go).

## Getting started

Requirements: Go 1.27, Docker with Compose v2.

```bash
make bin-deps   # protoc, golangci-lint and migrate into ./bin (for make lint and make generate)
make up         # builds the four services; starts Postgres, NATS and Temporal; applies migrations
make demo       # opens an account, deposits 100.00 EUR, pays 30.00 and waits for "succeeded"
make down       # stops everything and removes the volumes
```

| What                | Address                                                       |
|---------------------|---------------------------------------------------------------|
| Payment API         | http://localhost:8080/api/v1                                  |
| Ledger gRPC         | localhost:50051 (server reflection on, so `grpcurl` works)    |
| Mock bank           | http://localhost:8090/api/v1                                  |
| Temporal UI         | http://localhost:8233                                         |
| Temporal frontend   | localhost:7233                                                |
| NATS monitor        | http://localhost:8222 (streams: `/jsz?streams=true`)          |
| Postgres            | localhost:5433, user `login`, password `pass`                 |
| Probes and metrics  | `/live`, `/ready`, `/metrics` on each service's HTTP port: payment 8080, ledger 8081, notification 8082, mockbank 8090 |

Postgres is published on 5433, not 5432, because 5432 is often taken by a developer's own Postgres. No webhook URL is
configured in compose, so the notifier logs each notification: `docker compose logs -f notification`.

To run one service from source, stop its container and start it with `make run-<service>`. Each service reads a
`.env` file from its own directory, and the `.env.example` files point at the compose ports:

```bash
docker compose stop payment
cp services/payment/.env.example services/payment/.env
make run-payment
```

The containers call each other by compose service name, so a `ledger` or `mockbank` run from source is reachable only
by a `payment` that also runs from source.

## Trying the failure modes

The mock bank picks its behaviour from the beneficiary's prefix. These commands use `grpcurl`, `jq` and `uuidgen`.

```bash
# An account with 1000.00 EUR
ACCOUNT=$(grpcurl -plaintext -d '{"currency":"EUR"}' localhost:50051 ledger_v1.LedgerV1/OpenAccount | jq -r .id)
grpcurl -plaintext -d '{"account_id":"'$ACCOUNT'","amount":100000,"currency":"EUR","idempotency_key":"topup-'$ACCOUNT'"}' \
  localhost:50051 ledger_v1.LedgerV1/Deposit

# body <beneficiary>: a payment of 30.00 EUR; show <id>: the payment's status
body() { printf '{"account_id":"%s","amount":3000,"currency":"EUR","beneficiary":"%s"}' "$ACCOUNT" "$1"; }
show() { curl -s localhost:8080/api/v1/payments/$1 | jq -c '{status, failure_reason}'; }

DECLINE=$(curl -s --json "$(body DECLINE-1)" -H "Idempotency-Key: $(uuidgen)" localhost:8080/api/v1/payments | jq -r .id)
FLAKY=$(curl -s --json "$(body FLAKY-1)" -H "Idempotency-Key: $(uuidgen)" localhost:8080/api/v1/payments | jq -r .id)
TIMEOUT=$(curl -s --json "$(body TIMEOUT-1)" -H "Idempotency-Key: $(uuidgen)" localhost:8080/api/v1/payments | jq -r .id)
sleep 10; for id in $DECLINE $FLAKY $TIMEOUT; do show $id; done

# BLACKOUT turns the whole bank off, so it goes last
BLACKOUT=$(curl -s --json "$(body BLACKOUT-1)" -H "Idempotency-Key: $(uuidgen)" localhost:8080/api/v1/payments | jq -r .id)
sleep 10; show $BLACKOUT
grpcurl -plaintext -d '{"account_id":"'$ACCOUNT'"}' localhost:50051 ledger_v1.LedgerV1/GetBalance
```

| Beneficiary   | The mock bank                                               | The payment                                                      |
|---------------|-------------------------------------------------------------|------------------------------------------------------------------|
| `DECLINE-1`   | declines the transfer                                       | `failed`, `bank_declined`; the hold is released                  |
| `FLAKY-1`     | fails the first two attempts unprocessed, then succeeds     | `succeeded` on the third attempt; one transfer in the bank       |
| `TIMEOUT-1`   | processes the transfer, then answers every attempt with 504 | `succeeded`: the saga's `Cancel` returns the processed transfer  |
| `BLACKOUT-1`  | processes the transfer, then turns itself off               | `review`, 30.00 still held: `Transfer` and `Cancel` both failed  |

`GetBalance` then shows a balance of 94000 (940.00) with 3000 held: two payments captured, one released, one in
review. The Temporal UI shows each workflow's history, retries included.

The admin endpoints of the mock bank ignore its availability:

```bash
# Bank down before it sees the request: this payment also goes to review
curl -s -X PUT --json '{"available":false}' localhost:8090/api/v1/admin/availability
DOWN=$(curl -s --json "$(body DE-OK-1)" -H "Idempotency-Key: $(uuidgen)" localhost:8080/api/v1/payments | jq -r .id)
sleep 10; show $DOWN
curl -s -X PUT --json '{"available":true}' localhost:8090/api/v1/admin/availability

# A transfer the payment service never made, and a changed amount on a succeeded payment's transfer
curl -s --json '{"reference":"ROGUE-1","amount":500,"currency":"EUR","beneficiary":"X"}' localhost:8090/api/v1/admin/transfers
curl -s -X PATCH --json '{"amount":1}' localhost:8090/api/v1/admin/transfers/$FLAKY
```

Now run reconciliation. It runs every minute anyway (`RECONCILIATION_INTERVAL`); to run it now, open the Temporal UI,
go to **Schedules**, open `reconciliation` and choose **Schedule Actions → Trigger**, or from a shell:

```bash
docker compose exec temporal temporal schedule trigger --schedule-id reconciliation
sleep 5; show $BLACKOUT; show $DOWN
curl -s 'localhost:8080/api/v1/reconciliation/discrepancies?status=open' | jq -c '.discrepancies[] | {kind, reference}'
```

Reconciliation calls `Cancel` for both review payments and signals each workflow the bank's final answer: `BLACKOUT`
becomes `succeeded` (the bank had processed it), `DOWN` becomes `failed` with `bank_cancelled` and its hold is
released. It records `bank_unexpected` for `ROGUE-1` and `amount_mismatch` for the changed transfer, once each,
however many runs see them; setting the amount back resolves the second one on the next run.

## Testing

```bash
make test               # unit tests: race detector, shuffled order, no Docker
make integration-test   # everything above plus the integration and e2e tests (build tag integration): needs Docker
make lint               # golangci-lint
make generate           # regenerate protobuf, OpenAPI server and client code, and mocks
```

Unit tests cover the domain with table tests (payment state machine, double-entry invariant, reconciliation rules),
the use cases with mockery mocks, and both workflows in the Temporal SDK's test environment, one table row per branch
of the saga diagram. Integration tests run each service's adapters against real Postgres and NATS started by
testcontainers-go. The e2e suite in [`test/e2e`](test/e2e) starts Postgres, NATS and Temporal in containers, runs
all four services in the test process through their public `Run`, and drives them over HTTP and gRPC.

| #  | Scenario                                               | What it proves                                                          | Test                                                                     |
|----|--------------------------------------------------------|-------------------------------------------------------------------------|--------------------------------------------------------------------------|
| 1  | Successful payment                                     | the saga; postings balance; one notification                            | e2e `Test_Payment_Outcomes/success`, `Test_Notification_OnePerPayment`  |
| 2  | Insufficient funds                                     | refusal without a bank call                                             | e2e `Test_Payment_Outcomes/insufficient_funds`                           |
| 3  | Bank declines                                          | compensation: the hold is released, the balance restored                | e2e `Test_Payment_Outcomes/bank_decline`                                 |
| 4  | Transient bank errors, then success                    | retries; exactly one transfer in the bank                               | e2e `Test_Payment_Outcomes/transient_bank_errors`                        |
| 5  | Timeout after the bank processed the transfer          | unknown outcome: `Cancel` returns `succeeded`, the payment completes    | e2e `Test_Payment_Outcomes/timeout_after_processing`                     |
| 6  | Bank down before processing, then back                 | `review` with the hold kept; reconciliation gets `cancelled`: `failed`  | e2e `Test_Review_ResolvedByReconciliation`                               |
| 7  | Bank processed the transfer and went dark, then back   | `review`; reconciliation gets `succeeded`: `succeeded`                  | e2e `Test_Review_ResolvedByReconciliation`                               |
| 8  | A transfer request arrives after `Cancel`              | the bank fenced the reference: no money moves                           | mockbank `Test_CancelTransfer_FencesLateCreate`, `Test_HTTP_Cancel`      |
| 9  | Replay with the same key; same key, different body     | API idempotency, also under ten concurrent requests                     | e2e `Test_Payment_Idempotency`                                           |
| 10 | Parallel payments over the balance                     | locks: the balance never goes negative; the ledger stays balanced       | e2e `Test_Payment_ConcurrentOverdraft`, ledger `Test_Hold_Concurrent_NeverOverdraws` |
| 11 | An event delivered twice                               | inbox: one notification                                                 | notification `Test_Consumer_RedeliveryHasOneEffect`                      |
| 12 | Publishing to NATS fails                               | outbox: the event is not lost                                           | payment `Test_SendOutbox_PublishFailureKeepsEvents`                      |
| 13 | A rogue transfer and a changed amount in the bank      | reconciliation: each discrepancy recorded and announced once            | e2e `Test_Reconciliation_FindsBankDiscrepancies`                         |
| 14 | A payment stuck in `created`                           | reconciliation starts its workflow                                      | e2e `Test_Reconciliation_RestartsStuckCreated`                           |

CI ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)) runs lint, the unit tests with the race detector and a
`go mod tidy` check, a check that generated code is up to date, `make integration-test`, govulncheck, and a
multi-arch Docker build of each service. Dependabot keeps Go modules, actions and images up to date.

## Design decisions and trade-offs

**Orchestration, not choreography, for the payment.** A payment has a compensable step, a pivot, unknown outcomes
and an open-ended wait for reconciliation. As a Temporal workflow that logic is one function with durable state,
retry policies per step and a history that answers "where is payment X, and why". As choreography the same state
machine would be spread over event handlers in three services, each with its own retries, timeouts and correlation,
and "is anyone still working on this payment?" would have no single place to look. The price is a stateful dependency
to run and Temporal's rules for workflow code: determinism, and versioning, which this project leaves out.

**NATS carries facts, never commands.** Notifications fail the test "would the payment change if the receiver were
down?", so they travel over NATS after commit, and nothing in the saga waits for or reacts to an event. If an event
advanced a step, the broker's availability and the consumer's lag would sit on the money path, and the payment's
state would be split between Temporal's history and a stream: two systems doing one job.

**Outbox, not dual write.** A status change and its event are written in one Postgres transaction
([`transition.go`](services/payment/internal/usecase/transition.go)). Writing the row and publishing to NATS as two
steps either loses the event when the process dies in between or announces a change that then rolls back. The relay
takes a batch with `FOR UPDATE SKIP LOCKED`, deletes it and publishes it in the same transaction: if NATS fails, the
rollback puts the events back, and replicas skip each other's rows. One window is left: published, then the commit
fails. That gives a duplicate, which JetStream drops by `Nats-Msg-Id` (the event ID) within its 5-minute duplicate
window and the notifier's inbox drops after that. The costs are up to `OUTBOX_INTERVAL` (1s) of latency and a
transaction held open while a batch is published.

**An inbox, plus an idempotency key for the effect outside the database.** JetStream delivers at least once. An
idempotent consumer suffices when the effect is naturally idempotent, like an upsert by a business key; the notifier's
effect is a new row and an HTTP call. So it records the event ID in `inbox` and creates the notification in one
transaction, and a redelivered event hits the primary key and creates nothing. The webhook cannot join that
transaction, so delivery is at-least-once with `Idempotency-Key: <event_id>`: a recorded but unsent notification is
sent again on redelivery, a sent one never. The consumer pulls one message at a time, so a prefetched message cannot
spend its deliveries waiting in a buffer, and it gives the handler four fifths of `AckWait`, keeping the rest to
settle the message before the broker redelivers it. A message that cannot be decoded, or fails its last delivery, is
copied to `EVENTS_DLQ` on a fresh context and terminated.

**Pessimistic row locks in the ledger, always in one order.** Every money operation locks with `SELECT … FOR UPDATE`
in the same order: the customer account, then the hold, then `bank_cash`. With one global order transactions can wait
for each other but cannot deadlock; `Capture` and `Release` first read the hold without a lock only to learn which
account to lock first. Under the account lock a hold is placed only if `balance − held` covers it, and a
`CHECK (balance - held >= 0)` on customer accounts backs that up in the schema: ten parallel payments of 30.00 against
100.00 give exactly three successes. Optimistic locking or `SERIALIZABLE` would also be correct, but they turn
contention on a busy account into aborts and retry loops; with row locks contention is only waiting. The cost is that
`bank_cash` is one row per currency, so every capture and deposit in a currency queues on it; at scale it would be
split into several accounts or posted to in batches.

**Compensable, pivot and retriable steps.** `Hold` is compensable: `Release` undoes it. The bank transfer is the
pivot: once the bank has moved money, nothing in scope can undo it. `Capture` and `Complete` come after the pivot
and only go forward. The retry policies follow from that. Bank calls get a few attempts with backoff
(`SAGA_BANK_MAX_ATTEMPTS`, 3), so an unknown outcome reaches `Cancel` and review quickly. Calls to our own ledger and
database retry without limit: before the pivot waiting is harmless, after it giving up is not an option. Business
refusals (insufficient funds, a declined transfer) are return values, not errors, so nothing retries them. Errors no
retry can fix (a missing payment, an impossible transition, a hold in a conflicting state) are non-retryable `Fatal`
failures. After a successful `Hold`, a `Fatal` from `SendTransfer`, `Capture` or `Complete` means an invariant is
broken: the workflow fails loudly, visible in Temporal, without compensating, and reconciliation reports the payment
as `stuck`. It is deliberately not marked `review`: review means "the bank's outcome is unknown, waiting for a
signal", and that is not what happened. Such a payment needs a manual repair, which is out of scope.

**Never compensate on an unknown outcome; "not found" is not a refusal.** `Release` runs only after a definitive
`declined` or `cancelled`. A timeout, a 5xx, a `409`, a dropped connection or a status the adapter does not recognise
all mean "outcome unknown" ([`bank.go`](services/payment/internal/adapter/bank/bank.go)); the saga then asks the bank
to cancel, and if that is unknown too it marks the payment `review`, keeps the hold and waits. A `400` is a refusal:
the bank validates a request before it looks at any stored transfer, so nothing was processed. A `409` is not: a
transfer already exists under the payment's reference with another body and may have moved money, so `Cancel` fetches
its final state, and a different amount surfaces in reconciliation as `amount_mismatch`. Releasing on a guess is how
money is spent twice: the customer spends the released funds while the transfer the bank did receive goes out. For the
same reason the absence of a transfer proves nothing: a request that timed out on our side may still be queued at the
bank, and "not found" at 12:00 says nothing about 12:01. Reconciliation follows the same rule: it settles a `review`
payment only through `Cancel`, never from a missing statement line.

**What `Cancel` fencing assumes about the bank.** `Cancel(reference)` is idempotent and returns the transfer's final
state: `succeeded` or `declined` when the bank has processed the request, otherwise `cancelled`, after which the bank
fences the reference, so a late transfer request with that key moves no money and answers `cancelled`. One call turns
an unknown outcome into a definitive one, but it assumes a lot: that the bank deduplicates our reference for longer
than any request can be in flight, serialises cancel and create on one reference, and accepts a cancel for a transfer
it has never seen. Many banks offer none of that. Without it the only safe path is the slow one: keep the hold, leave
the payment in review, and conclude only from positive evidence (the transfer showing up in a lookup or a statement,
or the bank's guarantee that the request can no longer execute, such as an expiry or a cut-off), with an operator for
whatever remains.

**Payments and the ledger are separate services; the case for one database.** The ledger owns the money invariants
(double entry, holds, balances); payments own the API, the status machine and the saga. Separate, the ledger is a real
saga participant with a real compensation, and other flows (deposits today, pay-ins later) can post to it without
going through payments. The price is atomicity: a payment's status and its postings no longer change in one
transaction, which is why every step is idempotent and reconciliation compares holds with statuses
(`ledger_mismatch`). The case for one service on one database is strong: status and postings in one transaction, no
saga leg for the ledger, less code; don't distribute what has to be consistent. For one team, a modular monolith on
one database is the better default, split when ownership or load demands it. This project splits them because it
exists to show a saga across services.

**Idempotency lives in Postgres, not Redis.** The `Idempotency-Key` is a unique column of the payment row, inserted
with `ON CONFLICT DO NOTHING` together with a SHA-256 of the request: the key and the payment exist together or not
at all. A replay with the same body returns the same payment, the same key with another body gets `409`, and ten
concurrent requests with one key produce one payment, so one workflow, whose ID is derived from the payment's. Redis
`SET NX`, as in the template, either fails open when Redis is down or takes the API down with it, is not atomic with
the database write (the key can exist without a payment, or expire and admit a second one), and can be evicted. The
steps behind the API are idempotent the same way: the bank keys transfers by payment ID; the ledger keys holds by
payment ID and deposits by their key, answers a replay with a different payload with `AlreadyExists` instead of
applying it, and accepts holds and deposits only on customer accounts, so nobody can post against `bank_cash`
directly.

**Reconciliation is the last line of defence: windows select, point lookups decide.** A Temporal Schedule starts
`ReconciliationWorkflow` every `RECONCILIATION_INTERVAL` with overlapping runs skipped, which gives one run however many
replicas there are, without a distributed lock. A run compares payments, the bank and the ledger's holds. The time
window (`RECONCILIATION_LOOKBACK`, 24h, ending `RECONCILIATION_LAG`, 30s, ago to leave in-flight payments alone) only
selects candidates: payments updated in it, payments named in the bank's statement, and payments with open
discrepancies. The verdict on each candidate comes from point lookups (the bank's transfer by reference, the ledger's
hold by payment ID), because a record just outside a window would otherwise look missing. A bank transfer that matches
no payment is likewise re-checked by its own lookup on later runs instead of counting as resolved once it leaves the
statement window. All lookups finish before the first discrepancy is written, so a failed lookup fails the run, which
is retried and then replaced by the next scheduled one, and never becomes a discrepancy. A discrepancy is stored once
(a unique index on open `(reference, kind)`), announced once through the outbox, and resolved by the first run that no
longer reproduces it. Reconciliation repairs only two cases, a payment in `review` (it calls `Cancel` and signals the
bank's answer to the workflow) and a payment stuck in `created`; everything else it reports.

**Insert, then start; how a stuck `created` payment recovers.** `POST /payments` inserts the payment as `created`,
then starts the workflow `payment-<id>`, rejecting a duplicate ID and treating "already started" as success. The two
writes cannot share a transaction, so their order decides the failure mode: starting first could leave a workflow for
a payment that does not exist; inserting first leaves, at worst, a `created` row without a workflow. That row recovers
in two ways: the client's retry with the same key finds it and starts the workflow, and reconciliation starts every
payment that has stayed `created` longer than `RECONCILIATION_STUCK_AFTER` (5m). Starting is idempotent, so both can
happen.

### Known limitations

- If publishing a dead letter keeps failing through the message's last delivery, the message stays unacknowledged in
  `EVENTS` with no deliveries left, and a log line is its only trace.
- The reconciliation schedule is created once. Changing `RECONCILIATION_INTERVAL` later has no effect until the
  schedule is updated in Temporal, or deleted (`temporal schedule delete --schedule-id reconciliation`) for the
  service to recreate on start. Temporal aligns interval schedules to the Unix epoch, so `1h` fires on the hour.
- The inbox and the discrepancy table have no retention job.
- Workflows are not versioned: changing `PaymentWorkflow` while payments are in flight would break their replay.
- The mock bank keeps its state in memory; restarting it forgets every transfer.

## Project layout

```
api/                       contracts: OpenAPI (payment, mockbank), protobuf (ledger)
gen/                       generated code only
pkg/                       infrastructure and contracts, no domain code
  events/                  event envelope, types, subjects, stream configs: the contract between services
  metrics/                 Prometheus metrics on an injected registry
  natsjs/ temporalclient/  NATS JetStream and Temporal clients
  testinfra/               testcontainers helpers: Postgres, NATS, Temporal, migrations
  httpserver/ logger/ otel/ postgres/ render/ router/ transaction/   from the template
services/<name>/
  <name>.go                public Run(ctx, config.Config) error
  cmd/app/                 entrypoint: config, logger and OpenTelemetry, then the app
  config/                  environment configuration
  internal/app/            composition root
  internal/{domain,dto,usecase,adapter,controller}/
  migration/postgres/      SQL migrations (none in mockbank, which keeps state in memory)
  test/                    service-level tests
services/payment/internal/saga/   the workflows: the only business code that imports the Temporal SDK
test/e2e/                  whole-system tests
deploy/postgres/init.sql   creates the three databases for docker compose
```

Each service follows [go-clean-architecture-template](https://github.com/vbloher/go-clean-architecture-template):
layers `domain / dto / usecase / adapter / controller`, ports declared by the use case, a transaction manager that
carries the transaction in the context, pgx and goqu, zerolog, contract-first HTTP with the oapi-codegen strict server,
gRPC with reflection, mockery mocks, golang-migrate, and a graceful shutdown that stops controllers before adapters.
Two departures from the template let the e2e suite run all four services in one process:

- **A public `Run` per service.** `services/<name>/<name>.go` exposes `Run(ctx, config.Config) error` next to
  `internal`, because `test/e2e` cannot import another service's `internal/app`. Process-wide setup (the logger, the
  OpenTelemetry tracer) stays in `cmd/app/main.go`.
- **An injected metrics registry.** The template registers metrics in Prometheus's global registry, so a second
  service in the same process would panic on duplicate registration. Here `pkg/metrics` takes a registerer, and each
  service creates its own registry and serves it on `/metrics`.
