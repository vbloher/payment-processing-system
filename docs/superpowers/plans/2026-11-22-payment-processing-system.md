# Payment Processing System Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a small payment processing system in Go: four services in one monorepo, where an outbound payment runs as a Temporal saga across a double-entry ledger and a mock bank, facts flow through an outbox to NATS JetStream and an inbox-backed notifier, and a reconciliation run detects and repairs drift.

**Architecture:** `payment` owns the API, the saga, the outbox and reconciliation; it commands `ledger` over gRPC and `mockbank` over HTTP from Temporal activities. `payment` publishes facts through a transactional outbox to JetStream; `notification` consumes them through an inbox and delivers webhooks. Each service follows `go-clean-architecture-template` and owns its own database.

**Tech Stack:** Go 1.27.3, PostgreSQL 18 (pgx, goqu, golang-migrate), Temporal Go SDK, NATS JetStream (`nats.go/jetstream`), gRPC + protobuf, chi + oapi-codegen, zerolog, OpenTelemetry, Prometheus, testify, mockery v3, testcontainers-go.

**Spec:** `docs/superpowers/specs/2026-11-22-payment-processing-system-design.md` (written in Russian, identifiers in English). Read it before starting. This plan argues from it.

**Template:** `/Users/vbloher/dev/go-clean-architecture-template`. Read-only reference. When a step says "as in the template", open the named template file and follow its shape.

## Global Constraints

- Go `1.27.3`. Module `github.com/vbloher/payment-processing-system`. One `go.mod`.
- The template is copied and adapted. Never import it, never edit it.
- Every service keeps the template's conventions: layers `domain / dto / usecase / adapter / controller`; composition root `internal/app`; ports declared in `usecase`; `pkg/transaction` manager with the transaction in the context; pgx + goqu; zerolog; config through `caarlos0/env`; contract-first HTTP with the oapi-codegen strict server; gRPC with reflection; mockery v3 (testify template, mocks in `usecase/mocks`); golang-migrate SQL migrations; error wrapping `fmt.Errorf("pkg.Func: %w", err)`; spans `tracer.Start(ctx, "usecase X")` and `"adapter postgres X"`; graceful shutdown (controllers first, then adapters); one file per operation.
- No Redis. No Kafka.
- `pkg/` holds infrastructure and contracts only. Services share no domain code. A service never reads another service's database.
- Money is `int64` minor units plus an ISO-4217 currency matching `^[A-Z]{3}$`. Valid amounts: `1..1_000_000_000_000`.
- Every saga step is idempotent. The operation key in the ledger and in the bank is the payment ID.
- Compensate only on a definitive refusal (`declined`, `cancelled`). Never infer a refusal from "not found" or from a timeout.
- Temporal carries commands that decide a payment's outcome. NATS carries facts after commit. No event starts or advances a payment step; no workflow sends notifications.
- Tests use testify (`require`, `suite`), mockery mocks and table tests with `t.Run`. Unit tests need no Docker. Integration and e2e tests carry `//go:build integration` and use testcontainers-go. Images are pinned, never `latest`. Postgres image: `postgres:18-alpine`.
- Unit tests that reach traced code call `otel.SilentModeInit()` first: the tracer is a package global, as in the template.
- Code, comments, README and commit messages are in English. Commit messages are an imperative sentence with no prefix (template style).
- Dependencies shared with the template use the template's versions. New ones (`go.temporal.io/sdk`, `github.com/nats-io/nats.go`, `github.com/testcontainers/testcontainers-go` with its `postgres` and `nats` modules) use the latest stable release; check their current docs before writing against them.
- Before every commit: `go build ./... && go test ./... && make lint` pass.

## Review Focus

Inputs the spec implies but does not walk through. Each has a test in the task that owns the code.

1. **A payment for an account the ledger does not know, or in another currency.** Expected: the payment ends `failed` with a reason; it is not retried forever. Tests: Task 11 (`HoldFunds` mapping), Task 12 (workflow rows 3 and 4), Task 15 (`Test_Payment_Outcomes` rows 6 and 7).
2. **The bank or the ledger is unreachable while reconciliation runs.** Expected: the run fails and is retried; a failed lookup never becomes a discrepancy. Tests: Task 19 (rows 9 to 11).
3. **Invalid API input**: amount `0` or negative, currency `eur`, missing or 256-character `Idempotency-Key`, malformed UUID. Expected: `400`, no payment row, no workflow. Tests: Task 15 (`Test_Payment_Validation`).
4. **Two concurrent requests with the same `Idempotency-Key`.** Expected: one payment, one workflow, both callers get the same ID. Tests: Task 10 (`Test_CreatePayment_ConcurrentSameKey`), Task 15 (`Test_Payment_Idempotency`).
5. **A bank statement reference that is not a payment ID (`ROGUE-1`), and a signal sent to a workflow that already finished.** Expected: reconciliation records `bank_unexpected` and carries on; it does not crash or fail the run. Tests: Task 19 (rows 5 and 7).

## File Structure

```
api/
  ledger/grpc/ledger_v1.proto            ledger contract
  mockbank/http/bank_v1.yaml             mock bank contract (public + admin)
  payment/http/payment_v1.yaml           payment contract
gen/                                     generated code only
  ledger/grpc/ledger_v1/
  mockbank/http/{server,client}/
  payment/http/{server,client}/
pkg/
  httpserver logger otel postgres render router transaction   copied from the template
  metrics/        Prometheus metrics with an injected registry
  events/         event envelope, types, subjects, stream configs (the contract between services)
  natsjs/         NATS connection + JetStream handle
  temporalclient/ Temporal client constructor + zerolog adapter
  testinfra/      testcontainers helpers: Postgres, NATS, Temporal, migrations, ports
services/<name>/
  <name>.go       public Run(ctx, config.Config) error, so test/e2e can start the service
  cmd/app/main.go entrypoint
  config/         env configuration
  internal/app/   composition root
  internal/domain internal/dto internal/usecase internal/adapter internal/controller
  migration/postgres/   SQL migrations (not in mockbank)
  test/integration/     service-level integration tests
services/payment/internal/saga/   workflow definitions: the only business code that imports the Temporal SDK
test/e2e/                         whole-system tests
deploy/postgres/init.sql          creates the three databases for docker compose
docker-compose.yml Dockerfile Makefile .golangci.yml .mockery.yml .github/workflows/ci.yml README.md
```

Service names: `ledger`, `mockbank`, `payment`, `notification`. Databases: `ledger`, `payment`, `notification`.

Default ports: payment HTTP `8080`; ledger gRPC `50051`, HTTP `8081`; notification HTTP `8082`; mockbank HTTP `8090`.

---

## Phase 1: Foundation

### Task 1: Monorepo skeleton and shared packages

**Files:**
- Create: `go.mod`, `.gitignore`, `.dockerignore`, `.golangci.yml`, `Makefile`
- Create by copying from the template's `pkg/`: `httpserver/httpserver.go`, `logger/{logger.go,middleware.go,interceptor.go}`, `otel/{otel.go,http_middleware.go}`, `otel/tracer/{tracer.go,tracer_test.go}`, `postgres/postgres.go`, `transaction/{transaction.go,wrap.go}`, `render/{error.go,json.go}`, `router/{router.go,extract_path.go,writer_wrapper.go}`
- Create by adapting: `pkg/metrics/{const.go,http_server.go,process.go,middleware.go,handler.go}`
- Test: `pkg/metrics/metrics_test.go`

**Interfaces:**
- Produces:
  - `metrics.NewHTTPServer(reg prometheus.Registerer) *HTTPServer`
  - `metrics.NewProcess(reg prometheus.Registerer) *Process`
  - `metrics.Handler(reg *prometheus.Registry) http.Handler`
  - `otel.Config` gains `ServiceName string` (`env:"OTEL_SERVICE_NAME" envDefault:"app"`), used instead of the literal `"my-app"`
  - Everything else in the copied packages keeps the template's API.

- [ ] **Step 1: Initialise the module and copy the shared packages**

```bash
cd /Users/vbloher/dev/payment-processing-system
go mod init github.com/vbloher/payment-processing-system
T=/Users/vbloher/dev/go-clean-architecture-template
mkdir -p pkg && cp -R $T/pkg/{httpserver,logger,metrics,otel,postgres,transaction,render,router} pkg/
rm pkg/logger/kafka.go pkg/logger/example_intercepter.go
grep -rl 'go-clean-architecture-template' pkg | xargs sed -i '' 's#github.com/vbloher/go-clean-architecture-template#github.com/vbloher/payment-processing-system#g'
```

Set `go 1.27.3` in `go.mod`. For each module the copied code imports, `go get` the version pinned in the template's `go.mod`, then `go mod tidy`. Add the four tools at the template's versions with `go get -tool`: `oapi-codegen/v2/cmd/oapi-codegen`, `vektra/mockery/v3`, `protobuf/cmd/protoc-gen-go`, `grpc/cmd/protoc-gen-go-grpc`.

- [ ] **Step 2: Write the failing metrics tests**

```go
func TestNew_SeparateRegistriesDoNotCollide(t *testing.T) {
	for range 2 {
		reg := prometheus.NewRegistry()
		require.NotPanics(t, func() { metrics.NewHTTPServer(reg); metrics.NewProcess(reg) })
	}
}

func TestHandler_ServesItsRegistry(t *testing.T) {
	reg := prometheus.NewRegistry()
	metrics.NewHTTPServer(reg).TotalInc("GET /x", 200)

	rec := httptest.NewRecorder()
	metrics.Handler(reg).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	require.Contains(t, rec.Body.String(), `http_server_requests_total{method="GET /x",status="200"} 1`)
}
```

- [ ] **Step 3: Run them**

Run: `go test ./pkg/metrics/...`
Expected: FAIL to compile, "too many arguments in call to metrics.NewHTTPServer".

- [ ] **Step 4: Implement the metrics and otel changes**

Replace every `prometheus.MustRegister(x)` with `reg.MustRegister(x)`. `Handler` returns `promhttp.HandlerFor(reg, promhttp.HandlerOpts{})`. Add `ServiceName` to `otel.Config`.

- [ ] **Step 5: Add the repository tooling**

- `.gitignore`: the template's, plus `coverage.out`.
- `.dockerignore`: the template's.
- `.golangci.yml`: the template's, with exclusion paths `_test.go`, `gen/`, `test/`, `pkg/testinfra/`, and the per-path rules rewritten to `services/.*/internal/app` (contextcheck), `services/.*/internal/controller/grpc/` and `services/.*/internal/controller/http/v1` (wrapcheck).
- `Makefile`: variables and the `lint`, `vuln`, `bin-deps`, `protoc-install`, `lint-install`, `migrate-install` targets from the template, plus:

```make
.PHONY: test
test:
	go test -race -shuffle=on ./...

integration-test:
	go test -count=1 -tags=integration -timeout=20m ./...
```

- [ ] **Step 6: Verify**

Run: `go build ./... && go test ./... && make lint-install && make lint`
Expected: all tests PASS, linter reports 0 issues.

- [ ] **Step 7: Commit**

```bash
git add -A && git commit -m "Add monorepo skeleton and shared packages from the template"
```

### Task 2: Event contract

**Files:**
- Create: `pkg/events/events.go`, `pkg/events/stream.go`
- Test: `pkg/events/events_test.go`

**Interfaces:**
- Produces:

```go
type Type string

const (
	TypePaymentSucceeded      Type = "payment.succeeded"
	TypePaymentFailed         Type = "payment.failed"
	TypePaymentReviewRequired Type = "payment.review_required"
	TypeDiscrepancyFound      Type = "reconciliation.discrepancy_found"
)

func (t Type) Subject() string // "events." + string(t)

type Envelope struct {
	ID         uuid.UUID       `json:"id"`
	Type       Type            `json:"type"`
	OccurredAt time.Time       `json:"occurred_at"`
	PaymentID  uuid.UUID       `json:"payment_id"` // uuid.Nil when the event has no payment
	Data       json.RawMessage `json:"data"`
}

type PaymentData struct {
	AccountID     uuid.UUID `json:"account_id"`
	Amount        int64     `json:"amount"`
	Currency      string    `json:"currency"`
	Status        string    `json:"status"`
	FailureReason string    `json:"failure_reason,omitempty"`
}

type DiscrepancyData struct {
	DiscrepancyID uuid.UUID `json:"discrepancy_id"`
	Kind          string    `json:"kind"`
	Reference     string    `json:"reference"`
	Details       string    `json:"details,omitempty"`
}

var ErrInvalidEnvelope = errors.New("invalid event envelope")

func New(t Type, paymentID uuid.UUID, data any) (Envelope, error) // fresh ID, OccurredAt = now UTC
func (e Envelope) Marshal() ([]byte, error)
func Unmarshal(b []byte) (Envelope, error) // ErrInvalidEnvelope on bad JSON, zero ID or empty Type

const (
	StreamName       = "EVENTS"
	DLQStreamName    = "EVENTS_DLQ"
	DLQSubjectPrefix = "dlq."
)

func StreamConfig() jetstream.StreamConfig    // Name EVENTS, Subjects ["events.>"], Duplicates 5m, file storage
func DLQStreamConfig() jetstream.StreamConfig // Name EVENTS_DLQ, Subjects ["dlq.>"], file storage
```

- [ ] **Step 1: Write the failing tests**

```go
func TestType_Subject(t *testing.T) {
	require.Equal(t, "events.payment.succeeded", events.TypePaymentSucceeded.Subject())
	require.Equal(t, "events.reconciliation.discrepancy_found", events.TypeDiscrepancyFound.Subject())
}

func TestEnvelope_RoundTrip(t *testing.T) {
	paymentID := uuid.New()
	e, err := events.New(events.TypePaymentFailed, paymentID, events.PaymentData{Amount: 3000, Currency: "EUR", Status: "failed", FailureReason: "bank_declined"})
	require.NoError(t, err)
	require.NotEqual(t, uuid.Nil, e.ID)
	require.False(t, e.OccurredAt.IsZero())

	raw, err := e.Marshal()
	require.NoError(t, err)

	got, err := events.Unmarshal(raw)
	require.NoError(t, err)
	require.Equal(t, e.ID, got.ID)
	require.Equal(t, paymentID, got.PaymentID)
	require.JSONEq(t, `{"account_id":"00000000-0000-0000-0000-000000000000","amount":3000,"currency":"EUR","status":"failed","failure_reason":"bank_declined"}`, string(got.Data))
}

func TestUnmarshal_Invalid(t *testing.T) {
	for _, raw := range []string{`not json`, `{}`, `{"id":"` + uuid.NewString() + `"}`} {
		_, err := events.Unmarshal([]byte(raw))
		require.ErrorIs(t, err, events.ErrInvalidEnvelope)
	}
}

func TestStreamConfig(t *testing.T) {
	require.Equal(t, []string{"events.>"}, events.StreamConfig().Subjects)
	require.Equal(t, 5*time.Minute, events.StreamConfig().Duplicates)
	require.Equal(t, []string{"dlq.>"}, events.DLQStreamConfig().Subjects)
}
```

- [ ] **Step 2: Run them.** Run: `go test ./pkg/events/...` Expected: FAIL, package does not compile.
- [ ] **Step 3: Implement the signatures above.** `go get github.com/nats-io/nats.go@latest`.
- [ ] **Step 4: Run them.** Expected: PASS.
- [ ] **Step 5: Commit.** `git add -A && git commit -m "Add the event contract shared by services"`

### Task 3: NATS and Temporal clients, test infrastructure

**Files:**
- Create: `pkg/natsjs/natsjs.go`, `pkg/temporalclient/{client.go,logger.go}`, `pkg/testinfra/{postgres.go,nats.go,temporal.go,net.go}`
- Test: `pkg/testinfra/testinfra_test.go` (`//go:build integration`)

**Interfaces:**
- Consumes: `events.StreamConfig()` (Task 2), `postgres.Config` (Task 1).
- Produces:

```go
// pkg/natsjs
type Config struct {
	URL string `env:"NATS_URL,required"`
}
type Client struct {
	Conn *nats.Conn
	JS   jetstream.JetStream
}
func New(ctx context.Context, c Config) (*Client, error)
func (c *Client) EnsureStreams(ctx context.Context, configs ...jetstream.StreamConfig) error // CreateOrUpdateStream each
func (c *Client) Close()                                                                     // drains the connection

// pkg/temporalclient
type Config struct {
	HostPort  string `env:"TEMPORAL_HOST_PORT,required"`
	Namespace string `env:"TEMPORAL_NAMESPACE" envDefault:"default"`
}
func New(ctx context.Context, c Config) (client.Client, error) // client.DialContext, logger adapted to zerolog

// pkg/testinfra
type Postgres struct{ Host, Port, User, Password string }
func StartPostgres(t testing.TB, databases ...string) Postgres // creates each database; container ends with the test
func (p Postgres) Config(database string) postgres.Config
func (p Postgres) URL(database string) string // postgres://user:pass@host:port/db?sslmode=disable
func Migrate(t testing.TB, databaseURL, migrationsDir string) // golang-migrate Up, source file://<dir>
func StartNATS(t testing.TB) natsjs.Config                    // JetStream enabled
func StartTemporal(t testing.TB) temporalclient.Config
func FreePort(t testing.TB) string
func WaitHTTP(t testing.TB, url string) // polls until 200, fails the test after 30s
```

Images are package constants: `PostgresImage = "postgres:18-alpine"`; `NATSImage` and `TemporalImage` pinned to the current stable tags of `nats` and `temporalio/temporal`. There is no official testcontainers module for Temporal: start it as a generic container with command `server start-dev --ip 0.0.0.0`, exposed port `7233/tcp`, wait for the port, then retry `CheckHealth` for up to 30s.

- [ ] **Step 1: Check Docker.** Run: `docker info --format '{{.ServerVersion}}'`. Expected: a version. If it fails, stop and ask the user to start Docker (OrbStack).
- [ ] **Step 2: Write the failing integration tests**

```go
func TestStartPostgres_CreatesDatabases(t *testing.T) {
	pg := testinfra.StartPostgres(t, "one", "two")
	for _, name := range []string{"one", "two"} {
		conn, err := pgx.Connect(t.Context(), pg.URL(name))
		require.NoError(t, err)
		var got string
		require.NoError(t, conn.QueryRow(t.Context(), "SELECT current_database()").Scan(&got))
		require.Equal(t, name, got)
		require.NoError(t, conn.Close(t.Context()))
	}
}

func TestStartNATS_JetStreamDeduplicates(t *testing.T) {
	c, err := natsjs.New(t.Context(), testinfra.StartNATS(t))
	require.NoError(t, err)
	defer c.Close()
	require.NoError(t, c.EnsureStreams(t.Context(), events.StreamConfig(), events.DLQStreamConfig()))

	first, err := c.JS.Publish(t.Context(), "events.payment.succeeded", []byte("x"), jetstream.WithMsgID("id-1"))
	require.NoError(t, err)
	require.False(t, first.Duplicate)
	second, err := c.JS.Publish(t.Context(), "events.payment.succeeded", []byte("x"), jetstream.WithMsgID("id-1"))
	require.NoError(t, err)
	require.True(t, second.Duplicate)
}

func TestStartTemporal_AcceptsConnections(t *testing.T) {
	c, err := temporalclient.New(t.Context(), testinfra.StartTemporal(t))
	require.NoError(t, err)
	defer c.Close()
	_, err = c.CheckHealth(t.Context(), &client.CheckHealthRequest{})
	require.NoError(t, err)
}

func TestFreePort_IsUsable(t *testing.T) {
	l, err := net.Listen("tcp", net.JoinHostPort("", testinfra.FreePort(t)))
	require.NoError(t, err)
	require.NoError(t, l.Close())
}
```

- [ ] **Step 3: Run them.** Run: `go test -count=1 -tags=integration ./pkg/testinfra/...` Expected: FAIL, packages do not compile.
- [ ] **Step 4: Implement the three packages.**
- [ ] **Step 5: Run them.** Expected: PASS (first run pulls images).
- [ ] **Step 6: Commit.** `git add -A && git commit -m "Add NATS and Temporal clients and testcontainers helpers"`

---

## Phase 2: Ledger

### Task 4: Ledger domain

**Files:**
- Create: `services/ledger/internal/domain/{error.go,money.go,account.go,transaction.go,hold.go}`
- Test: `services/ledger/internal/domain/{money_test.go,account_test.go,transaction_test.go,hold_test.go}`

**Interfaces:**
- Produces:

```go
var (
	ErrNotFound            = errors.New("not found")
	ErrValidation          = errors.New("validation failed")
	ErrInsufficientFunds   = errors.New("insufficient funds")
	ErrCurrencyMismatch    = errors.New("currency mismatch")
	ErrHoldConflict        = errors.New("hold is in a conflicting state")
	ErrIdempotencyConflict = errors.New("idempotency key reused with different parameters")
	ErrUnbalanced          = errors.New("debits and credits are not equal")
)
func IsInvalidInput(err error) bool // ErrValidation, ErrCurrencyMismatch, ErrUnbalanced

const MaxAmount int64 = 1_000_000_000_000
func ValidateMoney(amount int64, currency string) error // ErrValidation unless 1 <= amount <= MaxAmount and currency matches ^[A-Z]{3}$

type Side string        // Debit = "debit", Credit = "credit"
type AccountKind string // KindCustomer = "customer", KindBankCash = "bank_cash"

type Account struct {
	ID                   uuid.UUID
	Kind                 AccountKind
	Currency             string
	NormalSide           Side
	Balance, Held        int64
	CreatedAt, UpdatedAt time.Time
}
func NewCustomerAccount(currency string) (Account, error) // credit-normal
func NewBankCashAccount(currency string) (Account, error) // debit-normal
func (a Account) Available() int64                        // Balance - Held
func (a *Account) Post(side Side, amount int64, currency string) error
func (a *Account) Reserve(amount int64, currency string) error
func (a *Account) Unreserve(amount int64)

type TransactionKind string // KindDeposit = "deposit", KindPayment = "payment"
type Entry struct {
	AccountID uuid.UUID
	Side      Side
	Amount    int64
}
type Transaction struct {
	ID                  uuid.UUID
	Kind                TransactionKind
	Reference, Currency string
	Entries             []Entry
	CreatedAt           time.Time
}
func NewTransaction(kind TransactionKind, reference, currency string, entries ...Entry) (Transaction, error)

type HoldStatus string // HoldActive = "active", HoldCaptured = "captured", HoldReleased = "released"
type Hold struct {
	PaymentID, AccountID uuid.UUID
	Amount               int64
	Currency             string
	Status               HoldStatus
	CreatedAt, UpdatedAt time.Time
}
func NewHold(paymentID, accountID uuid.UUID, amount int64, currency string) (Hold, error)
func (h Hold) SameRequest(accountID uuid.UUID, amount int64, currency string) bool
func (h *Hold) Capture() (changed bool, err error)
func (h *Hold) Release() (changed bool, err error)
```

Rules the signatures do not state:
- `Post`: currency differs → `ErrCurrencyMismatch`. The balance moves by `+amount` when `side == NormalSide`, else `-amount`. `int64` overflow → `ErrValidation`. For `KindCustomer`, a result below `Held` → `ErrInsufficientFunds`. On error the account is unchanged.
- `Reserve`: currency differs → `ErrCurrencyMismatch`; `Available() < amount` → `ErrInsufficientFunds`; else `Held += amount`.
- `NewTransaction`: reference non-empty, at least two entries, every amount valid, sum of debits equals sum of credits, else `ErrValidation` or `ErrUnbalanced`.

- [ ] **Step 1: Write the failing table tests**

`TestValidateMoney`: `(1,"EUR")` ok; `(1_000_000_000_000,"EUR")` ok; `(0,"EUR")`, `(-1,"EUR")`, `(1_000_000_000_001,"EUR")`, `(1,"eur")`, `(1,"EU")`, `(1,"EURO")` → `ErrValidation`.

`TestAccount_Post`:

| account | start balance / held | call | want balance | want error |
|---|---|---|---|---|
| customer EUR | 0 / 0 | `Post(Credit, 100, "EUR")` | 100 | nil |
| customer EUR | 100 / 0 | `Post(Debit, 30, "EUR")` | 70 | nil |
| customer EUR | 100 / 0 | `Post(Debit, 101, "EUR")` | 100 | `ErrInsufficientFunds` |
| customer EUR | 100 / 80 | `Post(Debit, 30, "EUR")` | 100 | `ErrInsufficientFunds` |
| customer EUR | 100 / 0 | `Post(Credit, 1, "USD")` | 100 | `ErrCurrencyMismatch` |
| customer EUR | `math.MaxInt64 - 1` / 0 | `Post(Credit, 2, "EUR")` | unchanged | `ErrValidation` |
| bank_cash EUR | 0 / 0 | `Post(Debit, 100, "EUR")` | 100 | nil |
| bank_cash EUR | 100 / 0 | `Post(Credit, 30, "EUR")` | 70 | nil |

`TestAccount_Reserve`: balance 100, `Reserve(60,"EUR")` → held 60, available 40; then `Reserve(41,"EUR")` → `ErrInsufficientFunds`, held 60; `Reserve(1,"USD")` → `ErrCurrencyMismatch`; `Unreserve(60)` → held 0.

`TestNewTransaction`: debit 100 + credit 100 → ok; debit 100 + credit 99 → `ErrUnbalanced`; one entry → `ErrValidation`; an entry with amount 0 → `ErrValidation`; empty reference → `ErrValidation`.

`TestHold_Transitions`:

| start | call | want changed | want status | want error |
|---|---|---|---|---|
| active | `Capture` | true | captured | nil |
| captured | `Capture` | false | captured | nil |
| released | `Capture` | false | released | `ErrHoldConflict` |
| active | `Release` | true | released | nil |
| released | `Release` | false | released | nil |
| captured | `Release` | false | captured | `ErrHoldConflict` |

`TestHold_SameRequest`: equal account, amount and currency → true; each differing in turn → false.

- [ ] **Step 2: Run them.** Run: `go test ./services/ledger/internal/domain/...` Expected: FAIL, undefined symbols.
- [ ] **Step 3: Implement the domain.**
- [ ] **Step 4: Run them.** Expected: PASS.
- [ ] **Step 5: Commit.** `git add -A && git commit -m "Add ledger domain: accounts, double-entry transactions, holds"`

### Task 5: Ledger persistence and use cases

**Files:**
- Create: `services/ledger/migration/postgres/20261122000001_init.{up,down}.sql`
- Create: `services/ledger/internal/dto/ledger.go`
- Create: `services/ledger/internal/usecase/{usecase.go,open_account.go,deposit.go,get_balance.go,hold.go,capture.go,release.go,get_holds.go}`
- Create: `services/ledger/internal/adapter/postgres/{postgres.go,account.go,transaction.go,hold.go}`
- Create: `.mockery.yml`; Modify: `Makefile` (add `generate`)
- Generated: `services/ledger/internal/usecase/mocks/mocks.go`
- Test: `services/ledger/internal/usecase/{hold_test.go,capture_test.go,release_test.go,deposit_test.go,helpers_test.go}`, `services/ledger/test/integration/{main_test.go,ledger_test.go}`

**Interfaces:**
- Consumes: Task 4 domain; `transaction.Manager`; `testinfra.StartPostgres`, `testinfra.Migrate`.
- Produces:

```go
// dto
type OpenAccountInput struct{ Currency string }
type OpenAccountOutput struct{ ID uuid.UUID }
type DepositInput struct {
	AccountID                uuid.UUID
	Amount                   int64
	Currency, IdempotencyKey string
}
type GetBalanceInput struct{ AccountID uuid.UUID }
type GetBalanceOutput struct {
	Balance, Held, Available int64
	Currency                 string
}
type HoldInput struct {
	PaymentID, AccountID uuid.UUID
	Amount               int64
	Currency             string
}
type CaptureInput struct{ PaymentID uuid.UUID }
type ReleaseInput struct{ PaymentID uuid.UUID }
type GetHoldsInput struct{ PaymentIDs []uuid.UUID }
type GetHoldsOutput struct{ Holds []domain.Hold }

// usecase
type Transactor interface {
	Wrap(ctx context.Context, fn func(ctx context.Context) error) error
}
type Postgres interface {
	CreateAccount(ctx context.Context, account domain.Account) error
	GetAccount(ctx context.Context, id uuid.UUID) (domain.Account, error)
	GetAccountForUpdate(ctx context.Context, id uuid.UUID) (domain.Account, error)
	GetBankCashForUpdate(ctx context.Context, currency string) (domain.Account, error) // creates it when absent
	UpdateAccountBalances(ctx context.Context, account domain.Account) error
	CreateTransaction(ctx context.Context, tx domain.Transaction) (created bool, err error) // false when (kind, reference) exists
	CreateHold(ctx context.Context, hold domain.Hold) error
	GetHold(ctx context.Context, paymentID uuid.UUID) (domain.Hold, error)
	GetHoldForUpdate(ctx context.Context, paymentID uuid.UUID) (domain.Hold, error)
	UpdateHoldStatus(ctx context.Context, hold domain.Hold) error
	GetHolds(ctx context.Context, paymentIDs []uuid.UUID) ([]domain.Hold, error)
}
func New(tx Transactor, postgres Postgres) *UseCase
func (u *UseCase) OpenAccount(ctx context.Context, in dto.OpenAccountInput) (dto.OpenAccountOutput, error)
func (u *UseCase) Deposit(ctx context.Context, in dto.DepositInput) error
func (u *UseCase) GetBalance(ctx context.Context, in dto.GetBalanceInput) (dto.GetBalanceOutput, error)
func (u *UseCase) Hold(ctx context.Context, in dto.HoldInput) error
func (u *UseCase) Capture(ctx context.Context, in dto.CaptureInput) error
func (u *UseCase) Release(ctx context.Context, in dto.ReleaseInput) error
func (u *UseCase) GetHolds(ctx context.Context, in dto.GetHoldsInput) (dto.GetHoldsOutput, error)
```

Missing rows return `domain.ErrNotFound` from every `Get*`.

Schema (`up`):

```sql
BEGIN;

CREATE TABLE account (
    id          UUID PRIMARY KEY,
    kind        TEXT        NOT NULL CHECK (kind IN ('customer', 'bank_cash')),
    currency    CHAR(3)     NOT NULL,
    normal_side TEXT        NOT NULL CHECK (normal_side IN ('debit', 'credit')),
    balance     BIGINT      NOT NULL DEFAULT 0,
    held        BIGINT      NOT NULL DEFAULT 0 CHECK (held >= 0),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (kind <> 'customer' OR balance - held >= 0)
);
CREATE UNIQUE INDEX account_bank_cash_currency ON account (currency) WHERE kind = 'bank_cash';

CREATE TABLE ledger_transaction (
    id         UUID PRIMARY KEY,
    kind       TEXT        NOT NULL CHECK (kind IN ('deposit', 'payment')),
    reference  TEXT        NOT NULL,
    currency   CHAR(3)     NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (kind, reference)
);

CREATE TABLE ledger_entry (
    id             BIGSERIAL PRIMARY KEY,
    transaction_id UUID        NOT NULL REFERENCES ledger_transaction (id),
    account_id     UUID        NOT NULL REFERENCES account (id),
    side           TEXT        NOT NULL CHECK (side IN ('debit', 'credit')),
    amount         BIGINT      NOT NULL CHECK (amount > 0),
    created_at     TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX ledger_entry_account ON ledger_entry (account_id);

CREATE TABLE hold (
    payment_id UUID PRIMARY KEY,
    account_id UUID        NOT NULL REFERENCES account (id),
    amount     BIGINT      NOT NULL CHECK (amount > 0),
    currency   CHAR(3)     NOT NULL,
    status     TEXT        NOT NULL CHECK (status IN ('active', 'captured', 'released')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE FUNCTION ledger_immutable() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'ledger rows are immutable';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ledger_entry_immutable BEFORE UPDATE OR DELETE ON ledger_entry
    FOR EACH ROW EXECUTE FUNCTION ledger_immutable();
CREATE TRIGGER ledger_transaction_immutable BEFORE UPDATE OR DELETE ON ledger_transaction
    FOR EACH ROW EXECUTE FUNCTION ledger_immutable();

COMMIT;
```

The `down` migration drops the four tables and the function.

Use case rules. **Lock order in every transaction: customer account, then hold row, then `bank_cash`.**

- `Hold`: validate money. In a transaction: `GetAccountForUpdate`; `GetHold`. A hold exists: `SameRequest` → return nil, else `ErrIdempotencyConflict`. No hold: `account.Reserve`, `CreateHold`, `UpdateAccountBalances`.
- `Capture`: `GetHold` without a lock to learn the account. In a transaction: `GetAccountForUpdate`, `GetHoldForUpdate`, `hold.Capture()`; unchanged → return nil. Changed: `GetBankCashForUpdate`; `account.Unreserve`; `account.Post(Debit)`; `bankCash.Post(Credit)`; `CreateTransaction(KindPayment, reference = payment ID)`; `UpdateHoldStatus`; `UpdateAccountBalances` for both.
- `Release`: same locking; `hold.Release()`; unchanged → nil; changed → `account.Unreserve`, `UpdateHoldStatus`, `UpdateAccountBalances`.
- `Deposit`: validate money. In a transaction: `GetAccountForUpdate`, `GetBankCashForUpdate`, `CreateTransaction(KindDeposit, reference = IdempotencyKey)` with entries Dr `bank_cash`, Cr customer. `created == false` → return nil without touching balances. Otherwise `Post` both and `UpdateAccountBalances` both.

`.mockery.yml`: the template's file with the `packages` key set to `github.com/vbloher/payment-processing-system/services/ledger/internal/usecase` (`config: {all: true}`). `Makefile`:

```make
generate:
	go generate ./...
	go tool mockery
```

- [ ] **Step 1: Write the ports and DTOs, generate mocks.** Run: `make generate`. Expected: `services/ledger/internal/usecase/mocks/mocks.go` exists with `NewPostgres` and `NewTransactor`.

- [ ] **Step 2: Write the failing unit tests** (`helpers_test.go` holds `newTransactor(t)` as in the template's `get_profile_test.go`)

`Test_Hold` (account: customer EUR, balance 10000, held 0; request 3000 EUR):

| case | mocks return | want error | must be called | must not be called |
|---|---|---|---|---|
| new hold | `GetHold` → `ErrNotFound` | nil | `CreateHold` (status active, amount 3000), `UpdateAccountBalances` (held 3000) | |
| insufficient funds | account balance 1000; `GetHold` → `ErrNotFound` | `ErrInsufficientFunds` | | `CreateHold`, `UpdateAccountBalances` |
| replay | `GetHold` → same account, 3000 EUR | nil | | `CreateHold`, `UpdateAccountBalances` |
| key reused | `GetHold` → amount 2000 | `ErrIdempotencyConflict` | | `CreateHold` |
| unknown account | `GetAccountForUpdate` → `ErrNotFound` | `ErrNotFound` | | `CreateHold` |
| other currency | request 3000 USD | `ErrCurrencyMismatch` | | `CreateHold` |
| bad amount | request 0 EUR | `ErrValidation` | | any Postgres call |

`Test_Capture` (hold 3000 EUR on a customer with balance 10000, held 3000; `bank_cash` balance 10000):

| case | hold status | want error | assertions |
|---|---|---|---|
| active | active | nil | `CreateTransaction` gets kind payment, reference = payment ID, entries `{customer, debit, 3000}` and `{bank_cash, credit, 3000}`; customer saved with balance 7000, held 0; `bank_cash` saved with balance 7000; hold saved captured |
| replay | captured | nil | no `CreateTransaction`, no `UpdateAccountBalances` |
| after release | released | `ErrHoldConflict` | no writes |
| missing | `GetHold` → `ErrNotFound` | `ErrNotFound` | no writes |

`Test_Release`: active → customer saved with held 0 and balance 10000, hold saved released; released → nil, no writes; captured → `ErrHoldConflict`; missing → `ErrNotFound`.

`Test_Deposit`: new (`CreateTransaction` → true) → customer saved with balance +5000, `bank_cash` saved with balance +5000, entries Dr `bank_cash` / Cr customer; duplicate (`CreateTransaction` → false) → nil and no `UpdateAccountBalances`; amount 0 → `ErrValidation`.

- [ ] **Step 3: Run them.** Run: `go test ./services/ledger/internal/usecase/...` Expected: FAIL, undefined `usecase.New`.
- [ ] **Step 4: Implement the use cases.** Expected after: unit tests PASS.

- [ ] **Step 5: Write the failing integration tests** (`//go:build integration`; suite starts `testinfra.StartPostgres(t, "ledger")`, migrates `../../migration/postgres`, builds `usecase.New(tx, postgres.New(tx))` over a real pool)

Two helper queries, both must return `0` wherever a test says "ledger is intact":

```sql
SELECT count(*) FROM account a
WHERE a.balance <> COALESCE((SELECT sum(CASE WHEN e.side = a.normal_side THEN e.amount ELSE -e.amount END)
                             FROM ledger_entry e WHERE e.account_id = a.id), 0);

SELECT count(*) FROM (SELECT currency, sum(CASE WHEN normal_side = 'debit' THEN balance ELSE -balance END) AS diff
                      FROM account GROUP BY currency) t WHERE diff <> 0;
```

- `Test_HoldCaptureRelease_Balances`: open EUR account; deposit 10000 with key `dep-1`; hold 3000 → balance 10000, held 3000, available 7000; capture → 7000, 0, 7000; second payment: hold 2000 then release → 7000, 0, 7000; ledger is intact.
- `Test_Deposit_Idempotent`: the same deposit twice → balance 10000.
- `Test_Hold_Concurrent_NeverOverdraws`: balance 10000; 20 goroutines hold 3000 each under distinct payment IDs → exactly 3 return nil and 17 return `ErrInsufficientFunds`; held 9000, available 1000.
- `Test_Capture_Concurrent_SamePayment`: 5 goroutines capture one hold → all nil; `SELECT count(*) FROM ledger_transaction WHERE reference = $1` is 1; balance 7000; ledger is intact.
- `Test_LedgerEntries_AreImmutable`: `UPDATE ledger_entry SET amount = 1` and `DELETE FROM ledger_entry` both return an error containing `immutable`.

- [ ] **Step 6: Run them.** Run: `go test -count=1 -tags=integration ./services/ledger/test/integration/...` Expected: FAIL, undefined `postgres.New`.
- [ ] **Step 7: Write the migration and the Postgres adapter.** `CreateTransaction` uses `ON CONFLICT (kind, reference) DO NOTHING` and reports rows affected; it inserts the entries only when the transaction row was inserted. `GetBankCashForUpdate` inserts with `ON CONFLICT DO NOTHING`, then selects `FOR UPDATE`.
- [ ] **Step 8: Run unit and integration tests.** Expected: PASS.
- [ ] **Step 9: Commit.** `git add -A && git commit -m "Add ledger use cases and Postgres adapter with holds and double-entry posting"`

### Task 6: Ledger gRPC API and service wiring

**Files:**
- Create: `api/ledger/grpc/ledger_v1.proto`; Modify: `Makefile` (`grpc_gen`, wired into `generate`)
- Generated: `gen/ledger/grpc/ledger_v1/{ledger_v1.pb.go,ledger_v1_grpc.pb.go}`
- Create: `services/ledger/internal/controller/grpc/grpc.go`, `services/ledger/internal/controller/grpc/v1/{v1.go,error.go,open_account.go,deposit.go,get_balance.go,hold.go,capture.go,release.go,get_holds.go}`
- Create: `services/ledger/internal/app/app.go`, `services/ledger/config/config.go`, `services/ledger/ledger.go`, `services/ledger/cmd/app/main.go`, `services/ledger/.env.example`
- Test: `services/ledger/test/integration/grpc_test.go`

**Interfaces:**
- Consumes: Task 5 use cases; `metrics.Handler`, `router.New`, `httpserver.New`.
- Produces:
  - `ledger.Run(ctx context.Context, c config.Config) error` in package `ledger` (`services/ledger/ledger.go`), a one-line call to `app.Run`.
  - `config.Config{HTTP httpserver.Config; GRPC grpc.Config; Logger logger.Config; OTEL otel.Config; Postgres postgres.Config}`, `config.New() (Config, error)` as in the template.
  - Generated client `ledger_v1.NewLedgerV1Client`.
  - HTTP `/live`, `/ready`, `/metrics` on `HTTP_PORT`.

Contract:

```proto
syntax = "proto3";

package ledger_v1;

import "google/protobuf/empty.proto";

option go_package = "github.com/vbloher/payment-processing-system/gen/ledger/grpc/ledger_v1";

service LedgerV1 {
  rpc OpenAccount(OpenAccountInput) returns (OpenAccountOutput);
  rpc Deposit(DepositInput) returns (google.protobuf.Empty);
  rpc GetBalance(GetBalanceInput) returns (GetBalanceOutput);
  rpc Hold(HoldInput) returns (google.protobuf.Empty);
  rpc Capture(CaptureInput) returns (google.protobuf.Empty);
  rpc Release(ReleaseInput) returns (google.protobuf.Empty);
  rpc GetHolds(GetHoldsInput) returns (GetHoldsOutput);
}

message OpenAccountInput { string currency = 1; }
message OpenAccountOutput { string id = 1; }
message DepositInput { string account_id = 1; int64 amount = 2; string currency = 3; string idempotency_key = 4; }
message GetBalanceInput { string account_id = 1; }
message GetBalanceOutput { int64 balance = 1; int64 held = 2; int64 available = 3; string currency = 4; }
message HoldInput { string payment_id = 1; string account_id = 2; int64 amount = 3; string currency = 4; }
message CaptureInput { string payment_id = 1; }
message ReleaseInput { string payment_id = 1; }
message GetHoldsInput { repeated string payment_ids = 1; }
message GetHoldsOutput {
  repeated Hold holds = 1;

  message Hold { string payment_id = 1; string account_id = 2; int64 amount = 3; string currency = 4; string status = 5; }
}
```

Error mapping in `v1/error.go`:

| domain error | gRPC code |
|---|---|
| `ErrNotFound` | `NotFound` |
| `ErrValidation`, `ErrCurrencyMismatch`, `ErrUnbalanced`, unparseable UUID | `InvalidArgument` |
| `ErrInsufficientFunds` | `FailedPrecondition` |
| `ErrHoldConflict` | `Aborted` |
| `ErrIdempotencyConflict` | `AlreadyExists` |
| anything else | `Internal` with a generic message |

`app.Run` follows the template's `internal/app/app.go`: pool, transaction manager, use case, gRPC server, HTTP server with `router.New()` plus `/metrics` from `metrics.Handler(reg)` where `reg := prometheus.NewRegistry()` is created inside `Run`.

- [ ] **Step 1: Write the contract and generate.** Add `grpc_gen` to the `Makefile` as in the template, with output `gen/ledger/grpc/ledger_v1` and proto path `api/ledger/grpc`; make `generate` depend on it. Run: `make protoc-install && make generate`. Expected: the two `.pb.go` files exist.

- [ ] **Step 2: Write the failing integration tests** (suite starts Postgres, migrates, runs `ledger.Run` in a goroutine on `testinfra.FreePort` ports, waits with `testinfra.WaitHTTP(".../ready")`, dials the gRPC client)

- `Test_GRPC_PaymentFlow`: `OpenAccount{EUR}`; `Deposit{10000, EUR, "dep-1"}`; `Hold{3000}`; `GetBalance` → `balance 10000, held 3000, available 7000, currency "EUR"`; `Capture`; `GetBalance` → `7000, 0, 7000`; `GetHolds` → one hold with status `captured`.
- `Test_GRPC_ErrorCodes`:

| call | want code |
|---|---|
| `Hold` on a random account ID | `NotFound` |
| `Hold` 3000 on a balance of 1000 | `FailedPrecondition` |
| `Hold` in USD on a EUR account | `InvalidArgument` |
| `Hold` with `account_id: "nope"` | `InvalidArgument` |
| `Hold` again, same payment ID, other amount | `AlreadyExists` |
| `Capture` after `Release` | `Aborted` |
| `Capture` on a random payment ID | `NotFound` |
| `GetBalance` on a random account ID | `NotFound` |

- `Test_HTTP_Probes`: `GET /ready` → 200; `GET /metrics` → 200.

- [ ] **Step 3: Run them.** Run: `go test -count=1 -tags=integration ./services/ledger/test/integration/...` Expected: FAIL, undefined `ledger.Run`.
- [ ] **Step 4: Implement the controller, app, config, `Run` and `main`.** `main.go` is the template's `cmd/app/main.go` with this service's config.
- [ ] **Step 5: Run them.** Expected: PASS.
- [ ] **Step 6: Commit.** `git add -A && git commit -m "Expose the ledger over gRPC and wire the service"`

---

## Phase 3: Mock bank

### Task 7: Mock bank core

**Files:**
- Create: `services/mockbank/internal/domain/{transfer.go,error.go}`, `services/mockbank/internal/dto/transfer.go`
- Create: `services/mockbank/internal/usecase/{usecase.go,create_transfer.go,cancel_transfer.go,get_transfer.go,get_statement.go,admin.go}`
- Create: `services/mockbank/internal/adapter/memory/memory.go`
- Test: `services/mockbank/internal/usecase/usecase_test.go`

**Interfaces:**
- Produces:

```go
// domain
type Status string // StatusSucceeded = "succeeded", StatusDeclined = "declined", StatusCancelled = "cancelled"

type Transfer struct {
	ID                    uuid.UUID
	Reference             string // the client's Idempotency-Key
	Amount                int64
	Currency, Beneficiary string
	Status                Status
	DeclineReason         string
	CreatedAt             time.Time
}

const (
	PrefixDecline  = "DECLINE"  // declined, reason DeclineReasonAccountClosed
	PrefixFlaky    = "FLAKY"    // the first FlakyFailures create attempts fail unprocessed, then success
	PrefixTimeout  = "TIMEOUT"  // processed, but every create call answers with a timeout
	PrefixBlackout = "BLACKOUT" // processed, then the bank turns itself unavailable

	FlakyFailures              = 2
	DeclineReasonAccountClosed = "account_closed"
)

var (
	ErrUnavailable = errors.New("bank unavailable")
	ErrTimeout     = errors.New("bank timeout")
	ErrNotFound    = errors.New("not found")
	ErrConflict    = errors.New("idempotency key reused with a different request")
	ErrValidation  = errors.New("validation failed")
)

// dto
type CreateTransferInput struct {
	Reference             string
	Amount                int64
	Currency, Beneficiary string
}

// usecase
type Storage interface {
	Get(reference string) (domain.Transfer, bool)
	Save(t domain.Transfer)
	List(from, to time.Time) []domain.Transfer
	NextAttempt(reference string) int // 1 on the first call for a reference
	Available() bool
	SetAvailable(available bool)
}
func New(storage Storage) *UseCase
func (u *UseCase) CreateTransfer(ctx context.Context, in dto.CreateTransferInput) (domain.Transfer, error)
func (u *UseCase) CancelTransfer(ctx context.Context, reference string) (domain.Transfer, error)
func (u *UseCase) GetTransfer(ctx context.Context, reference string) (domain.Transfer, error)
func (u *UseCase) GetStatement(ctx context.Context, from, to time.Time) ([]domain.Transfer, error) // succeeded transfers only
func (u *UseCase) SetAvailability(ctx context.Context, available bool)
func (u *UseCase) AddTransfer(ctx context.Context, in dto.CreateTransferInput) (domain.Transfer, error)   // admin: stores a succeeded transfer as is
func (u *UseCase) SetTransferAmount(ctx context.Context, reference string, amount int64) (domain.Transfer, error) // admin

// adapter/memory
func New() *Storage // starts available
```

The use case serialises all operations with one mutex. Behaviour:

`CreateTransfer`, in this order:
1. Bank unavailable → `ErrUnavailable`, nothing stored.
2. Empty reference, amount `< 1`, currency not `^[A-Z]{3}$` or empty beneficiary → `ErrValidation`.
3. A transfer exists for the reference: status `cancelled` → return it (the reference is fenced); amount, currency or beneficiary differ → `ErrConflict`; beneficiary has `PrefixTimeout` → `ErrTimeout`; otherwise return it.
4. No transfer, by beneficiary prefix: `DECLINE` → store declined, return it. `FLAKY` → `NextAttempt <= FlakyFailures` → `ErrUnavailable`, nothing stored; otherwise store succeeded, return it. `TIMEOUT` → store succeeded, return `ErrTimeout`. `BLACKOUT` → store succeeded, `SetAvailable(false)`, return `ErrUnavailable`. Anything else → store succeeded, return it.

`CancelTransfer`: unavailable → `ErrUnavailable`. A transfer exists → return it unchanged. None → store a `cancelled` transfer for the reference and return it.

`GetTransfer`, `GetStatement`: unavailable → `ErrUnavailable`; missing → `ErrNotFound`. The admin methods ignore availability.

- [ ] **Step 1: Write the failing tests** (real `memory.New()`, no mocks)

`Test_CreateTransfer_ByBeneficiary`:

| beneficiary | calls | want per call | stored after |
|---|---|---|---|
| `DE-OK-1` | 1 | succeeded | succeeded |
| `DECLINE-1` | 1 | declined, reason `account_closed` | declined |
| `FLAKY-1` | 3 | `ErrUnavailable`, `ErrUnavailable`, succeeded | succeeded |
| `TIMEOUT-1` | 2 | `ErrTimeout`, `ErrTimeout` | succeeded, exactly one transfer |
| `BLACKOUT-1` | 1 | `ErrUnavailable` | succeeded; `GetTransfer` → `ErrUnavailable` until `SetAvailability(true)` |

`Test_CreateTransfer_Idempotency`: same reference and body twice → the same transfer ID; same reference with another amount → `ErrConflict`.

`Test_CreateTransfer_Validation`: empty reference, amount 0, currency `eur`, empty beneficiary → `ErrValidation`.

`Test_CancelTransfer`:

| setup | want status |
|---|---|
| succeeded transfer exists | succeeded |
| declined transfer exists | declined |
| nothing exists | cancelled |
| cancelled twice | cancelled, same transfer ID both times |

`Test_CancelTransfer_FencesLateCreate` (spec scenario 8): `CancelTransfer("p1")` → cancelled; then `CreateTransfer{Reference: "p1", Amount: 3000, Currency: "EUR", Beneficiary: "DE-OK-1"}` → returns status cancelled, no error; `GetStatement` over all time is empty.

`Test_Unavailable`: after `SetAvailability(false)`, `CreateTransfer`, `CancelTransfer`, `GetTransfer` and `GetStatement` all return `ErrUnavailable` and nothing is stored; after `SetAvailability(true)` they work.

`Test_GetStatement`: one succeeded, one declined, one cancelled → only the succeeded one; a window that ends before it was created → empty.

`Test_Admin`: `AddTransfer{Reference: "ROGUE-1", ...}` appears in the statement; `SetTransferAmount("ROGUE-1", 1)` → amount 1; `SetTransferAmount` on a missing reference → `ErrNotFound`.

- [ ] **Step 2: Run them.** Run: `go test ./services/mockbank/...` Expected: FAIL, undefined symbols.
- [ ] **Step 3: Implement domain, storage and use case.**
- [ ] **Step 4: Run them.** Expected: PASS.
- [ ] **Step 5: Commit.** `git add -A && git commit -m "Add mock bank core with deterministic failure modes and cancel fencing"`

### Task 8: Mock bank HTTP API and service wiring

**Files:**
- Create: `api/mockbank/http/bank_v1.yaml`
- Create: `gen/mockbank/http/server/{generate.go,config.yaml}`, `gen/mockbank/http/client/{generate.go,config.yaml}` (generated: `server.gen.go`, `client.gen.go`)
- Create: `services/mockbank/internal/controller/http/router.go`, `services/mockbank/internal/controller/http/v1/{v1.go,error.go,transfers.go,admin.go}`
- Create: `services/mockbank/internal/app/app.go`, `services/mockbank/config/config.go`, `services/mockbank/mockbank.go`, `services/mockbank/cmd/app/main.go`, `services/mockbank/.env.example`
- Test: `services/mockbank/test/http_test.go` (no build tag: needs no Docker)

**Interfaces:**
- Consumes: Task 7 use case; `testinfra.FreePort`, `testinfra.WaitHTTP`.
- Produces:
  - `mockbank.Run(ctx context.Context, c config.Config) error`; `config.Config{HTTP httpserver.Config; Logger logger.Config; OTEL otel.Config}`.
  - Generated client package `http_client` at `gen/mockbank/http/client` (`NewClientWithResponses`).

Contract (`servers: [{url: /api/v1}]`, generator configs as in the template's `gen/http/profile_v1/*/config.yaml`):

| operationId | method and path | request | responses |
|---|---|---|---|
| `createTransfer` | `POST /transfers` | header `Idempotency-Key` (required, 1..255 chars); body `{amount: int64 >= 1, currency: ^[A-Z]{3}$, beneficiary: 1..140 chars}` | `200 Transfer`, `400`, `409`, `503`, `504` |
| `cancelTransfer` | `POST /transfers/{reference}/cancel` | | `200 Transfer`, `503` |
| `getTransfer` | `GET /transfers/{reference}` | | `200 Transfer`, `404`, `503` |
| `getStatement` | `GET /statement` | query `from`, `to` (date-time, required) | `200 {transfers: [Transfer]}`, `503` |
| `setAvailability` | `PUT /admin/availability` | body `{available: bool}` | `204` |
| `addTransfer` | `POST /admin/transfers` | body `{reference, amount, currency, beneficiary}` | `201 Transfer` |
| `setTransferAmount` | `PATCH /admin/transfers/{reference}` | body `{amount: int64 >= 1}` | `200 Transfer`, `404` |

`Transfer`: `{id: uuid, reference: string, amount: int64, currency: string, beneficiary: string, status: enum[succeeded, declined, cancelled], decline_reason?: string, created_at: date-time}`. Error bodies: `{error: string}`.

Error mapping: `ErrUnavailable` → 503, `ErrTimeout` → 504, `ErrNotFound` → 404, `ErrConflict` → 409, `ErrValidation` → 400.

- [ ] **Step 1: Write the contract and generate.** Run: `make generate`. Expected: `server.gen.go` and `client.gen.go` exist and `go build ./gen/...` passes.

- [ ] **Step 2: Write the failing tests** (start `mockbank.Run` on a free port, wait for `/ready`, use the generated client)

`Test_HTTP_CreateTransfer` (header `Idempotency-Key: <fresh uuid>`, body 3000 EUR):

| beneficiary | want status code | want body status |
|---|---|---|
| `DE-OK-1` | 200 | `succeeded` |
| `DECLINE-1` | 200 | `declined` |
| `FLAKY-1` (first call) | 503 | |
| `TIMEOUT-1` | 504 | |

`Test_HTTP_Cancel`: after `TIMEOUT-1`, cancel → 200 `succeeded`; cancel on a fresh reference → 200 `cancelled`; then create with that reference → 200 `cancelled`.

`Test_HTTP_Errors`: same key with another amount → 409; missing `Idempotency-Key` → 400; currency `eur` → 400; `GET /transfers/missing` → 404.

`Test_HTTP_Admin`: `PUT /admin/availability {false}` → 204, then create → 503; `{true}` → create → 200. `POST /admin/transfers {reference: "ROGUE-1", ...}` → 201 and the transfer is in `GET /statement`. `PATCH /admin/transfers/ROGUE-1 {amount: 1}` → 200 with amount 1.

- [ ] **Step 3: Run them.** Run: `go test ./services/mockbank/...` Expected: FAIL, undefined `mockbank.Run`.
- [ ] **Step 4: Implement handlers, router, app, config, `Run`, `main`.** The router follows the template's `internal/controller/http/router.go` (spec validation middleware, strict handler) with `/metrics` from `metrics.Handler(reg)`.
- [ ] **Step 5: Run them.** Expected: PASS.
- [ ] **Step 6: Commit.** `git add -A && git commit -m "Expose the mock bank over HTTP with admin fault controls"`

---

## Phase 4: Payment

### Task 9: Payment domain

**Files:**
- Create: `services/payment/internal/domain/{error.go,payment.go,status.go,bank.go,event.go}`
- Test: `services/payment/internal/domain/{payment_test.go,status_test.go,event_test.go}`

**Interfaces:**
- Consumes: `pkg/events` (Task 2).
- Produces:

```go
var (
	ErrNotFound               = errors.New("not found")
	ErrValidation             = errors.New("validation failed")
	ErrIdempotencyKeyRequired = errors.New("idempotency key required")
	ErrIdempotencyConflict    = errors.New("idempotency key reused with a different request")
	ErrInvalidTransition      = errors.New("invalid status transition")
	ErrInsufficientFunds      = errors.New("insufficient funds")
	ErrAccountNotFound        = errors.New("account not found")
	ErrLedgerRejected         = errors.New("ledger rejected the request")
	ErrLedgerConflict         = errors.New("ledger hold is in a conflicting state")
	ErrBankUnavailable        = errors.New("bank outcome unknown")
)
func IsInvalidInput(err error) bool // ErrValidation, ErrIdempotencyKeyRequired
func IsFatal(err error) bool        // ErrNotFound, ErrInvalidTransition, ErrValidation, ErrLedgerConflict

type Status string
const (
	StatusCreated    Status = "created"
	StatusProcessing Status = "processing"
	StatusSucceeded  Status = "succeeded"
	StatusFailed     Status = "failed"
	StatusReview     Status = "review"
)
func (s Status) IsTerminal() bool // succeeded, failed

type FailureReason string
const (
	ReasonNone              FailureReason = ""
	ReasonInsufficientFunds FailureReason = "insufficient_funds"
	ReasonAccountNotFound   FailureReason = "account_not_found"
	ReasonLedgerRejected    FailureReason = "ledger_rejected"
	ReasonBankDeclined      FailureReason = "bank_declined"
	ReasonBankCancelled     FailureReason = "bank_cancelled"
)

type BankOutcome string
const (
	BankSucceeded BankOutcome = "succeeded"
	BankDeclined  BankOutcome = "declined"
	BankCancelled BankOutcome = "cancelled"
)

type BankTransfer struct {
	ID, Reference string
	Amount        int64
	Currency      string
	Status        BankOutcome
}

type HoldState string // HoldAbsent = "", HoldActive = "active", HoldCaptured = "captured", HoldReleased = "released"

const MaxAmount int64 = 1_000_000_000_000

type Payment struct {
	ID                                 uuid.UUID
	IdempotencyKey, RequestHash        string
	AccountID                          uuid.UUID
	Amount                             int64
	Currency, Beneficiary, Description string
	Status                             Status
	FailureReason                      FailureReason
	BankReference                      string
	CreatedAt, UpdatedAt               time.Time
}
func NewPayment(idempotencyKey string, accountID uuid.UUID, amount int64, currency, beneficiary, description string) (Payment, error)
func RequestHash(accountID uuid.UUID, amount int64, currency, beneficiary, description string) string // sha256 hex
func (p *Payment) TransitionTo(next Status) (changed bool, err error)
func (p Payment) ToEvent(t events.Type) (Event, error)

type Event struct {
	ID      uuid.UUID // JetStream message ID and inbox key
	Subject string
	Payload []byte
	Headers map[string]string
}
```

`NewPayment` validation: key empty → `ErrIdempotencyKeyRequired`; key longer than 255, zero account ID, amount outside `1..MaxAmount`, currency not `^[A-Z]{3}$`, beneficiary empty or longer than 140, description longer than 255 → `ErrValidation`. Result: fresh ID, `StatusCreated`, `RequestHash` filled.

- [ ] **Step 1: Write the failing table tests**

`TestPayment_TransitionTo`:

| from | to | want changed | want error |
|---|---|---|---|
| created | processing | true | nil |
| created | created | false | nil |
| created | succeeded | false | `ErrInvalidTransition` |
| created | failed | false | `ErrInvalidTransition` |
| processing | succeeded | true | nil |
| processing | failed | true | nil |
| processing | review | true | nil |
| processing | processing | false | nil |
| processing | created | false | `ErrInvalidTransition` |
| review | succeeded | true | nil |
| review | failed | true | nil |
| review | review | false | nil |
| review | processing | false | `ErrInvalidTransition` |
| succeeded | succeeded | false | nil |
| succeeded | failed | false | `ErrInvalidTransition` |
| failed | failed | false | nil |
| failed | succeeded | false | `ErrInvalidTransition` |

On error the status is unchanged.

`TestNewPayment`: valid input → status created, non-zero ID, `RequestHash` equals `RequestHash(...)` of the same fields. Invalid rows: empty key → `ErrIdempotencyKeyRequired`; 256-char key, `uuid.Nil` account, amount 0, amount -1, amount `MaxAmount+1`, currency `eur`, empty beneficiary, 141-char beneficiary, 256-char description → `ErrValidation`.

`TestRequestHash`: equal inputs give equal hashes; changing each field in turn gives a different hash.

`TestPayment_ToEvent`: a failed payment with reason `bank_declined` → `Subject == "events.payment.failed"`, non-zero `ID`, and `events.Unmarshal(Payload)` yields the same `ID`, `PaymentID == p.ID`, and `Data` decoding to `PaymentData{AccountID: p.AccountID, Amount: p.Amount, Currency: p.Currency, Status: "failed", FailureReason: "bank_declined"}`.

- [ ] **Step 2: Run them.** Run: `go test ./services/payment/internal/domain/...` Expected: FAIL.
- [ ] **Step 3: Implement the domain.**
- [ ] **Step 4: Run them.** Expected: PASS.
- [ ] **Step 5: Commit.** `git add -A && git commit -m "Add payment domain with the status state machine"`

### Task 10: Payment persistence

**Files:**
- Create: `services/payment/migration/postgres/20261122000001_init.{up,down}.sql`
- Create: `services/payment/internal/adapter/postgres/{postgres.go,create_payment.go,get_payment.go,update_payment.go,list_payments.go,save_outbox.go,read_outbox.go}`
- Test: `services/payment/test/integration/{main_test.go,postgres_test.go}`

**Interfaces:**
- Consumes: Task 9 domain; `transaction.Manager`.
- Produces `postgres.New(tx *transaction.Manager) *Postgres` with:

```go
func (p *Postgres) CreatePayment(ctx context.Context, payment domain.Payment) (created bool, err error) // false on idempotency_key conflict
func (p *Postgres) GetPayment(ctx context.Context, id uuid.UUID) (domain.Payment, error)
func (p *Postgres) GetPaymentByIdempotencyKey(ctx context.Context, key string) (domain.Payment, error)
func (p *Postgres) GetPaymentForUpdate(ctx context.Context, id uuid.UUID) (domain.Payment, error)
func (p *Postgres) UpdatePayment(ctx context.Context, payment domain.Payment) error // status, failure_reason, bank_reference; sets updated_at = now
func (p *Postgres) GetPayments(ctx context.Context, ids []uuid.UUID) ([]domain.Payment, error)
func (p *Postgres) ListPaymentsByStatus(ctx context.Context, status domain.Status) ([]domain.Payment, error)
func (p *Postgres) ListPaymentsUpdatedBetween(ctx context.Context, from, to time.Time) ([]domain.Payment, error)
func (p *Postgres) SaveOutbox(ctx context.Context, events ...domain.Event) error
func (p *Postgres) ReadOutbox(ctx context.Context, limit int) ([]domain.Event, error) // read-and-delete, FOR UPDATE SKIP LOCKED
```

Missing rows return `domain.ErrNotFound`. `SaveOutbox` and `ReadOutbox` follow the template's `save_outbox_kafka.go` and `read_outbox_kafka.go`, including trace header injection.

Schema (`up`):

```sql
BEGIN;

CREATE TABLE payment (
    id              UUID PRIMARY KEY,
    idempotency_key TEXT        NOT NULL UNIQUE,
    request_hash    TEXT        NOT NULL,
    account_id      UUID        NOT NULL,
    amount          BIGINT      NOT NULL CHECK (amount > 0),
    currency        CHAR(3)     NOT NULL,
    beneficiary     TEXT        NOT NULL,
    description     TEXT        NOT NULL DEFAULT '',
    status          TEXT        NOT NULL CHECK (status IN ('created', 'processing', 'succeeded', 'failed', 'review')),
    failure_reason  TEXT        NOT NULL DEFAULT '',
    bank_reference  TEXT        NOT NULL DEFAULT '',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX payment_updated_at ON payment (updated_at);
CREATE INDEX payment_open_status ON payment (status) WHERE status IN ('created', 'processing', 'review');

CREATE TABLE outbox (
    id         BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    event_id   UUID        NOT NULL UNIQUE,
    subject    TEXT        NOT NULL,
    payload    BYTEA       NOT NULL,
    headers    JSONB       NOT NULL DEFAULT '{}'
);

COMMIT;
```

- [ ] **Step 1: Write the failing integration tests** (suite: `testinfra.StartPostgres(t, "payment")`, migrate, real pool)

- `Test_CreatePayment_IdempotencyKey`: first insert → `created == true`; a second payment with the same key → `created == false`, no error; `GetPaymentByIdempotencyKey` returns the first one's ID.
- `Test_CreatePayment_ConcurrentSameKey`: 20 goroutines insert distinct payments under one key → exactly one `created == true`, no errors, `SELECT count(*) FROM payment WHERE idempotency_key = $1` is 1.
- `Test_UpdatePayment`: set status failed, reason `bank_declined`, bank reference `b-1` → `GetPayment` returns them and a later `updated_at`.
- `Test_GetPayment_NotFound`: `GetPayment`, `GetPaymentForUpdate`, `GetPaymentByIdempotencyKey` → `ErrNotFound`.
- `Test_ListPayments`: three payments, one moved to review → `ListPaymentsByStatus(review)` returns it alone; `ListPaymentsUpdatedBetween(now-1m, now+1m)` returns three; a window in the past returns none; `GetPayments` with two of the IDs returns two.
- `Test_Outbox_SaveAndRead`: save two events → `ReadOutbox(10)` inside a committed transaction returns both with their `ID`, `Subject`, `Payload`; `SELECT count(*) FROM outbox` is 0. Saving the same `ID` twice → error.
- `Test_Outbox_RollbackKeepsEvents`: `ReadOutbox` in a transaction that returns an error → the count is still 2.

- [ ] **Step 2: Run them.** Run: `go test -count=1 -tags=integration ./services/payment/test/integration/...` Expected: FAIL, undefined `postgres.New`.
- [ ] **Step 3: Write the migration and the adapter.** `CreatePayment` uses `ON CONFLICT (idempotency_key) DO NOTHING` and reports rows affected.
- [ ] **Step 4: Run them.** Expected: PASS.
- [ ] **Step 5: Commit.** `git add -A && git commit -m "Add payment and outbox persistence"`

### Task 11: Payment use cases

**Files:**
- Create: `services/payment/internal/dto/payment.go`
- Create: `services/payment/internal/usecase/{usecase.go,create_payment.go,get_payment.go,transition.go,start_processing.go,hold_funds.go,send_transfer.go,cancel_transfer.go,capture_funds.go,release_funds.go,complete_payment.go,fail_payment.go,mark_for_review.go,send_outbox.go}`
- Modify: `.mockery.yml` (add the payment usecase package)
- Generated: `services/payment/internal/usecase/mocks/mocks.go`
- Test: `services/payment/internal/usecase/{helpers_test.go,create_payment_test.go,saga_steps_test.go,send_outbox_test.go}`

**Interfaces:**
- Consumes: Task 9 domain; the Postgres methods of Task 10.
- Produces:

```go
// dto
type CreatePaymentInput struct {
	IdempotencyKey                     string
	AccountID                          uuid.UUID
	Amount                             int64
	Currency, Beneficiary, Description string
}
type GetPaymentInput struct{ ID uuid.UUID }
type PaymentOutput struct{ Payment domain.Payment }

// usecase ports
type Transactor interface {
	Wrap(ctx context.Context, fn func(ctx context.Context) error) error
}
type Postgres interface { /* exactly the ten methods listed in Task 10 */ }
type Ledger interface {
	Hold(ctx context.Context, paymentID, accountID uuid.UUID, amount int64, currency string) error
	Capture(ctx context.Context, paymentID uuid.UUID) error
	Release(ctx context.Context, paymentID uuid.UUID) error
	GetHolds(ctx context.Context, paymentIDs []uuid.UUID) (map[uuid.UUID]domain.HoldState, error) // absent IDs are missing from the map
}
type Bank interface {
	Transfer(ctx context.Context, reference string, amount int64, currency, beneficiary string) (domain.BankTransfer, error)
	Cancel(ctx context.Context, reference string) (domain.BankTransfer, error)
	GetTransfer(ctx context.Context, reference string) (*domain.BankTransfer, error) // nil, nil when the bank has none
	GetStatement(ctx context.Context, from, to time.Time) ([]domain.BankTransfer, error)
}
type Publisher interface {
	Publish(ctx context.Context, events ...domain.Event) error
}
type Orchestrator interface {
	StartPayment(ctx context.Context, paymentID uuid.UUID) error // idempotent
	SignalBankOutcome(ctx context.Context, paymentID uuid.UUID, outcome domain.BankOutcome) error
}

func New(tx Transactor, postgres Postgres, ledger Ledger, bank Bank, publisher Publisher, orchestrator Orchestrator) *UseCase

// API
func (u *UseCase) CreatePayment(ctx context.Context, in dto.CreatePaymentInput) (dto.PaymentOutput, error)
func (u *UseCase) GetPayment(ctx context.Context, in dto.GetPaymentInput) (dto.PaymentOutput, error)
// saga steps, called from activities
func (u *UseCase) StartProcessing(ctx context.Context, paymentID uuid.UUID) error
func (u *UseCase) HoldFunds(ctx context.Context, paymentID uuid.UUID) (domain.FailureReason, error)
func (u *UseCase) SendTransfer(ctx context.Context, paymentID uuid.UUID) (domain.BankOutcome, error)
func (u *UseCase) CancelTransfer(ctx context.Context, paymentID uuid.UUID) (domain.BankOutcome, error)
func (u *UseCase) CaptureFunds(ctx context.Context, paymentID uuid.UUID) error
func (u *UseCase) ReleaseFunds(ctx context.Context, paymentID uuid.UUID) error
func (u *UseCase) CompletePayment(ctx context.Context, paymentID uuid.UUID) error
func (u *UseCase) FailPayment(ctx context.Context, paymentID uuid.UUID, reason domain.FailureReason) error
func (u *UseCase) MarkPaymentForReview(ctx context.Context, paymentID uuid.UUID) error
// outbox
func (u *UseCase) SendOutbox(ctx context.Context, limit int) (int, error)
```

Rules:
- `Ledger` and `Bank` adapters return `domain` errors: `Hold` → `ErrInsufficientFunds`, `ErrAccountNotFound`, `ErrLedgerRejected`; `Capture`/`Release` → `ErrLedgerConflict`; `Transfer`/`Cancel`/`GetTransfer`/`GetStatement` → `ErrBankUnavailable` when the outcome is unknown.
- `CreatePayment`: `NewPayment`; `CreatePayment` in Postgres. Not created → `GetPaymentByIdempotencyKey`; a different `RequestHash` → `ErrIdempotencyConflict`; otherwise continue with the stored payment. Then `orchestrator.StartPayment(id)`; its error is returned (the payment stays `created`).
- Status changes go through one private helper in `transition.go`: in a transaction, `GetPaymentForUpdate`, `TransitionTo`; unchanged → return nil with no writes; changed → apply the mutation, `UpdatePayment`, and `SaveOutbox` when the step has an event. Events: `CompletePayment` → `TypePaymentSucceeded`; `FailPayment` → `TypePaymentFailed`; `MarkPaymentForReview` → `TypePaymentReviewRequired`; `StartProcessing` → none.
- `HoldFunds`: load the payment, call `ledger.Hold`. `ErrInsufficientFunds` → `(ReasonInsufficientFunds, nil)`; `ErrAccountNotFound` → `(ReasonAccountNotFound, nil)`; `ErrLedgerRejected` → `(ReasonLedgerRejected, nil)`; nil → `(ReasonNone, nil)`; any other error → `("", err)`.
- `SendTransfer`: `bank.Transfer(reference = payment ID string, ...)`; on success store `BankReference = transfer.ID` when it is empty, return `transfer.Status`. `CancelTransfer`: the same with `bank.Cancel`. Errors are returned unchanged.
- `SendOutbox`: the template's `send_outbox_kafka.go` with `Publisher`.

- [ ] **Step 1: Write ports and DTOs, generate mocks.** Run: `make generate`. Expected: mocks for `Postgres`, `Ledger`, `Bank`, `Publisher`, `Orchestrator`, `Transactor`.

- [ ] **Step 2: Write the failing unit tests**

`Test_CreatePayment`:

| case | mocks | want | assertions |
|---|---|---|---|
| new | `CreatePayment` → true | nil | `StartPayment` called with the new ID; output status created |
| replay | `CreatePayment` → false; stored payment has the same hash | nil | output ID is the stored ID; `StartPayment` called with the stored ID |
| key reused | `CreatePayment` → false; stored hash differs | `ErrIdempotencyConflict` | `StartPayment` not called |
| invalid | amount 0 | `ErrValidation` | no Postgres call |
| no key | empty key | `ErrIdempotencyKeyRequired` | no Postgres call |
| start fails | `StartPayment` → error | that error | |

`Test_HoldFunds`:

| `ledger.Hold` returns | want reason | want error |
|---|---|---|
| nil | `""` | nil |
| `ErrInsufficientFunds` | `insufficient_funds` | nil |
| `ErrAccountNotFound` | `account_not_found` | nil |
| `ErrLedgerRejected` | `ledger_rejected` | nil |
| `errors.New("down")` | `""` | that error |

Also: payment missing → `ErrNotFound`. `ledger.Hold` receives the payment's ID, account, amount and currency.

`Test_SendTransfer`: bank returns `{ID: "b-1", Status: succeeded}` → outcome succeeded, `UpdatePayment` saves `BankReference == "b-1"`; returns declined → outcome declined; returns `ErrBankUnavailable` → that error, no `UpdatePayment`. `bank.Transfer` receives `reference == payment ID string`. `Test_CancelTransfer`: the same three rows against `bank.Cancel`, with `cancelled` in place of `declined`.

`Test_Transitions`:

| call | stored status | want error | `UpdatePayment` | `SaveOutbox` |
|---|---|---|---|---|
| `StartProcessing` | created | nil | status processing | not called |
| `StartProcessing` | processing | nil | not called | not called |
| `CompletePayment` | processing | nil | status succeeded | one event, subject `events.payment.succeeded` |
| `CompletePayment` | succeeded | nil | not called | not called |
| `CompletePayment` | failed | `ErrInvalidTransition` | not called | not called |
| `FailPayment(bank_declined)` | processing | nil | status failed, reason `bank_declined` | subject `events.payment.failed` |
| `FailPayment(bank_cancelled)` | review | nil | status failed | subject `events.payment.failed` |
| `MarkPaymentForReview` | processing | nil | status review | subject `events.payment.review_required` |
| any of them | `GetPaymentForUpdate` → `ErrNotFound` | `ErrNotFound` | not called | not called |

`Test_CaptureFunds` / `Test_ReleaseFunds`: delegate to the ledger with the payment ID; `ErrLedgerConflict` is returned unchanged.

`Test_SendOutbox_*`: the template's three tests (`Empty`, `Success`, `ProduceError`) against `Publisher`.

- [ ] **Step 3: Run them.** Run: `go test ./services/payment/internal/usecase/...` Expected: FAIL.
- [ ] **Step 4: Implement the use cases.**
- [ ] **Step 5: Run them.** Expected: PASS.
- [ ] **Step 6: Commit.** `git add -A && git commit -m "Add payment use cases: API, saga steps, outbox relay"`

### Task 12: Payment saga workflow

**Files:**
- Create: `services/payment/internal/saga/{names.go,payment_workflow.go}`
- Test: `services/payment/internal/saga/payment_workflow_test.go`

**Interfaces:**
- Consumes: `domain.Status`, `domain.FailureReason`, `domain.BankOutcome` (Task 9).
- Produces:

```go
package saga

const (
	PaymentTaskQueue  = "payment"
	BankOutcomeSignal = "bank-outcome" // payload: domain.BankOutcome
	ErrTypeFatal      = "Fatal"        // non-retryable ApplicationError type

	ActivityStartProcessing      = "StartProcessing"      // (uuid.UUID) error
	ActivityHoldFunds            = "HoldFunds"            // (uuid.UUID) (domain.FailureReason, error)
	ActivitySendTransfer         = "SendTransfer"         // (uuid.UUID) (domain.BankOutcome, error)
	ActivityCancelTransfer       = "CancelTransfer"       // (uuid.UUID) (domain.BankOutcome, error)
	ActivityCaptureFunds         = "CaptureFunds"         // (uuid.UUID) error
	ActivityReleaseFunds         = "ReleaseFunds"         // (uuid.UUID) error
	ActivityCompletePayment      = "CompletePayment"      // (uuid.UUID) error
	ActivityFailPayment          = "FailPayment"          // (FailPaymentInput) error
	ActivityMarkPaymentForReview = "MarkPaymentForReview" // (uuid.UUID) error
)

func PaymentWorkflowID(paymentID uuid.UUID) string // "payment-" + id

type PaymentInput struct {
	PaymentID            uuid.UUID
	ActivityTimeout      time.Duration // StartToClose of every activity
	RetryInitialInterval time.Duration
	RetryMaxInterval     time.Duration
	BankMaxAttempts      int32
}
type FailPaymentInput struct {
	PaymentID uuid.UUID
	Reason    domain.FailureReason
}

func PaymentWorkflow(ctx workflow.Context, in PaymentInput) (domain.Status, error)
```

The workflow calls activities **by name** with the constants above, so this package never imports the controller. Two activity option sets, both with `StartToCloseTimeout: in.ActivityTimeout`, backoff coefficient 2, `InitialInterval` and `MaximumInterval` from the input, `NonRetryableErrorTypes: []string{ErrTypeFatal}`:
- internal steps (everything except the two bank calls): `MaximumAttempts: 0` (unlimited);
- bank calls (`SendTransfer`, `CancelTransfer`): `MaximumAttempts: in.BankMaxAttempts`.

Flow (spec section 6.3):

```
StartProcessing
reason := HoldFunds
    reason != ""                      -> FailPayment(reason)                      -> return failed
outcome, err := SendTransfer
    err is ApplicationError(Fatal)    -> return the error
    err != nil (outcome unknown)      -> outcome, err = CancelTransfer
        err is ApplicationError(Fatal)-> return the error
        err != nil (still unknown)    -> MarkPaymentForReview; outcome = receive BankOutcomeSignal
switch outcome
    succeeded -> CaptureFunds; CompletePayment                                    -> return succeeded
    declined  -> ReleaseFunds; FailPayment(bank_declined)                         -> return failed
    cancelled -> ReleaseFunds; FailPayment(bank_cancelled)                        -> return failed
```

- [ ] **Step 1: Write the failing table test** (`testsuite.WorkflowTestSuite`). A helper registers a stub for every activity name with `env.RegisterActivityWithOptions(stub, activity.RegisterOptions{Name: saga.ActivityX})`, then each row sets `env.OnActivity(saga.ActivityX, mock.Anything, ...)`. Input: `ActivityTimeout: time.Second`, `RetryInitialInterval: time.Millisecond`, `RetryMaxInterval: 10 * time.Millisecond`, `BankMaxAttempts: 3`. Signals are sent with `env.RegisterDelayedCallback(func() { env.SignalWorkflow(saga.BankOutcomeSignal, outcome) }, time.Minute)`.

| # | HoldFunds | SendTransfer | CancelTransfer | signal | want result | must run | must not run |
|---|---|---|---|---|---|---|---|
| 1 | `""` | succeeded | | | succeeded | Capture, Complete | Release, Fail, Cancel |
| 2 | `insufficient_funds` | | | | failed | Fail(`insufficient_funds`) | SendTransfer, Release |
| 3 | `account_not_found` | | | | failed | Fail(`account_not_found`) | SendTransfer |
| 4 | `ledger_rejected` | | | | failed | Fail(`ledger_rejected`) | SendTransfer |
| 5 | `""` | declined | | | failed | Release, Fail(`bank_declined`) | Capture, Cancel |
| 6 | `""` | error | succeeded | | succeeded | Capture, Complete; SendTransfer exactly 3 times | Release |
| 7 | `""` | error | cancelled | | failed | Release, Fail(`bank_cancelled`) | Capture |
| 8 | `""` | error | declined | | failed | Release, Fail(`bank_declined`) | Capture |
| 9 | `""` | error | error | succeeded | succeeded | MarkPaymentForReview, Capture, Complete; CancelTransfer exactly 3 times | Release |
| 10 | `""` | error | error | cancelled | failed | MarkPaymentForReview, Release, Fail(`bank_cancelled`) | Capture |
| 11 | `""` | error | error | declined | failed | MarkPaymentForReview, Release, Fail(`bank_declined`) | Capture |
| 12 | `""` | succeeded | | | succeeded | CaptureFunds fails twice, then succeeds: called exactly 3 times | Release |

Two more tests:
- `Test_PaymentWorkflow_FatalStopsTheWorkflow`: `StartProcessing` returns `temporal.NewNonRetryableApplicationError("x", saga.ErrTypeFatal, nil)` → `env.GetWorkflowError()` is non-nil and `HoldFunds` never runs.
- `Test_PaymentWorkflow_FatalFromBankIsNotTreatedAsUnknown`: `SendTransfer` returns the same fatal error → workflow error, `CancelTransfer` and `ReleaseFunds` never run.

- [ ] **Step 2: Run it.** Run: `go test ./services/payment/internal/saga/...` Expected: FAIL, undefined `saga.PaymentWorkflow`. (`go get go.temporal.io/sdk@latest` first.)
- [ ] **Step 3: Implement the workflow.**
- [ ] **Step 4: Run it.** Expected: PASS.
- [ ] **Step 5: Commit.** `git add -A && git commit -m "Add the payment saga workflow"`

### Task 13: Ledger and bank adapters

**Files:**
- Create: `services/payment/internal/adapter/ledger/ledger.go`, `services/payment/internal/adapter/bank/bank.go`
- Test: `services/payment/internal/adapter/bank/bank_test.go` (no build tag), `services/payment/test/integration/ledger_adapter_test.go`

**Interfaces:**
- Consumes: `ledger_v1.NewLedgerV1Client` and `ledger.Run` (Task 6); `gen/mockbank/http/client` and `mockbank.Run` (Task 8); the `Ledger` and `Bank` ports (Task 11).
- Produces:

```go
// adapter/ledger
type Config struct {
	Addr string `env:"LEDGER_ADDR,required"`
}
func New(c Config) (*Ledger, error) // grpc.NewClient, insecure credentials, otelgrpc client handler
func (l *Ledger) Close()
// implements usecase.Ledger

// adapter/bank
type Config struct {
	URL     string        `env:"BANK_URL,required"` // e.g. http://localhost:8090/api/v1
	Timeout time.Duration `env:"BANK_TIMEOUT" envDefault:"3s"`
}
func New(c Config) (*Bank, error)
// implements usecase.Bank
```

Ledger mapping:

| gRPC code | `Hold` returns | `Capture` / `Release` return |
|---|---|---|
| OK | nil | nil |
| `NotFound` | `ErrAccountNotFound` | `ErrLedgerConflict` |
| `FailedPrecondition` | `ErrInsufficientFunds` | `ErrLedgerConflict` |
| `InvalidArgument`, `AlreadyExists` | `ErrLedgerRejected` | `ErrLedgerConflict` |
| `Aborted` | `ErrLedgerRejected` | `ErrLedgerConflict` |
| anything else | the wrapped error | the wrapped error |

Bank mapping:

| HTTP result | `Transfer` / `Cancel` | `GetTransfer` | `GetStatement` |
|---|---|---|---|
| 200 | the transfer | the transfer | the transfers |
| 400, 409 | a transfer with status `declined` | | |
| 404 | | `nil, nil` | |
| 5xx, timeout, connection error | `ErrBankUnavailable` | `ErrBankUnavailable` | `ErrBankUnavailable` |

`Transfer` sends `Idempotency-Key: <reference>`. `BankTransfer.ID` is the bank's transfer `id`.

- [ ] **Step 1: Write the failing bank adapter test** (starts `mockbank.Run` on a free port; `bank.Config{URL: ..., Timeout: 300 * time.Millisecond}`)

| call | want |
|---|---|
| `Transfer(ref, 3000, "EUR", "DE-OK-1")` | status succeeded, non-empty `ID`, `Reference == ref` |
| `Transfer(ref, 3000, "EUR", "DECLINE-1")` | status declined |
| `Transfer(ref, 3000, "EUR", "TIMEOUT-1")` | `ErrBankUnavailable` |
| then `Cancel(ref)` | status succeeded |
| `Cancel(fresh)` | status cancelled |
| then `Transfer(fresh, ...)` | status cancelled |
| `GetTransfer(missing)` | `nil, nil` |
| `GetStatement(now-1h, now+1h)` | contains the succeeded transfers only |
| bank set unavailable through the admin endpoint | `Transfer`, `Cancel`, `GetTransfer`, `GetStatement` → `ErrBankUnavailable` |
| mock bank stopped | `Transfer` → `ErrBankUnavailable` |

- [ ] **Step 2: Write the failing ledger adapter test** (`//go:build integration`; Postgres `ledger` + `ledger.Run`; fund an account with 1000 EUR through the generated client)

| call | want |
|---|---|
| `Hold` 500 EUR | nil |
| `Hold` 5000 EUR, new payment | `ErrInsufficientFunds` |
| `Hold` on a random account | `ErrAccountNotFound` |
| `Hold` 500 USD | `ErrLedgerRejected` |
| `Capture` of the first payment, twice | nil, nil |
| `Release` of the captured payment | `ErrLedgerConflict` |
| `Capture` of a random payment | `ErrLedgerConflict` |
| `GetHolds([first, random])` | map with `first → captured` and no entry for `random` |

- [ ] **Step 3: Run them.** Run: `go test ./services/payment/internal/adapter/... && go test -count=1 -tags=integration ./services/payment/test/integration/...` Expected: FAIL.
- [ ] **Step 4: Implement both adapters.**
- [ ] **Step 5: Run them.** Expected: PASS.
- [ ] **Step 6: Commit.** `git add -A && git commit -m "Add ledger and bank client adapters with outcome mapping"`

### Task 14: Outbox relay to JetStream

**Files:**
- Create: `services/payment/internal/adapter/nats_publisher/publisher.go`
- Create: `services/payment/internal/controller/outbox_worker/worker.go`
- Test: `services/payment/test/integration/outbox_test.go`

**Interfaces:**
- Consumes: `natsjs.Client` (Task 3), `events.StreamConfig` (Task 2), `usecase.SendOutbox` (Task 11), `metrics.Process` (Task 1).
- Produces:

```go
// adapter/nats_publisher
func New(c *natsjs.Client, m *metrics.Process) *Publisher
func (p *Publisher) Publish(ctx context.Context, events ...domain.Event) error
// one js.PublishMsg per event: Subject, Data = Payload, headers copied, jetstream.WithMsgID(event.ID.String())

// controller/outbox_worker: the template's worker, calling usecase.SendOutbox
type Config struct {
	Interval time.Duration `env:"OUTBOX_INTERVAL" envDefault:"1s"`
	Limit    int           `env:"OUTBOX_LIMIT"    envDefault:"100"`
}
func New(c Config, uc *usecase.UseCase) (*Worker, error)
func (w *Worker) Close()
```

Tracing in `Publish` mirrors the template's `kafka_producer/produce.go`: a producer span per event named `"nats publish " + subject` with attribute `messaging.message.id` = the event ID, trace context injected into the headers.

- [ ] **Step 1: Write the failing integration tests** (Postgres `payment` + NATS; streams ensured in `SetupSuite`; the use case is built with real Postgres and the publisher, and mocks for the ports it does not touch)

- `Test_SendOutbox_PublishesAndDeletes`: `SaveOutbox` two events (subjects `events.payment.succeeded` and `events.payment.failed`) → `SendOutbox(10)` returns 2; the outbox table is empty; the `EVENTS` stream holds 2 messages whose `Nats-Msg-Id` headers are the two event IDs and whose data equal the payloads.
- `Test_SendOutbox_PublishFailureKeepsEvents` (spec scenario 12): a use case built with a `Publisher` that returns an error → `SendOutbox` returns the error, the table still has the event; the same call with the real publisher → returns 1, the table is empty.
- `Test_Publish_SameEventIDIsDeduplicated`: publishing one event twice → the stream's message count grows by 1.
- `Test_OutboxWorker_DrainsInBackground`: worker with `Interval: 50 * time.Millisecond`; save an event → `require.Eventually` the table is empty within 5s.

- [ ] **Step 2: Run them.** Run: `go test -count=1 -tags=integration ./services/payment/test/integration/...` Expected: FAIL, undefined `nats_publisher.New`.
- [ ] **Step 3: Implement publisher and worker.**
- [ ] **Step 4: Run them.** Expected: PASS.
- [ ] **Step 5: Commit.** `git add -A && git commit -m "Relay the payment outbox to NATS JetStream"`

### Task 15: Payment API, Temporal wiring, first end-to-end scenarios

**Files:**
- Create: `api/payment/http/payment_v1.yaml`; `gen/payment/http/{server,client}/{generate.go,config.yaml}` (generated code beside them)
- Create: `services/payment/internal/adapter/orchestrator/orchestrator.go`
- Create: `services/payment/internal/controller/temporal_worker/{worker.go,activities.go}`
- Create: `services/payment/internal/controller/http/router.go`, `services/payment/internal/controller/http/v1/{v1.go,error.go,create_payment.go,get_payment.go}`
- Create: `services/payment/internal/app/app.go`, `services/payment/config/config.go`, `services/payment/payment.go`, `services/payment/cmd/app/main.go`, `services/payment/.env.example`
- Test: `services/payment/internal/controller/temporal_worker/activities_test.go`, `test/e2e/{main_test.go,helpers_test.go,payment_test.go}`

**Interfaces:**
- Consumes: everything from Tasks 9 to 14; `ledger.Run`, `mockbank.Run`; `temporalclient.New`.
- Produces:

```go
// adapter/orchestrator
type Config struct {
	ActivityTimeout      time.Duration `env:"SAGA_ACTIVITY_TIMEOUT"       envDefault:"10s"`
	RetryInitialInterval time.Duration `env:"SAGA_RETRY_INITIAL_INTERVAL" envDefault:"1s"`
	RetryMaxInterval     time.Duration `env:"SAGA_RETRY_MAX_INTERVAL"     envDefault:"30s"`
	BankMaxAttempts      int32         `env:"SAGA_BANK_MAX_ATTEMPTS"      envDefault:"3"`
}
func New(c client.Client, cfg Config) *Orchestrator
// StartPayment: ExecuteWorkflow(saga.PaymentWorkflow, saga.PaymentInput{...}) with
//   ID saga.PaymentWorkflowID(id), TaskQueue saga.PaymentTaskQueue,
//   WorkflowIDReusePolicy REJECT_DUPLICATE. An existing or finished run is not an error.
// SignalBankOutcome: SignalWorkflow(saga.PaymentWorkflowID(id), "", saga.BankOutcomeSignal, outcome)

// controller/temporal_worker
type Config struct{} // fields arrive in Task 20
func New(c client.Client, cfg Config, uc *usecase.UseCase) (*Worker, error) // registers PaymentWorkflow and every activity under its saga.Activity* name
func (w *Worker) Close()
type Activities struct{ /* usecase */ }
// one method per saga.Activity* constant, with the signature noted beside the constant

// service
func payment.Run(ctx context.Context, c config.Config) error
type config.Config struct {
	HTTP           httpserver.Config
	Logger         logger.Config
	OTEL           otel.Config
	Postgres       postgres.Config
	NATS           natsjs.Config
	Temporal       temporalclient.Config
	Ledger         ledger.Config
	Bank           bank.Config
	Saga           orchestrator.Config
	TemporalWorker temporal_worker.Config
	OutboxWorker   outbox_worker.Config
}
```

Activities register with `worker.RegisterActivityWithOptions(a.Method, activity.RegisterOptions{Name: saga.ActivityX})`. Every activity maps its use case error: `domain.IsFatal(err)` → `temporal.NewNonRetryableApplicationError(err.Error(), saga.ErrTypeFatal, err)`; anything else is returned as is.

HTTP contract (`servers: [{url: /api/v1}]`):

| operationId | method and path | request | responses |
|---|---|---|---|
| `createPayment` | `POST /payments` | header `Idempotency-Key` (required, 1..255); body `{account_id: uuid, amount: int64 1..1000000000000, currency: ^[A-Z]{3}$, beneficiary: 1..140, description?: 0..255}` | `202 Payment`, `400`, `409`, `500` |
| `getPayment` | `GET /payments/{id}` (uuid) | | `200 Payment`, `400`, `404`, `500` |

`Payment`: `{id, account_id, amount, currency, beneficiary, description, status: enum[created, processing, succeeded, failed, review], failure_reason, bank_reference, created_at, updated_at}`. Handler mapping: `ErrIdempotencyConflict` → 409; `IsInvalidInput` → 400; `ErrNotFound` → 404.

`app.Run` order: pool, transaction manager, NATS client + `EnsureStreams(events.StreamConfig(), events.DLQStreamConfig())`, Temporal client, ledger adapter, bank adapter, publisher, orchestrator, use case, Temporal worker, outbox worker, HTTP server. Close in reverse, as in the template.

e2e suite (`//go:build integration`, package `e2e`), produced for Tasks 17 and 20 to extend:

```go
type Suite struct {
	suite.Suite
	*require.Assertions

	// Both generated packages are named http_client; payment_client and bank_client are import aliases.
	payments  *payment_client.ClientWithResponses // gen/payment/http/client
	bank      *bank_client.ClientWithResponses    // gen/mockbank/http/client
	ledger    ledger_v1.LedgerV1Client
	temporal  client.Client
	paymentDB *pgx.Conn
	ledgerDB  *pgx.Conn
}

func (s *Suite) openAccount(currency string, deposit int64) uuid.UUID
func (s *Suite) createPayment(key string, accountID uuid.UUID, amount int64, currency, beneficiary string) (id uuid.UUID, httpStatus int)
func (s *Suite) waitStatus(id uuid.UUID, want string) payment_client.Payment // Eventually, 30s, polling every 100ms
func (s *Suite) balance(accountID uuid.UUID) (balance, held, available int64)
func (s *Suite) bankTransferStatus(reference string) string // "" when the bank answers 404
func (s *Suite) requireLedgerIntact()                       // the two queries from Task 5 return 0
```

`SetupSuite`: `StartPostgres(t, "payment", "ledger", "notification")`; migrate `payment` and `ledger`; `StartNATS`; `StartTemporal`; start `mockbank.Run`, `ledger.Run`, `payment.Run` in goroutines on free ports; `WaitHTTP` on each `/ready`. Saga config: `ActivityTimeout 2s`, `RetryInitialInterval 50ms`, `RetryMaxInterval 200ms`, `BankMaxAttempts 3`; bank timeout `500ms`; outbox interval `100ms`. `TearDownSuite` cancels the context and requires every `Run` to return nil.

- [ ] **Step 1: Write the contract and generate.** Run: `make generate`. Expected: server and client generated, `go build ./gen/...` passes.

- [ ] **Step 2: Write the failing activities test** (use case built from mocks)

| activity | use case result | want |
|---|---|---|
| `StartProcessing` | `ErrNotFound` | `*temporal.ApplicationError`, `Type() == "Fatal"`, `NonRetryable() == true` |
| `CompletePayment` | `ErrInvalidTransition` | same |
| `CaptureFunds` | `ErrLedgerConflict` | same |
| `CaptureFunds` | `errors.New("down")` | a plain error, not an `ApplicationError` |
| `SendTransfer` | `ErrBankUnavailable` | a plain error |
| `HoldFunds` | `ErrInsufficientFunds` from the ledger | `("insufficient_funds", nil)` |

- [ ] **Step 3: Write the failing e2e tests**

`Test_Payment_Outcomes` (each row: open a EUR account with the deposit, create the payment for 3000 unless stated, wait for the status):

| # | case | deposit | beneficiary | want status | want reason | want balance / held | want bank transfer |
|---|---|---|---|---|---|---|---|
| 1 | success | 10000 | `DE-OK-1` | succeeded | `""` | 7000 / 0 | succeeded |
| 2 | insufficient funds | 1000 | `DE-OK-2` | failed | `insufficient_funds` | 1000 / 0 | `""` |
| 3 | bank decline | 10000 | `DECLINE-1` | failed | `bank_declined` | 10000 / 0 | declined |
| 4 | transient bank errors | 10000 | `FLAKY-1` | succeeded | `""` | 7000 / 0 | succeeded |
| 5 | timeout after processing | 10000 | `TIMEOUT-1` | succeeded | `""` | 7000 / 0 | succeeded |
| 6 | unknown account | (random account ID) | `DE-OK-3` | failed | `account_not_found` | | `""` |
| 7 | other currency | 10000 EUR, payment in USD | `DE-OK-4` | failed | `ledger_rejected` | 10000 / 0 | `""` |

After every row: `requireLedgerIntact()`. For rows that succeed, `bank_reference` in the API response is non-empty.

`Test_Payment_Idempotency`:
- the same key and body twice → both `202`, the same ID;
- the same key with amount 3001 → `409`;
- 10 goroutines posting the same key and body → all `202` with one ID; `SELECT count(*) FROM payment WHERE idempotency_key = $1` is 1; the payment ends succeeded and the balance dropped by 3000 once.

`Test_Payment_Validation` (each → `400`, and `SELECT count(*) FROM payment` is unchanged):

| request |
|---|
| no `Idempotency-Key` header |
| `Idempotency-Key` of 256 characters |
| amount 0 |
| amount -5 |
| amount 1000000000001 |
| currency `eur` |
| `account_id: "nope"` |
| empty beneficiary |

`Test_Payment_Get`: unknown UUID → `404`; `GET /payments/nope` → `400`.

`Test_Payment_ConcurrentOverdraft` (spec scenario 10): deposit 10000; 10 payments of 3000 in parallel under distinct keys → exactly 3 end succeeded and 7 end failed with `insufficient_funds`; balance 1000, held 0; `requireLedgerIntact()`.

- [ ] **Step 4: Run them.** Run: `go test ./services/payment/... && go test -count=1 -tags=integration ./test/e2e/...` Expected: FAIL, undefined `payment.Run` and `temporal_worker.Activities`.
- [ ] **Step 5: Implement orchestrator, worker and activities, HTTP controller, app, config, `Run`, `main`.**
- [ ] **Step 6: Run them.** Expected: PASS.
- [ ] **Step 7: Commit.** `git add -A && git commit -m "Wire the payment service: HTTP API, Temporal worker, end-to-end payment scenarios"`

---

## Phase 5: Notification

### Task 16: Notification core

**Files:**
- Create: `services/notification/migration/postgres/20261122000001_init.{up,down}.sql`
- Create: `services/notification/internal/domain/{notification.go,error.go}`
- Create: `services/notification/internal/usecase/{usecase.go,handle_event.go}`
- Create: `services/notification/internal/adapter/postgres/{postgres.go,inbox.go,notification.go}`
- Modify: `.mockery.yml` (add the notification usecase package)
- Test: `services/notification/internal/usecase/handle_event_test.go`, `services/notification/test/integration/{main_test.go,postgres_test.go}`

**Interfaces:**
- Consumes: `events.Envelope` (Task 2).
- Produces:

```go
// domain
type Status string // StatusPending = "pending", StatusSent = "sent"
type Notification struct {
	ID, EventID      uuid.UUID
	EventType        string
	PaymentID        uuid.UUID
	Payload          []byte // the envelope as JSON
	Status           Status
	CreatedAt        time.Time
}
var ErrNotFound = errors.New("not found")
func NewNotification(e events.Envelope) (Notification, error) // pending, Payload = e.Marshal()

// usecase
type Transactor interface {
	Wrap(ctx context.Context, fn func(ctx context.Context) error) error
}
type Postgres interface {
	SaveInbox(ctx context.Context, eventID uuid.UUID, eventType string) (inserted bool, err error)
	CreateNotification(ctx context.Context, n domain.Notification) error
	GetNotificationByEventID(ctx context.Context, eventID uuid.UUID) (domain.Notification, error)
	MarkNotificationSent(ctx context.Context, id uuid.UUID) error
}
type Sender interface {
	Send(ctx context.Context, n domain.Notification) error
}
func New(tx Transactor, postgres Postgres, sender Sender) *UseCase
func (u *UseCase) HandleEvent(ctx context.Context, e events.Envelope) error
```

Schema:

```sql
BEGIN;

CREATE TABLE inbox (
    event_id    UUID PRIMARY KEY,
    event_type  TEXT        NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE notification (
    id         UUID PRIMARY KEY,
    event_id   UUID        NOT NULL UNIQUE REFERENCES inbox (event_id),
    event_type TEXT        NOT NULL,
    payment_id UUID        NOT NULL,
    payload    JSONB       NOT NULL,
    status     TEXT        NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'sent')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    sent_at    TIMESTAMPTZ
);

COMMIT;
```

`HandleEvent` (spec section 7.3):
1. In one transaction: `SaveInbox`; when inserted, `CreateNotification`.
2. `GetNotificationByEventID`. Status sent → return nil.
3. `sender.Send`; its error is returned.
4. `MarkNotificationSent`.

- [ ] **Step 1: Write ports, generate mocks.** Run: `make generate`.

- [ ] **Step 2: Write the failing unit test** `Test_HandleEvent`:

| case | mocks | want error | `CreateNotification` | `Send` | `MarkNotificationSent` |
|---|---|---|---|---|---|
| first delivery | `SaveInbox` → true; stored pending | nil | once | once | once |
| duplicate, already sent | `SaveInbox` → false; stored sent | nil | never | never | never |
| duplicate, not yet sent | `SaveInbox` → false; stored pending | nil | never | once | once |
| sender fails | `SaveInbox` → true; `Send` → error | that error | once | once | never |
| inbox write fails | `SaveInbox` → error | that error | never | never | never |

`Send` receives a notification whose `EventID` is the envelope ID.

- [ ] **Step 3: Write the failing integration test** (Postgres `notification`)

- `Test_SaveInbox_Deduplicates`: first call → true; second with the same ID → false, no error.
- `Test_SaveInbox_ConcurrentSameEvent`: 20 goroutines → exactly one true.
- `Test_Notification_Lifecycle`: after `SaveInbox`, `CreateNotification` → `GetNotificationByEventID` returns status pending and the payload; `MarkNotificationSent` → status sent and `sent_at` set; unknown event ID → `ErrNotFound`.

- [ ] **Step 4: Run them.** Run: `go test ./services/notification/... && go test -count=1 -tags=integration ./services/notification/test/integration/...` Expected: FAIL.
- [ ] **Step 5: Implement domain, use case, migration, adapter.** `SaveInbox` uses `ON CONFLICT (event_id) DO NOTHING`.
- [ ] **Step 6: Run them.** Expected: PASS.
- [ ] **Step 7: Commit.** `git add -A && git commit -m "Add notification core with an inbox for exactly-once effects"`

### Task 17: Notification consumer, webhook sender, service wiring

**Files:**
- Create: `services/notification/internal/adapter/webhook/webhook.go`
- Create: `services/notification/internal/controller/nats_consumer/consumer.go`
- Create: `services/notification/internal/app/app.go`, `services/notification/config/config.go`, `services/notification/notification.go`, `services/notification/cmd/app/main.go`, `services/notification/.env.example`
- Modify: `test/e2e/{main_test.go,helpers_test.go}`; Create: `test/e2e/notification_test.go`
- Test: `services/notification/test/integration/consumer_test.go`

**Interfaces:**
- Consumes: Task 16 use case; `natsjs.Client`; `events` stream configs; the e2e `Suite` (Task 15).
- Produces:

```go
// adapter/webhook
type Config struct {
	URL     string        `env:"NOTIFICATION_WEBHOOK_URL"`
	Timeout time.Duration `env:"NOTIFICATION_WEBHOOK_TIMEOUT" envDefault:"5s"`
}
func New(c Config) *Sender
// Send: empty URL -> log the notification and return nil.
// Otherwise POST Payload with Content-Type: application/json and Idempotency-Key: <EventID>; non-2xx is an error.

// controller/nats_consumer
type Config struct {
	Durable    string        `env:"NATS_CONSUMER_DURABLE"     envDefault:"notification"`
	MaxDeliver int           `env:"NATS_CONSUMER_MAX_DELIVER" envDefault:"5"`
	RetryDelay time.Duration `env:"NATS_CONSUMER_RETRY_DELAY" envDefault:"1s"`
	AckWait    time.Duration `env:"NATS_CONSUMER_ACK_WAIT"    envDefault:"30s"`
}
func New(ctx context.Context, c Config, client *natsjs.Client, uc *usecase.UseCase) (*Consumer, error)
func (c *Consumer) Close()

// service
func notification.Run(ctx context.Context, c config.Config) error
type config.Config struct {
	HTTP     httpserver.Config
	Logger   logger.Config
	OTEL     otel.Config
	Postgres postgres.Config
	NATS     natsjs.Config
	Consumer nats_consumer.Config
	Webhook  webhook.Config
}

// e2e additions
type webhookCall struct {
	IdempotencyKey string
	Envelope       events.Envelope
}
func (s *Suite) webhookCalls(paymentID uuid.UUID) []webhookCall // calls received so far for a payment
func (s *Suite) webhookCallsOfType(t events.Type) []webhookCall // calls received so far for an event type
```

Consumer: a durable pull consumer on stream `EVENTS`, filter `events.>`, explicit ack, `AckWait` and `MaxDeliver` from the config, consumed with `Consume`. Per message:
- `events.Unmarshal` fails → publish the raw message to `dlq.notification`, then `Term()`.
- `HandleEvent` succeeds → `Ack()`.
- `HandleEvent` fails and `Metadata().NumDelivered >= MaxDeliver` → publish to `dlq.notification`, then `Term()`.
- `HandleEvent` fails otherwise → `NakWithDelay(RetryDelay)`.

The consumer span continues the trace from the message headers, as the template's `kafka_consumer` does. It is named `"nats consume " + subject` and carries attribute `messaging.message.id` = the envelope ID. `app.Run` ensures both streams before creating the consumer.

- [ ] **Step 1: Write the failing integration tests** (Postgres `notification` + NATS + an `httptest.Server` as the webhook; `notification.Run` in-process with `RetryDelay 100ms`, `MaxDeliver 3`)

- `Test_Consumer_DeliversOnce`: publish one envelope → the webhook receives exactly one POST with `Idempotency-Key` equal to the envelope ID and a body that unmarshals to the envelope; one row in `notification`, status sent.
- `Test_Consumer_RedeliveryHasOneEffect` (spec scenario 11): publish the same envelope twice with different `Nats-Msg-Id` values, so the broker does not deduplicate → one row in `inbox`, one in `notification`, exactly one webhook call.
- `Test_Consumer_RetriesFailedDelivery`: the webhook answers 500 twice, then 200 → three calls, all with the same `Idempotency-Key`; one `notification` row, status sent.
- `Test_Consumer_PoisonGoesToDLQ`: publish `not json` on `events.payment.succeeded` → within 5s the `EVENTS_DLQ` stream holds one message on `dlq.notification` with that body; a valid envelope published afterwards is still delivered.
- `Test_Consumer_ExhaustedDeliveryGoesToDLQ`: the webhook always answers 500 → exactly 3 calls, then one message on `dlq.notification`; the `notification` row stays pending.
- `Test_Sender_NoURL`: `webhook.New(webhook.Config{})` → `Send` returns nil.

- [ ] **Step 2: Run them.** Run: `go test -count=1 -tags=integration ./services/notification/test/integration/...` Expected: FAIL, undefined `notification.Run`.
- [ ] **Step 3: Implement sender, consumer, app, config, `Run`, `main`.**
- [ ] **Step 4: Run them.** Expected: PASS.

- [ ] **Step 5: Extend the e2e suite and write the failing e2e tests.** `SetupSuite` now migrates `notification`, starts an `httptest.Server` that records calls, starts `notification.Run` with that URL, and installs a `tracetest.SpanRecorder` the way the template's `test/integration/main_test.go` does.

`Test_Tracing_OutboxToConsumer` (spec section 8): after a succeeded payment, take the envelope ID from its webhook call. The ended span named `nats consume events.payment.succeeded` with `messaging.message.id` equal to that ID shares its trace ID with spans named `adapter postgres SaveOutbox` and `nats publish events.payment.succeeded`. No ended span is named `usecase SendOutbox` (the outbox worker suppresses its own spans, as in the template).

`Test_Notification_OnePerPayment`:

| payment | want webhook calls | want envelope type | want `data.status` | want `data.failure_reason` |
|---|---|---|---|---|
| `DE-OK-1`, funded | exactly 1 | `payment.succeeded` | `succeeded` | absent |
| `DECLINE-1`, funded | exactly 1 | `payment.failed` | `failed` | `bank_declined` |

"Exactly 1" is checked with `Eventually` for the first call, then `Never` a second one for 1s. `Idempotency-Key` equals the envelope ID.

- [ ] **Step 6: Run the e2e suite.** Run: `go test -count=1 -tags=integration ./test/e2e/...` Expected: PASS.
- [ ] **Step 7: Commit.** `git add -A && git commit -m "Add the notification service: JetStream consumer, webhook delivery, dead-letter stream"`

---

## Phase 6: Reconciliation

### Task 18: Reconciliation rules

**Files:**
- Create: `services/payment/internal/domain/reconciliation.go`
- Test: `services/payment/internal/domain/reconciliation_test.go`

**Interfaces:**
- Consumes: `Payment`, `BankTransfer`, `HoldState`, `events` (Tasks 2 and 9).
- Produces:

```go
type DiscrepancyKind string
const (
	KindBankMissing    DiscrepancyKind = "bank_missing"
	KindBankUnexpected DiscrepancyKind = "bank_unexpected"
	KindAmountMismatch DiscrepancyKind = "amount_mismatch"
	KindLedgerMismatch DiscrepancyKind = "ledger_mismatch"
	KindStuck          DiscrepancyKind = "stuck"
)

type DiscrepancyStatus string // DiscrepancyOpen = "open", DiscrepancyResolved = "resolved"

type Discrepancy struct {
	ID         uuid.UUID
	Kind       DiscrepancyKind
	Reference  string    // payment ID as a string, or the bank reference when no payment exists
	PaymentID  uuid.UUID // uuid.Nil when no payment exists
	Details    string
	Status     DiscrepancyStatus
	CreatedAt  time.Time
	ResolvedAt time.Time
}
func NewDiscrepancy(kind DiscrepancyKind, reference string, paymentID uuid.UUID, details string) Discrepancy
func (d Discrepancy) ToEvent() (Event, error) // events.TypeDiscrepancyFound with DiscrepancyData

type ReconcileFacts struct {
	Payment    Payment
	Bank       *BankTransfer // nil when the bank has no transfer for the payment
	Hold       HoldState
	Now        time.Time
	StuckAfter time.Duration
}
func Reconcile(f ReconcileFacts) []DiscrepancyKind // sorted, no duplicates
```

- [ ] **Step 1: Write the failing table test.** Payment: 3000 EUR. `StuckAfter: 5 * time.Minute`.

| # | status | updated ago | bank | hold | want |
|---|---|---|---|---|---|
| 1 | succeeded | 1h | succeeded 3000 EUR | captured | none |
| 2 | succeeded | 1h | nil | captured | `bank_missing` |
| 3 | succeeded | 1h | declined | captured | `bank_missing` |
| 4 | succeeded | 1h | cancelled | captured | `bank_missing` |
| 5 | succeeded | 1h | succeeded 2999 EUR | captured | `amount_mismatch` |
| 6 | succeeded | 1h | succeeded 3000 USD | captured | `amount_mismatch` |
| 7 | succeeded | 1h | succeeded 3000 EUR | active | `ledger_mismatch` |
| 8 | succeeded | 1h | succeeded 3000 EUR | released | `ledger_mismatch` |
| 9 | succeeded | 1h | nil | absent | `bank_missing`, `ledger_mismatch` |
| 10 | failed | 1h | nil | absent | none |
| 11 | failed | 1h | cancelled | released | none |
| 12 | failed | 1h | declined | released | none |
| 13 | failed | 1h | succeeded 3000 EUR | released | `bank_unexpected` |
| 14 | failed | 1h | nil | active | `ledger_mismatch` |
| 15 | failed | 1h | nil | captured | `ledger_mismatch` |
| 16 | processing | 10s | nil | active | none |
| 17 | processing | 10m | succeeded 3000 EUR | active | `stuck` |
| 18 | review | 10m | nil | active | `stuck` |
| 19 | created | 10m | nil | absent | `stuck` |
| 20 | created | 10s | nil | absent | none |

`TestDiscrepancy_ToEvent`: subject `events.reconciliation.discrepancy_found`; the envelope's `PaymentID` is the discrepancy's; `Data` decodes to `DiscrepancyData{DiscrepancyID: d.ID, Kind: "bank_missing", Reference: d.Reference, Details: d.Details}`.

- [ ] **Step 2: Run it.** Run: `go test ./services/payment/internal/domain/...` Expected: FAIL.
- [ ] **Step 3: Implement.** A non-terminal payment yields only `stuck`, and only when `Now - UpdatedAt > StuckAfter`.
- [ ] **Step 4: Run it.** Expected: PASS.
- [ ] **Step 5: Commit.** `git add -A && git commit -m "Add reconciliation rules as a pure function"`

### Task 19: Reconciliation use case and persistence

**Files:**
- Create: `services/payment/migration/postgres/20261122000002_reconciliation.{up,down}.sql`
- Create: `services/payment/internal/adapter/postgres/discrepancy.go`
- Create: `services/payment/internal/usecase/{reconcile.go,get_discrepancies.go}`; Create: `services/payment/internal/dto/reconciliation.go`
- Modify: `services/payment/internal/usecase/usecase.go` (extend the `Postgres` port); regenerate mocks
- Test: `services/payment/internal/usecase/reconcile_test.go`, `services/payment/test/integration/discrepancy_test.go`

**Interfaces:**
- Consumes: Task 18 rules; ports of Task 11.
- Produces:

```go
// added to usecase.Postgres
ListOpenDiscrepancies(ctx context.Context) ([]domain.Discrepancy, error)
CreateDiscrepancy(ctx context.Context, d domain.Discrepancy) (created bool, err error) // false when an open one exists for (reference, kind)
ResolveDiscrepancy(ctx context.Context, id uuid.UUID) error
ListDiscrepancies(ctx context.Context, status domain.DiscrepancyStatus) ([]domain.Discrepancy, error) // "" lists all, newest first

// dto
type ReconcileInput struct {
	Now                        time.Time
	Lookback, Lag, StuckAfter  time.Duration
}
type ReconcileOutput struct {
	Checked, Found, Resolved, Signaled, Restarted int
}
type GetDiscrepanciesInput struct{ Status domain.DiscrepancyStatus }
type GetDiscrepanciesOutput struct{ Discrepancies []domain.Discrepancy }

func (u *UseCase) Reconcile(ctx context.Context, in dto.ReconcileInput) (dto.ReconcileOutput, error)
func (u *UseCase) GetDiscrepancies(ctx context.Context, in dto.GetDiscrepanciesInput) (dto.GetDiscrepanciesOutput, error)
```

Schema:

```sql
BEGIN;

CREATE TABLE reconciliation_discrepancy (
    id          UUID PRIMARY KEY,
    kind        TEXT        NOT NULL CHECK (kind IN ('bank_missing', 'bank_unexpected', 'amount_mismatch', 'ledger_mismatch', 'stuck')),
    reference   TEXT        NOT NULL,
    payment_id  UUID,
    details     TEXT        NOT NULL DEFAULT '',
    status      TEXT        NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'resolved')),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    resolved_at TIMESTAMPTZ
);
CREATE UNIQUE INDEX reconciliation_discrepancy_open ON reconciliation_discrepancy (reference, kind) WHERE status = 'open';

COMMIT;
```

`Reconcile` algorithm (spec section 8). `from = Now - Lookback`, `to = Now - Lag`.

1. **Restart stuck starts.** For each `ListPaymentsByStatus(created)` with `UpdatedAt < Now - StuckAfter`: `orchestrator.StartPayment`. An error is logged and the run continues. Count `Restarted`.
2. **Resolve reviews.** For each `ListPaymentsByStatus(review)`: `bank.Cancel(payment ID)`. An error → skip this payment. Otherwise `orchestrator.SignalBankOutcome(id, transfer.Status)`; a signal error is logged and the run continues. Count `Signaled`.
3. **Collect candidates.** `statement := bank.GetStatement(from, Now)`. Candidates are the union, by ID, of `ListPaymentsUpdatedBetween(from, to)`, `GetPayments` for statement references that parse as UUIDs, and `GetPayments` for the payment IDs of `ListOpenDiscrepancies`. A statement entry whose reference is not a UUID, or is a UUID with no payment, is a finding `(reference, bank_unexpected)` with `PaymentID = uuid.Nil`.
4. **Look up facts.** `ledger.GetHolds` for the terminal candidates. For each terminal candidate, `bank.GetTransfer(payment ID)`. Non-terminal candidates get no lookups.
5. **Apply the rules.** `domain.Reconcile` per candidate → findings `(payment ID string, kind)`.
6. **Diff against the open discrepancies.** A finding with no open discrepancy: in one transaction `CreateDiscrepancy` and, when created, `SaveOutbox(d.ToEvent())`; count `Found`. An open discrepancy with no finding: `ResolveDiscrepancy`; count `Resolved`.

Any error from `GetStatement`, `GetTransfer`, `GetHolds` or a Postgres read aborts the run with that error before step 6 writes anything.

- [ ] **Step 1: Extend the port, regenerate mocks.** Run: `make generate`.

- [ ] **Step 2: Write the failing unit test** `Test_Reconcile`. Input: `Lookback 1h`, `Lag 0`, `StuckAfter 5m`.

| # | setup | want error | assertions |
|---|---|---|---|
| 1 | succeeded payment in the window; `GetTransfer` → nil; hold captured; no open discrepancies | nil | `CreateDiscrepancy` once with kind `bank_missing`, reference = payment ID; `SaveOutbox` once with subject `events.reconciliation.discrepancy_found`; `Found == 1` |
| 2 | as 1, and an open `bank_missing` for that payment exists | nil | no `CreateDiscrepancy`, no `SaveOutbox`, no `ResolveDiscrepancy`; `Found == 0` |
| 3 | open `bank_missing` exists; `GetTransfer` now → succeeded 3000 EUR; hold captured | nil | `ResolveDiscrepancy(that ID)` once; `Resolved == 1` |
| 4 | review payment; `bank.Cancel` → succeeded | nil | `SignalBankOutcome(id, succeeded)` once; `Signaled == 1` |
| 5 | review payment; `bank.Cancel` → cancelled; `SignalBankOutcome` → error | nil | the run completes; `Signaled == 0` |
| 6 | review payment; `bank.Cancel` → `ErrBankUnavailable` | nil | no `SignalBankOutcome` |
| 7 | statement has `{Reference: "ROGUE-1", Status: succeeded}`; no payments | nil | `CreateDiscrepancy` with kind `bank_unexpected`, reference `ROGUE-1`, `PaymentID == uuid.Nil` |
| 8 | statement references a random UUID; `GetPayments` → empty | nil | `CreateDiscrepancy` with kind `bank_unexpected`, reference = that UUID string |
| 9 | succeeded payment; `GetTransfer` → `ErrBankUnavailable` | that error | no `CreateDiscrepancy`, no `ResolveDiscrepancy` |
| 10 | succeeded payment; `GetHolds` → error | that error | no writes |
| 11 | `GetStatement` → `ErrBankUnavailable` | that error | no writes |
| 12 | created payment, updated 10m ago | nil | `StartPayment(id)` once; `Restarted == 1`; `CreateDiscrepancy` with kind `stuck` |
| 13 | created payment, updated 10s ago | nil | no `StartPayment`, no `CreateDiscrepancy` |
| 14 | processing payment, updated 10s ago, in the window | nil | no `GetTransfer` call for it |

`Test_GetDiscrepancies`: passes the status through and returns what Postgres returns.

- [ ] **Step 3: Write the failing integration test** (Postgres `payment`)

- `Test_CreateDiscrepancy_OneOpenPerReferenceAndKind`: first → true; the same `(reference, kind)` again → false, no error; after `ResolveDiscrepancy`, creating it again → true.
- `Test_ListDiscrepancies`: one open, one resolved → `ListOpenDiscrepancies` has 1; `ListDiscrepancies("")` has 2; `ListDiscrepancies(resolved)` has 1 with `ResolvedAt` set.
- `Test_Discrepancy_NilPaymentID`: a discrepancy with `PaymentID == uuid.Nil` round-trips as `uuid.Nil`.

- [ ] **Step 4: Run them.** Run: `go test ./services/payment/... && go test -count=1 -tags=integration ./services/payment/test/integration/...` Expected: FAIL.
- [ ] **Step 5: Implement migration, adapter, use cases.** `uuid.Nil` is stored as SQL `NULL`.
- [ ] **Step 6: Run them.** Expected: PASS.
- [ ] **Step 7: Commit.** `git add -A && git commit -m "Add the reconciliation use case and discrepancy storage"`

### Task 20: Reconciliation workflow, schedule, API, end-to-end scenarios

**Files:**
- Create: `services/payment/internal/saga/reconciliation_workflow.go`; Modify: `services/payment/internal/saga/names.go`
- Modify: `services/payment/internal/controller/temporal_worker/{worker.go,activities.go}`
- Modify: `api/payment/http/payment_v1.yaml`; Create: `services/payment/internal/controller/http/v1/get_discrepancies.go`
- Modify: `test/e2e/{main_test.go,helpers_test.go}`; Create: `test/e2e/reconciliation_test.go`
- Test: `services/payment/internal/saga/reconciliation_workflow_test.go`

**Interfaces:**
- Consumes: `usecase.Reconcile`, `usecase.GetDiscrepancies` (Task 19); the e2e `Suite`.
- Produces:

```go
// saga
const (
	ReconciliationTaskQueue  = "reconciliation"
	ReconciliationScheduleID = "reconciliation"
	ActivityReconcile        = "Reconcile" // () (ReconcileResult, error)
)
type ReconcileResult struct {
	Checked, Found, Resolved, Signaled, Restarted int
}
func ReconciliationWorkflow(ctx workflow.Context) (ReconcileResult, error)
// one activity: StartToCloseTimeout 1m, MaximumAttempts 3

// temporal_worker.Config gains
ReconciliationInterval   time.Duration `env:"RECONCILIATION_INTERVAL"    envDefault:"1m"`
ReconciliationLookback   time.Duration `env:"RECONCILIATION_LOOKBACK"    envDefault:"24h"`
ReconciliationLag        time.Duration `env:"RECONCILIATION_LAG"         envDefault:"30s"`
ReconciliationStuckAfter time.Duration `env:"RECONCILIATION_STUCK_AFTER" envDefault:"5m"`

// e2e additions
func (s *Suite) reconcile()                  // runs ReconciliationWorkflow to completion
func (s *Suite) setBankAvailable(available bool)
func (s *Suite) openDiscrepancies(reference string) []string // kinds, from GET /reconciliation/discrepancies?status=open
```

Worker: a second `worker.Worker` on `saga.ReconciliationTaskQueue` registers `ReconciliationWorkflow` and the `Reconcile` activity. The activity calls `uc.Reconcile` with `Now: time.Now()` and the three durations from the config. On start the worker creates the schedule and ignores `temporal.ErrScheduleAlreadyRunning`:

```go
client.ScheduleOptions{
	ID:      saga.ReconciliationScheduleID,
	Spec:    client.ScheduleSpec{Intervals: []client.ScheduleIntervalSpec{{Every: cfg.ReconciliationInterval}}},
	Action:  &client.ScheduleWorkflowAction{ID: "reconciliation", Workflow: saga.ReconciliationWorkflow, TaskQueue: saga.ReconciliationTaskQueue},
	Overlap: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
}
```

HTTP: `getDiscrepancies`, `GET /reconciliation/discrepancies`, optional query `status: enum[open, resolved]` → `200 {discrepancies: [Discrepancy]}`. `Discrepancy`: `{id, kind, reference, payment_id?: uuid, details, status, created_at, resolved_at?}`.

`test/e2e` cannot import `services/payment/internal/saga`. `s.reconcile()` runs the workflow by name: `s.temporal.ExecuteWorkflow(ctx, client.StartWorkflowOptions{TaskQueue: "reconciliation"}, "ReconciliationWorkflow")` then `.Get(ctx, nil)`. The suite sets `ReconciliationInterval: time.Hour` so the schedule never fires during tests, `Lookback: time.Hour`, `Lag: 0`, `StuckAfter: time.Second`.

- [ ] **Step 1: Write the failing workflow test.** `Reconcile` returns `{Found: 2}` → the workflow returns `{Found: 2}`. `Reconcile` fails twice and then succeeds → called 3 times, the workflow succeeds. `Reconcile` always fails → workflow error.

- [ ] **Step 2: Extend the contract, generate, write the failing e2e tests.** Every test that makes the bank unavailable restores it with `s.T().Cleanup`.

`Test_Review_ResolvedByReconciliation` (spec scenarios 6 and 7; deposit 10000, payment 3000):

| case | setup | after `waitStatus(review)` | then | want status | want reason | want balance / held |
|---|---|---|---|---|---|---|
| bank down before processing | `setBankAvailable(false)`, beneficiary `DE-OK-1` | held 3000; webhook got `payment.review_required` | `setBankAvailable(true)`, `reconcile()` | failed | `bank_cancelled` | 10000 / 0 |
| bank processed, then went dark | beneficiary `BLACKOUT-1` | held 3000 | `setBankAvailable(true)`, `reconcile()` | succeeded | `""` | 7000 / 0 |

After each row `requireLedgerIntact()`, and a second `reconcile()` leaves the status unchanged.

`Test_Reconciliation_FindsBankDiscrepancies` (spec scenario 13):
- `POST /admin/transfers {reference: "ROGUE-1", amount: 500, currency: "EUR", beneficiary: "X"}`; `reconcile()` twice → `openDiscrepancies("ROGUE-1") == ["bank_unexpected"]`, and `SELECT count(*) FROM reconciliation_discrepancy WHERE reference = 'ROGUE-1'` is 1.
- A succeeded payment; `PATCH /admin/transfers/{paymentID} {amount: 1}`; `reconcile()` twice → `openDiscrepancies(paymentID) == ["amount_mismatch"]`, one row.
- The webhook received exactly one `reconciliation.discrepancy_found` envelope per discrepancy.
- `PATCH` the amount back to 3000; `reconcile()` → `openDiscrepancies(paymentID)` is empty and the row's status is `resolved`.

`Test_Reconciliation_RestartsStuckCreated` (spec scenario 14): fund an account; insert a `payment` row directly with status `created`, a valid `request_hash`, beneficiary `DE-OK-9` and `updated_at = now() - interval '1 minute'`; `reconcile()` → `waitStatus(id, "succeeded")`; balance dropped by the amount.

`Test_Reconciliation_CleanRunFindsNothing`: after a fresh succeeded payment, `reconcile()` → no open discrepancy for it.

- [ ] **Step 3: Run them.** Run: `go test ./services/payment/... && go test -count=1 -tags=integration ./test/e2e/...` Expected: FAIL, undefined `saga.ReconciliationWorkflow`.
- [ ] **Step 4: Implement workflow, activity, schedule, handler.**
- [ ] **Step 5: Run the full suite.** Run: `make test && make integration-test` Expected: PASS.
- [ ] **Step 6: Commit.** `git add -A && git commit -m "Schedule reconciliation in Temporal and expose discrepancies"`

---

## Phase 7: Delivery

### Task 21: Local environment

**Files:**
- Create: `Dockerfile`, `docker-compose.yml`, `deploy/postgres/init.sql`, `scripts/demo.sh`
- Modify: `Makefile`, each `services/<name>/.env.example`

**Interfaces:**
- Consumes: the four `cmd/app` entrypoints and their env variables.
- Produces: `make up`, `make down`, `make run-<service>`, `make demo`.

Decisions:
- `Dockerfile`: the template's, with `ARG SERVICE` and `go build ... ./services/${SERVICE}/cmd/app`.
- `deploy/postgres/init.sql`: `CREATE DATABASE payment; CREATE DATABASE ledger; CREATE DATABASE notification;`, mounted into `/docker-entrypoint-initdb.d/`.
- `docker-compose.yml` services: `postgres` (`postgres:18-alpine`, healthcheck as in the template); `nats` (pinned tag, command `-js -m 8222`, ports 4222 and 8222); `temporal` (pinned `temporalio/temporal`, command `server start-dev --ip 0.0.0.0`, ports 7233 and 8233); three one-shot `migrate-<db>` services on a pinned `migrate/migrate` image, each mounting `services/<name>/migration/postgres` and running `up`; `ledger`, `mockbank`, `payment`, `notification` built from the `Dockerfile` with `SERVICE` set. Services wait for `postgres` healthy and their migration `service_completed_successfully`. Ports: the defaults from the File Structure section.
- `Makefile`:

```make
up:
	docker compose up -d --build --wait

down:
	docker compose down -v

run-%:
	cd services/$* && go run ./cmd/app

demo:
	./scripts/demo.sh
```

- `scripts/demo.sh` (bash, `set -euo pipefail`): opens a EUR account and deposits 10000 through `grpcurl -plaintext localhost:50051`, posts a 3000 payment to `DE-OK-1` with `curl`, polls `GET /api/v1/payments/{id}` until the status is terminal, prints the payment and the balance. It uses `go run github.com/fullstorydev/grpcurl/cmd/grpcurl@latest` when `grpcurl` is not installed.
- `.env.example` per service lists every variable its `config.Config` reads, with values that work against `make up` infrastructure on localhost.

- [ ] **Step 1: Write the files above.**
- [ ] **Step 2: Validate the compose file.** Run: `docker compose config --quiet` Expected: no output, exit 0.
- [ ] **Step 3: Start the system.** Run: `make up` Expected: all containers healthy or completed; `curl -fsS localhost:8080/ready` returns 200.
- [ ] **Step 4: Run the demo.** Run: `make demo` Expected: the last lines show `"status":"succeeded"` and `available` of `7000`.
- [ ] **Step 5: Stop.** Run: `make down` Expected: containers and volumes removed.
- [ ] **Step 6: Commit.** `git add -A && git commit -m "Add docker compose environment and a demo script"`

### Task 22: README and CI

**Files:**
- Create: `README.md`, `.github/workflows/ci.yml`, `.github/dependabot.yml`

**Interfaces:**
- Consumes: everything.

README sections, in English:
1. What this is, in three sentences, and the architecture diagram from spec section 5.
2. The role table from spec section 5 and the rule "would the payment change if the receiver were down?".
3. The saga diagram from spec section 6.3 and the status table from 6.2.
4. Getting started: `make bin-deps`, `make up`, `make demo`; a table of addresses (payment API, ledger gRPC, mock bank, Temporal UI `localhost:8233`, NATS monitor `localhost:8222`).
5. Trying the failure modes: one `curl` per mock bank prefix (`DECLINE`, `FLAKY`, `TIMEOUT`, `BLACKOUT`), the admin calls, and how to trigger reconciliation from the Temporal UI.
6. Testing: `make test`, `make integration-test`, and the scenario table from spec section 10.
7. **Design decisions and trade-offs**, one short paragraph each: orchestration vs choreography for the payment; why NATS carries only facts; outbox vs dual write; inbox vs an idempotent consumer; pessimistic row locks in the ledger and the lock order; compensable, pivot and retriable steps; never compensating on an unknown outcome and why "not found" is not a refusal; what `Cancel` fencing assumes about the bank and what to do without it; why payments and the ledger are separate services, and the case for keeping them in one database; why idempotency lives in Postgres and not Redis; reconciliation as the last line of defence, point lookups vs time windows; insert-then-start and how a stuck `created` payment recovers.
8. Project layout and the two deliberate departures from the template (public `Run`, injected metrics registry).

CI: the template's workflow with these changes: the integration job runs `make integration-test` and no `docker compose` steps; the docker job is a matrix over `service: [ledger, mockbank, payment, notification]` passing `build-args: SERVICE=${{ matrix.service }}`. `dependabot.yml`: the template's.

- [ ] **Step 1: Write README, CI workflow, dependabot config.**
- [ ] **Step 2: Verify generated code is current.** Run: `make generate && git status --porcelain` Expected: only the three new files.
- [ ] **Step 3: Run everything once more.** Run: `make lint && make test && make integration-test` Expected: 0 lint issues, all tests PASS.
- [ ] **Step 4: Check the workflow parses.** Run: `go run github.com/rhysd/actionlint/cmd/actionlint@latest` Expected: no findings.
- [ ] **Step 5: Commit.** `git add -A && git commit -m "Add README with design trade-offs and CI"`

---

## Spec coverage

| Spec section | Tasks |
|---|---|
| 5 roles and services | 6, 8, 15, 17 |
| 6.1 API, idempotency, insert-then-start | 10, 11, 15 |
| 6.2 statuses | 9, 11 |
| 6.3 workflow, retries, cancel, signal | 7, 12, 13, 15, 20 |
| 7.1 ledger | 4, 5, 6 |
| 7.2 mock bank | 7, 8 |
| 7.3 notification, inbox, DLQ | 16, 17 |
| 8 events and outbox | 2, 10, 14 |
| 8 reconciliation | 18, 19, 20 |
| 9 monorepo, public `Run`, injected registry | 1, 6, 8, 15, 17 |
| 10 scenarios 1 to 5 | 15 |
| 10 scenarios 6, 7 | 20 |
| 10 scenario 8 | 7, 8, 13 |
| 10 scenario 9 | 10, 15 |
| 10 scenario 10 | 5, 15 |
| 10 scenario 11 | 16, 17 |
| 10 scenario 12 | 14 |
| 10 scenarios 13, 14 | 20 |
| 11 run, CI, README | 21, 22 |
