package temporal_worker_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"

	"github.com/vbloher/payment-processing-system/pkg/otel"
	"github.com/vbloher/payment-processing-system/services/payment/internal/controller/temporal_worker"
	"github.com/vbloher/payment-processing-system/services/payment/internal/domain"
	"github.com/vbloher/payment-processing-system/services/payment/internal/saga"
	"github.com/vbloher/payment-processing-system/services/payment/internal/usecase"
	"github.com/vbloher/payment-processing-system/services/payment/internal/usecase/mocks"
)

const Any = mock.Anything

// config has distinct durations, so that a test can tell which one the Reconcile activity used where.
var config = temporal_worker.Config{
	ReconciliationInterval:   time.Minute,
	ReconciliationLookback:   2 * time.Hour,
	ReconciliationLag:        30 * time.Second,
	ReconciliationStuckAfter: 5 * time.Minute,
}

type deps struct {
	postgres     *mocks.Postgres
	ledger       *mocks.Ledger
	bank         *mocks.Bank
	orchestrator *mocks.Orchestrator
}

func newActivities(t *testing.T) (*temporal_worker.Activities, deps) {
	t.Helper()

	tx := mocks.NewTransactor(t)
	tx.EXPECT().Wrap(Any, Any).RunAndReturn(func(ctx context.Context, fn func(context.Context) error) error {
		return fn(ctx)
	}).Maybe()

	d := deps{
		postgres: mocks.NewPostgres(t), ledger: mocks.NewLedger(t), bank: mocks.NewBank(t),
		orchestrator: mocks.NewOrchestrator(t),
	}
	uc := usecase.New(tx, d.postgres, d.ledger, d.bank, mocks.NewPublisher(t), d.orchestrator)

	return temporal_worker.NewActivities(uc, config), d
}

func payment(id uuid.UUID, status domain.Status) domain.Payment {
	return domain.Payment{
		ID: id, AccountID: uuid.New(), Amount: 3000, Currency: "EUR", Beneficiary: "DE-OK-1", Status: status,
	}
}

func requireFatal(t *testing.T, err error, cause error) {
	t.Helper()

	var appErr *temporal.ApplicationError

	require.ErrorAs(t, err, &appErr)
	require.Equal(t, saga.ErrTypeFatal, appErr.Type())
	require.True(t, appErr.NonRetryable())
	require.ErrorIs(t, err, cause)
}

func requirePlain(t *testing.T, err error, cause error) {
	t.Helper()

	var appErr *temporal.ApplicationError

	require.Error(t, err)
	require.NotErrorAs(t, err, &appErr)
	require.ErrorIs(t, err, cause)
}

func Test_Activities_ErrorMapping(t *testing.T) {
	otel.SilentModeInit()

	errDown := errors.New("down")

	tests := []struct {
		name  string
		call  func(t *testing.T, a *temporal_worker.Activities, d deps, id uuid.UUID) error
		check func(t *testing.T, err error)
	}{
		{
			name: "StartProcessing: not found is fatal",
			call: func(t *testing.T, a *temporal_worker.Activities, d deps, id uuid.UUID) error {
				d.postgres.EXPECT().GetPaymentForUpdate(Any, id).Return(domain.Payment{}, domain.ErrNotFound)

				return a.StartProcessing(t.Context(), id)
			},
			check: func(t *testing.T, err error) { requireFatal(t, err, domain.ErrNotFound) },
		},
		{
			name: "CompletePayment: invalid transition is fatal",
			call: func(t *testing.T, a *temporal_worker.Activities, d deps, id uuid.UUID) error {
				d.postgres.EXPECT().GetPaymentForUpdate(Any, id).Return(payment(id, domain.StatusCreated), nil)

				return a.CompletePayment(t.Context(), id)
			},
			check: func(t *testing.T, err error) { requireFatal(t, err, domain.ErrInvalidTransition) },
		},
		{
			name: "CaptureFunds: ledger conflict is fatal",
			call: func(t *testing.T, a *temporal_worker.Activities, d deps, id uuid.UUID) error {
				d.ledger.EXPECT().Capture(Any, id).Return(fmt.Errorf("ledger.Capture: %w", domain.ErrLedgerConflict))

				return a.CaptureFunds(t.Context(), id)
			},
			check: func(t *testing.T, err error) { requireFatal(t, err, domain.ErrLedgerConflict) },
		},
		{
			name: "CaptureFunds: other error is retryable",
			call: func(t *testing.T, a *temporal_worker.Activities, d deps, id uuid.UUID) error {
				d.ledger.EXPECT().Capture(Any, id).Return(errDown)

				return a.CaptureFunds(t.Context(), id)
			},
			check: func(t *testing.T, err error) { requirePlain(t, err, errDown) },
		},
		{
			name: "SendTransfer: bank unavailable is retryable",
			call: func(t *testing.T, a *temporal_worker.Activities, d deps, id uuid.UUID) error {
				p := payment(id, domain.StatusProcessing)
				d.postgres.EXPECT().GetPayment(Any, id).Return(p, nil)
				d.bank.EXPECT().Transfer(Any, id.String(), p.Amount, p.Currency, p.Beneficiary).
					Return(domain.BankTransfer{}, domain.ErrBankUnavailable)

				outcome, err := a.SendTransfer(t.Context(), id)
				require.Empty(t, outcome)

				return err
			},
			check: func(t *testing.T, err error) { requirePlain(t, err, domain.ErrBankUnavailable) },
		},
		{
			name: "HoldFunds: insufficient funds is a failure reason, not an error",
			call: func(t *testing.T, a *temporal_worker.Activities, d deps, id uuid.UUID) error {
				p := payment(id, domain.StatusProcessing)
				d.postgres.EXPECT().GetPayment(Any, id).Return(p, nil)
				d.ledger.EXPECT().Hold(Any, id, p.AccountID, p.Amount, p.Currency).
					Return(fmt.Errorf("ledger.Hold: %w", domain.ErrInsufficientFunds))

				reason, err := a.HoldFunds(t.Context(), id)
				require.Equal(t, domain.ReasonInsufficientFunds, reason)

				return err
			},
			check: func(t *testing.T, err error) { require.NoError(t, err) },
		},
		{
			// A failed lookup fails the run for Temporal to retry; it never becomes a discrepancy.
			name: "Reconcile: bank unavailable is retryable",
			call: func(t *testing.T, a *temporal_worker.Activities, d deps, _ uuid.UUID) error {
				d.postgres.EXPECT().ListPaymentsByStatus(Any, Any).Return(nil, nil)
				d.postgres.EXPECT().ListOpenDiscrepancies(Any).Return(nil, nil)
				d.bank.EXPECT().GetStatement(Any, Any, Any).Return(nil, domain.ErrBankUnavailable)

				result, err := a.Reconcile(t.Context())
				require.Zero(t, result)

				return err
			},
			check: func(t *testing.T, err error) { requirePlain(t, err, domain.ErrBankUnavailable) },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a, d := newActivities(t)

			tt.check(t, tt.call(t, a, d, uuid.New()))
		})
	}
}

// Test_Activities_RegisteredUnderSagaNames runs every activity by its saga name through the
// Temporal test environment: a name that is not registered fails with "unable to find
// activityType", and a fatal use case error must reach Temporal as a non-retryable "Fatal".
func Test_Activities_RegisteredUnderSagaNames(t *testing.T) {
	otel.SilentModeInit()

	id := uuid.New()

	tests := []struct {
		name string
		args []any
	}{
		{saga.ActivityStartProcessing, []any{id}},
		{saga.ActivityHoldFunds, []any{id}},
		{saga.ActivitySendTransfer, []any{id}},
		{saga.ActivityCancelTransfer, []any{id}},
		{saga.ActivityCaptureFunds, []any{id}},
		{saga.ActivityReleaseFunds, []any{id}},
		{saga.ActivityCompletePayment, []any{id}},
		{saga.ActivityFailPayment, []any{saga.FailPaymentInput{PaymentID: id, Reason: domain.ReasonBankDeclined}}},
		{saga.ActivityMarkPaymentForReview, []any{id}},
		{saga.ActivityReconcile, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a, d := newActivities(t)

			// Every use case fails fatally on its first call, so each activity ends right there.
			d.postgres.EXPECT().GetPayment(Any, id).Return(domain.Payment{}, domain.ErrNotFound).Maybe()
			d.postgres.EXPECT().GetPaymentForUpdate(Any, id).Return(domain.Payment{}, domain.ErrNotFound).Maybe()
			d.ledger.EXPECT().Capture(Any, id).Return(domain.ErrLedgerConflict).Maybe()
			d.ledger.EXPECT().Release(Any, id).Return(domain.ErrLedgerConflict).Maybe()
			d.postgres.EXPECT().ListPaymentsByStatus(Any, Any).Return(nil, domain.ErrNotFound).Maybe()

			var suite testsuite.WorkflowTestSuite

			suite.SetLogger(log.NewStructuredLogger(slog.New(slog.DiscardHandler))) // expected failures

			env := suite.NewTestActivityEnvironment()
			a.Register(env)
			a.RegisterReconciliation(env)

			_, err := env.ExecuteActivity(tt.name, tt.args...)

			var appErr *temporal.ApplicationError

			require.ErrorAs(t, err, &appErr)
			require.Equal(t, saga.ErrTypeFatal, appErr.Type(), err.Error())
			require.True(t, appErr.NonRetryable())
		})
	}
}

// Test_Activities_Reconcile checks that a run is reconciled as of the activity's clock with the configured
// durations: the statement spans Lookback up to now, the payment window ends Lag before now, and only a
// payment created more than StuckAfter ago is restarted.
func Test_Activities_Reconcile(t *testing.T) {
	otel.SilentModeInit()

	a, d := newActivities(t)
	before := time.Now()

	stale, fresh := payment(uuid.New(), domain.StatusCreated), payment(uuid.New(), domain.StatusCreated)
	stale.UpdatedAt = before.Add(-config.ReconciliationStuckAfter - time.Minute)
	fresh.UpdatedAt = before.Add(-config.ReconciliationStuckAfter + time.Minute)

	var statementFrom, statementTo, windowFrom, windowTo time.Time

	d.postgres.EXPECT().ListPaymentsByStatus(Any, domain.StatusCreated).Return([]domain.Payment{stale, fresh}, nil)
	d.postgres.EXPECT().ListPaymentsByStatus(Any, domain.StatusReview).Return(nil, nil)
	d.postgres.EXPECT().ListOpenDiscrepancies(Any).Return(nil, nil)
	d.orchestrator.EXPECT().StartPayment(Any, stale.ID).Return(nil).Once() // fresh is not expected
	d.bank.EXPECT().GetStatement(Any, Any, Any).RunAndReturn(
		func(_ context.Context, from, to time.Time) ([]domain.BankTransfer, error) {
			statementFrom, statementTo = from, to

			return nil, nil
		})
	d.postgres.EXPECT().ListPaymentsUpdatedBetween(Any, Any, Any).RunAndReturn(
		func(_ context.Context, from, to time.Time) ([]domain.Payment, error) {
			windowFrom, windowTo = from, to

			return nil, nil
		})

	got, err := a.Reconcile(t.Context())
	after := time.Now()

	require.NoError(t, err)
	require.Equal(t, saga.ReconcileResult{Restarted: 1}, got)

	require.False(t, statementTo.Before(before) || statementTo.After(after), "the statement ends now")
	require.Equal(t, config.ReconciliationLookback, statementTo.Sub(statementFrom))
	require.Equal(t, statementFrom, windowFrom)
	require.Equal(t, config.ReconciliationLag, statementTo.Sub(windowTo))
}
