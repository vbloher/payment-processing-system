package usecase_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/vbloher/payment-processing-system/pkg/otel"
	"github.com/vbloher/payment-processing-system/services/payment/internal/domain"
	"github.com/vbloher/payment-processing-system/services/payment/internal/dto"
)

// reconcileCase describes the world a Reconcile run sees. Lookups are served from it; every write is recorded.
type reconcileCase struct {
	created, review, window, known []domain.Payment
	statement                      []domain.BankTransfer
	statementErr                   error
	open                           []domain.Discrepancy
	holds                          map[uuid.UUID]domain.HoldState
	holdsErr                       error
	transfers                      map[uuid.UUID]*domain.BankTransfer
	transferErr                    error
	cancel                         domain.BankTransfer
	cancelErr                      error
	signalErr                      error
	startErr                       error
}

type reconcileRecord struct {
	created      []domain.Discrepancy
	resolved     []uuid.UUID
	outbox       []domain.Event
	started      []uuid.UUID
	signaled     []uuid.UUID
	signaledWith []domain.BankOutcome
	transferFor  []uuid.UUID
}

func (c reconcileCase) run(t *testing.T, now time.Time) (dto.ReconcileOutput, reconcileRecord, error) {
	t.Helper()

	u, d := newUseCase(t)

	var rec reconcileRecord

	pool := append(append([]domain.Payment{}, c.known...), c.window...)

	d.postgres.EXPECT().ListPaymentsByStatus(Any, domain.StatusCreated).Return(c.created, nil).Maybe()
	d.postgres.EXPECT().ListPaymentsByStatus(Any, domain.StatusReview).Return(c.review, nil).Maybe()
	d.postgres.EXPECT().ListPaymentsUpdatedBetween(Any, now.Add(-time.Hour), now).Return(c.window, nil).Maybe()
	d.postgres.EXPECT().ListOpenDiscrepancies(Any).Return(c.open, nil).Maybe()
	d.postgres.EXPECT().GetPayments(Any, Any).RunAndReturn(func(_ context.Context, ids []uuid.UUID) ([]domain.Payment, error) {
		var out []domain.Payment

		for _, p := range pool {
			for _, id := range ids {
				if p.ID == id {
					out = append(out, p)

					break
				}
			}
		}

		return out, nil
	}).Maybe()
	d.postgres.EXPECT().CreateDiscrepancy(Any, Any).RunAndReturn(func(_ context.Context, x domain.Discrepancy) (bool, error) {
		rec.created = append(rec.created, x)

		return true, nil
	}).Maybe()
	d.postgres.EXPECT().ResolveDiscrepancy(Any, Any).RunAndReturn(func(_ context.Context, id uuid.UUID) error {
		rec.resolved = append(rec.resolved, id)

		return nil
	}).Maybe()
	d.postgres.EXPECT().SaveOutbox(Any, Any).RunAndReturn(func(_ context.Context, e ...domain.Event) error {
		rec.outbox = append(rec.outbox, e...)

		return nil
	}).Maybe()

	d.bank.EXPECT().GetStatement(Any, now.Add(-time.Hour), now).Return(c.statement, c.statementErr).Maybe()
	d.bank.EXPECT().GetTransfer(Any, Any).RunAndReturn(func(_ context.Context, ref string) (*domain.BankTransfer, error) {
		id, err := uuid.Parse(ref)
		require.NoError(t, err)

		rec.transferFor = append(rec.transferFor, id)

		return c.transfers[id], c.transferErr
	}).Maybe()
	d.bank.EXPECT().Cancel(Any, Any).Return(c.cancel, c.cancelErr).Maybe()

	d.ledger.EXPECT().GetHolds(Any, Any).Return(c.holds, c.holdsErr).Maybe()

	d.orchestrator.EXPECT().StartPayment(Any, Any).RunAndReturn(func(_ context.Context, id uuid.UUID) error {
		rec.started = append(rec.started, id)

		return c.startErr
	}).Maybe()
	d.orchestrator.EXPECT().SignalBankOutcome(Any, Any, Any).RunAndReturn(
		func(_ context.Context, id uuid.UUID, o domain.BankOutcome) error {
			if c.signalErr != nil {
				return c.signalErr
			}

			rec.signaled = append(rec.signaled, id)
			rec.signaledWith = append(rec.signaledWith, o)

			return nil
		}).Maybe()

	out, err := u.Reconcile(t.Context(), dto.ReconcileInput{
		Now: now, Lookback: time.Hour, StuckAfter: 5 * time.Minute,
	})

	return out, rec, err
}

func Test_Reconcile(t *testing.T) {
	otel.SilentModeInit()

	now := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	errSignal := errors.New("workflow execution already completed")
	errHolds := errors.New("ledger down")

	// succeeded builds a succeeded payment updated a minute ago.
	succeeded := func() domain.Payment {
		p := newPayment(domain.StatusSucceeded)
		p.UpdatedAt = now.Add(-time.Minute)

		return p
	}
	withStatus := func(s domain.Status, age time.Duration) domain.Payment {
		p := newPayment(s)
		p.UpdatedAt = now.Add(-age)

		return p
	}

	ok := succeeded()
	okTransfer := &domain.BankTransfer{Reference: ok.ID.String(), Amount: 3000, Currency: "EUR", Status: domain.BankSucceeded}
	openMissing := domain.Discrepancy{
		ID: uuid.New(), Kind: domain.KindBankMissing, Reference: ok.ID.String(), PaymentID: ok.ID,
		Status: domain.DiscrepancyOpen,
	}
	rev := withStatus(domain.StatusReview, time.Minute)
	randomRef := uuid.New()
	stuck := withStatus(domain.StatusCreated, 10*time.Minute)
	fresh := withStatus(domain.StatusCreated, 10*time.Second)
	proc := withStatus(domain.StatusProcessing, 10*time.Second)

	t.Run("1 bank_missing is recorded and emitted", func(t *testing.T) {
		out, rec, err := reconcileCase{
			window: []domain.Payment{ok}, holds: map[uuid.UUID]domain.HoldState{ok.ID: domain.HoldCaptured},
		}.run(t, now)
		require.NoError(t, err)
		require.Len(t, rec.created, 1)
		require.Equal(t, domain.KindBankMissing, rec.created[0].Kind)
		require.Equal(t, ok.ID.String(), rec.created[0].Reference)
		require.Equal(t, ok.ID, rec.created[0].PaymentID)
		require.Len(t, rec.outbox, 1)
		require.Equal(t, "events.reconciliation.discrepancy_found", rec.outbox[0].Subject)
		require.Equal(t, 1, out.Found)
		require.Equal(t, 1, out.Checked)
	})

	t.Run("2 an open discrepancy is not recorded again", func(t *testing.T) {
		out, rec, err := reconcileCase{
			window: []domain.Payment{ok}, holds: map[uuid.UUID]domain.HoldState{ok.ID: domain.HoldCaptured},
			open: []domain.Discrepancy{openMissing},
		}.run(t, now)
		require.NoError(t, err)
		require.Empty(t, rec.created)
		require.Empty(t, rec.outbox)
		require.Empty(t, rec.resolved)
		require.Zero(t, out.Found)
	})

	t.Run("3 a discrepancy that no longer reproduces is resolved", func(t *testing.T) {
		out, rec, err := reconcileCase{
			known: []domain.Payment{ok}, open: []domain.Discrepancy{openMissing},
			holds:     map[uuid.UUID]domain.HoldState{ok.ID: domain.HoldCaptured},
			transfers: map[uuid.UUID]*domain.BankTransfer{ok.ID: okTransfer},
		}.run(t, now)
		require.NoError(t, err)
		require.Equal(t, []uuid.UUID{openMissing.ID}, rec.resolved)
		require.Empty(t, rec.created)
		require.Equal(t, 1, out.Resolved)
	})

	t.Run("4 a review payment is cancelled and the outcome signaled", func(t *testing.T) {
		out, rec, err := reconcileCase{
			review: []domain.Payment{rev}, cancel: domain.BankTransfer{Status: domain.BankSucceeded},
		}.run(t, now)
		require.NoError(t, err)
		require.Equal(t, []uuid.UUID{rev.ID}, rec.signaled)
		require.Equal(t, []domain.BankOutcome{domain.BankSucceeded}, rec.signaledWith)
		require.Equal(t, 1, out.Signaled)
	})

	t.Run("5 a failing signal does not fail the run", func(t *testing.T) {
		out, rec, err := reconcileCase{
			review: []domain.Payment{rev}, cancel: domain.BankTransfer{Status: domain.BankCancelled}, signalErr: errSignal,
		}.run(t, now)
		require.NoError(t, err)
		require.Empty(t, rec.signaled)
		require.Zero(t, out.Signaled)
	})

	t.Run("6 an unavailable bank skips the review payment", func(t *testing.T) {
		out, rec, err := reconcileCase{review: []domain.Payment{rev}, cancelErr: domain.ErrBankUnavailable}.run(t, now)
		require.NoError(t, err)
		require.Empty(t, rec.signaled)
		require.Zero(t, out.Signaled)
	})

	t.Run("7 a non-UUID statement reference is bank_unexpected", func(t *testing.T) {
		_, rec, err := reconcileCase{
			statement: []domain.BankTransfer{{Reference: "ROGUE-1", Status: domain.BankSucceeded}},
		}.run(t, now)
		require.NoError(t, err)
		require.Len(t, rec.created, 1)
		require.Equal(t, domain.KindBankUnexpected, rec.created[0].Kind)
		require.Equal(t, "ROGUE-1", rec.created[0].Reference)
		require.Equal(t, uuid.Nil, rec.created[0].PaymentID)
		require.Len(t, rec.outbox, 1)
	})

	t.Run("8 a statement UUID without a payment is bank_unexpected", func(t *testing.T) {
		_, rec, err := reconcileCase{
			statement: []domain.BankTransfer{{Reference: randomRef.String(), Status: domain.BankSucceeded}},
		}.run(t, now)
		require.NoError(t, err)
		require.Len(t, rec.created, 1)
		require.Equal(t, domain.KindBankUnexpected, rec.created[0].Kind)
		require.Equal(t, randomRef.String(), rec.created[0].Reference)
		require.Equal(t, uuid.Nil, rec.created[0].PaymentID)
	})

	t.Run("9 a failed bank lookup aborts before any write", func(t *testing.T) {
		_, rec, err := reconcileCase{
			window: []domain.Payment{ok}, open: []domain.Discrepancy{openMissing},
			holds: map[uuid.UUID]domain.HoldState{ok.ID: domain.HoldCaptured}, transferErr: domain.ErrBankUnavailable,
		}.run(t, now)
		require.ErrorIs(t, err, domain.ErrBankUnavailable)
		require.Empty(t, rec.created)
		require.Empty(t, rec.resolved)
		require.Empty(t, rec.outbox)
	})

	t.Run("10 a failed ledger lookup aborts before any write", func(t *testing.T) {
		_, rec, err := reconcileCase{window: []domain.Payment{ok}, holdsErr: errHolds}.run(t, now)
		require.ErrorIs(t, err, errHolds)
		require.Empty(t, rec.created)
		require.Empty(t, rec.resolved)
		require.Empty(t, rec.outbox)
	})

	t.Run("11 a failed statement aborts before any write", func(t *testing.T) {
		_, rec, err := reconcileCase{
			window: []domain.Payment{ok}, open: []domain.Discrepancy{openMissing}, statementErr: domain.ErrBankUnavailable,
		}.run(t, now)
		require.ErrorIs(t, err, domain.ErrBankUnavailable)
		require.Empty(t, rec.created)
		require.Empty(t, rec.resolved)
		require.Empty(t, rec.outbox)
	})

	t.Run("12 a stuck created payment is restarted and recorded", func(t *testing.T) {
		out, rec, err := reconcileCase{created: []domain.Payment{stuck}, window: []domain.Payment{stuck}}.run(t, now)
		require.NoError(t, err)
		require.Equal(t, []uuid.UUID{stuck.ID}, rec.started)
		require.Equal(t, 1, out.Restarted)
		require.Len(t, rec.created, 1)
		require.Equal(t, domain.KindStuck, rec.created[0].Kind)
	})

	t.Run("12b a failing restart is logged and the run carries on", func(t *testing.T) {
		out, rec, err := reconcileCase{
			created: []domain.Payment{stuck}, window: []domain.Payment{stuck}, startErr: errSignal,
		}.run(t, now)
		require.NoError(t, err)
		require.Zero(t, out.Restarted)
		require.Len(t, rec.created, 1)
	})

	t.Run("13 a fresh created payment is left alone", func(t *testing.T) {
		_, rec, err := reconcileCase{created: []domain.Payment{fresh}, window: []domain.Payment{fresh}}.run(t, now)
		require.NoError(t, err)
		require.Empty(t, rec.started)
		require.Empty(t, rec.created)
	})

	t.Run("14 a non-terminal payment gets no bank lookup", func(t *testing.T) {
		_, rec, err := reconcileCase{window: []domain.Payment{proc}}.run(t, now)
		require.NoError(t, err)
		require.Empty(t, rec.transferFor)
		require.Empty(t, rec.created)
	})

	t.Run("a created discrepancy that lost the race emits no event", func(t *testing.T) {
		u, d := newUseCase(t)
		d.postgres.EXPECT().ListPaymentsByStatus(Any, Any).Return(nil, nil)
		d.postgres.EXPECT().ListPaymentsUpdatedBetween(Any, Any, Any).Return(nil, nil)
		d.postgres.EXPECT().ListOpenDiscrepancies(Any).Return(nil, nil)
		d.bank.EXPECT().GetStatement(Any, Any, Any).Return([]domain.BankTransfer{{Reference: "ROGUE-1"}}, nil)
		d.postgres.EXPECT().CreateDiscrepancy(Any, Any).Return(false, nil)

		out, err := u.Reconcile(t.Context(), dto.ReconcileInput{Now: now, Lookback: time.Hour, StuckAfter: time.Minute})
		require.NoError(t, err)
		require.Zero(t, out.Found)
	})
}

func Test_GetDiscrepancies(t *testing.T) {
	otel.SilentModeInit()

	u, d := newUseCase(t)
	want := []domain.Discrepancy{{ID: uuid.New(), Kind: domain.KindStuck}}

	d.postgres.EXPECT().ListDiscrepancies(Any, domain.DiscrepancyOpen).Return(want, nil)

	out, err := u.GetDiscrepancies(t.Context(), dto.GetDiscrepanciesInput{Status: domain.DiscrepancyOpen})
	require.NoError(t, err)
	require.Equal(t, want, out.Discrepancies)
}
