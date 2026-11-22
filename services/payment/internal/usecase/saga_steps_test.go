package usecase_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/vbloher/payment-processing-system/pkg/otel"
	"github.com/vbloher/payment-processing-system/services/payment/internal/domain"
	"github.com/vbloher/payment-processing-system/services/payment/internal/usecase"
)

func Test_HoldFunds(t *testing.T) {
	otel.SilentModeInit()

	errDown := errors.New("down")

	tests := []struct {
		name       string
		ledgerErr  error
		wantReason domain.FailureReason
		wantErr    error
	}{
		{"ok", nil, domain.ReasonNone, nil},
		{"insufficient funds", domain.ErrInsufficientFunds, domain.ReasonInsufficientFunds, nil},
		{"account not found", domain.ErrAccountNotFound, domain.ReasonAccountNotFound, nil},
		{"rejected", domain.ErrLedgerRejected, domain.ReasonLedgerRejected, nil},
		{"other error", errDown, domain.ReasonNone, errDown},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u, d := newUseCase(t)
			p := newPayment(domain.StatusProcessing)

			d.postgres.EXPECT().GetPayment(Any, p.ID).Return(p, nil)
			d.ledger.EXPECT().Hold(Any, p.ID, p.AccountID, p.Amount, p.Currency).Return(tt.ledgerErr)

			reason, err := u.HoldFunds(t.Context(), p.ID)
			require.Equal(t, tt.wantReason, reason)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}

	t.Run("payment missing", func(t *testing.T) {
		u, d := newUseCase(t)
		id := uuid.New()

		d.postgres.EXPECT().GetPayment(Any, id).Return(domain.Payment{}, domain.ErrNotFound)

		_, err := u.HoldFunds(t.Context(), id)
		require.ErrorIs(t, err, domain.ErrNotFound)
	})
}

type transferStep struct {
	name    string
	call    func(u *usecase.UseCase) func(context.Context, uuid.UUID) (domain.BankOutcome, error)
	expect  func(d deps, ref string, tr domain.BankTransfer, err error)
	otherOK domain.BankOutcome
}

func Test_SendAndCancelTransfer(t *testing.T) {
	otel.SilentModeInit()

	steps := []transferStep{
		{
			name: "SendTransfer",
			call: func(u *usecase.UseCase) func(context.Context, uuid.UUID) (domain.BankOutcome, error) {
				return u.SendTransfer
			},
			expect: func(d deps, ref string, tr domain.BankTransfer, err error) {
				d.bank.EXPECT().Transfer(Any, ref, int64(3000), "EUR", "ACME").Return(tr, err)
			},
			otherOK: domain.BankDeclined,
		},
		{
			name: "CancelTransfer",
			call: func(u *usecase.UseCase) func(context.Context, uuid.UUID) (domain.BankOutcome, error) {
				return u.CancelTransfer
			},
			expect: func(d deps, ref string, tr domain.BankTransfer, err error) {
				d.bank.EXPECT().Cancel(Any, ref).Return(tr, err)
			},
			otherOK: domain.BankCancelled,
		},
	}

	for _, s := range steps {
		t.Run(s.name+" succeeded stores the reference", func(t *testing.T) {
			u, d := newUseCase(t)
			p := newPayment(domain.StatusProcessing)
			stored := p
			stored.BankReference = ""

			d.postgres.EXPECT().GetPayment(Any, p.ID).Return(p, nil)
			s.expect(d, p.ID.String(), domain.BankTransfer{ID: "b-1", Status: domain.BankSucceeded}, nil)
			d.postgres.EXPECT().GetPaymentForUpdate(Any, p.ID).Return(stored, nil)
			d.postgres.EXPECT().UpdatePayment(Any, Any).RunAndReturn(func(_ context.Context, got domain.Payment) error {
				require.Equal(t, "b-1", got.BankReference)
				require.Equal(t, domain.StatusProcessing, got.Status)

				return nil
			})

			outcome, err := s.call(u)(t.Context(), p.ID)
			require.NoError(t, err)
			require.Equal(t, domain.BankSucceeded, outcome)
		})

		t.Run(s.name+" keeps an existing reference", func(t *testing.T) {
			u, d := newUseCase(t)
			p := newPayment(domain.StatusProcessing)
			p.BankReference = "b-0"

			d.postgres.EXPECT().GetPayment(Any, p.ID).Return(p, nil)
			s.expect(d, p.ID.String(), domain.BankTransfer{ID: "b-1", Status: domain.BankSucceeded}, nil)

			outcome, err := s.call(u)(t.Context(), p.ID)
			require.NoError(t, err)
			require.Equal(t, domain.BankSucceeded, outcome)
		})

		t.Run(s.name+" other outcome", func(t *testing.T) {
			u, d := newUseCase(t)
			p := newPayment(domain.StatusProcessing)
			p.BankReference = "b-1"

			d.postgres.EXPECT().GetPayment(Any, p.ID).Return(p, nil)
			s.expect(d, p.ID.String(), domain.BankTransfer{ID: "b-1", Status: s.otherOK}, nil)

			outcome, err := s.call(u)(t.Context(), p.ID)
			require.NoError(t, err)
			require.Equal(t, s.otherOK, outcome)
		})

		t.Run(s.name+" bank unavailable", func(t *testing.T) {
			u, d := newUseCase(t)
			p := newPayment(domain.StatusProcessing)

			d.postgres.EXPECT().GetPayment(Any, p.ID).Return(p, nil)
			s.expect(d, p.ID.String(), domain.BankTransfer{}, domain.ErrBankUnavailable)

			_, err := s.call(u)(t.Context(), p.ID)
			require.ErrorIs(t, err, domain.ErrBankUnavailable)
		})
	}
}

func Test_Transitions(t *testing.T) {
	otel.SilentModeInit()

	type call func(u *usecase.UseCase, id uuid.UUID) error

	start := func(u *usecase.UseCase, id uuid.UUID) error { return u.StartProcessing(t.Context(), id) }
	complete := func(u *usecase.UseCase, id uuid.UUID) error { return u.CompletePayment(t.Context(), id) }
	review := func(u *usecase.UseCase, id uuid.UUID) error { return u.MarkPaymentForReview(t.Context(), id) }
	fail := func(reason domain.FailureReason) call {
		return func(u *usecase.UseCase, id uuid.UUID) error { return u.FailPayment(t.Context(), id, reason) }
	}

	tests := []struct {
		name       string
		call       call
		stored     domain.Status
		wantErr    error
		wantStatus domain.Status // empty: UpdatePayment not called
		wantReason domain.FailureReason
		wantEvent  string // empty: SaveOutbox not called
	}{
		{"start from created", start, domain.StatusCreated, nil, domain.StatusProcessing, "", ""},
		{"start repeated", start, domain.StatusProcessing, nil, "", "", ""},
		{"complete", complete, domain.StatusProcessing, nil, domain.StatusSucceeded, "", "events.payment.succeeded"},
		{"complete repeated", complete, domain.StatusSucceeded, nil, "", "", ""},
		{"complete after failed", complete, domain.StatusFailed, domain.ErrInvalidTransition, "", "", ""},
		{
			"fail declined", fail(domain.ReasonBankDeclined), domain.StatusProcessing, nil,
			domain.StatusFailed, domain.ReasonBankDeclined, "events.payment.failed",
		},
		{
			"fail cancelled from review", fail(domain.ReasonBankCancelled), domain.StatusReview, nil,
			domain.StatusFailed, domain.ReasonBankCancelled, "events.payment.failed",
		},
		{"review", review, domain.StatusProcessing, nil, domain.StatusReview, "", "events.payment.review_required"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u, d := newUseCase(t)
			p := newPayment(tt.stored)

			d.postgres.EXPECT().GetPaymentForUpdate(Any, p.ID).Return(p, nil)

			if tt.wantStatus != "" {
				d.postgres.EXPECT().UpdatePayment(Any, Any).RunAndReturn(func(_ context.Context, got domain.Payment) error {
					require.Equal(t, tt.wantStatus, got.Status)
					require.Equal(t, tt.wantReason, got.FailureReason)

					return nil
				})
			}

			if tt.wantEvent != "" {
				d.postgres.EXPECT().SaveOutbox(Any, Any).RunAndReturn(func(_ context.Context, evs ...domain.Event) error {
					require.Len(t, evs, 1)
					require.Equal(t, tt.wantEvent, evs[0].Subject)

					return nil
				})
			}

			err := tt.call(u, p.ID)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})

		t.Run(tt.name+" payment not found", func(t *testing.T) {
			u, d := newUseCase(t)
			id := uuid.New()

			d.postgres.EXPECT().GetPaymentForUpdate(Any, id).Return(domain.Payment{}, domain.ErrNotFound)

			require.ErrorIs(t, tt.call(u, id), domain.ErrNotFound)
		})
	}
}

func Test_CaptureFunds_ReleaseFunds(t *testing.T) {
	otel.SilentModeInit()

	t.Run("capture delegates", func(t *testing.T) {
		u, d := newUseCase(t)
		id := uuid.New()

		d.ledger.EXPECT().Capture(Any, id).Return(nil)
		require.NoError(t, u.CaptureFunds(t.Context(), id))
	})

	t.Run("capture conflict", func(t *testing.T) {
		u, d := newUseCase(t)
		id := uuid.New()

		d.ledger.EXPECT().Capture(Any, id).Return(domain.ErrLedgerConflict)
		require.ErrorIs(t, u.CaptureFunds(t.Context(), id), domain.ErrLedgerConflict)
	})

	t.Run("release delegates", func(t *testing.T) {
		u, d := newUseCase(t)
		id := uuid.New()

		d.ledger.EXPECT().Release(Any, id).Return(nil)
		require.NoError(t, u.ReleaseFunds(t.Context(), id))
	})

	t.Run("release conflict", func(t *testing.T) {
		u, d := newUseCase(t)
		id := uuid.New()

		d.ledger.EXPECT().Release(Any, id).Return(domain.ErrLedgerConflict)
		require.ErrorIs(t, u.ReleaseFunds(t.Context(), id), domain.ErrLedgerConflict)
	})
}
