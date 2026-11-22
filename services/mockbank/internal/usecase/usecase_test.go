package usecase_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/vbloher/payment-processing-system/pkg/otel"
	"github.com/vbloher/payment-processing-system/services/mockbank/internal/adapter/memory"
	"github.com/vbloher/payment-processing-system/services/mockbank/internal/domain"
	"github.com/vbloher/payment-processing-system/services/mockbank/internal/dto"
	"github.com/vbloher/payment-processing-system/services/mockbank/internal/usecase"
)

var (
	allFrom = time.Unix(0, 0)
	allTo   = time.Now().Add(24 * time.Hour)
)

func newUseCase(t *testing.T) *usecase.UseCase {
	t.Helper()
	otel.SilentModeInit()

	return usecase.New(memory.New())
}

func input(reference, beneficiary string) dto.CreateTransferInput {
	return dto.CreateTransferInput{Reference: reference, Amount: 3000, Currency: "EUR", Beneficiary: beneficiary}
}

func statement(t *testing.T, uc *usecase.UseCase) []domain.Transfer {
	t.Helper()

	res, err := uc.GetStatement(context.Background(), allFrom, allTo)
	require.NoError(t, err)

	return res
}

func Test_CreateTransfer_ByBeneficiary(t *testing.T) {
	tests := []struct {
		name         string
		beneficiary  string
		wantErrs     []error // per call; nil means success
		wantStatuses []domain.Status
		wantStored   domain.Status
		wantReason   string
		wantCount    int
	}{
		{"ok", "DE-OK-1", []error{nil}, []domain.Status{domain.StatusSucceeded}, domain.StatusSucceeded, "", 1},
		{"decline", "DECLINE-1", []error{nil}, []domain.Status{domain.StatusDeclined}, domain.StatusDeclined,
			domain.DeclineReasonAccountClosed, 0},
		{"flaky", "FLAKY-1", []error{domain.ErrUnavailable, domain.ErrUnavailable, nil},
			[]domain.Status{"", "", domain.StatusSucceeded}, domain.StatusSucceeded, "", 1},
		{"timeout", "TIMEOUT-1", []error{domain.ErrTimeout, domain.ErrTimeout}, []domain.Status{"", ""},
			domain.StatusSucceeded, "", 1},
		{"blackout", "BLACKOUT-1", []error{domain.ErrUnavailable}, []domain.Status{""}, domain.StatusSucceeded, "", 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			uc := newUseCase(t)
			ctx := context.Background()

			for i, wantErr := range tt.wantErrs {
				got, err := uc.CreateTransfer(ctx, input("p1", tt.beneficiary))
				if wantErr != nil {
					require.ErrorIs(t, err, wantErr, "call %d", i+1)

					continue
				}

				require.NoError(t, err, "call %d", i+1)
				require.Equal(t, tt.wantStatuses[i], got.Status)
				require.Equal(t, tt.wantReason, got.DeclineReason)
			}

			if tt.name == "blackout" {
				_, err := uc.GetTransfer(ctx, "p1")
				require.ErrorIs(t, err, domain.ErrUnavailable)

				uc.SetAvailability(ctx, true)
			}

			stored, err := uc.GetTransfer(ctx, "p1")
			require.NoError(t, err)
			require.Equal(t, tt.wantStored, stored.Status)
			require.Len(t, statement(t, uc), tt.wantCount)
		})
	}
}

func Test_CreateTransfer_Idempotency(t *testing.T) {
	uc := newUseCase(t)
	ctx := context.Background()

	first, err := uc.CreateTransfer(ctx, input("p1", "DE-OK-1"))
	require.NoError(t, err)

	second, err := uc.CreateTransfer(ctx, input("p1", "DE-OK-1"))
	require.NoError(t, err)
	require.Equal(t, first.ID, second.ID)

	other := input("p1", "DE-OK-1")
	other.Amount = 3001
	_, err = uc.CreateTransfer(ctx, other)
	require.ErrorIs(t, err, domain.ErrConflict)
}

func Test_CreateTransfer_Validation(t *testing.T) {
	tests := []struct {
		name string
		in   dto.CreateTransferInput
	}{
		{"empty reference", dto.CreateTransferInput{Amount: 1, Currency: "EUR", Beneficiary: "B"}},
		{"zero amount", dto.CreateTransferInput{Reference: "p1", Amount: 0, Currency: "EUR", Beneficiary: "B"}},
		{"lowercase currency", dto.CreateTransferInput{Reference: "p1", Amount: 1, Currency: "eur", Beneficiary: "B"}},
		{"empty beneficiary", dto.CreateTransferInput{Reference: "p1", Amount: 1, Currency: "EUR"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			uc := newUseCase(t)

			_, err := uc.CreateTransfer(context.Background(), tt.in)
			require.ErrorIs(t, err, domain.ErrValidation)
		})
	}
}

func Test_CancelTransfer(t *testing.T) {
	tests := []struct {
		name       string
		setup      func(t *testing.T, uc *usecase.UseCase)
		wantStatus domain.Status
	}{
		{"succeeded exists", func(t *testing.T, uc *usecase.UseCase) {
			t.Helper()
			_, err := uc.CreateTransfer(context.Background(), input("p1", "DE-OK-1"))
			require.NoError(t, err)
		}, domain.StatusSucceeded},
		{"declined exists", func(t *testing.T, uc *usecase.UseCase) {
			t.Helper()
			_, err := uc.CreateTransfer(context.Background(), input("p1", "DECLINE-1"))
			require.NoError(t, err)
		}, domain.StatusDeclined},
		{"nothing exists", func(t *testing.T, _ *usecase.UseCase) { t.Helper() }, domain.StatusCancelled},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			uc := newUseCase(t)
			tt.setup(t, uc)

			got, err := uc.CancelTransfer(context.Background(), "p1")
			require.NoError(t, err)
			require.Equal(t, tt.wantStatus, got.Status)
		})
	}

	t.Run("cancelled twice", func(t *testing.T) {
		uc := newUseCase(t)

		first, err := uc.CancelTransfer(context.Background(), "p1")
		require.NoError(t, err)

		second, err := uc.CancelTransfer(context.Background(), "p1")
		require.NoError(t, err)
		require.Equal(t, domain.StatusCancelled, second.Status)
		require.Equal(t, first.ID, second.ID)
	})
}

func Test_CancelTransfer_FencesLateCreate(t *testing.T) {
	uc := newUseCase(t)
	ctx := context.Background()

	cancelled, err := uc.CancelTransfer(ctx, "p1")
	require.NoError(t, err)
	require.Equal(t, domain.StatusCancelled, cancelled.Status)

	late, err := uc.CreateTransfer(ctx, input("p1", "DE-OK-1"))
	require.NoError(t, err)
	require.Equal(t, domain.StatusCancelled, late.Status)
	require.Empty(t, statement(t, uc))
}

func Test_Unavailable(t *testing.T) {
	uc := newUseCase(t)
	ctx := context.Background()

	_, err := uc.CreateTransfer(ctx, input("p0", "DE-OK-1"))
	require.NoError(t, err)

	uc.SetAvailability(ctx, false)

	_, err = uc.CreateTransfer(ctx, input("p1", "DE-OK-1"))
	require.ErrorIs(t, err, domain.ErrUnavailable)

	_, err = uc.CancelTransfer(ctx, "p2")
	require.ErrorIs(t, err, domain.ErrUnavailable)

	_, err = uc.GetTransfer(ctx, "p0")
	require.ErrorIs(t, err, domain.ErrUnavailable)

	_, err = uc.GetStatement(ctx, allFrom, allTo)
	require.ErrorIs(t, err, domain.ErrUnavailable)

	uc.SetAvailability(ctx, true)

	_, err = uc.GetTransfer(ctx, "p1")
	require.ErrorIs(t, err, domain.ErrNotFound, "nothing was stored while unavailable")

	_, err = uc.GetTransfer(ctx, "p2")
	require.ErrorIs(t, err, domain.ErrNotFound, "nothing was stored while unavailable")

	_, err = uc.CreateTransfer(ctx, input("p1", "DE-OK-1"))
	require.NoError(t, err)
	_, err = uc.CancelTransfer(ctx, "p2")
	require.NoError(t, err)
	require.Len(t, statement(t, uc), 2)
}

func Test_GetStatement(t *testing.T) {
	uc := newUseCase(t)
	ctx := context.Background()

	ok, err := uc.CreateTransfer(ctx, input("p1", "DE-OK-1"))
	require.NoError(t, err)
	_, err = uc.CreateTransfer(ctx, input("p2", "DECLINE-1"))
	require.NoError(t, err)
	_, err = uc.CancelTransfer(ctx, "p3")
	require.NoError(t, err)

	got := statement(t, uc)
	require.Len(t, got, 1)
	require.Equal(t, ok.ID, got[0].ID)

	empty, err := uc.GetStatement(ctx, allFrom, ok.CreatedAt.Add(-time.Hour))
	require.NoError(t, err)
	require.Empty(t, empty)
}

func Test_Admin(t *testing.T) {
	uc := newUseCase(t)
	ctx := context.Background()

	_, err := uc.AddTransfer(ctx, input("ROGUE-1", "DE-OK-1"))
	require.NoError(t, err)

	got := statement(t, uc)
	require.Len(t, got, 1)
	require.Equal(t, "ROGUE-1", got[0].Reference)

	updated, err := uc.SetTransferAmount(ctx, "ROGUE-1", 1)
	require.NoError(t, err)
	require.EqualValues(t, 1, updated.Amount)
	require.EqualValues(t, 1, statement(t, uc)[0].Amount)

	_, err = uc.SetTransferAmount(ctx, "MISSING", 1)
	require.ErrorIs(t, err, domain.ErrNotFound)
}
