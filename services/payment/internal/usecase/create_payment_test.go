package usecase_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/vbloher/payment-processing-system/pkg/otel"
	"github.com/vbloher/payment-processing-system/services/payment/internal/domain"
	"github.com/vbloher/payment-processing-system/services/payment/internal/dto"
)

func Test_CreatePayment(t *testing.T) {
	otel.SilentModeInit()

	accountID := uuid.New()
	in := dto.CreatePaymentInput{
		IdempotencyKey: "key-1", AccountID: accountID, Amount: 3000, Currency: "EUR", Beneficiary: "ACME", Description: "inv",
	}
	hash := domain.RequestHash(accountID, 3000, "EUR", "ACME", "inv")
	errStart := errors.New("temporal down")

	t.Run("new", func(t *testing.T) {
		u, d := newUseCase(t)

		var created domain.Payment

		d.postgres.EXPECT().CreatePayment(Any, Any).RunAndReturn(func(_ context.Context, p domain.Payment) (bool, error) {
			created = p

			return true, nil
		})
		d.orchestrator.EXPECT().StartPayment(Any, Any).RunAndReturn(func(_ context.Context, id uuid.UUID) error {
			require.Equal(t, created.ID, id)

			return nil
		})

		out, err := u.CreatePayment(t.Context(), in)
		require.NoError(t, err)
		require.Equal(t, created.ID, out.Payment.ID)
		require.Equal(t, domain.StatusCreated, out.Payment.Status)
	})

	t.Run("replay", func(t *testing.T) {
		u, d := newUseCase(t)
		stored := newPayment(domain.StatusProcessing)
		stored.RequestHash = hash

		d.postgres.EXPECT().CreatePayment(Any, Any).Return(false, nil)
		d.postgres.EXPECT().GetPaymentByIdempotencyKey(Any, "key-1").Return(stored, nil)
		d.orchestrator.EXPECT().StartPayment(Any, stored.ID).Return(nil)

		out, err := u.CreatePayment(t.Context(), in)
		require.NoError(t, err)
		require.Equal(t, stored.ID, out.Payment.ID)
	})

	t.Run("key reused", func(t *testing.T) {
		u, d := newUseCase(t)
		stored := newPayment(domain.StatusCreated)
		stored.RequestHash = "other"

		d.postgres.EXPECT().CreatePayment(Any, Any).Return(false, nil)
		d.postgres.EXPECT().GetPaymentByIdempotencyKey(Any, "key-1").Return(stored, nil)

		_, err := u.CreatePayment(t.Context(), in)
		require.ErrorIs(t, err, domain.ErrIdempotencyConflict)
	})

	t.Run("invalid", func(t *testing.T) {
		u, _ := newUseCase(t)
		bad := in
		bad.Amount = 0

		_, err := u.CreatePayment(t.Context(), bad)
		require.ErrorIs(t, err, domain.ErrValidation)
	})

	t.Run("no key", func(t *testing.T) {
		u, _ := newUseCase(t)
		bad := in
		bad.IdempotencyKey = ""

		_, err := u.CreatePayment(t.Context(), bad)
		require.ErrorIs(t, err, domain.ErrIdempotencyKeyRequired)
	})

	t.Run("start fails", func(t *testing.T) {
		u, d := newUseCase(t)

		d.postgres.EXPECT().CreatePayment(Any, Any).Return(true, nil)
		d.orchestrator.EXPECT().StartPayment(Any, Any).Return(errStart)

		_, err := u.CreatePayment(t.Context(), in)
		require.ErrorIs(t, err, errStart)
	})
}
