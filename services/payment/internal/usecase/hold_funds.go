package usecase

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"

	"github.com/vbloher/payment-processing-system/pkg/otel/tracer"
	"github.com/vbloher/payment-processing-system/services/payment/internal/domain"
)

// HoldFunds places the ledger hold. A definitive refusal is returned as a failure reason with a nil
// error, so the workflow can compensate; any other error is returned for the activity to retry.
func (u *UseCase) HoldFunds(ctx context.Context, paymentID uuid.UUID) (domain.FailureReason, error) {
	ctx, span := tracer.Start(ctx, "usecase HoldFunds")
	defer span.End()

	payment, err := u.postgres.GetPayment(ctx, paymentID)
	if err != nil {
		return domain.ReasonNone, fmt.Errorf("postgres.GetPayment: %w", err)
	}

	err = u.ledger.Hold(ctx, payment.ID, payment.AccountID, payment.Amount, payment.Currency)

	switch {
	case err == nil:
		return domain.ReasonNone, nil
	case errors.Is(err, domain.ErrInsufficientFunds):
		return domain.ReasonInsufficientFunds, nil
	case errors.Is(err, domain.ErrAccountNotFound):
		return domain.ReasonAccountNotFound, nil
	case errors.Is(err, domain.ErrLedgerRejected):
		return domain.ReasonLedgerRejected, nil
	default:
		return domain.ReasonNone, fmt.Errorf("ledger.Hold: %w", err)
	}
}
