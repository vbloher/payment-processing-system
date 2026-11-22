package usecase

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/vbloher/payment-processing-system/pkg/otel/tracer"
)

func (u *UseCase) ReleaseFunds(ctx context.Context, paymentID uuid.UUID) error {
	ctx, span := tracer.Start(ctx, "usecase ReleaseFunds")
	defer span.End()

	err := u.ledger.Release(ctx, paymentID)
	if err != nil {
		return fmt.Errorf("ledger.Release: %w", err)
	}

	return nil
}
