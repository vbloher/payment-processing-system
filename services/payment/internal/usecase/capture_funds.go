package usecase

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/vbloher/payment-processing-system/pkg/otel/tracer"
)

func (u *UseCase) CaptureFunds(ctx context.Context, paymentID uuid.UUID) error {
	ctx, span := tracer.Start(ctx, "usecase CaptureFunds")
	defer span.End()

	err := u.ledger.Capture(ctx, paymentID)
	if err != nil {
		return fmt.Errorf("ledger.Capture: %w", err)
	}

	return nil
}
