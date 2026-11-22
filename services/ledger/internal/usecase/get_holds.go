package usecase

import (
	"context"
	"fmt"

	"github.com/vbloher/payment-processing-system/pkg/otel/tracer"
	"github.com/vbloher/payment-processing-system/services/ledger/internal/dto"
)

func (u *UseCase) GetHolds(ctx context.Context, in dto.GetHoldsInput) (dto.GetHoldsOutput, error) {
	ctx, span := tracer.Start(ctx, "usecase GetHolds")
	defer span.End()

	holds, err := u.postgres.GetHolds(ctx, in.PaymentIDs)
	if err != nil {
		return dto.GetHoldsOutput{}, fmt.Errorf("postgres.GetHolds: %w", err)
	}

	return dto.GetHoldsOutput{Holds: holds}, nil
}
