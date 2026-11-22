package usecase

import (
	"context"
	"fmt"

	"github.com/vbloher/payment-processing-system/pkg/otel/tracer"
	"github.com/vbloher/payment-processing-system/services/payment/internal/dto"
)

func (u *UseCase) GetDiscrepancies(
	ctx context.Context, in dto.GetDiscrepanciesInput,
) (dto.GetDiscrepanciesOutput, error) {
	ctx, span := tracer.Start(ctx, "usecase GetDiscrepancies")
	defer span.End()

	list, err := u.postgres.ListDiscrepancies(ctx, in.Status)
	if err != nil {
		return dto.GetDiscrepanciesOutput{}, fmt.Errorf("postgres.ListDiscrepancies: %w", err)
	}

	return dto.GetDiscrepanciesOutput{Discrepancies: list}, nil
}
