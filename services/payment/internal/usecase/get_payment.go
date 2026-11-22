package usecase

import (
	"context"
	"fmt"

	"github.com/vbloher/payment-processing-system/pkg/otel/tracer"
	"github.com/vbloher/payment-processing-system/services/payment/internal/dto"
)

func (u *UseCase) GetPayment(ctx context.Context, in dto.GetPaymentInput) (dto.PaymentOutput, error) {
	ctx, span := tracer.Start(ctx, "usecase GetPayment")
	defer span.End()

	payment, err := u.postgres.GetPayment(ctx, in.ID)
	if err != nil {
		return dto.PaymentOutput{}, fmt.Errorf("postgres.GetPayment: %w", err)
	}

	return dto.PaymentOutput{Payment: payment}, nil
}
