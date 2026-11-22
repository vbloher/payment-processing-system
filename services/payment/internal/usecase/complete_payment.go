package usecase

import (
	"context"

	"github.com/google/uuid"

	"github.com/vbloher/payment-processing-system/pkg/events"
	"github.com/vbloher/payment-processing-system/pkg/otel/tracer"
	"github.com/vbloher/payment-processing-system/services/payment/internal/domain"
)

func (u *UseCase) CompletePayment(ctx context.Context, paymentID uuid.UUID) error {
	ctx, span := tracer.Start(ctx, "usecase CompletePayment")
	defer span.End()

	return u.transition(ctx, paymentID, domain.StatusSucceeded, events.TypePaymentSucceeded, nil)
}
