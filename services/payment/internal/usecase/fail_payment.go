package usecase

import (
	"context"

	"github.com/google/uuid"

	"github.com/vbloher/payment-processing-system/pkg/events"
	"github.com/vbloher/payment-processing-system/pkg/otel/tracer"
	"github.com/vbloher/payment-processing-system/services/payment/internal/domain"
)

func (u *UseCase) FailPayment(ctx context.Context, paymentID uuid.UUID, reason domain.FailureReason) error {
	ctx, span := tracer.Start(ctx, "usecase FailPayment")
	defer span.End()

	return u.transition(ctx, paymentID, domain.StatusFailed, events.TypePaymentFailed, func(p *domain.Payment) {
		p.FailureReason = reason
	})
}
