package usecase

import (
	"context"

	"github.com/google/uuid"

	"github.com/vbloher/payment-processing-system/pkg/events"
	"github.com/vbloher/payment-processing-system/pkg/otel/tracer"
	"github.com/vbloher/payment-processing-system/services/payment/internal/domain"
)

func (u *UseCase) MarkPaymentForReview(ctx context.Context, paymentID uuid.UUID) error {
	ctx, span := tracer.Start(ctx, "usecase MarkPaymentForReview")
	defer span.End()

	return u.transition(ctx, paymentID, domain.StatusReview, events.TypePaymentReviewRequired, nil)
}
