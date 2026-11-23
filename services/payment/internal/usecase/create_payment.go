package usecase

import (
	"context"
	"fmt"
	"time"

	"github.com/vbloher/payment-processing-system/pkg/otel/tracer"
	"github.com/vbloher/payment-processing-system/services/payment/internal/domain"
	"github.com/vbloher/payment-processing-system/services/payment/internal/dto"
)

// startTimeout bounds the workflow start, which no longer ends with the request.
const startTimeout = 10 * time.Second

// CreatePayment inserts the payment first and starts the workflow second. If the start fails the
// payment stays "created" and the caller's retry with the same key finds it and starts it again.
func (u *UseCase) CreatePayment(ctx context.Context, in dto.CreatePaymentInput) (dto.PaymentOutput, error) {
	ctx, span := tracer.Start(ctx, "usecase CreatePayment")
	defer span.End()

	payment, err := domain.NewPayment(
		in.IdempotencyKey, in.AccountID, in.Amount, in.Currency, in.Beneficiary, in.Description,
	)
	if err != nil {
		return dto.PaymentOutput{}, fmt.Errorf("domain.NewPayment: %w", err)
	}

	created, err := u.postgres.CreatePayment(ctx, payment)
	if err != nil {
		return dto.PaymentOutput{}, fmt.Errorf("postgres.CreatePayment: %w", err)
	}

	if !created {
		stored, err := u.postgres.GetPaymentByIdempotencyKey(ctx, in.IdempotencyKey)
		if err != nil {
			return dto.PaymentOutput{}, fmt.Errorf("postgres.GetPaymentByIdempotencyKey: %w", err)
		}

		if stored.RequestHash != payment.RequestHash {
			return dto.PaymentOutput{}, domain.ErrIdempotencyConflict
		}

		payment = stored
	}

	// Once the row exists, a client that disconnects must not keep its workflow from starting: the payment
	// would wait in "created" for reconciliation.
	startCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), startTimeout)
	defer cancel()

	err = u.orchestrator.StartPayment(startCtx, payment.ID)
	if err != nil {
		return dto.PaymentOutput{}, fmt.Errorf("orchestrator.StartPayment: %w", err)
	}

	return dto.PaymentOutput{Payment: payment}, nil
}
