package usecase

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/vbloher/payment-processing-system/pkg/otel/tracer"
	"github.com/vbloher/payment-processing-system/services/mockbank/internal/domain"
	"github.com/vbloher/payment-processing-system/services/mockbank/internal/dto"
)

// SetAvailability turns the whole bank on or off. Admin: it ignores availability.
func (u *UseCase) SetAvailability(ctx context.Context, available bool) {
	_, span := tracer.Start(ctx, "usecase SetAvailability")
	defer span.End()

	u.mu.Lock()
	defer u.mu.Unlock()

	u.storage.SetAvailable(available)
}

// AddTransfer stores a succeeded transfer as is, e.g. a transfer unknown to the payment service.
func (u *UseCase) AddTransfer(ctx context.Context, in dto.CreateTransferInput) (domain.Transfer, error) {
	_, span := tracer.Start(ctx, "usecase AddTransfer")
	defer span.End()

	err := domain.ValidateRequest(in.Reference, in.Amount, in.Currency, in.Beneficiary)
	if err != nil {
		return domain.Transfer{}, fmt.Errorf("domain.ValidateRequest: %w", err)
	}

	u.mu.Lock()
	defer u.mu.Unlock()

	t := domain.Transfer{
		ID:          uuid.New(),
		Reference:   in.Reference,
		Amount:      in.Amount,
		Currency:    in.Currency,
		Beneficiary: in.Beneficiary,
		Status:      domain.StatusSucceeded,
		CreatedAt:   time.Now().UTC(),
	}
	u.storage.Save(t)

	return t, nil
}

// SetTransferAmount changes the amount of a stored transfer, to simulate drift.
func (u *UseCase) SetTransferAmount(ctx context.Context, reference string, amount int64) (domain.Transfer, error) {
	_, span := tracer.Start(ctx, "usecase SetTransferAmount")
	defer span.End()

	u.mu.Lock()
	defer u.mu.Unlock()

	t, ok := u.storage.Get(reference)
	if !ok {
		return domain.Transfer{}, fmt.Errorf("reference %s: %w", reference, domain.ErrNotFound)
	}

	t.Amount = amount
	u.storage.Save(t)

	return t, nil
}
