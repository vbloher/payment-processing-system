package usecase

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/vbloher/payment-processing-system/pkg/otel/tracer"
	"github.com/vbloher/payment-processing-system/services/mockbank/internal/domain"
)

// CancelTransfer returns an existing transfer unchanged. For an unknown reference it stores a cancelled
// transfer that fences the reference: a late create returns it and moves no money.
func (u *UseCase) CancelTransfer(ctx context.Context, reference string) (domain.Transfer, error) {
	_, span := tracer.Start(ctx, "usecase CancelTransfer")
	defer span.End()

	u.mu.Lock()
	defer u.mu.Unlock()

	if !u.storage.Available() {
		return domain.Transfer{}, fmt.Errorf("storage.Available: %w", domain.ErrUnavailable)
	}

	existing, ok := u.storage.Get(reference)
	if ok {
		return existing, nil
	}

	t := domain.Transfer{
		ID:        uuid.New(),
		Reference: reference,
		Status:    domain.StatusCancelled,
		CreatedAt: time.Now().UTC(),
	}
	u.storage.Save(t)

	return t, nil
}
