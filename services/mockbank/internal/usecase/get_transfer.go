package usecase

import (
	"context"
	"fmt"

	"github.com/vbloher/payment-processing-system/pkg/otel/tracer"
	"github.com/vbloher/payment-processing-system/services/mockbank/internal/domain"
)

func (u *UseCase) GetTransfer(ctx context.Context, reference string) (domain.Transfer, error) {
	_, span := tracer.Start(ctx, "usecase GetTransfer")
	defer span.End()

	u.mu.Lock()
	defer u.mu.Unlock()

	if !u.storage.Available() {
		return domain.Transfer{}, fmt.Errorf("storage.Available: %w", domain.ErrUnavailable)
	}

	t, ok := u.storage.Get(reference)
	if !ok {
		return domain.Transfer{}, fmt.Errorf("reference %s: %w", reference, domain.ErrNotFound)
	}

	return t, nil
}
