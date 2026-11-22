package usecase

import (
	"context"
	"fmt"
	"time"

	"github.com/vbloher/payment-processing-system/pkg/otel/tracer"
	"github.com/vbloher/payment-processing-system/services/mockbank/internal/domain"
)

// GetStatement returns the succeeded transfers created within [from, to].
func (u *UseCase) GetStatement(ctx context.Context, from, to time.Time) ([]domain.Transfer, error) {
	_, span := tracer.Start(ctx, "usecase GetStatement")
	defer span.End()

	u.mu.Lock()
	defer u.mu.Unlock()

	if !u.storage.Available() {
		return nil, fmt.Errorf("storage.Available: %w", domain.ErrUnavailable)
	}

	var res []domain.Transfer

	for _, t := range u.storage.List(from, to) {
		if t.Status == domain.StatusSucceeded {
			res = append(res, t)
		}
	}

	return res, nil
}
