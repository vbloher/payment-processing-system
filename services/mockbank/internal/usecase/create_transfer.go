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

// CreateTransfer creates a transfer idempotently by reference. The beneficiary prefix selects a failure mode.
func (u *UseCase) CreateTransfer(ctx context.Context, in dto.CreateTransferInput) (domain.Transfer, error) {
	_, span := tracer.Start(ctx, "usecase CreateTransfer")
	defer span.End()

	u.mu.Lock()
	defer u.mu.Unlock()

	if !u.storage.Available() {
		return domain.Transfer{}, fmt.Errorf("storage.Available: %w", domain.ErrUnavailable)
	}

	err := domain.ValidateRequest(in.Reference, in.Amount, in.Currency, in.Beneficiary)
	if err != nil {
		return domain.Transfer{}, fmt.Errorf("domain.ValidateRequest: %w", err)
	}

	existing, ok := u.storage.Get(in.Reference)
	if ok {
		return u.replay(existing, in)
	}

	t := domain.Transfer{
		ID:          uuid.New(),
		Reference:   in.Reference,
		Amount:      in.Amount,
		Currency:    in.Currency,
		Beneficiary: in.Beneficiary,
		Status:      domain.StatusSucceeded,
		CreatedAt:   time.Now().UTC(),
	}

	switch {
	case t.HasPrefix(domain.PrefixDecline):
		t.Status = domain.StatusDeclined
		t.DeclineReason = domain.DeclineReasonAccountClosed
		u.storage.Save(t)

		return t, nil
	case t.HasPrefix(domain.PrefixFlaky):
		if u.storage.NextAttempt(in.Reference) <= domain.FlakyFailures {
			return domain.Transfer{}, fmt.Errorf("flaky bank: %w", domain.ErrUnavailable)
		}

		u.storage.Save(t)

		return t, nil
	case t.HasPrefix(domain.PrefixTimeout):
		u.storage.Save(t)

		return domain.Transfer{}, fmt.Errorf("timeout bank: %w", domain.ErrTimeout)
	case t.HasPrefix(domain.PrefixBlackout):
		u.storage.Save(t)
		u.storage.SetAvailable(false)

		return domain.Transfer{}, fmt.Errorf("blackout bank: %w", domain.ErrUnavailable)
	default:
		u.storage.Save(t)

		return t, nil
	}
}

func (u *UseCase) replay(existing domain.Transfer, in dto.CreateTransferInput) (domain.Transfer, error) {
	if existing.Status == domain.StatusCancelled {
		return existing, nil // the reference is fenced
	}

	if !existing.SameRequest(in.Amount, in.Currency, in.Beneficiary) {
		return domain.Transfer{}, fmt.Errorf("reference %s: %w", in.Reference, domain.ErrConflict)
	}

	if existing.HasPrefix(domain.PrefixTimeout) {
		return domain.Transfer{}, fmt.Errorf("timeout bank: %w", domain.ErrTimeout)
	}

	return existing, nil
}
