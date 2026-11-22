package usecase

import (
	"context"
	"fmt"

	"github.com/vbloher/payment-processing-system/pkg/otel/tracer"
	"github.com/vbloher/payment-processing-system/services/ledger/internal/domain"
	"github.com/vbloher/payment-processing-system/services/ledger/internal/dto"
)

func (u *UseCase) OpenAccount(ctx context.Context, in dto.OpenAccountInput) (dto.OpenAccountOutput, error) {
	ctx, span := tracer.Start(ctx, "usecase OpenAccount")
	defer span.End()

	account, err := domain.NewCustomerAccount(in.Currency)
	if err != nil {
		return dto.OpenAccountOutput{}, fmt.Errorf("domain.NewCustomerAccount: %w", err)
	}

	err = u.postgres.CreateAccount(ctx, account)
	if err != nil {
		return dto.OpenAccountOutput{}, fmt.Errorf("postgres.CreateAccount: %w", err)
	}

	return dto.OpenAccountOutput{ID: account.ID}, nil
}
