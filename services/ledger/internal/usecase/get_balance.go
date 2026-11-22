package usecase

import (
	"context"
	"fmt"

	"github.com/vbloher/payment-processing-system/pkg/otel/tracer"
	"github.com/vbloher/payment-processing-system/services/ledger/internal/dto"
)

func (u *UseCase) GetBalance(ctx context.Context, in dto.GetBalanceInput) (dto.GetBalanceOutput, error) {
	ctx, span := tracer.Start(ctx, "usecase GetBalance")
	defer span.End()

	account, err := u.postgres.GetAccount(ctx, in.AccountID)
	if err != nil {
		return dto.GetBalanceOutput{}, fmt.Errorf("postgres.GetAccount: %w", err)
	}

	return dto.GetBalanceOutput{
		Balance:   account.Balance,
		Held:      account.Held,
		Available: account.Available(),
		Currency:  account.Currency,
	}, nil
}
