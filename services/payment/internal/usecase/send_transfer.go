package usecase

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/vbloher/payment-processing-system/pkg/otel/tracer"
	"github.com/vbloher/payment-processing-system/services/payment/internal/domain"
)

// SendTransfer asks the bank to transfer the money. The payment ID is the bank reference, so a
// retry is idempotent on the bank side. Errors, including ErrBankUnavailable, are returned as is:
// an unknown outcome is never turned into a refusal.
func (u *UseCase) SendTransfer(ctx context.Context, paymentID uuid.UUID) (domain.BankOutcome, error) {
	ctx, span := tracer.Start(ctx, "usecase SendTransfer")
	defer span.End()

	payment, err := u.postgres.GetPayment(ctx, paymentID)
	if err != nil {
		return "", fmt.Errorf("postgres.GetPayment: %w", err)
	}

	transfer, err := u.bank.Transfer(ctx, payment.ID.String(), payment.Amount, payment.Currency, payment.Beneficiary)
	if err != nil {
		return "", fmt.Errorf("bank.Transfer: %w", err)
	}

	err = u.storeBankReference(ctx, payment, transfer)
	if err != nil {
		return "", err
	}

	return transfer.Status, nil
}
