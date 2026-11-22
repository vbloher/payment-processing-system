package postgres

import (
	"context"
	"fmt"

	"github.com/vbloher/payment-processing-system/pkg/otel/tracer"
	"github.com/vbloher/payment-processing-system/services/payment/internal/domain"
)

// CreatePayment inserts the payment. It reports false, without an error,
// when a payment with the same idempotency key already exists.
func (p *Postgres) CreatePayment(ctx context.Context, payment domain.Payment) (bool, error) {
	ctx, span := tracer.Start(ctx, "adapter postgres CreatePayment")
	defer span.End()

	const sql = `INSERT INTO payment (id, idempotency_key, request_hash, account_id, amount, currency, beneficiary,
                                     description, status, failure_reason, bank_reference, created_at, updated_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                    ON CONFLICT (idempotency_key) DO NOTHING`

	tag, err := p.tx.TryExtractTX(ctx).Exec(ctx, sql,
		payment.ID, payment.IdempotencyKey, payment.RequestHash, payment.AccountID, payment.Amount, payment.Currency,
		payment.Beneficiary, payment.Description, string(payment.Status), string(payment.FailureReason),
		payment.BankReference, payment.CreatedAt, payment.UpdatedAt)
	if err != nil {
		return false, fmt.Errorf("txOrPool.Exec: %w", err)
	}

	return tag.RowsAffected() == 1, nil
}
