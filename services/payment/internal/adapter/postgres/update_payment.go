package postgres

import (
	"context"
	"fmt"

	"github.com/vbloher/payment-processing-system/pkg/otel/tracer"
	"github.com/vbloher/payment-processing-system/services/payment/internal/domain"
)

// UpdatePayment writes the mutable fields and sets updated_at to the statement time.
func (p *Postgres) UpdatePayment(ctx context.Context, payment domain.Payment) error {
	ctx, span := tracer.Start(ctx, "adapter postgres UpdatePayment")
	defer span.End()

	const sql = `UPDATE payment SET status = $2, failure_reason = $3, bank_reference = $4, updated_at = clock_timestamp()
                    WHERE id = $1`

	tag, err := p.tx.TryExtractTX(ctx).Exec(ctx, sql,
		payment.ID, string(payment.Status), string(payment.FailureReason), payment.BankReference)
	if err != nil {
		return fmt.Errorf("txOrPool.Exec: %w", err)
	}

	if tag.RowsAffected() == 0 {
		return domain.ErrNotFound
	}

	return nil
}
