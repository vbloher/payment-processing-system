package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/vbloher/payment-processing-system/pkg/otel/tracer"
	"github.com/vbloher/payment-processing-system/services/payment/internal/domain"
)

func (p *Postgres) GetPayments(ctx context.Context, ids []uuid.UUID) ([]domain.Payment, error) {
	ctx, span := tracer.Start(ctx, "adapter postgres GetPayments")
	defer span.End()

	return p.listPayments(ctx, "SELECT "+paymentColumns+" FROM payment WHERE id = ANY($1) ORDER BY created_at, id", ids)
}

func (p *Postgres) ListPaymentsByStatus(ctx context.Context, status domain.Status) ([]domain.Payment, error) {
	ctx, span := tracer.Start(ctx, "adapter postgres ListPaymentsByStatus")
	defer span.End()

	return p.listPayments(ctx,
		"SELECT "+paymentColumns+" FROM payment WHERE status = $1 ORDER BY created_at, id", string(status))
}

// ListPaymentsUpdatedBetween returns payments with from <= updated_at < to.
func (p *Postgres) ListPaymentsUpdatedBetween(ctx context.Context, from, to time.Time) ([]domain.Payment, error) {
	ctx, span := tracer.Start(ctx, "adapter postgres ListPaymentsUpdatedBetween")
	defer span.End()

	return p.listPayments(ctx,
		"SELECT "+paymentColumns+" FROM payment WHERE updated_at >= $1 AND updated_at < $2 ORDER BY created_at, id",
		from, to)
}

func (p *Postgres) listPayments(ctx context.Context, sql string, args ...any) ([]domain.Payment, error) {
	rows, err := p.tx.TryExtractTX(ctx).Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("txOrPool.Query: %w", err)
	}

	defer rows.Close()

	payments := make([]domain.Payment, 0)

	for rows.Next() {
		payment, scanErr := scanPayment(rows)
		if scanErr != nil {
			return nil, fmt.Errorf("scanPayment: %w", scanErr)
		}

		payments = append(payments, payment)
	}

	err = rows.Err()
	if err != nil {
		return nil, fmt.Errorf("rows.Err: %w", err)
	}

	return payments, nil
}
