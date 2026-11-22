package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/vbloher/payment-processing-system/pkg/otel/tracer"
	"github.com/vbloher/payment-processing-system/services/payment/internal/domain"
)

const paymentColumns = `id, idempotency_key, request_hash, account_id, amount, currency, beneficiary, description,
                        status, failure_reason, bank_reference, created_at, updated_at`

func scanPayment(row pgx.Row) (domain.Payment, error) {
	var (
		payment domain.Payment
		status  string
		reason  string
	)

	err := row.Scan(&payment.ID, &payment.IdempotencyKey, &payment.RequestHash, &payment.AccountID, &payment.Amount,
		&payment.Currency, &payment.Beneficiary, &payment.Description, &status, &reason, &payment.BankReference,
		&payment.CreatedAt, &payment.UpdatedAt)
	if err != nil {
		return domain.Payment{}, err //nolint:wrapcheck // wrapped by callers
	}

	payment.Status, err = parseStatus(status)
	if err != nil {
		return domain.Payment{}, err
	}

	payment.FailureReason = domain.FailureReason(reason)

	return payment, nil
}

func parseStatus(s string) (domain.Status, error) {
	status := domain.Status(s)

	switch status {
	case domain.StatusCreated, domain.StatusProcessing, domain.StatusSucceeded, domain.StatusFailed,
		domain.StatusReview:
		return status, nil
	}

	return "", fmt.Errorf("unknown payment status %q", s)
}

func (p *Postgres) GetPayment(ctx context.Context, id uuid.UUID) (domain.Payment, error) {
	ctx, span := tracer.Start(ctx, "adapter postgres GetPayment")
	defer span.End()

	return p.getPayment(ctx, "SELECT "+paymentColumns+" FROM payment WHERE id = $1", id)
}

func (p *Postgres) GetPaymentForUpdate(ctx context.Context, id uuid.UUID) (domain.Payment, error) {
	ctx, span := tracer.Start(ctx, "adapter postgres GetPaymentForUpdate")
	defer span.End()

	return p.getPayment(ctx, "SELECT "+paymentColumns+" FROM payment WHERE id = $1 FOR UPDATE", id)
}

func (p *Postgres) GetPaymentByIdempotencyKey(ctx context.Context, key string) (domain.Payment, error) {
	ctx, span := tracer.Start(ctx, "adapter postgres GetPaymentByIdempotencyKey")
	defer span.End()

	return p.getPayment(ctx, "SELECT "+paymentColumns+" FROM payment WHERE idempotency_key = $1", key)
}

func (p *Postgres) getPayment(ctx context.Context, sql string, arg any) (domain.Payment, error) {
	payment, err := scanPayment(p.tx.TryExtractTX(ctx).QueryRow(ctx, sql, arg))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.Payment{}, domain.ErrNotFound
		}

		return domain.Payment{}, fmt.Errorf("txOrPool.QueryRow: %w", err)
	}

	return payment, nil
}
