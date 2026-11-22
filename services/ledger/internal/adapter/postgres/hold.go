package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/vbloher/payment-processing-system/pkg/otel/tracer"
	"github.com/vbloher/payment-processing-system/services/ledger/internal/domain"
)

const holdColumns = `payment_id, account_id, amount, currency, status, created_at, updated_at`

func scanHold(row pgx.Row) (domain.Hold, error) {
	var (
		h      domain.Hold
		status string
	)

	err := row.Scan(&h.PaymentID, &h.AccountID, &h.Amount, &h.Currency, &status, &h.CreatedAt, &h.UpdatedAt)
	if err != nil {
		return domain.Hold{}, err //nolint:wrapcheck // wrapped by callers
	}

	h.Status = domain.HoldStatus(status)

	return h, nil
}

func (p *Postgres) CreateHold(ctx context.Context, hold domain.Hold) error {
	ctx, span := tracer.Start(ctx, "adapter postgres CreateHold")
	defer span.End()

	const sql = `INSERT INTO hold (payment_id, account_id, amount, currency, status, created_at, updated_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7)`

	_, err := p.tx.TryExtractTX(ctx).Exec(ctx, sql,
		hold.PaymentID, hold.AccountID, hold.Amount, hold.Currency, string(hold.Status),
		hold.CreatedAt, hold.UpdatedAt)
	if err != nil {
		return fmt.Errorf("txOrPool.Exec: %w", err)
	}

	return nil
}

func (p *Postgres) GetHold(ctx context.Context, paymentID uuid.UUID) (domain.Hold, error) {
	ctx, span := tracer.Start(ctx, "adapter postgres GetHold")
	defer span.End()

	return p.getHold(ctx, "SELECT "+holdColumns+" FROM hold WHERE payment_id = $1", paymentID)
}

func (p *Postgres) GetHoldForUpdate(ctx context.Context, paymentID uuid.UUID) (domain.Hold, error) {
	ctx, span := tracer.Start(ctx, "adapter postgres GetHoldForUpdate")
	defer span.End()

	return p.getHold(ctx, "SELECT "+holdColumns+" FROM hold WHERE payment_id = $1 FOR UPDATE", paymentID)
}

func (p *Postgres) getHold(ctx context.Context, sql string, paymentID uuid.UUID) (domain.Hold, error) {
	hold, err := scanHold(p.tx.TryExtractTX(ctx).QueryRow(ctx, sql, paymentID))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.Hold{}, domain.ErrNotFound
		}

		return domain.Hold{}, fmt.Errorf("txOrPool.QueryRow: %w", err)
	}

	return hold, nil
}

func (p *Postgres) UpdateHoldStatus(ctx context.Context, hold domain.Hold) error {
	ctx, span := tracer.Start(ctx, "adapter postgres UpdateHoldStatus")
	defer span.End()

	const sql = `UPDATE hold SET status = $2, updated_at = $3 WHERE payment_id = $1`

	tag, err := p.tx.TryExtractTX(ctx).Exec(ctx, sql, hold.PaymentID, string(hold.Status), hold.UpdatedAt)
	if err != nil {
		return fmt.Errorf("txOrPool.Exec: %w", err)
	}

	if tag.RowsAffected() == 0 {
		return domain.ErrNotFound
	}

	return nil
}

// GetHolds returns the holds that exist; unknown payment IDs are skipped.
func (p *Postgres) GetHolds(ctx context.Context, paymentIDs []uuid.UUID) ([]domain.Hold, error) {
	ctx, span := tracer.Start(ctx, "adapter postgres GetHolds")
	defer span.End()

	const sql = `SELECT ` + holdColumns + ` FROM hold WHERE payment_id = ANY($1) ORDER BY created_at, payment_id`

	rows, err := p.tx.TryExtractTX(ctx).Query(ctx, sql, paymentIDs)
	if err != nil {
		return nil, fmt.Errorf("txOrPool.Query: %w", err)
	}
	defer rows.Close()

	holds := make([]domain.Hold, 0, len(paymentIDs))

	for rows.Next() {
		hold, err := scanHold(rows)
		if err != nil {
			return nil, fmt.Errorf("scanHold: %w", err)
		}

		holds = append(holds, hold)
	}

	err = rows.Err()
	if err != nil {
		return nil, fmt.Errorf("rows.Err: %w", err)
	}

	return holds, nil
}
