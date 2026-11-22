package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/vbloher/payment-processing-system/pkg/otel/tracer"
	"github.com/vbloher/payment-processing-system/services/payment/internal/domain"
)

const discrepancyColumns = "id, kind, reference, payment_id, details, status, created_at, resolved_at"

func scanDiscrepancy(row pgx.Row) (domain.Discrepancy, error) {
	var (
		d          domain.Discrepancy
		kind       string
		status     string
		paymentID  *uuid.UUID
		resolvedAt *time.Time
	)

	err := row.Scan(&d.ID, &kind, &d.Reference, &paymentID, &d.Details, &status, &d.CreatedAt, &resolvedAt)
	if err != nil {
		return domain.Discrepancy{}, err //nolint:wrapcheck // wrapped by callers
	}

	d.Kind = domain.DiscrepancyKind(kind)
	d.Status = domain.DiscrepancyStatus(status)

	if paymentID != nil {
		d.PaymentID = *paymentID
	}

	if resolvedAt != nil {
		d.ResolvedAt = *resolvedAt
	}

	return d, nil
}

// CreateDiscrepancy reports false when an open discrepancy for (reference, kind) already exists.
// uuid.Nil is stored as NULL. created_at comes from the caller, not from the database clock.
func (p *Postgres) CreateDiscrepancy(ctx context.Context, d domain.Discrepancy) (bool, error) {
	ctx, span := tracer.Start(ctx, "adapter postgres CreateDiscrepancy")
	defer span.End()

	var paymentID any
	if d.PaymentID != uuid.Nil {
		paymentID = d.PaymentID
	}

	tag, err := p.tx.TryExtractTX(ctx).Exec(ctx,
		`INSERT INTO reconciliation_discrepancy (id, kind, reference, payment_id, details, status, created_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)
		 ON CONFLICT (reference, kind) WHERE status = 'open' DO NOTHING`,
		d.ID, string(d.Kind), d.Reference, paymentID, d.Details, string(d.Status), d.CreatedAt)
	if err != nil {
		return false, fmt.Errorf("txOrPool.Exec: %w", err)
	}

	return tag.RowsAffected() == 1, nil
}

// ResolveDiscrepancy marks an open discrepancy resolved. An unknown or already resolved ID is ErrNotFound.
func (p *Postgres) ResolveDiscrepancy(ctx context.Context, id uuid.UUID) error {
	ctx, span := tracer.Start(ctx, "adapter postgres ResolveDiscrepancy")
	defer span.End()

	tag, err := p.tx.TryExtractTX(ctx).Exec(ctx,
		`UPDATE reconciliation_discrepancy SET status = 'resolved', resolved_at = now()
		 WHERE id = $1 AND status = 'open'`, id)
	if err != nil {
		return fmt.Errorf("txOrPool.Exec: %w", err)
	}

	if tag.RowsAffected() == 0 {
		return domain.ErrNotFound
	}

	return nil
}

func (p *Postgres) ListOpenDiscrepancies(ctx context.Context) ([]domain.Discrepancy, error) {
	ctx, span := tracer.Start(ctx, "adapter postgres ListOpenDiscrepancies")
	defer span.End()

	return p.listDiscrepancies(ctx,
		"SELECT "+discrepancyColumns+" FROM reconciliation_discrepancy WHERE status = 'open' ORDER BY created_at, id")
}

// ListDiscrepancies returns discrepancies newest first; an empty status lists all.
func (p *Postgres) ListDiscrepancies(
	ctx context.Context, status domain.DiscrepancyStatus,
) ([]domain.Discrepancy, error) {
	ctx, span := tracer.Start(ctx, "adapter postgres ListDiscrepancies")
	defer span.End()

	return p.listDiscrepancies(ctx,
		"SELECT "+discrepancyColumns+` FROM reconciliation_discrepancy
		 WHERE ($1 = '' OR status = $1) ORDER BY created_at DESC, id`, string(status))
}

func (p *Postgres) listDiscrepancies(ctx context.Context, sql string, args ...any) ([]domain.Discrepancy, error) {
	rows, err := p.tx.TryExtractTX(ctx).Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("txOrPool.Query: %w", err)
	}

	defer rows.Close()

	result := make([]domain.Discrepancy, 0)

	for rows.Next() {
		d, scanErr := scanDiscrepancy(rows)
		if scanErr != nil {
			return nil, fmt.Errorf("scanDiscrepancy: %w", scanErr)
		}

		result = append(result, d)
	}

	err = rows.Err()
	if err != nil {
		return nil, fmt.Errorf("rows.Err: %w", err)
	}

	return result, nil
}
