package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/vbloher/payment-processing-system/pkg/otel/tracer"
	"github.com/vbloher/payment-processing-system/services/notification/internal/domain"
)

func (p *Postgres) CreateNotification(ctx context.Context, n domain.Notification) error {
	ctx, span := tracer.Start(ctx, "adapter postgres CreateNotification")
	defer span.End()

	const sql = `INSERT INTO notification (id, event_id, event_type, payment_id, payload, status, created_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7)`

	_, err := p.tx.TryExtractTX(ctx).Exec(ctx, sql,
		n.ID, n.EventID, n.EventType, n.PaymentID, n.Payload, string(n.Status), n.CreatedAt)
	if err != nil {
		return fmt.Errorf("txOrPool.Exec: %w", err)
	}

	return nil
}

func (p *Postgres) GetNotificationByEventID(ctx context.Context, eventID uuid.UUID) (domain.Notification, error) {
	ctx, span := tracer.Start(ctx, "adapter postgres GetNotificationByEventID")
	defer span.End()

	const sql = `SELECT id, event_id, event_type, payment_id, payload, status, created_at
                    FROM notification WHERE event_id = $1`

	var (
		n      domain.Notification
		status string
	)

	err := p.tx.TryExtractTX(ctx).QueryRow(ctx, sql, eventID).
		Scan(&n.ID, &n.EventID, &n.EventType, &n.PaymentID, &n.Payload, &status, &n.CreatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.Notification{}, domain.ErrNotFound
		}

		return domain.Notification{}, fmt.Errorf("txOrPool.QueryRow: %w", err)
	}

	n.Status = domain.Status(status)

	return n, nil
}

func (p *Postgres) MarkNotificationSent(ctx context.Context, id uuid.UUID) error {
	ctx, span := tracer.Start(ctx, "adapter postgres MarkNotificationSent")
	defer span.End()

	const sql = `UPDATE notification SET status = $2, sent_at = CURRENT_TIMESTAMP WHERE id = $1`

	tag, err := p.tx.TryExtractTX(ctx).Exec(ctx, sql, id, string(domain.StatusSent))
	if err != nil {
		return fmt.Errorf("txOrPool.Exec: %w", err)
	}

	if tag.RowsAffected() == 0 {
		return domain.ErrNotFound
	}

	return nil
}
