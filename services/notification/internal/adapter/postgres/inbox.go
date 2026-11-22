package postgres

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/vbloher/payment-processing-system/pkg/otel/tracer"
)

// SaveInbox records the event and reports false when it was already recorded.
func (p *Postgres) SaveInbox(ctx context.Context, eventID uuid.UUID, eventType string) (bool, error) {
	ctx, span := tracer.Start(ctx, "adapter postgres SaveInbox")
	defer span.End()

	const sql = `INSERT INTO inbox (event_id, event_type) VALUES ($1, $2) ON CONFLICT (event_id) DO NOTHING`

	tag, err := p.tx.TryExtractTX(ctx).Exec(ctx, sql, eventID, eventType)
	if err != nil {
		return false, fmt.Errorf("txOrPool.Exec: %w", err)
	}

	return tag.RowsAffected() == 1, nil
}
