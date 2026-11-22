package postgres

import (
	"context"
	"fmt"
	"sort"

	"github.com/vbloher/payment-processing-system/pkg/otel/tracer"
	"github.com/vbloher/payment-processing-system/services/payment/internal/domain"
)

// ReadOutbox takes up to limit events and deletes them in the caller's transaction.
// Rows locked by another relay are skipped; a rollback puts the events back.
func (p *Postgres) ReadOutbox(ctx context.Context, limit int) ([]domain.Event, error) {
	ctx, span := tracer.Start(ctx, "adapter postgres ReadOutbox")
	defer span.End()

	const sql = `WITH taken AS (SELECT id
                                   FROM outbox
                                   ORDER BY id
                                   LIMIT $1 FOR UPDATE SKIP LOCKED)
                    DELETE FROM outbox
                    WHERE id IN (SELECT id FROM taken)
                    RETURNING id, event_id, subject, payload, headers`

	rows, err := p.tx.TryExtractTX(ctx).Query(ctx, sql, limit)
	if err != nil {
		return nil, fmt.Errorf("txOrPool.Query: %w", err)
	}

	defer rows.Close()

	type row struct {
		id    int64
		event domain.Event
	}

	taken := make([]row, 0, limit)

	for rows.Next() {
		var r row

		err = rows.Scan(&r.id, &r.event.ID, &r.event.Subject, &r.event.Payload, &r.event.Headers)
		if err != nil {
			return nil, fmt.Errorf("rows.Scan: %w", err)
		}

		taken = append(taken, r)
	}

	err = rows.Err()
	if err != nil {
		return nil, fmt.Errorf("rows.Err: %w", err)
	}

	// DELETE ... RETURNING has no defined order; restore insertion order.
	sort.Slice(taken, func(i, j int) bool { return taken[i].id < taken[j].id })

	events := make([]domain.Event, len(taken))
	for i, r := range taken {
		events[i] = r.event
	}

	return events, nil
}
