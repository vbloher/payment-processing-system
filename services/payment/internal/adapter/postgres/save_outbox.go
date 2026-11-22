package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"

	"github.com/doug-martin/goqu/v9"

	_ "github.com/doug-martin/goqu/v9/dialect/postgres" // registers the postgres dialect

	"github.com/vbloher/payment-processing-system/pkg/otel/tracer"
	"github.com/vbloher/payment-processing-system/services/payment/internal/domain"
)

// SaveOutbox stores the events in the caller's transaction, adding the current trace context to their headers.
func (p *Postgres) SaveOutbox(ctx context.Context, events ...domain.Event) error {
	ctx, span := tracer.Start(ctx, "adapter postgres SaveOutbox")
	defer span.End()

	if len(events) == 0 {
		return nil
	}

	batch := make([]any, 0, len(events))

	for _, e := range events {
		headers := make(map[string]string, len(e.Headers))
		maps.Copy(headers, e.Headers)
		tracer.Inject(ctx, headers)

		h, err := json.Marshal(headers)
		if err != nil {
			return fmt.Errorf("json.Marshal: %w", err)
		}

		batch = append(batch, goqu.Record{
			"event_id": e.ID,
			"subject":  e.Subject,
			"payload":  e.Payload,
			"headers":  string(h),
		})
	}

	sql, args, err := goqu.Dialect("postgres").Insert("outbox").Prepared(true).Rows(batch...).ToSQL()
	if err != nil {
		return fmt.Errorf("goqu.Insert.ToSQL: %w", err)
	}

	_, err = p.tx.TryExtractTX(ctx).Exec(ctx, sql, args...)
	if err != nil {
		return fmt.Errorf("txOrPool.Exec: %w", err)
	}

	return nil
}
