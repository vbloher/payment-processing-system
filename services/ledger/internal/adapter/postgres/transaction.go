package postgres

import (
	"context"
	"fmt"

	"github.com/vbloher/payment-processing-system/pkg/otel/tracer"
	"github.com/vbloher/payment-processing-system/services/ledger/internal/domain"
)

// CreateTransaction stores the transaction and its entries. It reports false and stores
// nothing when a transaction with the same (kind, reference) already exists.
func (p *Postgres) CreateTransaction(ctx context.Context, tx domain.Transaction) (bool, error) {
	ctx, span := tracer.Start(ctx, "adapter postgres CreateTransaction")
	defer span.End()

	const insertTx = `INSERT INTO ledger_transaction (id, kind, reference, currency, created_at)
                    VALUES ($1, $2, $3, $4, $5) ON CONFLICT (kind, reference) DO NOTHING`

	const insertEntry = `INSERT INTO ledger_entry (transaction_id, account_id, side, amount, created_at)
                    VALUES ($1, $2, $3, $4, $5)`

	txOrPool := p.tx.TryExtractTX(ctx)

	tag, err := txOrPool.Exec(ctx, insertTx, tx.ID, string(tx.Kind), tx.Reference, tx.Currency, tx.CreatedAt)
	if err != nil {
		return false, fmt.Errorf("txOrPool.Exec transaction: %w", err)
	}

	if tag.RowsAffected() == 0 {
		return false, nil
	}

	for _, e := range tx.Entries {
		_, err = txOrPool.Exec(ctx, insertEntry, tx.ID, e.AccountID, string(e.Side), e.Amount, tx.CreatedAt)
		if err != nil {
			return false, fmt.Errorf("txOrPool.Exec entry: %w", err)
		}
	}

	return true, nil
}
