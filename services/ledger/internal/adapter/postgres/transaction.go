package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

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

// GetTransaction returns the transaction with its entries, or ErrNotFound.
func (p *Postgres) GetTransaction(
	ctx context.Context, kind domain.TransactionKind, reference string,
) (domain.Transaction, error) {
	ctx, span := tracer.Start(ctx, "adapter postgres GetTransaction")
	defer span.End()

	const selectTx = `SELECT id, currency, created_at FROM ledger_transaction WHERE kind = $1 AND reference = $2`

	const selectEntries = `SELECT account_id, side, amount FROM ledger_entry WHERE transaction_id = $1 ORDER BY id`

	txOrPool := p.tx.TryExtractTX(ctx)
	tx := domain.Transaction{Kind: kind, Reference: reference}

	err := txOrPool.QueryRow(ctx, selectTx, string(kind), reference).Scan(&tx.ID, &tx.Currency, &tx.CreatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.Transaction{}, domain.ErrNotFound
		}

		return domain.Transaction{}, fmt.Errorf("txOrPool.QueryRow: %w", err)
	}

	rows, err := txOrPool.Query(ctx, selectEntries, tx.ID)
	if err != nil {
		return domain.Transaction{}, fmt.Errorf("txOrPool.Query: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			e    domain.Entry
			side string
		)

		err = rows.Scan(&e.AccountID, &side, &e.Amount)
		if err != nil {
			return domain.Transaction{}, fmt.Errorf("rows.Scan: %w", err)
		}

		e.Side = domain.Side(side)
		tx.Entries = append(tx.Entries, e)
	}

	err = rows.Err()
	if err != nil {
		return domain.Transaction{}, fmt.Errorf("rows.Err: %w", err)
	}

	return tx, nil
}
