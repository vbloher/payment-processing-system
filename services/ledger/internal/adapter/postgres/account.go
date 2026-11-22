package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/vbloher/payment-processing-system/pkg/otel/tracer"
	"github.com/vbloher/payment-processing-system/services/ledger/internal/domain"
)

const getAccountSQL = `SELECT kind, currency, normal_side, balance, held, created_at, updated_at
                    FROM account WHERE id = $1`

func (p *Postgres) CreateAccount(ctx context.Context, account domain.Account) error {
	ctx, span := tracer.Start(ctx, "adapter postgres CreateAccount")
	defer span.End()

	const sql = `INSERT INTO account (id, kind, currency, normal_side, balance, held, created_at, updated_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`

	_, err := p.tx.TryExtractTX(ctx).Exec(ctx, sql,
		account.ID, string(account.Kind), account.Currency, string(account.NormalSide),
		account.Balance, account.Held, account.CreatedAt, account.UpdatedAt)
	if err != nil {
		return fmt.Errorf("txOrPool.Exec: %w", err)
	}

	return nil
}

func (p *Postgres) GetAccount(ctx context.Context, id uuid.UUID) (domain.Account, error) {
	ctx, span := tracer.Start(ctx, "adapter postgres GetAccount")
	defer span.End()

	return p.getAccount(ctx, getAccountSQL, id)
}

func (p *Postgres) GetAccountForUpdate(ctx context.Context, id uuid.UUID) (domain.Account, error) {
	ctx, span := tracer.Start(ctx, "adapter postgres GetAccountForUpdate")
	defer span.End()

	return p.getAccount(ctx, getAccountSQL+" FOR UPDATE", id)
}

// GetBankCashForUpdate locks the bank_cash account of the currency, creating it when absent.
func (p *Postgres) GetBankCashForUpdate(ctx context.Context, currency string) (domain.Account, error) {
	ctx, span := tracer.Start(ctx, "adapter postgres GetBankCashForUpdate")
	defer span.End()

	account, err := domain.NewBankCashAccount(currency)
	if err != nil {
		return domain.Account{}, fmt.Errorf("domain.NewBankCashAccount: %w", err)
	}

	const insertSQL = `INSERT INTO account (id, kind, currency, normal_side, created_at, updated_at)
                    VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT DO NOTHING`

	txOrPool := p.tx.TryExtractTX(ctx)

	_, err = txOrPool.Exec(ctx, insertSQL,
		account.ID, string(account.Kind), account.Currency, string(account.NormalSide),
		account.CreatedAt, account.UpdatedAt)
	if err != nil {
		return domain.Account{}, fmt.Errorf("txOrPool.Exec: %w", err)
	}

	const selectSQL = `SELECT id, kind, currency, normal_side, balance, held, created_at, updated_at
                    FROM account WHERE kind = 'bank_cash' AND currency = $1 FOR UPDATE`

	var (
		kind, normalSide string
		found            domain.Account
	)

	err = txOrPool.QueryRow(ctx, selectSQL, currency).Scan(
		&found.ID, &kind, &found.Currency, &normalSide,
		&found.Balance, &found.Held, &found.CreatedAt, &found.UpdatedAt)
	if err != nil {
		return domain.Account{}, fmt.Errorf("txOrPool.QueryRow: %w", err)
	}

	found.Kind = domain.AccountKind(kind)
	found.NormalSide = domain.Side(normalSide)

	return found, nil
}

func (p *Postgres) getAccount(ctx context.Context, sql string, id uuid.UUID) (domain.Account, error) {
	var (
		kind, normalSide string
		createdAt        time.Time
		updatedAt        time.Time
		account          = domain.Account{ID: id}
	)

	err := p.tx.TryExtractTX(ctx).QueryRow(ctx, sql, id).Scan(
		&kind, &account.Currency, &normalSide, &account.Balance, &account.Held, &createdAt, &updatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.Account{}, domain.ErrNotFound
		}

		return domain.Account{}, fmt.Errorf("txOrPool.QueryRow: %w", err)
	}

	account.Kind = domain.AccountKind(kind)
	account.NormalSide = domain.Side(normalSide)
	account.CreatedAt = createdAt
	account.UpdatedAt = updatedAt

	return account, nil
}

func (p *Postgres) UpdateAccountBalances(ctx context.Context, account domain.Account) error {
	ctx, span := tracer.Start(ctx, "adapter postgres UpdateAccountBalances")
	defer span.End()

	const sql = `UPDATE account SET balance = $2, held = $3, updated_at = $4 WHERE id = $1`

	tag, err := p.tx.TryExtractTX(ctx).Exec(ctx, sql, account.ID, account.Balance, account.Held, account.UpdatedAt)
	if err != nil {
		return fmt.Errorf("txOrPool.Exec: %w", err)
	}

	if tag.RowsAffected() == 0 {
		return domain.ErrNotFound
	}

	return nil
}
