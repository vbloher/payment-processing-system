package postgres

import "github.com/vbloher/payment-processing-system/pkg/transaction"

type Postgres struct {
	tx *transaction.Manager
}

func New(tx *transaction.Manager) *Postgres {
	return &Postgres{
		tx: tx,
	}
}
