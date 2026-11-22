//go:build integration

package integration

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/vbloher/payment-processing-system/pkg/otel"
	"github.com/vbloher/payment-processing-system/pkg/postgres"
	"github.com/vbloher/payment-processing-system/pkg/testinfra"
	"github.com/vbloher/payment-processing-system/pkg/transaction"
	adapter "github.com/vbloher/payment-processing-system/services/ledger/internal/adapter/postgres"
	"github.com/vbloher/payment-processing-system/services/ledger/internal/usecase"
)

func Test_Integration(t *testing.T) {
	suite.Run(t, &Suite{})
}

type Suite struct {
	suite.Suite
	*require.Assertions

	pool *pgxpool.Pool
	uc   *usecase.UseCase
}

func (s *Suite) SetupSuite() {
	s.Assertions = s.Require()

	otel.SilentModeInit()

	pg := testinfra.StartPostgres(s.T(), "ledger")

	migrations, err := filepath.Abs("../../migration/postgres")
	s.NoError(err)

	testinfra.Migrate(s.T(), pg.URL("ledger"), migrations)

	pool, err := postgres.New(context.Background(), pg.Config("ledger"))
	s.NoError(err)

	s.T().Cleanup(pool.Close)

	s.pool = pool.Pool

	tx := transaction.New(pool)
	s.uc = usecase.New(tx, adapter.New(tx))
}

func (s *Suite) SetupTest() {
	s.Assertions = s.Require()
}

// requireLedgerIntact checks that every balance equals the sum of its entries
// and that debits equal credits within every currency.
func (s *Suite) requireLedgerIntact() {
	const balancesMatchEntries = `
SELECT count(*) FROM account a
WHERE a.balance <> COALESCE((SELECT sum(CASE WHEN e.side = a.normal_side THEN e.amount ELSE -e.amount END)
                             FROM ledger_entry e WHERE e.account_id = a.id), 0)`

	const currenciesBalance = `
SELECT count(*) FROM (SELECT currency, sum(CASE WHEN normal_side = 'debit' THEN balance ELSE -balance END) AS diff
                      FROM account GROUP BY currency) t WHERE diff <> 0`

	for _, query := range []string{balancesMatchEntries, currenciesBalance} {
		var broken int

		s.NoError(s.pool.QueryRow(context.Background(), query).Scan(&broken))
		s.Zero(broken, query)
	}
}
