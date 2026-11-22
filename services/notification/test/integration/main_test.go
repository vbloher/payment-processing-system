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
	adapter "github.com/vbloher/payment-processing-system/services/notification/internal/adapter/postgres"
)

func Test_Integration(t *testing.T) {
	suite.Run(t, &Suite{})
}

type Suite struct {
	suite.Suite
	*require.Assertions

	pool *pgxpool.Pool
	pg   *adapter.Postgres
}

func (s *Suite) SetupSuite() {
	s.Assertions = s.Require()

	otel.SilentModeInit()

	container := testinfra.StartPostgres(s.T(), "notification")

	migrations, err := filepath.Abs("../../migration/postgres")
	s.NoError(err)

	testinfra.Migrate(s.T(), container.URL("notification"), migrations)

	pool, err := postgres.New(context.Background(), container.Config("notification"))
	s.NoError(err)

	s.T().Cleanup(pool.Close)

	s.pool = pool.Pool
	s.pg = adapter.New(transaction.New(pool))
}

func (s *Suite) SetupTest() {
	s.Assertions = s.Require()
}
