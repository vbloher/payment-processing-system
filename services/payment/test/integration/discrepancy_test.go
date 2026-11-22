//go:build integration

package integration

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/vbloher/payment-processing-system/pkg/otel"
	"github.com/vbloher/payment-processing-system/pkg/postgres"
	"github.com/vbloher/payment-processing-system/pkg/testinfra"
	"github.com/vbloher/payment-processing-system/pkg/transaction"
	adapter "github.com/vbloher/payment-processing-system/services/payment/internal/adapter/postgres"
	"github.com/vbloher/payment-processing-system/services/payment/internal/domain"
)

func Test_Discrepancy(t *testing.T) {
	suite.Run(t, &DiscrepancySuite{})
}

// DiscrepancySuite owns its Postgres: its tests clear the discrepancy table between runs.
type DiscrepancySuite struct {
	suite.Suite
	*require.Assertions

	pool *pgxpool.Pool
	pg   *adapter.Postgres
}

func (s *DiscrepancySuite) SetupSuite() {
	s.Assertions = s.Require()

	otel.SilentModeInit()

	container := testinfra.StartPostgres(s.T(), "payment")

	migrations, err := filepath.Abs("../../migration/postgres")
	s.NoError(err)

	testinfra.Migrate(s.T(), container.URL("payment"), migrations)

	pool, err := postgres.New(context.Background(), container.Config("payment"))
	s.NoError(err)

	s.T().Cleanup(pool.Close)

	s.pool = pool.Pool
	s.pg = adapter.New(transaction.New(pool))
}

func (s *DiscrepancySuite) SetupTest() {
	s.Assertions = s.Require()

	_, err := s.pool.Exec(context.Background(), "DELETE FROM reconciliation_discrepancy")
	s.NoError(err)
}

func (s *DiscrepancySuite) Test_CreateDiscrepancy_OneOpenPerReferenceAndKind() {
	ctx := context.Background()
	ref := uuid.NewString()
	first := domain.NewDiscrepancy(domain.KindBankMissing, ref, uuid.New(), "first")

	created, err := s.pg.CreateDiscrepancy(ctx, first)
	s.NoError(err)
	s.True(created)

	created, err = s.pg.CreateDiscrepancy(ctx, domain.NewDiscrepancy(domain.KindBankMissing, ref, uuid.New(), "again"))
	s.NoError(err)
	s.False(created)

	other, err := s.pg.CreateDiscrepancy(ctx, domain.NewDiscrepancy(domain.KindStuck, ref, uuid.New(), "other kind"))
	s.NoError(err)
	s.True(other)

	s.NoError(s.pg.ResolveDiscrepancy(ctx, first.ID))

	created, err = s.pg.CreateDiscrepancy(ctx, domain.NewDiscrepancy(domain.KindBankMissing, ref, uuid.New(), "after resolve"))
	s.NoError(err)
	s.True(created)
}

func (s *DiscrepancySuite) Test_ListDiscrepancies() {
	ctx := context.Background()
	open := domain.NewDiscrepancy(domain.KindStuck, "a", uuid.New(), "open")
	resolved := domain.NewDiscrepancy(domain.KindLedgerMismatch, "b", uuid.New(), "resolved")
	open.CreatedAt = time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
	resolved.CreatedAt = open.CreatedAt.Add(-time.Hour)

	for _, d := range []domain.Discrepancy{open, resolved} {
		created, err := s.pg.CreateDiscrepancy(ctx, d)
		s.NoError(err)
		s.True(created)
	}

	s.NoError(s.pg.ResolveDiscrepancy(ctx, resolved.ID))

	got, err := s.pg.ListOpenDiscrepancies(ctx)
	s.NoError(err)
	s.Len(got, 1)
	s.Equal(open.ID, got[0].ID)
	s.Equal(domain.DiscrepancyOpen, got[0].Status)
	s.True(got[0].ResolvedAt.IsZero())

	all, err := s.pg.ListDiscrepancies(ctx, "")
	s.NoError(err)
	s.Len(all, 2)
	s.Equal(open.ID, all[0].ID, "newest first")

	res, err := s.pg.ListDiscrepancies(ctx, domain.DiscrepancyResolved)
	s.NoError(err)
	s.Len(res, 1)
	s.Equal(resolved.ID, res[0].ID)
	s.False(res[0].ResolvedAt.IsZero())

	s.Error(s.pg.ResolveDiscrepancy(ctx, uuid.New()), "an unknown ID is an error")
}

func (s *DiscrepancySuite) Test_Discrepancy_NilPaymentID() {
	ctx := context.Background()
	d := domain.NewDiscrepancy(domain.KindBankUnexpected, "ROGUE-1", uuid.Nil, "no payment")

	created, err := s.pg.CreateDiscrepancy(ctx, d)
	s.NoError(err)
	s.True(created)

	var isNull bool

	s.NoError(s.pool.QueryRow(ctx, "SELECT payment_id IS NULL FROM reconciliation_discrepancy WHERE id = $1", d.ID).Scan(&isNull))
	s.True(isNull)

	got, err := s.pg.ListOpenDiscrepancies(ctx)
	s.NoError(err)
	s.Len(got, 1)
	s.Equal(uuid.Nil, got[0].PaymentID)
	s.Equal("ROGUE-1", got[0].Reference)
}
