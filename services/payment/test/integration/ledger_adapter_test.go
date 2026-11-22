//go:build integration

package integration

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/vbloher/payment-processing-system/gen/ledger/grpc/ledger_v1"
	"github.com/vbloher/payment-processing-system/pkg/httpserver"
	"github.com/vbloher/payment-processing-system/pkg/otel"
	"github.com/vbloher/payment-processing-system/pkg/testinfra"
	"github.com/vbloher/payment-processing-system/services/ledger"
	"github.com/vbloher/payment-processing-system/services/ledger/config"
	adapter "github.com/vbloher/payment-processing-system/services/payment/internal/adapter/ledger"
	"github.com/vbloher/payment-processing-system/services/payment/internal/domain"
)

func Test_LedgerAdapter(t *testing.T) {
	suite.Run(t, &LedgerAdapterSuite{})
}

type LedgerAdapterSuite struct {
	suite.Suite
	*require.Assertions

	cancel  context.CancelFunc
	done    chan error
	client  ledger_v1.LedgerV1Client
	adapter *adapter.Ledger
}

func (s *LedgerAdapterSuite) SetupSuite() {
	s.Assertions = s.Require()

	otel.SilentModeInit()

	pg := testinfra.StartPostgres(s.T(), "ledger")

	migrations, err := filepath.Abs("../../../ledger/migration/postgres")
	s.NoError(err)

	testinfra.Migrate(s.T(), pg.URL("ledger"), migrations)

	httpPort := testinfra.FreePort(s.T())
	grpcPort := testinfra.FreePort(s.T())

	cfg := config.Config{
		HTTP:     httpserver.Config{Port: httpPort},
		Postgres: pg.Config("ledger"),
	}
	cfg.GRPC.Port = grpcPort

	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	s.done = make(chan error, 1)

	go func() { s.done <- ledger.Run(ctx, cfg) }()

	testinfra.WaitHTTP(s.T(), "http://127.0.0.1:"+httpPort+"/ready")

	addr := net.JoinHostPort("127.0.0.1", grpcPort)

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	s.NoError(err)

	s.T().Cleanup(func() { _ = conn.Close() })

	s.client = ledger_v1.NewLedgerV1Client(conn)

	s.adapter, err = adapter.New(adapter.Config{Addr: addr})
	s.NoError(err)

	s.T().Cleanup(s.adapter.Close)
}

func (s *LedgerAdapterSuite) TearDownSuite() {
	s.cancel()

	select {
	case err := <-s.done:
		s.NoError(err)
	case <-time.After(30 * time.Second):
		s.Fail("ledger.Run did not return after cancel")
	}
}

func (s *LedgerAdapterSuite) SetupTest() {
	s.Assertions = s.Require()
}

func (s *LedgerAdapterSuite) Test_Mapping() {
	ctx := s.T().Context()

	opened, err := s.client.OpenAccount(ctx, &ledger_v1.OpenAccountInput{Currency: "EUR"})
	s.NoError(err)

	account := uuid.MustParse(opened.GetId())

	_, err = s.client.Deposit(ctx, &ledger_v1.DepositInput{
		AccountId: opened.GetId(), Amount: 1000, Currency: "EUR", IdempotencyKey: uuid.NewString(),
	})
	s.NoError(err)

	first, random := uuid.New(), uuid.New()

	s.Run("hold", func() {
		s.NoError(s.adapter.Hold(ctx, first, account, 500, "EUR"))
	})

	s.Run("insufficient funds", func() {
		s.ErrorIs(s.adapter.Hold(ctx, uuid.New(), account, 5000, "EUR"), domain.ErrInsufficientFunds)
	})

	s.Run("unknown account", func() {
		s.ErrorIs(s.adapter.Hold(ctx, uuid.New(), uuid.New(), 500, "EUR"), domain.ErrAccountNotFound)
	})

	s.Run("currency mismatch", func() {
		s.ErrorIs(s.adapter.Hold(ctx, uuid.New(), account, 500, "USD"), domain.ErrLedgerRejected)
	})

	s.Run("capture is idempotent", func() {
		s.NoError(s.adapter.Capture(ctx, first))
		s.NoError(s.adapter.Capture(ctx, first))
	})

	s.Run("release of a captured hold conflicts", func() {
		s.ErrorIs(s.adapter.Release(ctx, first), domain.ErrLedgerConflict)
	})

	s.Run("capture of an unknown payment conflicts", func() {
		s.ErrorIs(s.adapter.Capture(ctx, random), domain.ErrLedgerConflict)
	})

	s.Run("get holds", func() {
		got, err := s.adapter.GetHolds(ctx, []uuid.UUID{first, random})
		s.NoError(err)
		s.Equal(map[uuid.UUID]domain.HoldState{first: domain.HoldCaptured}, got)
	})
}
