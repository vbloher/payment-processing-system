//go:build integration

package integration

import (
	"context"
	"net"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/vbloher/payment-processing-system/gen/ledger/grpc/ledger_v1"
	"github.com/vbloher/payment-processing-system/pkg/httpserver"
	"github.com/vbloher/payment-processing-system/pkg/otel"
	"github.com/vbloher/payment-processing-system/pkg/testinfra"
	"github.com/vbloher/payment-processing-system/services/ledger"
	"github.com/vbloher/payment-processing-system/services/ledger/config"
	ledgergrpc "github.com/vbloher/payment-processing-system/services/ledger/internal/controller/grpc"
)

func Test_GRPC(t *testing.T) {
	suite.Run(t, &GRPCSuite{})
}

type GRPCSuite struct {
	suite.Suite
	*require.Assertions

	httpPort string
	client   ledger_v1.LedgerV1Client
	done     chan error
	cancel   context.CancelFunc
}

func (s *GRPCSuite) SetupSuite() {
	s.Assertions = s.Require()

	otel.SilentModeInit()

	pg := testinfra.StartPostgres(s.T(), "ledger")

	migrations, err := filepath.Abs("../../migration/postgres")
	s.NoError(err)

	testinfra.Migrate(s.T(), pg.URL("ledger"), migrations)

	s.httpPort = testinfra.FreePort(s.T())
	grpcPort := testinfra.FreePort(s.T())

	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	s.done = make(chan error, 1)

	go func() {
		s.done <- ledger.Run(ctx, config.Config{
			HTTP:     httpserver.Config{Port: s.httpPort},
			GRPC:     ledgergrpc.Config{Port: grpcPort},
			Postgres: pg.Config("ledger"),
		})
	}()

	testinfra.WaitHTTP(s.T(), "http://127.0.0.1:"+s.httpPort+"/ready")

	conn, err := grpc.NewClient(net.JoinHostPort("127.0.0.1", grpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	s.NoError(err)

	s.T().Cleanup(func() { _ = conn.Close() })

	s.client = ledger_v1.NewLedgerV1Client(conn)
}

func (s *GRPCSuite) TearDownSuite() {
	s.cancel()

	select {
	case err := <-s.done:
		s.NoError(err)
	case <-time.After(30 * time.Second):
		s.Fail("ledger.Run did not return after cancel")
	}
}

func (s *GRPCSuite) SetupTest() {
	s.Assertions = s.Require()
}

func (s *GRPCSuite) openFunded(amount int64, key string) string {
	ctx := context.Background()

	out, err := s.client.OpenAccount(ctx, &ledger_v1.OpenAccountInput{Currency: "EUR"})
	s.NoError(err)

	_, err = s.client.Deposit(ctx, &ledger_v1.DepositInput{
		AccountId: out.GetId(), Amount: amount, Currency: "EUR", IdempotencyKey: key,
	})
	s.NoError(err)

	return out.GetId()
}

func (s *GRPCSuite) hold(paymentID, accountID string, amount int64, currency string) error {
	_, err := s.client.Hold(context.Background(), &ledger_v1.HoldInput{
		PaymentId: paymentID, AccountId: accountID, Amount: amount, Currency: currency,
	})

	return err
}

func (s *GRPCSuite) Test_GRPC_PaymentFlow() {
	ctx := context.Background()
	account := s.openFunded(10000, "dep-1")
	payment := uuid.NewString()

	s.NoError(s.hold(payment, account, 3000, "EUR"))

	bal, err := s.client.GetBalance(ctx, &ledger_v1.GetBalanceInput{AccountId: account})
	s.NoError(err)
	s.EqualValues(10000, bal.GetBalance())
	s.EqualValues(3000, bal.GetHeld())
	s.EqualValues(7000, bal.GetAvailable())
	s.Equal("EUR", bal.GetCurrency())

	_, err = s.client.Capture(ctx, &ledger_v1.CaptureInput{PaymentId: payment})
	s.NoError(err)

	bal, err = s.client.GetBalance(ctx, &ledger_v1.GetBalanceInput{AccountId: account})
	s.NoError(err)
	s.EqualValues(7000, bal.GetBalance())
	s.EqualValues(0, bal.GetHeld())
	s.EqualValues(7000, bal.GetAvailable())

	holds, err := s.client.GetHolds(ctx, &ledger_v1.GetHoldsInput{PaymentIds: []string{payment, uuid.NewString()}})
	s.NoError(err)
	s.Require().Len(holds.GetHolds(), 1)
	s.Equal("captured", holds.GetHolds()[0].GetStatus())
	s.Equal(payment, holds.GetHolds()[0].GetPaymentId())
	s.Equal(account, holds.GetHolds()[0].GetAccountId())
}

func (s *GRPCSuite) Test_GRPC_ErrorCodes() {
	ctx := context.Background()
	account := s.openFunded(1000, "dep-errors")

	released := uuid.NewString()
	s.NoError(s.hold(released, account, 100, "EUR"))

	_, err := s.client.Release(ctx, &ledger_v1.ReleaseInput{PaymentId: released})
	s.NoError(err)

	dup := uuid.NewString()
	s.NoError(s.hold(dup, account, 100, "EUR"))

	tests := []struct {
		name string
		call func() error
		want codes.Code
	}{
		{"hold on unknown account", func() error { return s.hold(uuid.NewString(), uuid.NewString(), 100, "EUR") }, codes.NotFound},
		{"hold over balance", func() error { return s.hold(uuid.NewString(), account, 3000, "EUR") }, codes.FailedPrecondition},
		{"hold in other currency", func() error { return s.hold(uuid.NewString(), account, 100, "USD") }, codes.InvalidArgument},
		{"hold with malformed account id", func() error { return s.hold(uuid.NewString(), "nope", 100, "EUR") }, codes.InvalidArgument},
		{"hold replay with other amount", func() error { return s.hold(dup, account, 200, "EUR") }, codes.AlreadyExists},
		{"capture after release", func() error {
			_, err := s.client.Capture(ctx, &ledger_v1.CaptureInput{PaymentId: released})

			return err
		}, codes.Aborted},
		{"capture unknown payment", func() error {
			_, err := s.client.Capture(ctx, &ledger_v1.CaptureInput{PaymentId: uuid.NewString()})

			return err
		}, codes.NotFound},
		{"balance of unknown account", func() error {
			_, err := s.client.GetBalance(ctx, &ledger_v1.GetBalanceInput{AccountId: uuid.NewString()})

			return err
		}, codes.NotFound},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			s.Equal(tt.want, status.Code(tt.call()))
		})
	}
}

func (s *GRPCSuite) Test_HTTP_Probes() {
	for _, path := range []string{"/live", "/ready", "/metrics"} {
		s.Run(path, func() {
			req, err := http.NewRequestWithContext(context.Background(), http.MethodGet,
				"http://127.0.0.1:"+s.httpPort+path, http.NoBody)
			s.NoError(err)

			resp, err := http.DefaultClient.Do(req)
			s.NoError(err)

			defer func() { _ = resp.Body.Close() }()

			s.Equal(http.StatusOK, resp.StatusCode)
		})
	}
}
