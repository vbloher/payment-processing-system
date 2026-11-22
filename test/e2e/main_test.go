//go:build integration

package e2e

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.temporal.io/sdk/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/vbloher/payment-processing-system/gen/ledger/grpc/ledger_v1"
	"github.com/vbloher/payment-processing-system/pkg/httpserver"
	"github.com/vbloher/payment-processing-system/pkg/otel/tracer"
	"github.com/vbloher/payment-processing-system/pkg/temporalclient"
	"github.com/vbloher/payment-processing-system/pkg/testinfra"
	"github.com/vbloher/payment-processing-system/services/ledger"
	"github.com/vbloher/payment-processing-system/services/mockbank"
	"github.com/vbloher/payment-processing-system/services/notification"
	"github.com/vbloher/payment-processing-system/services/payment"

	bank_client "github.com/vbloher/payment-processing-system/gen/mockbank/http/client"
	payment_client "github.com/vbloher/payment-processing-system/gen/payment/http/client"
	ledger_config "github.com/vbloher/payment-processing-system/services/ledger/config"
	mockbank_config "github.com/vbloher/payment-processing-system/services/mockbank/config"
	notification_config "github.com/vbloher/payment-processing-system/services/notification/config"
	payment_config "github.com/vbloher/payment-processing-system/services/payment/config"
	otelapi "go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

const stopTimeout = 30 * time.Second

func Test_E2E(t *testing.T) {
	suite.Run(t, &Suite{})
}

type Suite struct {
	suite.Suite
	*require.Assertions

	// Both generated packages are named http_client; payment_client and bank_client are import aliases.
	payments  *payment_client.ClientWithResponses // gen/payment/http/client
	bank      *bank_client.ClientWithResponses    // gen/mockbank/http/client
	ledger    ledger_v1.LedgerV1Client
	temporal  client.Client
	paymentDB *pgx.Conn
	ledgerDB  *pgx.Conn

	paymentURL string                  // base URL of the payment API, for requests the generated client cannot express
	webhook    *webhookRecorder        // the notification service's webhook
	spans      *tracetest.SpanRecorder // spans of every service
	services   []service               // started services, in start order
	parents    []*require.Assertions
}

// service is a service running in-process through its public Run.
type service struct {
	name   string
	cancel context.CancelFunc
	done   chan error
}

func (s *Suite) SetupSuite() {
	s.Assertions = s.Require()

	// The tracer and the propagator are package globals; Run leaves them to the caller. Every
	// service records into one span recorder, and the TraceContext propagator carries the trace
	// through the outbox and the NATS headers, as in the template's integration suite.
	s.spans = tracetest.NewSpanRecorder()
	otelapi.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(s.spans)))
	otelapi.SetTextMapPropagator(propagation.TraceContext{})
	tracer.Init(otelapi.Tracer(""))

	pg := testinfra.StartPostgres(s.T(), "payment", "ledger", "notification")
	s.migrate(pg, "payment")
	s.migrate(pg, "ledger")
	s.migrate(pg, "notification")

	natsConfig := testinfra.StartNATS(s.T())
	temporalConfig := testinfra.StartTemporal(s.T())

	s.webhook = newWebhookRecorder(s.T())
	s.T().Cleanup(s.webhook.Close)

	// TearDownSuite does not run when SetupSuite fails. Registered after the containers and the
	// webhook, this cleanup then stops the services before the things they use.
	s.T().Cleanup(s.stopServices)

	bankPort := testinfra.FreePort(s.T())
	s.start("mockbank", bankPort, func(ctx context.Context) error {
		return mockbank.Run(ctx, mockbank_config.Config{HTTP: httpserver.Config{Port: bankPort}})
	})

	ledgerHTTPPort, ledgerGRPCPort := testinfra.FreePort(s.T()), testinfra.FreePort(s.T())
	ledgerConfig := ledger_config.Config{HTTP: httpserver.Config{Port: ledgerHTTPPort}, Postgres: pg.Config("ledger")}
	ledgerConfig.GRPC.Port = ledgerGRPCPort
	s.start("ledger", ledgerHTTPPort, func(ctx context.Context) error { return ledger.Run(ctx, ledgerConfig) })

	bankURL := "http://127.0.0.1:" + bankPort + "/api/v1"
	ledgerAddr := net.JoinHostPort("127.0.0.1", ledgerGRPCPort)

	paymentPort := testinfra.FreePort(s.T())
	paymentConfig := payment_config.Config{
		HTTP:     httpserver.Config{Port: paymentPort},
		Postgres: pg.Config("payment"),
		NATS:     natsConfig,
		Temporal: temporalConfig,
	}
	paymentConfig.Ledger.Addr = ledgerAddr
	paymentConfig.Bank.URL = bankURL
	paymentConfig.Bank.Timeout = 500 * time.Millisecond
	paymentConfig.Saga.ActivityTimeout = 2 * time.Second
	paymentConfig.Saga.RetryInitialInterval = 50 * time.Millisecond
	paymentConfig.Saga.RetryMaxInterval = 200 * time.Millisecond
	paymentConfig.Saga.BankMaxAttempts = 3
	paymentConfig.OutboxWorker.Interval = 100 * time.Millisecond
	paymentConfig.OutboxWorker.Limit = 100
	// The tests run reconciliation themselves (reconcile). Temporal aligns intervals to the epoch, so the
	// schedule still fires on the hour; such a run is harmless: reconciliation is idempotent and tolerates
	// a concurrent run.
	paymentConfig.TemporalWorker.ReconciliationInterval = time.Hour
	paymentConfig.TemporalWorker.ReconciliationLookback = time.Hour
	paymentConfig.TemporalWorker.ReconciliationLag = 0
	paymentConfig.TemporalWorker.ReconciliationStuckAfter = time.Second
	s.start("payment", paymentPort, func(ctx context.Context) error { return payment.Run(ctx, paymentConfig) })

	notificationPort := testinfra.FreePort(s.T())
	notificationConfig := notification_config.Config{
		HTTP:     httpserver.Config{Port: notificationPort},
		Postgres: pg.Config("notification"),
		NATS:     natsConfig,
	}
	notificationConfig.Consumer.Durable = "notification"
	notificationConfig.Consumer.MaxDeliver = 5
	notificationConfig.Consumer.RetryDelay = 100 * time.Millisecond
	notificationConfig.Consumer.AckWait = 10 * time.Second
	notificationConfig.Webhook.URL = s.webhook.URL
	notificationConfig.Webhook.Timeout = 2 * time.Second
	s.start("notification", notificationPort, func(ctx context.Context) error {
		return notification.Run(ctx, notificationConfig)
	})

	s.connect(pg, temporalConfig, ledgerAddr, bankURL, "http://127.0.0.1:"+paymentPort+"/api/v1")
}

func (s *Suite) TearDownSuite() {
	s.stopServices()
}

func (s *Suite) SetupTest() {
	s.Assertions = s.Require()
}

// SetupSubTest binds the assertions to the subtest, so that a failure stops only that subtest.
func (s *Suite) SetupSubTest() {
	s.parents = append(s.parents, s.Assertions)
	s.Assertions = s.Require()
}

func (s *Suite) TearDownSubTest() {
	s.Assertions = s.parents[len(s.parents)-1]
	s.parents = s.parents[:len(s.parents)-1]
}

func (s *Suite) migrate(pg testinfra.Postgres, service string) {
	dir, err := filepath.Abs(filepath.Join("..", "..", "services", service, "migration", "postgres"))
	s.NoError(err)

	testinfra.Migrate(s.T(), pg.URL(service), dir)
}

// start runs a service in a goroutine and waits until its HTTP server answers /ready.
func (s *Suite) start(name, httpPort string, run func(ctx context.Context) error) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)

	go func() { done <- run(ctx) }()

	s.services = append(s.services, service{name: name, cancel: cancel, done: done})

	testinfra.WaitHTTP(s.T(), "http://127.0.0.1:"+httpPort+"/ready")
}

// stopServices stops the services in reverse start order and requires every Run to return nil.
func (s *Suite) stopServices() {
	for i := len(s.services) - 1; i >= 0; i-- {
		svc := s.services[i]
		svc.cancel()

		select {
		case err := <-svc.done:
			s.Assert().NoError(err, "%s.Run", svc.name)
		case <-time.After(stopTimeout):
			s.Assert().Fail(svc.name + ".Run did not return after its context was cancelled")
		}
	}

	s.services = nil
}

// connect opens the clients the tests use; they close when the suite ends.
func (s *Suite) connect(
	pg testinfra.Postgres, temporalConfig temporalclient.Config, ledgerAddr, bankURL, paymentURL string,
) {
	var err error

	s.paymentURL = paymentURL

	s.payments, err = payment_client.NewClientWithResponses(paymentURL)
	s.NoError(err)

	s.bank, err = bank_client.NewClientWithResponses(bankURL)
	s.NoError(err)

	conn, err := grpc.NewClient(ledgerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	s.NoError(err)
	s.T().Cleanup(func() { _ = conn.Close() })

	s.ledger = ledger_v1.NewLedgerV1Client(conn)

	s.temporal, err = temporalclient.New(context.Background(), temporalConfig)
	s.NoError(err)
	s.T().Cleanup(s.temporal.Close)

	s.paymentDB, err = pgx.Connect(context.Background(), pg.URL("payment"))
	s.NoError(err)
	s.T().Cleanup(func() { _ = s.paymentDB.Close(context.Background()) })

	s.ledgerDB, err = pgx.Connect(context.Background(), pg.URL("ledger"))
	s.NoError(err)
	s.T().Cleanup(func() { _ = s.ledgerDB.Close(context.Background()) })
}
