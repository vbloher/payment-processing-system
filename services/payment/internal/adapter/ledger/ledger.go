package ledger

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/vbloher/payment-processing-system/gen/ledger/grpc/ledger_v1"
	"github.com/vbloher/payment-processing-system/services/payment/internal/domain"
)

type Config struct {
	Addr string `env:"LEDGER_ADDR,required"`
}

// Ledger is the gRPC client of the ledger service.
type Ledger struct {
	conn   *grpc.ClientConn
	client ledger_v1.LedgerV1Client
}

func New(c Config) (*Ledger, error) {
	conn, err := grpc.NewClient(c.Addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
	if err != nil {
		return nil, fmt.Errorf("ledger.New: %w", err)
	}

	return &Ledger{conn: conn, client: ledger_v1.NewLedgerV1Client(conn)}, nil
}

func (l *Ledger) Close() {
	_ = l.conn.Close()
}

func (l *Ledger) Hold(
	ctx context.Context, paymentID, accountID uuid.UUID, amount int64, currency string,
) error {
	_, err := l.client.Hold(ctx, &ledger_v1.HoldInput{
		PaymentId: paymentID.String(),
		AccountId: accountID.String(),
		Amount:    amount,
		Currency:  currency,
	})
	if err != nil {
		return fmt.Errorf("ledger.Hold: %w", mapHoldError(err))
	}

	return nil
}

func (l *Ledger) Capture(ctx context.Context, paymentID uuid.UUID) error {
	_, err := l.client.Capture(ctx, &ledger_v1.CaptureInput{PaymentId: paymentID.String()})
	if err != nil {
		return fmt.Errorf("ledger.Capture: %w", mapSettleError(err))
	}

	return nil
}

func (l *Ledger) Release(ctx context.Context, paymentID uuid.UUID) error {
	_, err := l.client.Release(ctx, &ledger_v1.ReleaseInput{PaymentId: paymentID.String()})
	if err != nil {
		return fmt.Errorf("ledger.Release: %w", mapSettleError(err))
	}

	return nil
}

func (l *Ledger) GetHolds(ctx context.Context, paymentIDs []uuid.UUID) (map[uuid.UUID]domain.HoldState, error) {
	ids := make([]string, len(paymentIDs))
	for i, id := range paymentIDs {
		ids[i] = id.String()
	}

	out, err := l.client.GetHolds(ctx, &ledger_v1.GetHoldsInput{PaymentIds: ids})
	if err != nil {
		return nil, fmt.Errorf("ledger.GetHolds: %w", err)
	}

	holds := make(map[uuid.UUID]domain.HoldState, len(out.GetHolds()))

	for _, h := range out.GetHolds() {
		id, err := uuid.Parse(h.GetPaymentId())
		if err != nil {
			return nil, fmt.Errorf("ledger.GetHolds: payment id %q: %w", h.GetPaymentId(), err)
		}

		state, err := holdState(h.GetStatus())
		if err != nil {
			return nil, fmt.Errorf("ledger.GetHolds: payment %s: %w", id, err)
		}

		holds[id] = state
	}

	return holds, nil
}

func holdState(s string) (domain.HoldState, error) {
	for _, state := range []domain.HoldState{domain.HoldActive, domain.HoldCaptured, domain.HoldReleased} {
		if s == string(state) {
			return state, nil
		}
	}

	return domain.HoldAbsent, fmt.Errorf("unknown hold status %q", s)
}

// mapHoldError translates a Hold failure. Codes outside the table (Unavailable,
// DeadlineExceeded, Internal...) stay unmapped so the activity retries them.
func mapHoldError(err error) error {
	code := status.Code(err)

	var domainErr error

	// Every other code is deliberately unmapped (retryable) and falls to default.
	//exhaustive:ignore
	switch code {
	case codes.NotFound:
		domainErr = domain.ErrAccountNotFound
	case codes.FailedPrecondition:
		domainErr = domain.ErrInsufficientFunds
	case codes.InvalidArgument, codes.AlreadyExists, codes.Aborted:
		domainErr = domain.ErrLedgerRejected
	default:
		return err
	}

	return fmt.Errorf("%w: %w", domainErr, err)
}

// mapSettleError translates a Capture or Release failure: every definitive refusal is a conflict.
func mapSettleError(err error) error {
	// Every other code is deliberately unmapped (retryable) and falls to default.
	//exhaustive:ignore
	switch code := status.Code(err); code {
	case codes.NotFound, codes.FailedPrecondition, codes.InvalidArgument, codes.AlreadyExists, codes.Aborted:
		return fmt.Errorf("%w: %w", domain.ErrLedgerConflict, err)
	default:
		return err
	}
}
