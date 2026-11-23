package bank

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	http_client "github.com/vbloher/payment-processing-system/gen/mockbank/http/client"
	"github.com/vbloher/payment-processing-system/services/payment/internal/domain"
)

type Config struct {
	URL     string        `env:"BANK_URL,required"` // e.g. http://localhost:8090/api/v1
	Timeout time.Duration `env:"BANK_TIMEOUT"      envDefault:"3s"`
}

// Bank is the HTTP client of the mock bank.
type Bank struct {
	client *http_client.ClientWithResponses
}

func New(c Config) (*Bank, error) {
	httpClient := &http.Client{
		Timeout:   c.Timeout,
		Transport: otelhttp.NewTransport(http.DefaultTransport),
	}

	client, err := http_client.NewClientWithResponses(c.URL, http_client.WithHTTPClient(httpClient))
	if err != nil {
		return nil, fmt.Errorf("bank.New: %w", err)
	}

	return &Bank{client: client}, nil
}

func (b *Bank) Transfer(
	ctx context.Context, reference string, amount int64, currency, beneficiary string,
) (domain.BankTransfer, error) {
	res, err := b.client.CreateTransferWithResponse(ctx,
		&http_client.CreateTransferParams{IdempotencyKey: reference},
		http_client.CreateTransferJSONRequestBody{Amount: amount, Currency: currency, Beneficiary: beneficiary})
	if err != nil {
		return domain.BankTransfer{}, fmt.Errorf("bank.Transfer: %w: %w", domain.ErrBankUnavailable, err)
	}

	switch res.StatusCode() {
	case http.StatusOK:
		return toTransfer("bank.Transfer", res.JSON200)
	case http.StatusBadRequest:
		// A definitive refusal: the bank validates the request before it looks up any stored transfer,
		// so a 400 means nothing was processed.
		return domain.BankTransfer{
			Reference: reference, Amount: amount, Currency: currency, Status: domain.BankDeclined,
		}, nil
	case http.StatusConflict:
		// Not a refusal: a transfer already exists under this reference with another body, and the bank may
		// have moved money for it. Unknown, so the saga asks Cancel for the stored transfer's final state.
		return domain.BankTransfer{}, unexpected("bank.Transfer", res.StatusCode())
	default:
		return domain.BankTransfer{}, unexpected("bank.Transfer", res.StatusCode())
	}
}

func (b *Bank) Cancel(ctx context.Context, reference string) (domain.BankTransfer, error) {
	res, err := b.client.CancelTransferWithResponse(ctx, reference)
	if err != nil {
		return domain.BankTransfer{}, fmt.Errorf("bank.Cancel: %w: %w", domain.ErrBankUnavailable, err)
	}

	if res.StatusCode() != http.StatusOK {
		return domain.BankTransfer{}, unexpected("bank.Cancel", res.StatusCode())
	}

	return toTransfer("bank.Cancel", res.JSON200)
}

func (b *Bank) GetTransfer(ctx context.Context, reference string) (*domain.BankTransfer, error) {
	res, err := b.client.GetTransferWithResponse(ctx, reference)
	if err != nil {
		return nil, fmt.Errorf("bank.GetTransfer: %w: %w", domain.ErrBankUnavailable, err)
	}

	switch res.StatusCode() {
	case http.StatusOK:
		t, err := toTransfer("bank.GetTransfer", res.JSON200)
		if err != nil {
			return nil, err
		}

		return &t, nil
	case http.StatusNotFound:
		return nil, nil //nolint:nilnil // the port documents nil, nil as "no such transfer".
	default:
		return nil, unexpected("bank.GetTransfer", res.StatusCode())
	}
}

func (b *Bank) GetStatement(ctx context.Context, from, to time.Time) ([]domain.BankTransfer, error) {
	res, err := b.client.GetStatementWithResponse(ctx, &http_client.GetStatementParams{From: from, To: to})
	if err != nil {
		return nil, fmt.Errorf("bank.GetStatement: %w: %w", domain.ErrBankUnavailable, err)
	}

	if res.StatusCode() != http.StatusOK || res.JSON200 == nil {
		return nil, unexpected("bank.GetStatement", res.StatusCode())
	}

	transfers := make([]domain.BankTransfer, 0, len(res.JSON200.Transfers))

	for i := range res.JSON200.Transfers {
		t, err := toTransfer("bank.GetStatement", &res.JSON200.Transfers[i])
		if err != nil {
			return nil, err
		}

		transfers = append(transfers, t)
	}

	return transfers, nil
}

// unexpected treats any status outside the mapping as "outcome unknown".
func unexpected(op string, code int) error {
	return fmt.Errorf("%s: %w: http status %d", op, domain.ErrBankUnavailable, code)
}

// toTransfer accepts only the three valid outcomes; anything else is unknown, never a refusal.
func toTransfer(op string, t *http_client.Transfer) (domain.BankTransfer, error) {
	if t == nil {
		return domain.BankTransfer{}, fmt.Errorf("%s: %w: empty body", op, domain.ErrBankUnavailable)
	}

	status := domain.BankOutcome(t.Status)

	switch status {
	case domain.BankSucceeded, domain.BankDeclined, domain.BankCancelled:
	default:
		return domain.BankTransfer{}, fmt.Errorf("%s: %w: unknown status %q", op, domain.ErrBankUnavailable, t.Status)
	}

	return domain.BankTransfer{
		ID:        t.ID.String(),
		Reference: t.Reference,
		Amount:    t.Amount,
		Currency:  t.Currency,
		Status:    status,
	}, nil
}
