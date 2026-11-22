package test_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	http_client "github.com/vbloher/payment-processing-system/gen/mockbank/http/client"
	"github.com/vbloher/payment-processing-system/pkg/httpserver"
	"github.com/vbloher/payment-processing-system/pkg/otel"
	"github.com/vbloher/payment-processing-system/pkg/testinfra"
	"github.com/vbloher/payment-processing-system/services/mockbank"
	"github.com/vbloher/payment-processing-system/services/mockbank/config"
)

// startBank runs the mock bank in-process and returns a generated client for it.
func startBank(t *testing.T) *http_client.ClientWithResponses {
	t.Helper()

	otel.SilentModeInit() // the tracer is a package global; Run leaves it to the caller.

	port := testinfra.FreePort(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)

	go func() {
		done <- mockbank.Run(ctx, config.Config{HTTP: httpserver.Config{Port: port}})
	}()

	t.Cleanup(func() {
		cancel()

		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(30 * time.Second):
			t.Fatal("mockbank.Run did not return after the context was cancelled")
		}
	})

	base := "http://127.0.0.1:" + port
	testinfra.WaitHTTP(t, base+"/ready")

	c, err := http_client.NewClientWithResponses(base + "/api/v1")
	require.NoError(t, err)

	return c
}

func create(
	t *testing.T, c *http_client.ClientWithResponses, key, beneficiary string, amount int64,
) *http_client.CreateTransferResponse {
	t.Helper()

	res, err := c.CreateTransferWithResponse(t.Context(), &http_client.CreateTransferParams{IdempotencyKey: key},
		http_client.CreateTransferJSONRequestBody{Amount: amount, Currency: "EUR", Beneficiary: beneficiary})
	require.NoError(t, err)

	return res
}

func Test_HTTP_CreateTransfer(t *testing.T) {
	c := startBank(t)

	tests := []struct {
		name        string
		beneficiary string
		wantCode    int
		wantStatus  http_client.TransferStatus
	}{
		{"ok", "DE-OK-1", http.StatusOK, http_client.TransferStatusSucceeded},
		{"declined", "DECLINE-1", http.StatusOK, http_client.TransferStatusDeclined},
		{"flaky first call", "FLAKY-1", http.StatusServiceUnavailable, ""},
		{"timeout", "TIMEOUT-1", http.StatusGatewayTimeout, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := create(t, c, uuid.NewString(), tt.beneficiary, 3000)
			require.Equal(t, tt.wantCode, res.StatusCode())

			if tt.wantCode == http.StatusOK {
				require.Equal(t, tt.wantStatus, res.JSON200.Status)
				require.EqualValues(t, 3000, res.JSON200.Amount)
			}
		})
	}
}

func Test_HTTP_Cancel(t *testing.T) {
	c := startBank(t)

	t.Run("after timeout the transfer is succeeded", func(t *testing.T) {
		key := uuid.NewString()
		require.Equal(t, http.StatusGatewayTimeout, create(t, c, key, "TIMEOUT-1", 3000).StatusCode())

		res, err := c.CancelTransferWithResponse(t.Context(), key)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, res.StatusCode())
		require.Equal(t, http_client.TransferStatusSucceeded, res.JSON200.Status)
	})

	t.Run("fresh reference is fenced", func(t *testing.T) {
		key := uuid.NewString()

		res, err := c.CancelTransferWithResponse(t.Context(), key)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, res.StatusCode())
		require.Equal(t, http_client.TransferStatusCancelled, res.JSON200.Status)

		late := create(t, c, key, "DE-OK-1", 3000)
		require.Equal(t, http.StatusOK, late.StatusCode())
		require.Equal(t, http_client.TransferStatusCancelled, late.JSON200.Status)
	})
}

func Test_HTTP_Errors(t *testing.T) {
	c := startBank(t)

	t.Run("same key another amount", func(t *testing.T) {
		key := uuid.NewString()
		require.Equal(t, http.StatusOK, create(t, c, key, "DE-OK-1", 3000).StatusCode())

		res := create(t, c, key, "DE-OK-1", 3001)
		require.Equal(t, http.StatusConflict, res.StatusCode())
		require.NotNil(t, res.JSON409)
		require.NotEmpty(t, res.JSON409.Error)
	})

	t.Run("missing idempotency key", func(t *testing.T) {
		res, err := c.CreateTransferWithResponse(t.Context(), &http_client.CreateTransferParams{},
			http_client.CreateTransferJSONRequestBody{Amount: 3000, Currency: "EUR", Beneficiary: "DE-OK-1"},
			func(_ context.Context, req *http.Request) error {
				req.Header.Del("Idempotency-Key")

				return nil
			})
		require.NoError(t, err)
		require.Equal(t, http.StatusBadRequest, res.StatusCode())
	})

	t.Run("lowercase currency", func(t *testing.T) {
		res, err := c.CreateTransferWithResponse(t.Context(), &http_client.CreateTransferParams{IdempotencyKey: uuid.NewString()},
			http_client.CreateTransferJSONRequestBody{Amount: 3000, Currency: "eur", Beneficiary: "DE-OK-1"})
		require.NoError(t, err)
		require.Equal(t, http.StatusBadRequest, res.StatusCode())
	})

	t.Run("unknown transfer", func(t *testing.T) {
		res, err := c.GetTransferWithResponse(t.Context(), "missing")
		require.NoError(t, err)
		require.Equal(t, http.StatusNotFound, res.StatusCode())
	})
}

func Test_HTTP_Admin(t *testing.T) {
	c := startBank(t)

	t.Run("availability", func(t *testing.T) {
		res, err := c.SetAvailabilityWithResponse(t.Context(), http_client.SetAvailabilityJSONRequestBody{Available: false})
		require.NoError(t, err)
		require.Equal(t, http.StatusNoContent, res.StatusCode())
		require.Equal(t, http.StatusServiceUnavailable, create(t, c, uuid.NewString(), "DE-OK-1", 3000).StatusCode())

		res, err = c.SetAvailabilityWithResponse(t.Context(), http_client.SetAvailabilityJSONRequestBody{Available: true})
		require.NoError(t, err)
		require.Equal(t, http.StatusNoContent, res.StatusCode())
		require.Equal(t, http.StatusOK, create(t, c, uuid.NewString(), "DE-OK-1", 3000).StatusCode())
	})

	t.Run("rogue transfer and drift", func(t *testing.T) {
		added, err := c.AddTransferWithResponse(t.Context(), http_client.AddTransferJSONRequestBody{
			Reference: "ROGUE-1", Amount: 500, Currency: "EUR", Beneficiary: "DE-ROGUE",
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, added.StatusCode())

		stmt, err := c.GetStatementWithResponse(t.Context(), &http_client.GetStatementParams{
			From: time.Now().Add(-time.Hour), To: time.Now().Add(time.Hour),
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, stmt.StatusCode())

		var refs []string
		for _, tr := range stmt.JSON200.Transfers {
			refs = append(refs, tr.Reference)
		}

		require.Contains(t, refs, "ROGUE-1")

		patched, err := c.SetTransferAmountWithResponse(t.Context(), "ROGUE-1", http_client.SetTransferAmountJSONRequestBody{Amount: 1})
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, patched.StatusCode())
		require.EqualValues(t, 1, patched.JSON200.Amount)
	})
}
