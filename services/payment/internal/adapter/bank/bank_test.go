package bank_test

import (
	"context"
	"net/http"
	"net/http/httptest"
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
	"github.com/vbloher/payment-processing-system/services/payment/internal/adapter/bank"
	"github.com/vbloher/payment-processing-system/services/payment/internal/domain"
)

// startBank runs the mock bank in-process; stop shuts it down and waits for Run to return.
func startBank(t *testing.T) (url string, stop func()) {
	t.Helper()

	otel.SilentModeInit()

	port := testinfra.FreePort(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)

	go func() {
		done <- mockbank.Run(ctx, config.Config{HTTP: httpserver.Config{Port: port}})
	}()

	stopped := false
	stop = func() {
		if stopped {
			return
		}

		stopped = true

		cancel()

		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(30 * time.Second):
			t.Fatal("mockbank.Run did not return after the context was cancelled")
		}
	}

	t.Cleanup(stop)

	base := "http://127.0.0.1:" + port
	testinfra.WaitHTTP(t, base+"/ready")

	return base + "/api/v1", stop
}

func newBank(t *testing.T, url string) *bank.Bank {
	t.Helper()

	b, err := bank.New(bank.Config{URL: url, Timeout: 300 * time.Millisecond})
	require.NoError(t, err)

	return b
}

func Test_Bank(t *testing.T) {
	url, _ := startBank(t)
	b := newBank(t, url)
	ctx := t.Context()

	okRef := uuid.NewString()
	declineRef := uuid.NewString()
	timeoutRef := uuid.NewString()
	freshRef := uuid.NewString()
	driftRef := uuid.NewString()

	t.Run("transfer succeeds", func(t *testing.T) {
		got, err := b.Transfer(ctx, okRef, 3000, "EUR", "DE-OK-1")
		require.NoError(t, err)
		require.Equal(t, domain.BankSucceeded, got.Status)
		require.NotEmpty(t, got.ID)
		require.Equal(t, okRef, got.Reference)
		require.EqualValues(t, 3000, got.Amount)
		require.Equal(t, "EUR", got.Currency)
	})

	t.Run("transfer declined", func(t *testing.T) {
		got, err := b.Transfer(ctx, declineRef, 3000, "EUR", "DECLINE-1")
		require.NoError(t, err)
		require.Equal(t, domain.BankDeclined, got.Status)
	})

	t.Run("transfer timeout is unknown, then cancel wins", func(t *testing.T) {
		_, err := b.Transfer(ctx, timeoutRef, 3000, "EUR", "TIMEOUT-1")
		require.ErrorIs(t, err, domain.ErrBankUnavailable)

		got, err := b.Cancel(ctx, timeoutRef)
		require.NoError(t, err)
		require.Equal(t, domain.BankSucceeded, got.Status)
	})

	t.Run("cancel of an unknown reference fences it", func(t *testing.T) {
		got, err := b.Cancel(ctx, freshRef)
		require.NoError(t, err)
		require.Equal(t, domain.BankCancelled, got.Status)

		got, err = b.Transfer(ctx, freshRef, 3000, "EUR", "DE-OK-1")
		require.NoError(t, err)
		require.Equal(t, domain.BankCancelled, got.Status)
	})

	t.Run("transfer replayed with a changed amount is unknown, then cancel returns the stored transfer", func(t *testing.T) {
		_, err := b.Transfer(ctx, driftRef, 3000, "EUR", "TIMEOUT-1")
		require.ErrorIs(t, err, domain.ErrBankUnavailable)

		admin, err := http_client.NewClientWithResponses(url)
		require.NoError(t, err)

		res, err := admin.SetTransferAmountWithResponse(ctx, driftRef, http_client.SetTransferAmountJSONRequestBody{Amount: 1})
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, res.StatusCode())

		got, err := b.Transfer(ctx, driftRef, 3000, "EUR", "TIMEOUT-1")
		require.ErrorIs(t, err, domain.ErrBankUnavailable, "a 409 must never read as a refusal")
		require.Empty(t, got.Status)

		got, err = b.Cancel(ctx, driftRef)
		require.NoError(t, err)
		require.Equal(t, domain.BankSucceeded, got.Status)
		require.EqualValues(t, 1, got.Amount)
	})

	t.Run("get transfer", func(t *testing.T) {
		got, err := b.GetTransfer(ctx, okRef)
		require.NoError(t, err)
		require.NotNil(t, got)
		require.Equal(t, domain.BankSucceeded, got.Status)

		missing, err := b.GetTransfer(ctx, uuid.NewString())
		require.NoError(t, err)
		require.Nil(t, missing)
	})

	t.Run("statement holds succeeded transfers only", func(t *testing.T) {
		got, err := b.GetStatement(ctx, time.Now().Add(-time.Hour), time.Now().Add(time.Hour))
		require.NoError(t, err)

		refs := make([]string, 0, len(got))
		for _, tr := range got {
			require.Equal(t, domain.BankSucceeded, tr.Status)

			refs = append(refs, tr.Reference)
		}

		require.ElementsMatch(t, []string{okRef, timeoutRef, driftRef}, refs)
	})

	t.Run("unavailable bank", func(t *testing.T) {
		admin, err := http_client.NewClientWithResponses(url)
		require.NoError(t, err)

		set := func(available bool) {
			res, err := admin.SetAvailabilityWithResponse(ctx, http_client.SetAvailabilityJSONRequestBody{Available: available})
			require.NoError(t, err)
			require.Equal(t, http.StatusNoContent, res.StatusCode())
		}

		set(false)
		t.Cleanup(func() { set(true) })

		_, err = b.Transfer(ctx, uuid.NewString(), 3000, "EUR", "DE-OK-1")
		require.ErrorIs(t, err, domain.ErrBankUnavailable)

		_, err = b.Cancel(ctx, uuid.NewString())
		require.ErrorIs(t, err, domain.ErrBankUnavailable)

		_, err = b.GetTransfer(ctx, okRef)
		require.ErrorIs(t, err, domain.ErrBankUnavailable)

		_, err = b.GetStatement(ctx, time.Now().Add(-time.Hour), time.Now().Add(time.Hour))
		require.ErrorIs(t, err, domain.ErrBankUnavailable)
	})
}

func Test_Bank_Stopped(t *testing.T) {
	url, stop := startBank(t)
	b := newBank(t, url)

	stop()

	_, err := b.Transfer(t.Context(), uuid.NewString(), 3000, "EUR", "DE-OK-1")
	require.ErrorIs(t, err, domain.ErrBankUnavailable)
}

func Test_Bank_TransferStatus(t *testing.T) {
	tests := []struct {
		name    string
		code    int
		want    domain.BankOutcome
		wantErr error
	}{
		{"400 is a refusal: the request was rejected before any transfer was looked at", http.StatusBadRequest, domain.BankDeclined, nil},
		{"409 is unknown: a transfer already exists under the reference", http.StatusConflict, "", domain.ErrBankUnavailable},
		{"503 is unknown", http.StatusServiceUnavailable, "", domain.ErrBankUnavailable},
		{"504 is unknown", http.StatusGatewayTimeout, "", domain.ErrBankUnavailable},
		{"500 is unknown", http.StatusInternalServerError, "", domain.ErrBankUnavailable},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tt.code)
				_, _ = w.Write([]byte(`{"error":"request failed"}`))
			}))
			t.Cleanup(srv.Close)

			b := newBank(t, srv.URL)

			got, err := b.Transfer(t.Context(), "r", 1, "EUR", "b")
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				require.Equal(t, domain.BankTransfer{}, got)

				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.want, got.Status)
		})
	}
}

func Test_Bank_UnknownStatus(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{"empty status", `{"id":"` + uuid.NewString() + `","reference":"r","amount":1,"currency":"EUR","beneficiary":"b","status":"","created_at":"2026-01-01T00:00:00Z"}`},
		{"unknown status", `{"id":"` + uuid.NewString() + `","reference":"r","amount":1,"currency":"EUR","beneficiary":"b","status":"pending","created_at":"2026-01-01T00:00:00Z"}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(tt.body))
			}))
			t.Cleanup(srv.Close)

			b := newBank(t, srv.URL)

			_, err := b.Transfer(t.Context(), "r", 1, "EUR", "b")
			require.ErrorIs(t, err, domain.ErrBankUnavailable)

			got, err := b.GetTransfer(t.Context(), "r")
			require.ErrorIs(t, err, domain.ErrBankUnavailable)
			require.Nil(t, got)
		})
	}
}
