package webhook_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/vbloher/payment-processing-system/pkg/events"
	"github.com/vbloher/payment-processing-system/pkg/otel"
	"github.com/vbloher/payment-processing-system/services/notification/internal/adapter/webhook"
	"github.com/vbloher/payment-processing-system/services/notification/internal/domain"
)

func notification(t *testing.T) domain.Notification {
	t.Helper()

	otel.SilentModeInit()

	e, err := events.New(events.TypePaymentSucceeded, uuid.New(), events.PaymentData{Amount: 100, Currency: "EUR"})
	require.NoError(t, err)

	n, err := domain.NewNotification(e)
	require.NoError(t, err)

	return n
}

func Test_Send_PostsPayload(t *testing.T) {
	n := notification(t)

	var (
		method, contentType, key string
		body                     []byte
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method, contentType, key = r.Method, r.Header.Get("Content-Type"), r.Header.Get("Idempotency-Key")
		body, _ = io.ReadAll(r.Body)

		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	err := webhook.New(webhook.Config{URL: srv.URL, Timeout: time.Second}).Send(t.Context(), n)
	require.NoError(t, err)
	require.Equal(t, http.MethodPost, method)
	require.Equal(t, "application/json", contentType)
	require.Equal(t, n.EventID.String(), key)
	require.Equal(t, n.Payload, body)
}

func Test_Send_Status(t *testing.T) {
	tests := []struct {
		name    string
		status  int
		wantErr bool
	}{
		{name: "200", status: http.StatusOK},
		{name: "204", status: http.StatusNoContent},
		{name: "redirect is not followed", status: http.StatusFound, wantErr: true},
		{name: "400", status: http.StatusBadRequest, wantErr: true},
		{name: "500", status: http.StatusInternalServerError, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mux := http.NewServeMux()
			mux.HandleFunc("/hook", func(w http.ResponseWriter, r *http.Request) {
				if tt.status == http.StatusFound {
					http.Redirect(w, r, "/elsewhere", tt.status)

					return
				}

				w.WriteHeader(tt.status)
			})
			mux.HandleFunc("/elsewhere", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })

			srv := httptest.NewServer(mux)
			t.Cleanup(srv.Close)

			err := webhook.New(webhook.Config{URL: srv.URL + "/hook", Timeout: time.Second}).Send(t.Context(), notification(t))
			if !tt.wantErr {
				require.NoError(t, err)

				return
			}

			require.ErrorContains(t, err, http.StatusText(tt.status))
		})
	}
}

func Test_Send_ErrorCarriesBoundedBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = io.WriteString(w, "upstream down "+strings.Repeat("x", 1<<20))
	}))
	t.Cleanup(srv.Close)

	err := webhook.New(webhook.Config{URL: srv.URL, Timeout: time.Second}).Send(t.Context(), notification(t))
	require.ErrorContains(t, err, "502")
	require.ErrorContains(t, err, "upstream down")
	require.Less(t, len(err.Error()), 1024)
}

func Test_Send_Timeout(t *testing.T) {
	// The handler holds the request until the client gives up and closes the connection. The
	// server notices the close only once the request body has been read.
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		<-r.Context().Done()
	}))
	t.Cleanup(srv.Close)

	err := webhook.New(webhook.Config{URL: srv.URL, Timeout: 50 * time.Millisecond}).Send(t.Context(), notification(t))
	require.Error(t, err)
}
