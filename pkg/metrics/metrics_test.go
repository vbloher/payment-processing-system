package metrics_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/vbloher/payment-processing-system/pkg/metrics"
)

func TestNew_SeparateRegistriesDoNotCollide(t *testing.T) {
	for range 2 {
		reg := prometheus.NewRegistry()
		require.NotPanics(t, func() { metrics.NewHTTPServer(reg); metrics.NewProcess(reg) })
	}
}

func TestHandler_ServesItsRegistry(t *testing.T) {
	reg := prometheus.NewRegistry()
	metrics.NewHTTPServer(reg).TotalInc("GET /x", 200)

	rec := httptest.NewRecorder()
	metrics.Handler(reg).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	require.Contains(t, rec.Body.String(), `http_server_requests_total{method="GET /x",status="200"} 1`)
}
