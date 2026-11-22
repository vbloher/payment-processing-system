// Package ledger exposes the ledger service so that tests can start it in-process.
package ledger

import (
	"context"

	"github.com/vbloher/payment-processing-system/services/ledger/config"
	"github.com/vbloher/payment-processing-system/services/ledger/internal/app"
)

// Run starts the service and blocks until ctx is cancelled.
func Run(ctx context.Context, c config.Config) error {
	return app.Run(ctx, c) //nolint:wrapcheck // one-line entry point, the error is already wrapped by app.Run.
}
