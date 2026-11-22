// Package notification exposes the notification service so that tests can start it in-process.
package notification

import (
	"context"

	"github.com/vbloher/payment-processing-system/services/notification/config"
	"github.com/vbloher/payment-processing-system/services/notification/internal/app"
)

// Run starts the service and blocks until ctx is cancelled.
func Run(ctx context.Context, c config.Config) error {
	return app.Run(ctx, c) //nolint:wrapcheck // one-line entry point, the error is already wrapped by app.Run.
}
