package temporalclient

import (
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	tlog "go.temporal.io/sdk/log"
)

// zerologAdapter adapts the Temporal SDK logger to zerolog.
type zerologAdapter struct {
	logger zerolog.Logger
}

func newLogger() tlog.Logger {
	return &zerologAdapter{logger: log.Logger}
}

func (a *zerologAdapter) Debug(msg string, keyvals ...any) { a.write(a.logger.Debug(), msg, keyvals) }

func (a *zerologAdapter) Info(msg string, keyvals ...any) { a.write(a.logger.Info(), msg, keyvals) }

func (a *zerologAdapter) Warn(msg string, keyvals ...any) { a.write(a.logger.Warn(), msg, keyvals) }

func (a *zerologAdapter) Error(msg string, keyvals ...any) { a.write(a.logger.Error(), msg, keyvals) }

// With returns a logger that adds keyvals to every entry.
func (a *zerologAdapter) With(keyvals ...any) tlog.Logger {
	return &zerologAdapter{logger: a.logger.With().Fields(keyvals).Logger()}
}

func (a *zerologAdapter) write(event *zerolog.Event, msg string, keyvals []any) {
	event.Fields(keyvals).Msg(msg)
}
