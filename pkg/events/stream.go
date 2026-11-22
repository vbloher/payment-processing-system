package events

import (
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const (
	StreamName       = "EVENTS"
	DLQStreamName    = "EVENTS_DLQ"
	DLQSubjectPrefix = "dlq."
)

// StreamConfig is the JetStream configuration of the events stream. Duplicates is the
// window in which JetStream drops a repeated message ID on publish.
func StreamConfig() jetstream.StreamConfig {
	return jetstream.StreamConfig{
		Name:       StreamName,
		Subjects:   []string{"events.>"},
		Duplicates: 5 * time.Minute,
		Storage:    jetstream.FileStorage,
	}
}

// DLQStreamConfig is the JetStream configuration of the dead-letter stream.
func DLQStreamConfig() jetstream.StreamConfig {
	return jetstream.StreamConfig{
		Name:     DLQStreamName,
		Subjects: []string{DLQSubjectPrefix + ">"},
		Storage:  jetstream.FileStorage,
	}
}
