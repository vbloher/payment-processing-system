package nats_consumer_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/vbloher/payment-processing-system/services/notification/internal/controller/nats_consumer"
)

// The handler gets four fifths of AckWait; the rest is kept to settle the message.
func Test_Config_HandlerTimeout(t *testing.T) {
	require.Equal(t, 24*time.Second, nats_consumer.Config{AckWait: 30 * time.Second}.HandlerTimeout())
	require.Equal(t, 1600*time.Millisecond, nats_consumer.Config{AckWait: 2 * time.Second}.HandlerTimeout())
}

func Test_New_RejectsInvalidConfig(t *testing.T) {
	valid := nats_consumer.Config{Durable: "notification", MaxDeliver: 5, RetryDelay: time.Second, AckWait: 30 * time.Second}

	tests := []struct {
		name   string
		mutate func(c *nats_consumer.Config)
	}{
		{name: "empty durable", mutate: func(c *nats_consumer.Config) { c.Durable = "" }},
		// 0 would dead-letter every failed event on its first delivery.
		{name: "max deliver 0", mutate: func(c *nats_consumer.Config) { c.MaxDeliver = 0 }},
		{name: "max deliver -1", mutate: func(c *nats_consumer.Config) { c.MaxDeliver = -1 }},
		{name: "ack wait 0", mutate: func(c *nats_consumer.Config) { c.AckWait = 0 }},
		{name: "negative retry delay", mutate: func(c *nats_consumer.Config) { c.RetryDelay = -time.Second }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := valid
			tt.mutate(&c)

			// The config is checked before the client is used, so none is needed.
			_, err := nats_consumer.New(t.Context(), c, nil, nil)
			require.ErrorIs(t, err, nats_consumer.ErrInvalidConfig)
		})
	}
}
