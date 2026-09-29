package domains

import (
	"context"
	"time"

	"github.com/uptrace/bun"
)

type (
	Channel    string
	Events     []string
	ChangeType int
)

const (
	ChangeTypeCreated ChangeType = iota
	ChangeTypeUpdated
	ChangeTypeDeleted
)

type CQRSQueuePayload[TData any] struct {
	EventID    string     `json:"event_id"`
	ChangeType ChangeType `json:"change_type"`
	Payload    TData      `json:"payload"`
}

type ProcessedEvent struct {
	bun.BaseModel `bun:"table:processed_events,alias:pe"`
	EventID       string    `bun:"event_id,pk"`
	Channel       string    `bun:"channel,notnull"`
	CreatedAt     time.Time `bun:"created_at,nullzero,notnull,default:current_timestamp"`
}

type LogService interface {
	Log(ctx context.Context, message string)
	Error(ctx context.Context, message string)
	Warn(ctx context.Context, message string)
	Panic(ctx context.Context, message string)
	Success(ctx context.Context, message string)
}

type BroadcastService interface {
	Broadcast(channels []Channel, events Events, payload any) error
}

// MessageBrokerService is deliberately transport-agnostic: no method here
// references a concrete pub/sub client type, so implementing it (Kafka,
// NATS, an HTTP webhook receiver for Debezium Server, or anything else)
// never forces a specific broker library onto a consumer of this package
// who doesn't use that implementation. src/kafka.Broker is one real
// implementation, not the only one this interface allows.
type MessageBrokerService interface {
	Publish(ctx context.Context, topic string, key, value []byte) error
	Subscribe(ctx context.Context, topic string, handler func(key, value []byte) error) error
}

type SQLService interface {
	Ping(ctx context.Context) error
	Client() *bun.DB
}
