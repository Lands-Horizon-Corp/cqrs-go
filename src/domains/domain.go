package domains

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
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

type MessageBrokerService interface {
	Client() *kafka.Client
	Publish(ctx context.Context, topic string, key, value []byte) error
	Subscribe(ctx context.Context, topic string, handler func(key, value []byte) error) error
}

type SQLService interface {
	Ping(ctx context.Context) error
	Client() *bun.DB
}
