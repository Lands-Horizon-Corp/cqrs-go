package repository

import (
	"context"
	"time"

	"github.com/Lands-Horizon-Corp/cqrs-go/utils"
	"github.com/go-playground/validator/v10"
	"github.com/segmentio/kafka-go"
	"github.com/uptrace/bun"
)

type (
	Channel   string
	Events    []string
	EventType int
)

const (
	EventTypeCreated EventType = iota
	EventTypeUpdated
	EventTypeDeleted
)

type CQRSQueuePayload[TData any] struct {
	EventID   string    `json:"event_id"`
	EventType EventType `json:"event_type"`
	Payload   TData     `json:"payload"`
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

type RepositoryImpl[TData any, TResponse any, TRequest any, TID any] struct {
	WriteDB *bun.DB
	ReadDB  *bun.DB

	Channel  Channel
	Dispatch func(channel Channel, events Events, payload *TResponse) error

	Created func(*TData) Events
	Updated func(*TData) Events
	Deleted func(*TData) Events

	ToResource func(*TData) *TResponse

	ColumnDefaultID   string
	ColumnDefaultSort string

	Preloads []string

	Tabular func(data *TData) map[string]any

	LogService           LogService
	BroadcastService     BroadcastService
	MessageBrokerService MessageBrokerService
	SQLService           SQLService
	Validator            *validator.Validate

	stringSlicePool     *utils.BufferPool[string]
	stringSetPool       *utils.MapPool[string, bool]
	processedEventsPool *utils.BufferPool[ProcessedEvent]
}

func NewRepository[TData any, TResponse any, TRequest any, TID any](
	params RepositoryImpl[TData, TResponse, TRequest, TID],
) *RepositoryImpl[TData, TResponse, TRequest, TID] {
	if params.ColumnDefaultID == "" {
		params.ColumnDefaultID = "id"
	}
	if params.ColumnDefaultSort == "" {
		params.ColumnDefaultSort = "updated_at DESC"
	}
	if params.Validator == nil {
		params.Validator = validator.New()
	}
	return &RepositoryImpl[TData, TResponse, TRequest, TID]{
		WriteDB:              params.WriteDB,
		ReadDB:               params.ReadDB,
		Channel:              params.Channel,
		Dispatch:             params.Dispatch,
		Created:              params.Created,
		Updated:              params.Updated,
		Deleted:              params.Deleted,
		ToResource:           params.ToResource,
		ColumnDefaultID:      params.ColumnDefaultID,
		ColumnDefaultSort:    params.ColumnDefaultSort,
		Preloads:             params.Preloads,
		Tabular:              params.Tabular,
		LogService:           params.LogService,
		BroadcastService:     params.BroadcastService,
		MessageBrokerService: params.MessageBrokerService,
		SQLService:           params.SQLService,
		Validator:            params.Validator,

		stringSlicePool:     utils.NewBufferPool[string](),
		stringSetPool:       utils.NewMapPool[string, bool](),
		processedEventsPool: utils.NewBufferPool[ProcessedEvent](),
	}
}
