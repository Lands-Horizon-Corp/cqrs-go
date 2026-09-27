package repository

import (
	"context"
	"encoding/json"

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

type CQRSQueuePayload struct {
	EventType EventType       `json:"event_type"`
	Payload   json.RawMessage `json:"payload"`
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
	Publish(ctx context.Context, topic string, key, value []byte) error
	Subscribe(ctx context.Context, topic string, handler func(key, value []byte) error) error
}

type RepositoryImpl[TData any, TResponse any, TRequest any, TID any] struct {
	WriteDB *bun.DB
	ReadDB  *bun.DB
	Queue   *kafka.Writer
	Reader  *kafka.Reader

	Channel  Channel
	Dispatch func(channel Channel, events Events, payload *TResponse) error

	Created func(*TData) Events
	Updated func(*TData) Events
	Deleted func(*TData) Events

	ToData     func(TRequest) *TData
	ToResource func(*TData) *TResponse

	ColumnDefaultID   string
	ColumnDefaultSort string

	Preloads []string

	Tabular func(data *TData) map[string]any

	LogService           LogService
	BroadcastService     BroadcastService
	MessageBrokerService MessageBrokerService
	Validator            *validator.Validate
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
	return &RepositoryImpl[TData, TResponse, TRequest, TID]{
		WriteDB:              params.WriteDB,
		ReadDB:               params.ReadDB,
		Queue:                params.Queue,
		Channel:              params.Channel,
		Dispatch:             params.Dispatch,
		Created:              params.Created,
		Updated:              params.Updated,
		Deleted:              params.Deleted,
		ToResource:           params.ToResource,
		ToData:               params.ToData,
		ColumnDefaultID:      params.ColumnDefaultID,
		ColumnDefaultSort:    params.ColumnDefaultSort,
		Preloads:             params.Preloads,
		Tabular:              params.Tabular,
		Reader:               params.Reader,
		LogService:           params.LogService,
		BroadcastService:     params.BroadcastService,
		MessageBrokerService: params.MessageBrokerService,
		Validator:            params.Validator,
	}
}
