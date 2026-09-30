package domains

import (
	"context"
	"time"

	"github.com/uptrace/bun"
)

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

type CacheService interface {
	Get(ctx context.Context, key string) ([]byte, error)
	Set(ctx context.Context, key string, value any, ttl time.Duration) error
}

type SQLService interface {
	Ping(ctx context.Context) error
	Client() *bun.DB
}

type PaginationService[T any] interface {
	Paginate(ctx context.Context, pagination Pagination) (PaginationResult[T], error)
	PaginateFilter(ctx context.Context, filter StructuredFilter, pagination Pagination) (PaginationResult[T], error)
	Filter(ctx context.Context, filter StructuredFilter) (PaginationResult[T], error)

	FilterWithTx(ctx context.Context, tx *bun.Tx, filter StructuredFilter, pagination Pagination) (PaginationResult[T], error)
}
