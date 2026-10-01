package domains

import (
	"context"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/uptrace/bun"
)

type LogService interface {
	Log(ctx context.Context, message string)
	Error(ctx context.Context, message string)
	Warn(ctx context.Context, message string)
	Panic(ctx context.Context, message string)
	Success(ctx context.Context, message string)
	Fatal(ctx context.Context, message string)
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
type PaginationService[TData any, TID comparable] interface {
	Paginate(ctx context.Context, pagination Pagination) (PaginationResult[TData], error)
	PaginateFilter(ctx context.Context, filter StructuredFilter, pagination Pagination) (PaginationResult[TData], error)
	Filter(ctx context.Context, filter StructuredFilter) ([]*TData, error)
	FilterWithTx(ctx context.Context, tx *bun.Tx, filter StructuredFilter) ([]*TData, error)
	PaginateWithHertz(ctx context.Context, tx *bun.Tx, filter StructuredFilter, reqCtx *app.RequestContext) (PaginationResult[TData], error)
	Count(ctx context.Context, filter StructuredFilter) (int64, error)
	CountWithTx(ctx context.Context, tx *bun.Tx, filter StructuredFilter) (int64, error)
	Exists(ctx context.Context, filter StructuredFilter) (bool, error)
	ExistsWithTx(ctx context.Context, tx *bun.Tx, filter StructuredFilter) (bool, error)
	Find(ctx context.Context, filter StructuredFilter, preloads ...string) ([]*TData, error)
	FindWithTx(ctx context.Context, tx *bun.Tx, filter StructuredFilter, preloads ...string) ([]*TData, error)
	FindOne(ctx context.Context, filter StructuredFilter, preloads ...string) (*TData, error)
	FindOneWithTx(ctx context.Context, tx *bun.Tx, filter StructuredFilter, preloads ...string) (*TData, error)
	GetMax(ctx context.Context, field string, filter StructuredFilter, preloads ...string) (*TData, error)
	GetMin(ctx context.Context, field string, filter StructuredFilter, preloads ...string) (*TData, error)
	GetMaxWithTx(ctx context.Context, tx *bun.Tx, field string, filter StructuredFilter, preloads ...string) (*TData, error)
	GetMinWithTx(ctx context.Context, tx *bun.Tx, field string, filter StructuredFilter, preloads ...string) (*TData, error)
}
