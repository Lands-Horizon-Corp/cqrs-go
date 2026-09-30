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

// PaginationService mirrors the three type parameters of its one real
// implementation (pagination.PaginationService[TData, TRequest, TID]) so a
// struct that already carries all three as its own type parameters —
// CQRSImpl, for one — can hold this interface typed with exactly its own
// TData/TRequest/TID, instead of having to fix one of them to a concrete
// type just to name the interface. There is no TResponse here: pagination
// only ever hands back the raw TData rows straight from SQL, never a
// ToResource-converted view.
type PaginationService[TData any, TRequest any, TID comparable] interface {
	Paginate(ctx context.Context, pagination Pagination) (PaginationResult[TData], error)
	PaginateFilter(ctx context.Context, filter StructuredFilter, pagination Pagination) (PaginationResult[TData], error)
	Filter(ctx context.Context, filter StructuredFilter) (PaginationResult[TData], error)

	FilterWithTx(ctx context.Context, tx *bun.Tx, filter StructuredFilter, pagination Pagination) (PaginationResult[TData], error)
}
