package pagination

import (
	"context"

	"github.com/uptrace/bun"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/domains"
)

// The methods in this file exist solely so *PaginationService[TData,
// TResponse, TRequest, TID] satisfies domains.PaginationService[TResponse]
// — a single-type-parameter interface a consumer can depend on without
// knowing PaginationService's full generic shape (TData/TRequest/TID are
// construction-time details, not part of the caller-facing contract),
// making it swappable/mockable wherever it's injected as a dependency.
// Pagination itself (returning *domains.PaginationResult and accepting
// variadic preloads) remains the primary, richer API these all delegate to.
var _ domains.PaginationService[any] = (*PaginationService[struct{}, any, any, string])(nil)

// Paginate is Pagination with no preload relations, returning
// domains.PaginationResult by value to satisfy the interface's signature.
func (c *PaginationService[TData, TResponse, TRequest, TID]) Paginate(
	ctx context.Context, pagination domains.Pagination,
) (domains.PaginationResult[TResponse], error) {
	result, err := c.Pagination(ctx, pagination)
	if err != nil {
		return domains.PaginationResult[TResponse]{}, err
	}
	return *result, nil
}

// PaginateFilter is Paginate with filter overriding whatever
// StructuredFilter pagination.Filter already carries — the caller-supplied
// filter always wins.
func (c *PaginationService[TData, TResponse, TRequest, TID]) PaginateFilter(
	ctx context.Context, filter domains.StructuredFilter, pagination domains.Pagination,
) (domains.PaginationResult[TResponse], error) {
	pagination.Filter = filter
	return c.Paginate(ctx, pagination)
}

// Filter is PaginateFilter against a zero-value domains.Pagination (default
// page size, no cursor) — a convenience for callers that only need the
// first page of a filtered result.
func (c *PaginationService[TData, TResponse, TRequest, TID]) Filter(
	ctx context.Context, filter domains.StructuredFilter,
) (domains.PaginationResult[TResponse], error) {
	return c.PaginateFilter(ctx, filter, domains.Pagination{})
}

// FilterWithTx is PaginateFilter run against a caller-supplied *bun.Tx
// instead of ReadSQLService's own client — e.g. reading back rows written
// earlier in the same transaction, before it commits and becomes visible
// through a separate connection.
func (c *PaginationService[TData, TResponse, TRequest, TID]) FilterWithTx(
	ctx context.Context, tx *bun.Tx, filter domains.StructuredFilter, pagination domains.Pagination,
) (domains.PaginationResult[TResponse], error) {
	if err := c.checkReady(); err != nil {
		return domains.PaginationResult[TResponse]{}, err
	}
	pagination.Filter = filter
	result, err := c.paginate(ctx, tx, pagination)
	if err != nil {
		return domains.PaginationResult[TResponse]{}, err
	}
	return *result, nil
}
