package pagination

import (
	"context"

	"github.com/uptrace/bun"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/domains"
)

// Paginate is Pagination with no preload relations, returning
// domains.PaginationResult by value to satisfy the interface's signature.
func (c *PaginationService[TData, TRequest, TID]) Paginate(
	ctx context.Context, pagination domains.Pagination,
) (domains.PaginationResult[TData], error) {
	result, err := c.Pagination(ctx, pagination)
	if err != nil {
		return domains.PaginationResult[TData]{}, err
	}
	return *result, nil
}

// PaginateFilter is Paginate with filter overriding whatever
// StructuredFilter pagination.Filter already carries — the caller-supplied
// filter always wins.
func (c *PaginationService[TData, TRequest, TID]) PaginateFilter(
	ctx context.Context, filter domains.StructuredFilter, pagination domains.Pagination,
) (domains.PaginationResult[TData], error) {
	pagination.Filter = filter
	return c.Paginate(ctx, pagination)
}

// Filter is PaginateFilter against a zero-value domains.Pagination (default
// page size, no cursor) — a convenience for callers that only need the
// first page of a filtered result.
func (c *PaginationService[TData, TRequest, TID]) Filter(
	ctx context.Context, filter domains.StructuredFilter,
) (domains.PaginationResult[TData], error) {
	return c.PaginateFilter(ctx, filter, domains.Pagination{})
}

// FilterWithTx is PaginateFilter run against a caller-supplied *bun.Tx
// instead of ReadSQLService's own client — e.g. reading back rows written
// earlier in the same transaction, before it commits and becomes visible
// through a separate connection.
func (c *PaginationService[TData, TRequest, TID]) FilterWithTx(
	ctx context.Context, tx *bun.Tx, filter domains.StructuredFilter, pagination domains.Pagination,
) (domains.PaginationResult[TData], error) {
	if err := c.checkReady(); err != nil {
		return domains.PaginationResult[TData]{}, err
	}
	pagination.Filter = filter
	result, err := c.paginate(ctx, tx, pagination)
	if err != nil {
		return domains.PaginationResult[TData]{}, err
	}
	return *result, nil
}
