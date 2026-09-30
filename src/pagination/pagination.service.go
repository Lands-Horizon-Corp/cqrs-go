package pagination

import (
	"context"

	"github.com/uptrace/bun"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/domains"
)

// Paginate is Pagination with no preload relations, returning
// domains.PaginationResult by value to satisfy the interface's signature.
func (c *PaginationService[TData, TID]) Paginate(
	ctx context.Context, pagination domains.Pagination,
) (domains.PaginationResult[TData], error) {
	result, err := c.Pagination(ctx, pagination)
	if err != nil {
		return domains.PaginationResult[TData]{}, err
	}
	return *result, nil
}

// PaginateFilter combines filter (typically backend-hardcoded — e.g.
// tenant scoping) with whatever StructuredFilter pagination.Filter already
// carries (typically frontend-supplied) as "(filter) AND
// (pagination.Filter)" — neither one overrides or clobbers the other, each
// keeps its own internal Logic (AND/OR among its own Filters). This is the
// whole reason filter is a separate parameter from pagination in the first
// place: pagination (page size, cursor, and whatever filter the frontend
// sent) comes from the caller/request, filter is what the backend adds on
// top unconditionally.
func (c *PaginationService[TData, TID]) PaginateFilter(
	ctx context.Context, filter domains.StructuredFilter, pagination domains.Pagination,
) (domains.PaginationResult[TData], error) {
	if err := c.checkReady(); err != nil {
		return domains.PaginationResult[TData]{}, err
	}
	result, err := c.paginate(ctx, c.ReadSQLService.Client(), filter, pagination)
	if err != nil {
		return domains.PaginationResult[TData]{}, err
	}
	return *result, nil
}

// Filter is PaginateFilter against a zero-value domains.Pagination (default
// page size, no cursor, no frontend-supplied filter), returning just the
// matched rows rather than a full PaginationResult — a convenience for
// callers that only need a one-shot filtered lookup, not real pagination:
// there's no cursor exposed here to request a second page with anyway, so
// wrapping the result in cursor/page-size metadata nobody can act on would
// be misleading.
func (c *PaginationService[TData, TID]) Filter(
	ctx context.Context, filter domains.StructuredFilter,
) ([]*TData, error) {
	result, err := c.PaginateFilter(ctx, filter, domains.Pagination{})
	if err != nil {
		return nil, err
	}
	return result.Data, nil
}

// FilterWithTx is Filter run against a caller-supplied *bun.Tx instead of
// ReadSQLService's own client — e.g. reading back rows written earlier in
// the same transaction, before it commits and becomes visible through a
// separate connection. It's just a filter, the same way Filter is — no
// pagination parameter and no PaginationResult wrapper, since a
// transactional read-your-writes lookup like this has no frontend request
// behind it to carry page size/cursor for.
func (c *PaginationService[TData, TID]) FilterWithTx(
	ctx context.Context, tx *bun.Tx, filter domains.StructuredFilter,
) ([]*TData, error) {
	if err := c.checkReady(); err != nil {
		return nil, err
	}
	result, err := c.paginate(ctx, tx, filter, domains.Pagination{})
	if err != nil {
		return nil, err
	}
	return result.Data, nil
}
