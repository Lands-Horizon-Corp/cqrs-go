package pagination

import (
	"context"

	"github.com/cloudwego/hertz/pkg/app"
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
	result, err := c.paginate(ctx, c.ReadSQLService.Client(), filter, pagination, false)
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

// FilterWithTx is Filter run against a caller-supplied *bun.Tx instead of a
// plain client. That tx is expected to have been begun on WriteSQLService
// (the writer), not ReadSQLService: a transaction only shows its own
// uncommitted work to callers sharing that same connection, and in a real
// deployment ReadSQLService may point at a replica that doesn't even share
// the writer's connection, let alone an open transaction on it — e.g.
// reading back rows written earlier in the same transaction, before it
// commits and becomes visible through a separate connection. It's just a
// filter, the same way Filter is — no
// pagination parameter and no PaginationResult wrapper, since a
// transactional read-your-writes lookup like this has no frontend request
// behind it to carry page size/cursor for.
//
// Every matched row is locked ("SELECT ... FOR UPDATE" — see paginate's own
// doc comment for why every *WithTx fetch does this): the whole reason to
// reach for this instead of Filter is "I'm about to act on these rows
// inside this same transaction."
func (c *PaginationService[TData, TID]) FilterWithTx(
	ctx context.Context, tx *bun.Tx, filter domains.StructuredFilter,
) ([]*TData, error) {
	if err := c.checkReady(); err != nil {
		return nil, err
	}
	result, err := c.paginate(ctx, tx, filter, domains.Pagination{}, true)
	if err != nil {
		return nil, err
	}
	return result.Data, nil
}

// PaginateWithHertz is PaginateFilter run against a caller-supplied *bun.Tx
// (the same shape as FilterWithTx), except the domains.Pagination half
// (Filter/SortFields/PageSize/Cursor) is parsed directly off the incoming
// Hertz request via domains.Pagination.Parse — the "?filter=...&sort=...
// &pageSize=...&cursor=..." query string a frontend actually sends —
// instead of requiring the caller to have already parsed one. filter stays
// a separate parameter the same reason it is everywhere else in this file:
// it's typically backend-hardcoded (tenant scoping, ...) and must apply
// unconditionally alongside whatever the request's own filter says, never
// overriding it (see PaginateFilter's doc comment for the AND-merge).
//
// domains.Pagination.Parse already is this project's Hertz equivalent of
// an older project's echo-based parseFilters/parseSort/parseQuery:
// Query("filter")/Query("sort") go through the same unescape -> base64
// decode -> JSON unmarshal chain (utils.DecodeQueryParam, generic instead
// of duplicated per type), and PageSize/Cursor are populated by Hertz's
// own ctx.BindAndValidate against domains.Pagination's `query:"..."`
// struct tags rather than hand-written parsePageSize/parsePageIndex
// functions — there's no pageIndex/offset concept here at all, since this
// whole system is keyset/cursor-based pagination, not offset-based.
func (c *PaginationService[TData, TID]) PaginateWithHertz(
	ctx context.Context, tx *bun.Tx, filter domains.StructuredFilter, reqCtx *app.RequestContext,
) (domains.PaginationResult[TData], error) {
	if err := c.checkReady(); err != nil {
		return domains.PaginationResult[TData]{}, err
	}
	var pagination domains.Pagination
	if err := pagination.Parse(reqCtx); err != nil {
		return domains.PaginationResult[TData]{}, err
	}
	// forUpdate is deliberately false here, unlike FilterWithTx: this is a
	// browsing/listing path (a paginated page of results for display), not
	// a "read this row because I'm about to write it" one — locking every
	// row of a paginated listing by default would be a surprising and
	// likely harmful default for what's usually a read-only request.
	result, err := c.paginate(ctx, tx, filter, pagination, false)
	if err != nil {
		return domains.PaginationResult[TData]{}, err
	}
	return *result, nil
}
