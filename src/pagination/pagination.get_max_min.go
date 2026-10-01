package pagination

import (
	"context"
	"database/sql"

	"github.com/uptrace/bun"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/domains"
)

// GetMax returns the row in TData's table matching filter whose field holds
// the highest value, or sql.ErrNoRows if none match. field is validated the
// same way any other sort field is (resolveSortFields), so an unknown
// column errors clearly rather than silently building a broken ORDER BY —
// and resolveSortFields' own ColumnDefaultID tiebreaker still applies, so
// ties on field resolve deterministically the same way every other
// cursor-less lookup in this package does.
func (c *PaginationService[TData, TID]) GetMax(
	ctx context.Context, field string, filter domains.StructuredFilter, preloads ...string,
) (*TData, error) {
	if err := c.checkReady(); err != nil {
		return nil, err
	}
	return c.extreme(ctx, c.ReadSQLService.Client(), field, domains.SortOrderDesc, filter, preloads...)
}

// GetMin is GetMax's opposite: the row whose field holds the lowest value.
func (c *PaginationService[TData, TID]) GetMin(
	ctx context.Context, field string, filter domains.StructuredFilter, preloads ...string,
) (*TData, error) {
	if err := c.checkReady(); err != nil {
		return nil, err
	}
	return c.extreme(ctx, c.ReadSQLService.Client(), field, domains.SortOrderAsc, filter, preloads...)
}

// GetMaxWithTx is GetMax run against a caller-supplied *bun.Tx instead of a
// plain client. That tx is expected to come from WriteSQLService (the
// writer), not ReadSQLService — a transaction only shows its own
// uncommitted work to callers sharing that same connection, and
// ReadSQLService may point at a replica that doesn't even share it — e.g.
// finding the row with the highest field value written earlier in the same
// transaction, before it commits and becomes visible through a separate
// connection.
func (c *PaginationService[TData, TID]) GetMaxWithTx(
	ctx context.Context, tx *bun.Tx, field string, filter domains.StructuredFilter, preloads ...string,
) (*TData, error) {
	if err := c.checkReady(); err != nil {
		return nil, err
	}
	return c.extreme(ctx, tx, field, domains.SortOrderDesc, filter, preloads...)
}

// GetMinWithTx is GetMin's *bun.Tx counterpart (see GetMaxWithTx's doc
// comment).
func (c *PaginationService[TData, TID]) GetMinWithTx(
	ctx context.Context, tx *bun.Tx, field string, filter domains.StructuredFilter, preloads ...string,
) (*TData, error) {
	if err := c.checkReady(); err != nil {
		return nil, err
	}
	return c.extreme(ctx, tx, field, domains.SortOrderAsc, filter, preloads...)
}

// extreme runs filter through paginate with SortFields forced to
// [{field, order}] (order=desc for max, asc for min) and PageSize 1,
// unwrapping the single resulting row the same way findOne does —
// translating "no match" into sql.ErrNoRows rather than a silent nil.
func (c *PaginationService[TData, TID]) extreme(
	ctx context.Context, db bun.IDB, field string, order domains.SortOrder,
	filter domains.StructuredFilter, preloads ...string,
) (*TData, error) {
	result, err := c.paginate(ctx, db, filter, domains.Pagination{
		PageSize: 1,
		Filter:   domains.StructuredFilter{SortFields: []domains.SortField{{Field: field, Order: order}}},
	}, preloads...)
	if err != nil {
		return nil, err
	}
	if len(result.Data) == 0 {
		return nil, sql.ErrNoRows
	}
	return result.Data[0], nil
}
