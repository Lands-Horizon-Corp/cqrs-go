package pagination

import (
	"context"
	"database/sql"

	"github.com/uptrace/bun"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/domains"
)

// FindOne returns the first row in TData's table matching filter, or
// sql.ErrNoRows if none match — the single-row counterpart to Filter, which
// returns every match. It shares paginate's exact filtering/sorting/preload
// machinery (via a PageSize of 1), the same way Filter does, so "first" is
// whatever paginate's own default ordering (ColumnDefaultSort, falling back
// to ColumnDefaultID) would put first — not an arbitrary row.
func (c *PaginationService[TData, TID]) FindOne(
	ctx context.Context, filter domains.StructuredFilter, preloads ...string,
) (*TData, error) {
	if err := c.checkReady(); err != nil {
		return nil, err
	}
	return c.findOne(ctx, c.ReadSQLService.Client(), filter, preloads...)
}

// FindOneWithTx is FindOne run against a caller-supplied *bun.Tx instead of
// a plain client. That tx is expected to come from WriteSQLService (the
// writer), not ReadSQLService — see FilterWithTx's doc comment in
// pagination.service.go for why — e.g. finding a row written earlier in the
// same transaction, before it commits and becomes visible through a
// separate connection.
func (c *PaginationService[TData, TID]) FindOneWithTx(
	ctx context.Context, tx *bun.Tx, filter domains.StructuredFilter, preloads ...string,
) (*TData, error) {
	if err := c.checkReady(); err != nil {
		return nil, err
	}
	return c.findOne(ctx, tx, filter, preloads...)
}

// findOne runs filter through paginate with PageSize 1 and unwraps the
// single resulting row, translating "no match" into sql.ErrNoRows — the
// same not-found signal UpdateByID/DeleteByID use elsewhere in this
// project — rather than a silent nil with no error.
func (c *PaginationService[TData, TID]) findOne(
	ctx context.Context, db bun.IDB, filter domains.StructuredFilter, preloads ...string,
) (*TData, error) {
	result, err := c.paginate(ctx, db, filter, domains.Pagination{PageSize: 1}, preloads...)
	if err != nil {
		return nil, err
	}
	if len(result.Data) == 0 {
		return nil, sql.ErrNoRows
	}
	return result.Data[0], nil
}
