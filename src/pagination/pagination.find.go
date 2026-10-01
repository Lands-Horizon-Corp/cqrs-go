package pagination

import (
	"context"
	"database/sql"

	"github.com/uptrace/bun"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/domains"
)

// Find returns the first row in TData's table matching filter, or
// sql.ErrNoRows if none match — the single-row counterpart to Filter, which
// returns every match. It shares paginate's exact filtering/sorting/preload
// machinery (via a PageSize of 1), the same way Filter does, so "first" is
// whatever paginate's own default ordering (ColumnDefaultSort, falling back
// to ColumnDefaultID) would put first — not an arbitrary row.
func (c *PaginationService[TData, TID]) Find(
	ctx context.Context, filter domains.StructuredFilter, preloads ...string,
) (*TData, error) {
	if err := c.checkReady(); err != nil {
		return nil, err
	}
	return c.find(ctx, c.ReadSQLService.Client(), filter, preloads...)
}

// FindWithTx is Find run against a caller-supplied *bun.Tx instead of
// ReadSQLService's own client — e.g. finding a row written earlier in the
// same transaction, before it commits and becomes visible through a
// separate connection (see FilterWithTx's doc comment for why this shape
// exists alongside the plain version).
func (c *PaginationService[TData, TID]) FindWithTx(
	ctx context.Context, tx *bun.Tx, filter domains.StructuredFilter, preloads ...string,
) (*TData, error) {
	if err := c.checkReady(); err != nil {
		return nil, err
	}
	return c.find(ctx, tx, filter, preloads...)
}

// find runs filter through paginate with PageSize 1 and unwraps the single
// resulting row, translating "no match" into sql.ErrNoRows — the same
// not-found signal UpdateByID/DeleteByID use elsewhere in this project —
// rather than a silent nil with no error.
func (c *PaginationService[TData, TID]) find(
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
