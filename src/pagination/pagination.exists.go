package pagination

import (
	"context"
	"fmt"

	"github.com/uptrace/bun"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/domains"
)

// Exists reports whether any row in TData's table matches filter — the
// same "how many rows would Filter(ctx, filter) have returned" question
// Count answers, but as a single boolean rather than a row count, and
// without ever scanning a row back. A zero-value domains.StructuredFilter{}
// asks "does this table have any rows at all". filter is treated the same
// trusted/hardcoded way Filter/FilterWithTx/Count treat theirs: an unknown
// field in it is a real error, not something normalizeFilters would
// silently drop.
func (c *PaginationService[TData, TID]) Exists(
	ctx context.Context, filter domains.StructuredFilter,
) (bool, error) {
	if err := c.checkReady(); err != nil {
		return false, err
	}
	return c.exists(ctx, c.ReadSQLService.Client(), filter)
}

// ExistsWithTx is Exists run against a caller-supplied *bun.Tx instead of
// ReadSQLService's own client — e.g. checking for a row written earlier in
// the same transaction, before it commits and becomes visible through a
// separate connection (see FilterWithTx's doc comment for why this shape
// exists alongside the plain version).
func (c *PaginationService[TData, TID]) ExistsWithTx(
	ctx context.Context, tx *bun.Tx, filter domains.StructuredFilter,
) (bool, error) {
	if err := c.checkReady(); err != nil {
		return false, err
	}
	return c.exists(ctx, tx, filter)
}

// exists applies filter to an EXISTS query against db — bun's
// SelectQuery.Exists, not a COUNT(*): the dialect generates a
// short-circuiting "SELECT EXISTS(...)" (or equivalent) that stops at the
// first match instead of visiting every matching row the way Count does.
func (c *PaginationService[TData, TID]) exists(
	ctx context.Context, db bun.IDB, filter domains.StructuredFilter,
) (bool, error) {
	var data []TData
	q := db.NewSelect().Model(&data)
	q, err := c.applyFilters(q, filter)
	if err != nil {
		return false, fmt.Errorf("applying filters: %w", err)
	}
	ok, err := q.Exists(ctx)
	if err != nil {
		return false, fmt.Errorf("checking existence: %w", err)
	}
	return ok, nil
}
