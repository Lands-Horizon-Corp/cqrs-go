package cqrs

import (
	"context"

	"github.com/uptrace/bun"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/domains"
)

// Count returns the number of rows in TData's table matching filter — "how
// many rows would Filter(ctx, filter) have returned", without scanning any
// of them back. A zero-value domains.StructuredFilter{} counts every row.
func (c *CQRSImpl[TData, TResponse, TRequest, TID]) Count(
	ctx context.Context, filter domains.StructuredFilter,
) (int64, error) {
	return c.paginationService.Count(ctx, filter)
}

// CountWithTx is Count run against a caller-supplied *bun.Tx instead of a
// plain client. That tx is expected to come from WriteSQLService (the
// writer), not ReadSQLService — a transaction only shows its own
// uncommitted work to callers sharing that same connection, and
// ReadSQLService may point at a replica that doesn't even share it — e.g.
// counting rows written earlier in the same transaction, before it commits
// and becomes visible through a separate connection.
func (c *CQRSImpl[TData, TResponse, TRequest, TID]) CountWithTx(
	ctx context.Context, tx *bun.Tx, filter domains.StructuredFilter,
) (int64, error) {
	return c.paginationService.CountWithTx(ctx, tx, filter)
}
