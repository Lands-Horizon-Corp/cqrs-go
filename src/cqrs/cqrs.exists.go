package cqrs

import (
	"context"

	"github.com/uptrace/bun"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/domains"
)

// Exists reports whether any row in TData's table matches filter, without
// scanning a row back. A zero-value domains.StructuredFilter{} asks "does
// this table have any rows at all".
func (c *CQRSImpl[TData, TResponse, TRequest, TID]) Exists(
	ctx context.Context, filter domains.StructuredFilter,
) (bool, error) {
	return c.PaginationService.Exists(ctx, filter)
}

// ExistsWithTx is Exists run against a caller-supplied *bun.Tx instead of a
// plain client. That tx is expected to come from WriteSQLService (the
// writer), not ReadSQLService — a transaction only shows its own
// uncommitted work to callers sharing that same connection, and
// ReadSQLService may point at a replica that doesn't even share it — e.g.
// checking for a row written earlier in the same transaction, before it
// commits and becomes visible through a separate connection.
func (c *CQRSImpl[TData, TResponse, TRequest, TID]) ExistsWithTx(
	ctx context.Context, tx *bun.Tx, filter domains.StructuredFilter,
) (bool, error) {
	return c.PaginationService.ExistsWithTx(ctx, tx, filter)
}
