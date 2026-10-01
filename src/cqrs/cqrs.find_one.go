package cqrs

import (
	"context"

	"github.com/uptrace/bun"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/domains"
)

// FindOne returns the first row in TData's table matching filter, or
// sql.ErrNoRows if none match — the single-row counterpart to Filter, which
// returns every match.
func (c *CQRSImpl[TData, TResponse, TRequest, TID]) FindOne(
	ctx context.Context, filter domains.StructuredFilter, preloads ...string,
) (*TData, error) {
	return c.PaginationService.FindOne(ctx, filter, preloads...)
}

// FindOneFormat is FindOne with the matched row converted through
// ToResource, for callers that want the TResponse-shaped view instead of
// TData itself.
func (c *CQRSImpl[TData, TResponse, TRequest, TID]) FindOneFormat(
	ctx context.Context, filter domains.StructuredFilter, preloads ...string,
) (*TResponse, error) {
	result, err := c.FindOne(ctx, filter, preloads...)
	if err != nil {
		return nil, err
	}
	if c.ToResource == nil {
		return nil, nil
	}
	return c.ToResource(result), nil
}

// FindOneWithTx is FindOne run against a caller-supplied *bun.Tx instead of
// a plain client. That tx is expected to come from WriteSQLService (the
// writer), not ReadSQLService — a transaction only shows its own
// uncommitted work to callers sharing that same connection, and
// ReadSQLService may point at a replica that doesn't even share it — e.g.
// finding a row written earlier in the same transaction, before it commits
// and becomes visible through a separate connection.
func (c *CQRSImpl[TData, TResponse, TRequest, TID]) FindOneWithTx(
	ctx context.Context, tx *bun.Tx, filter domains.StructuredFilter, preloads ...string,
) (*TData, error) {
	return c.PaginationService.FindOneWithTx(ctx, tx, filter, preloads...)
}

// FindOneWithTxFormat is FindOneWithTx with the matched row converted
// through ToResource, for callers that want the TResponse-shaped view
// instead of TData itself.
func (c *CQRSImpl[TData, TResponse, TRequest, TID]) FindOneWithTxFormat(
	ctx context.Context, tx *bun.Tx, filter domains.StructuredFilter, preloads ...string,
) (*TResponse, error) {
	result, err := c.FindOneWithTx(ctx, tx, filter, preloads...)
	if err != nil {
		return nil, err
	}
	if c.ToResource == nil {
		return nil, nil
	}
	return c.ToResource(result), nil
}
