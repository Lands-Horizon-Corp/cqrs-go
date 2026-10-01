package cqrs

import (
	"context"

	"github.com/uptrace/bun"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/domains"
)

// Find returns the first row in TData's table matching filter, or
// sql.ErrNoRows if none match — the single-row counterpart to Filter, which
// returns every match.
func (c *CQRSImpl[TData, TResponse, TRequest, TID]) Find(
	ctx context.Context, filter domains.StructuredFilter, preloads ...string,
) (*TData, error) {
	return c.paginationService.Find(ctx, filter, preloads...)
}

// FindFormat is Find with the matched row converted through ToResource,
// for callers that want the TResponse-shaped view instead of TData itself.
func (c *CQRSImpl[TData, TResponse, TRequest, TID]) FindFormat(
	ctx context.Context, filter domains.StructuredFilter, preloads ...string,
) (*TResponse, error) {
	result, err := c.Find(ctx, filter, preloads...)
	if err != nil {
		return nil, err
	}
	if c.ToResource == nil {
		return nil, nil
	}
	return c.ToResource(result), nil
}

// FindWithTx is Find run against a caller-supplied *bun.Tx instead of
// ReadSQLService's own client — e.g. finding a row written earlier in the
// same transaction, before it commits and becomes visible through a
// separate connection (see FilterWithTx's doc comment in
// cqrs.pagination.go for why this shape exists alongside the plain
// version).
func (c *CQRSImpl[TData, TResponse, TRequest, TID]) FindWithTx(
	ctx context.Context, tx *bun.Tx, filter domains.StructuredFilter, preloads ...string,
) (*TData, error) {
	return c.paginationService.FindWithTx(ctx, tx, filter, preloads...)
}

// FindWithTxFormat is FindWithTx with the matched row converted through
// ToResource, for callers that want the TResponse-shaped view instead of
// TData itself.
func (c *CQRSImpl[TData, TResponse, TRequest, TID]) FindWithTxFormat(
	ctx context.Context, tx *bun.Tx, filter domains.StructuredFilter, preloads ...string,
) (*TResponse, error) {
	result, err := c.FindWithTx(ctx, tx, filter, preloads...)
	if err != nil {
		return nil, err
	}
	if c.ToResource == nil {
		return nil, nil
	}
	return c.ToResource(result), nil
}
