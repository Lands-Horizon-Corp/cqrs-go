package cqrs

import (
	"context"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/domains"
	"github.com/uptrace/bun"
)

// Max returns the row in TData's table matching filter whose field holds
// the highest value, or sql.ErrNoRows if none match.
func (c *CQRSImpl[TData, TResponse, TRequest, TID]) Max(
	ctx context.Context, field string, filter domains.StructuredFilter, preloads ...string,
) (*TData, error) {
	return c.PaginationService.GetMax(ctx, field, filter, preloads...)
}

// MaxFormat is Max with the matched row converted through ToResource, for
// callers that want the TResponse-shaped view instead of TData itself.
func (c *CQRSImpl[TData, TResponse, TRequest, TID]) MaxFormat(
	ctx context.Context, field string, filter domains.StructuredFilter, preloads ...string,
) (*TResponse, error) {
	result, err := c.Max(ctx, field, filter, preloads...)
	if err != nil {
		return nil, err
	}
	if c.ToResource == nil {
		return nil, nil
	}
	return c.ToResource(result), nil
}

// Min is Max's opposite: the row whose field holds the lowest value.
func (c *CQRSImpl[TData, TResponse, TRequest, TID]) Min(
	ctx context.Context, field string, filter domains.StructuredFilter, preloads ...string,
) (*TData, error) {
	return c.PaginationService.GetMin(ctx, field, filter, preloads...)
}

// MinFormat is Min with the matched row converted through ToResource, for
// callers that want the TResponse-shaped view instead of TData itself.
func (c *CQRSImpl[TData, TResponse, TRequest, TID]) MinFormat(
	ctx context.Context, field string, filter domains.StructuredFilter, preloads ...string,
) (*TResponse, error) {
	result, err := c.Min(ctx, field, filter, preloads...)
	if err != nil {
		return nil, err
	}
	if c.ToResource == nil {
		return nil, nil
	}
	return c.ToResource(result), nil
}

// MaxWithTx is Max run against a caller-supplied *bun.Tx instead of a plain
// client. That tx is expected to come from WriteSQLService (the writer),
// not ReadSQLService — a transaction only shows its own uncommitted work
// to callers sharing that same connection, and ReadSQLService may point at
// a replica that doesn't even share it — e.g. finding the row with the
// highest field value written earlier in the same transaction, before it
// commits and becomes visible through a separate connection.
func (c *CQRSImpl[TData, TResponse, TRequest, TID]) MaxWithTx(
	ctx context.Context, tx *bun.Tx, field string, filter domains.StructuredFilter, preloads ...string,
) (*TData, error) {
	return c.PaginationService.GetMaxWithTx(ctx, tx, field, filter, preloads...)
}

// MaxWithTxFormat is MaxWithTx with the matched row converted through
// ToResource, for callers that want the TResponse-shaped view instead of
// TData itself.
func (c *CQRSImpl[TData, TResponse, TRequest, TID]) MaxWithTxFormat(
	ctx context.Context, tx *bun.Tx, field string, filter domains.StructuredFilter, preloads ...string,
) (*TResponse, error) {
	result, err := c.MaxWithTx(ctx, tx, field, filter, preloads...)
	if err != nil {
		return nil, err
	}
	if c.ToResource == nil {
		return nil, nil
	}
	return c.ToResource(result), nil
}

// MinWithTx is Min's *bun.Tx counterpart (see MaxWithTx's doc comment).
func (c *CQRSImpl[TData, TResponse, TRequest, TID]) MinWithTx(
	ctx context.Context, tx *bun.Tx, field string, filter domains.StructuredFilter, preloads ...string,
) (*TData, error) {
	return c.PaginationService.GetMinWithTx(ctx, tx, field, filter, preloads...)
}

// MinWithTxFormat is MinWithTx with the matched row converted through
// ToResource, for callers that want the TResponse-shaped view instead of
// TData itself.
func (c *CQRSImpl[TData, TResponse, TRequest, TID]) MinWithTxFormat(
	ctx context.Context, tx *bun.Tx, field string, filter domains.StructuredFilter, preloads ...string,
) (*TResponse, error) {
	result, err := c.MinWithTx(ctx, tx, field, filter, preloads...)
	if err != nil {
		return nil, err
	}
	if c.ToResource == nil {
		return nil, nil
	}
	return c.ToResource(result), nil
}
