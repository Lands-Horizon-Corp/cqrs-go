package cqrs

import (
	"context"

	"github.com/uptrace/bun"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/domains"
)

// GetByID returns the row in TData's table whose ColumnDefaultID column
// equals id, or sql.ErrNoRows if none match. It's FindOne scoped to a
// single "ColumnDefaultID = id" filter term, under the naming convention
// UpdateByID/DeleteByID already use in this package — use FindOne directly
// instead when the lookup is by some other filter, not by primary key.
func (c *CQRSImpl[TData, TResponse, TRequest, TID]) GetByID(
	ctx context.Context, id TID, preloads ...string,
) (*TData, error) {
	return c.FindOne(ctx, c.idFilter(id), preloads...)
}

// GetByIDFormat is GetByID with the matched row converted through
// ToResource, for callers that want the TResponse-shaped view instead of
// TData itself.
func (c *CQRSImpl[TData, TResponse, TRequest, TID]) GetByIDFormat(
	ctx context.Context, id TID, preloads ...string,
) (*TResponse, error) {
	return c.FindOneFormat(ctx, c.idFilter(id), preloads...)
}

// GetByIDWithTx is GetByID run against a caller-supplied *bun.Tx instead of
// a plain client. That tx is expected to come from WriteSQLService (the
// writer), not ReadSQLService — a transaction only shows its own
// uncommitted work to callers sharing that same connection, and
// ReadSQLService may point at a replica that doesn't even share it — e.g.
// finding a row written earlier in the same transaction, before it commits
// and becomes visible through a separate connection.
func (c *CQRSImpl[TData, TResponse, TRequest, TID]) GetByIDWithTx(
	ctx context.Context, tx *bun.Tx, id TID, preloads ...string,
) (*TData, error) {
	return c.FindOneWithTx(ctx, tx, c.idFilter(id), preloads...)
}

// GetByIDWithTxFormat is GetByIDWithTx with the matched row converted
// through ToResource, for callers that want the TResponse-shaped view
// instead of TData itself.
func (c *CQRSImpl[TData, TResponse, TRequest, TID]) GetByIDWithTxFormat(
	ctx context.Context, tx *bun.Tx, id TID, preloads ...string,
) (*TResponse, error) {
	return c.FindOneWithTxFormat(ctx, tx, c.idFilter(id), preloads...)
}

// idFilter builds the one-term StructuredFilter "ColumnDefaultID = id" that
// GetByID and its WithTx/Format variants run through FindOne.
func (c *CQRSImpl[TData, TResponse, TRequest, TID]) idFilter(id TID) domains.StructuredFilter {
	return domains.StructuredFilter{
		Filters: []domains.Filter{{Field: c.ColumnDefaultID, Mode: domains.ModeEqual, Value: id}},
	}
}
