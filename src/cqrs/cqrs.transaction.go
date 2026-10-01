package cqrs

import (
	"context"
	"fmt"

	"github.com/uptrace/bun"
)

// Start begins a new transaction on WriteSQLService — the writer, not
// ReadSQLService (see FilterWithTx's doc comment in cqrs.pagination.go for
// why: only the writer's connection is guaranteed to see its own
// uncommitted work). The returned bun.Tx is what every ...WithTx method
// (CreateWithTx, UpdateByIDWithTx, FindWithTx, GetByIDWithTx, ...) expects,
// and what End below expects back to finish it — it's returned to the
// caller rather than stored on c itself, since one CQRSImpl instance is
// typically shared across concurrent requests and a transaction is
// inherently single-request state.
func (c *CQRSImpl[TData, TResponse, TRequest, TID]) Start(ctx context.Context) (bun.Tx, error) {
	tx, err := c.WriteSQLService.Client().BeginTx(ctx, nil)
	if err != nil {
		return bun.Tx{}, fmt.Errorf("starting transaction: %w", err)
	}
	return tx, nil
}

// End finishes a transaction started with Start: commits tx if err is nil,
// or rolls it back and returns err unchanged otherwise. The usual call
// shape is
//
//	tx, err := c.Start(ctx)
//	if err != nil {
//		return err
//	}
//	defer func() { err = c.End(ctx, tx, err) }()
//	// ... call ...WithTx methods with tx, assigning to the same named err ...
//
// so the deferred End always sees whichever error (if any) the body
// produced, and a rollback failure is only surfaced when there wasn't
// already a more specific error to report — err is the reason this
// transaction is ending, and that's what the caller actually needs back,
// not a rollback's own (usually uninteresting) failure.
func (c *CQRSImpl[TData, TResponse, TRequest, TID]) End(ctx context.Context, tx bun.Tx, err error) error {
	if err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			return fmt.Errorf("rolling back transaction after error (%w): %w", err, rbErr)
		}
		return err
	}
	if commitErr := tx.Commit(); commitErr != nil {
		return fmt.Errorf("committing transaction: %w", commitErr)
	}
	return nil
}
