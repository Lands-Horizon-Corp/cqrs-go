package cqrs

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/uptrace/bun"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/utils"
)

// IncrementByID atomically adds delta to field on the row whose
// ColumnDefaultID column equals id, returning the row as it stands after
// the increment, or sql.ErrNoRows if no such row exists.
//
// This exists specifically to avoid the classic "lost update" race a
// ledger (or anything else tracking a running balance/counter) hits if it
// instead does GetByID -> add delta in Go -> UpdateByID with the new
// total: two concurrent callers doing that read the same starting value,
// and whichever one's UpdateByID commits last silently discards the
// other's change — both callers believe their delta was applied, but only
// one actually was. Here the arithmetic happens in a single
// "SET field = field + ?" statement evaluated by the database itself,
// which Postgres can only ever apply serially against a given row (its own
// row-level locking during the UPDATE itself enforces that), so there is
// no window where two concurrent increments can observe the same
// pre-update value — unlike GetByID+UpdateByID's fully exposed
// read/modify/write gap, which even a transaction alone would not close
// without additional locking.
func (c *CQRSImpl[TData, TResponse, TRequest, TID]) IncrementByID(
	ctx context.Context, id TID, field string, delta float64,
) (*TData, error) {
	return incrementByID[TData](ctx, c.WriteSQLService.Client(), c.ColumnDefaultID, id, field, delta)
}

// IncrementByIDWithTx is IncrementByID run against a caller-supplied
// *bun.Tx instead of a plain client. That tx is expected to come from
// WriteSQLService (the writer), not ReadSQLService — a transaction only
// shows its own uncommitted work to callers sharing that same connection,
// and ReadSQLService may point at a replica that doesn't even share it.
func (c *CQRSImpl[TData, TResponse, TRequest, TID]) IncrementByIDWithTx(
	ctx context.Context, tx bun.Tx, id TID, field string, delta float64,
) (*TData, error) {
	return incrementByID[TData](ctx, tx, c.ColumnDefaultID, id, field, delta)
}

func incrementByID[TData any](
	ctx context.Context, db bun.IDB, columnDefaultID string, id any, field string, delta float64,
) (*TData, error) {
	if utils.BunColumnFieldIndex[TData](field) == -1 {
		return nil, fmt.Errorf("increment: unknown field %q", field)
	}
	var data TData
	_, err := db.NewUpdate().
		Model((*TData)(nil)).
		Set("? = ? + ?", bun.Ident(field), bun.Ident(field), delta).
		Where("? = ?", bun.Ident(columnDefaultID), id).
		Returning("*").
		Exec(ctx, &data)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, sql.ErrNoRows
		}
		return nil, fmt.Errorf("incrementing %s: %w", field, err)
	}
	return &data, nil
}
