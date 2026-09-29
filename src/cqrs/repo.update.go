package cqrs

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/uptrace/bun"
)

func (c *CQRSImpl[TData, TResponse, TRequest, TID]) UpdateByID(
	ctx context.Context,
	id TID,
	data TData,
) (*TResponse, error) {
	if c.Validator != nil {
		if err := c.Validator.StructCtx(ctx, &data); err != nil {
			return nil, fmt.Errorf("validating request payload: %w", err)
		}
	}
	res, err := c.WriteSQLService.Client().NewUpdate().
		Model(&data).
		Where("? = ?", bun.Ident(c.ColumnDefaultID), id).
		Returning("*").
		Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("updating record: %w", err)
	}
	rows, err := res.RowsAffected()
	if err == nil && rows == 0 {
		return nil, sql.ErrNoRows
	}
	if c.ToResource != nil {
		return c.ToResource(&data), nil
	}
	return nil, nil
}

func (c *CQRSImpl[TData, TResponse, TRequest, TID]) UpdateByIDWithTx(
	ctx context.Context,
	tx bun.Tx,
	id TID,
	data TData,
) (*TResponse, error) {
	if c.Validator != nil {
		if err := c.Validator.StructCtx(ctx, &data); err != nil {
			return nil, fmt.Errorf("validating request payload: %w", err)
		}
	}
	res, err := tx.NewUpdate().
		Model(&data).
		Where("? = ?", bun.Ident(c.ColumnDefaultID), id).
		Returning("*").
		Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("updating record in tx: %w", err)
	}
	rows, err := res.RowsAffected()
	if err == nil && rows == 0 {
		return nil, sql.ErrNoRows
	}
	if c.ToResource != nil {
		return c.ToResource(&data), nil
	}
	return nil, nil
}
