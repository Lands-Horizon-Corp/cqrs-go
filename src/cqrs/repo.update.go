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
	preload ...string,
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
	if err := c.applyPreloads(ctx, c.WriteSQLService.Client(), &data, c.Preload(preload...)); err != nil {
		return nil, err
	}
	if c.ToResource != nil {
		return c.ToResource(&data), nil
	}
	return nil, nil
}

// UpdateMany bulk-updates every row in data in a single statement, matched
// by primary key (bun's Bulk() update: an UPDATE ... FROM VALUES(...)
// joined back to the table by PK). Unlike UpdateByID, it does not report
// which IDs (if any) didn't exist — consistent with DeleteMany, which has
// the same silent-no-op-for-missing-IDs behavior for the same reason: a
// bulk statement doesn't get an individual not-found signal per row.
func (c *CQRSImpl[TData, TResponse, TRequest, TID]) UpdateMany(
	ctx context.Context,
	data []TData,
	preload ...string,
) ([]*TResponse, error) {
	if len(data) == 0 {
		return []*TResponse{}, nil
	}
	if c.Validator != nil {
		for i := range data {
			if err := c.Validator.StructCtx(ctx, &data[i]); err != nil {
				return nil, fmt.Errorf("validating request payload at index %d: %w", i, err)
			}
		}
	}

	_, err := c.WriteSQLService.Client().NewUpdate().
		Model(&data).
		Bulk().
		Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("bulk updating records: %w", err)
	}
	resolvedPreload := c.Preload(preload...)
	for i := range data {
		if err := c.applyPreloads(ctx, c.WriteSQLService.Client(), &data[i], resolvedPreload); err != nil {
			return nil, fmt.Errorf("loading preloads for record at index %d: %w", i, err)
		}
	}

	if c.ToResource == nil {
		return nil, nil
	}
	responses := make([]*TResponse, 0, len(data))
	for i := range data {
		if res := c.ToResource(&data[i]); res != nil {
			responses = append(responses, res)
		}
	}
	return responses, nil
}

// UpdateManyWithTx is UpdateMany run against a caller-supplied transaction.
func (c *CQRSImpl[TData, TResponse, TRequest, TID]) UpdateManyWithTx(
	ctx context.Context,
	tx bun.Tx,
	data []TData,
	preload ...string,
) ([]*TResponse, error) {
	if len(data) == 0 {
		return []*TResponse{}, nil
	}
	if c.Validator != nil {
		for i := range data {
			if err := c.Validator.StructCtx(ctx, &data[i]); err != nil {
				return nil, fmt.Errorf("validating request payload at index %d: %w", i, err)
			}
		}
	}

	_, err := tx.NewUpdate().
		Model(&data).
		Bulk().
		Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("bulk updating records in tx: %w", err)
	}
	resolvedPreload := c.Preload(preload...)
	for i := range data {
		if err := c.applyPreloads(ctx, tx, &data[i], resolvedPreload); err != nil {
			return nil, fmt.Errorf("loading preloads for record at index %d: %w", i, err)
		}
	}

	if c.ToResource == nil {
		return nil, nil
	}
	responses := make([]*TResponse, 0, len(data))
	for i := range data {
		if res := c.ToResource(&data[i]); res != nil {
			responses = append(responses, res)
		}
	}
	return responses, nil
}

func (c *CQRSImpl[TData, TResponse, TRequest, TID]) UpdateByIDWithTx(
	ctx context.Context,
	tx bun.Tx,
	id TID,
	data TData,
	preload ...string,
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
	if err := c.applyPreloads(ctx, tx, &data, c.Preload(preload...)); err != nil {
		return nil, err
	}
	if c.ToResource != nil {
		return c.ToResource(&data), nil
	}
	return nil, nil
}
