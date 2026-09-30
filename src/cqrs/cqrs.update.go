package cqrs

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/uptrace/bun"
)

// UpdateByID updates the row matching id and returns the persisted row
// itself (TData) rather than running it through ToResource. Use
// UpdateByIDFormat instead when the caller wants the TResponse-shaped view.
func (c *CQRSImpl[TData, TResponse, TRequest, TID]) UpdateByID(
	ctx context.Context,
	id TID,
	data TData,
	preload ...string,
) (*TData, error) {
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
	if err := c.applyPreloads(ctx, c.WriteSQLService.Client(), &data, preload...); err != nil {
		return nil, err
	}
	return &data, nil
}

// UpdateByIDFormat is UpdateByID with the persisted row converted through
// ToResource, for callers that want the TResponse-shaped view instead of
// TData itself.
func (c *CQRSImpl[TData, TResponse, TRequest, TID]) UpdateByIDFormat(
	ctx context.Context,
	id TID,
	data TData,
	preload ...string,
) (*TResponse, error) {
	result, err := c.UpdateByID(ctx, id, data, preload...)
	if err != nil {
		return nil, err
	}
	if c.ToResource == nil {
		return nil, nil
	}
	return c.ToResource(result), nil
}

// UpdateMany bulk-updates data and returns the persisted rows themselves
// (TData) rather than running them through ToResource. Use UpdateManyFormat
// instead when the caller wants the TResponse-shaped view.
func (c *CQRSImpl[TData, TResponse, TRequest, TID]) UpdateMany(
	ctx context.Context,
	data []TData,
	preload ...string,
) ([]*TData, error) {
	if len(data) == 0 {
		return []*TData{}, nil
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
	if err := c.applyPreloadsMany(ctx, c.WriteSQLService.Client(), &data, preload...); err != nil {
		return nil, fmt.Errorf("loading preloads: %w", err)
	}

	out := make([]*TData, len(data))
	for i := range data {
		out[i] = &data[i]
	}
	return out, nil
}

// UpdateManyFormat is UpdateMany with each persisted row converted through
// ToResource, for callers that want the TResponse-shaped view instead of
// TData itself.
func (c *CQRSImpl[TData, TResponse, TRequest, TID]) UpdateManyFormat(
	ctx context.Context,
	data []TData,
	preload ...string,
) ([]*TResponse, error) {
	result, err := c.UpdateMany(ctx, data, preload...)
	if err != nil {
		return nil, err
	}
	if c.ToResource == nil {
		return nil, nil
	}
	responses := make([]*TResponse, 0, len(result))
	for _, d := range result {
		if res := c.ToResource(d); res != nil {
			responses = append(responses, res)
		}
	}
	return responses, nil
}

// UpdateManyWithTx is UpdateMany run against a caller-supplied transaction,
// returning the persisted rows themselves (TData) rather than running them
// through ToResource. Use UpdateManyWithTxFormat instead when the caller
// wants the TResponse-shaped view.
func (c *CQRSImpl[TData, TResponse, TRequest, TID]) UpdateManyWithTx(
	ctx context.Context,
	tx bun.Tx,
	data []TData,
	preload ...string,
) ([]*TData, error) {
	if len(data) == 0 {
		return []*TData{}, nil
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
	if err := c.applyPreloadsMany(ctx, tx, &data, preload...); err != nil {
		return nil, fmt.Errorf("loading preloads: %w", err)
	}

	out := make([]*TData, len(data))
	for i := range data {
		out[i] = &data[i]
	}
	return out, nil
}

// UpdateManyWithTxFormat is UpdateManyWithTx with each persisted row
// converted through ToResource, for callers that want the TResponse-shaped
// view instead of TData itself.
func (c *CQRSImpl[TData, TResponse, TRequest, TID]) UpdateManyWithTxFormat(
	ctx context.Context,
	tx bun.Tx,
	data []TData,
	preload ...string,
) ([]*TResponse, error) {
	result, err := c.UpdateManyWithTx(ctx, tx, data, preload...)
	if err != nil {
		return nil, err
	}
	if c.ToResource == nil {
		return nil, nil
	}
	responses := make([]*TResponse, 0, len(result))
	for _, d := range result {
		if res := c.ToResource(d); res != nil {
			responses = append(responses, res)
		}
	}
	return responses, nil
}

// UpdateByIDWithTx is UpdateByID run against a caller-supplied transaction,
// returning the persisted row itself (TData) rather than running it through
// ToResource. Use UpdateByIDWithTxFormat instead when the caller wants the
// TResponse-shaped view.
func (c *CQRSImpl[TData, TResponse, TRequest, TID]) UpdateByIDWithTx(
	ctx context.Context,
	tx bun.Tx,
	id TID,
	data TData,
	preload ...string,
) (*TData, error) {
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
	if err := c.applyPreloads(ctx, tx, &data, preload...); err != nil {
		return nil, err
	}
	return &data, nil
}

// UpdateByIDWithTxFormat is UpdateByIDWithTx with the persisted row
// converted through ToResource, for callers that want the TResponse-shaped
// view instead of TData itself.
func (c *CQRSImpl[TData, TResponse, TRequest, TID]) UpdateByIDWithTxFormat(
	ctx context.Context,
	tx bun.Tx,
	id TID,
	data TData,
	preload ...string,
) (*TResponse, error) {
	result, err := c.UpdateByIDWithTx(ctx, tx, id, data, preload...)
	if err != nil {
		return nil, err
	}
	if c.ToResource == nil {
		return nil, nil
	}
	return c.ToResource(result), nil
}
