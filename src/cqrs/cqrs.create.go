package cqrs

import (
	"context"
	"fmt"

	"github.com/uptrace/bun"
)

// Create inserts data and returns the persisted row itself (TData, with
// whatever the database filled in via RETURNING * — defaults, generated
// IDs, timestamps) rather than running it through ToResource. Use
// CreateFormat instead when the caller wants the TResponse-shaped view.
func (c *CQRSImpl[TData, TResponse, TRequest, TID]) Create(
	ctx context.Context,
	data TData,
	preload ...string,
) (*TData, error) {
	if c.Validator != nil {
		if err := c.Validator.StructCtx(ctx, &data); err != nil {
			return nil, fmt.Errorf("validating request payload: %w", err)
		}
	}
	_, err := c.WriteSQLService.Client().NewInsert().
		Model(&data).
		Returning("*").
		Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("inserting record: %w", err)
	}
	if err := c.applyPreloads(ctx, c.WriteSQLService.Client(), &data, preload...); err != nil {
		return nil, err
	}
	return &data, nil
}

// CreateFormat is Create with the persisted row converted through
// ToResource, for callers that want the TResponse-shaped view instead of
// TData itself.
func (c *CQRSImpl[TData, TResponse, TRequest, TID]) CreateFormat(
	ctx context.Context,
	data TData,
	preload ...string,
) (*TResponse, error) {
	result, err := c.Create(ctx, data, preload...)
	if err != nil {
		return nil, err
	}
	if c.ToResource == nil {
		return nil, nil
	}
	return c.ToResource(result), nil
}

// CreateMany bulk-inserts data and returns the persisted rows themselves
// (TData) rather than running them through ToResource. Use CreateManyFormat
// instead when the caller wants the TResponse-shaped view.
func (c *CQRSImpl[TData, TResponse, TRequest, TID]) CreateMany(
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
	_, err := c.WriteSQLService.Client().NewInsert().
		Model(&data).
		Returning("*").
		Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("bulk inserting records: %w", err)
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

// CreateManyFormat is CreateMany with each persisted row converted through
// ToResource, for callers that want the TResponse-shaped view instead of
// TData itself.
func (c *CQRSImpl[TData, TResponse, TRequest, TID]) CreateManyFormat(
	ctx context.Context,
	data []TData,
	preload ...string,
) ([]*TResponse, error) {
	result, err := c.CreateMany(ctx, data, preload...)
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

// CreateWithTx is Create run against a caller-supplied transaction,
// returning the persisted row itself (TData) rather than running it through
// ToResource. Use CreateWithTxFormat instead when the caller wants the
// TResponse-shaped view.
func (c *CQRSImpl[TData, TResponse, TRequest, TID]) CreateWithTx(
	ctx context.Context,
	tx bun.Tx,
	data TData,
	preload ...string,
) (*TData, error) {
	if c.Validator != nil {
		if err := c.Validator.StructCtx(ctx, &data); err != nil {
			return nil, fmt.Errorf("validating request payload: %w", err)
		}
	}
	_, err := tx.NewInsert().
		Model(&data).
		Returning("*").
		Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("inserting record: %w", err)
	}
	if err := c.applyPreloads(ctx, tx, &data, preload...); err != nil {
		return nil, err
	}
	return &data, nil
}

// CreateWithTxFormat is CreateWithTx with the persisted row converted
// through ToResource, for callers that want the TResponse-shaped view
// instead of TData itself.
func (c *CQRSImpl[TData, TResponse, TRequest, TID]) CreateWithTxFormat(
	ctx context.Context,
	tx bun.Tx,
	data TData,
	preload ...string,
) (*TResponse, error) {
	result, err := c.CreateWithTx(ctx, tx, data, preload...)
	if err != nil {
		return nil, err
	}
	if c.ToResource == nil {
		return nil, nil
	}
	return c.ToResource(result), nil
}

// CreateManyWithTx is CreateMany run against a caller-supplied transaction,
// returning the persisted rows themselves (TData) rather than running them
// through ToResource. Use CreateManyWithTxFormat instead when the caller
// wants the TResponse-shaped view.
func (c *CQRSImpl[TData, TResponse, TRequest, TID]) CreateManyWithTx(
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
	_, err := tx.NewInsert().
		Model(&data).
		Returning("*").
		Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("bulk inserting records in tx: %w", err)
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

// CreateManyWithTxFormat is CreateManyWithTx with each persisted row
// converted through ToResource, for callers that want the TResponse-shaped
// view instead of TData itself.
func (c *CQRSImpl[TData, TResponse, TRequest, TID]) CreateManyWithTxFormat(
	ctx context.Context,
	tx bun.Tx,
	data []TData,
	preload ...string,
) ([]*TResponse, error) {
	result, err := c.CreateManyWithTx(ctx, tx, data, preload...)
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
