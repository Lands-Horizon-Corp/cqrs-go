package cqrs

import (
	"context"
	"fmt"

	"github.com/uptrace/bun"
)

func (c *CQRSImpl[TData, TResponse, TRequest, TID]) Create(
	ctx context.Context,
	data TData,
	preload ...string,
) (*TResponse, error) {
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

	if c.ToResource != nil {
		return c.ToResource(&data), nil
	}
	return nil, nil
}

func (c *CQRSImpl[TData, TResponse, TRequest, TID]) CreateMany(
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

func (c *CQRSImpl[TData, TResponse, TRequest, TID]) CreateWithTx(
	ctx context.Context,
	tx bun.Tx,
	data TData,
	preload ...string,
) (*TResponse, error) {
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
	if c.ToResource != nil {
		return c.ToResource(&data), nil
	}
	return nil, nil
}

func (c *CQRSImpl[TData, TResponse, TRequest, TID]) CreateManyWithTx(
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
