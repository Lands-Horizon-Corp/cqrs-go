package repository

import (
	"context"
	"fmt"

	"github.com/uptrace/bun"
)

func (r *RepositoryImpl[TData, TResponse, TRequest, TID]) Create(
	ctx context.Context,
	data TData,
) (*TResponse, error) {
	if r.Validator != nil {
		if err := r.Validator.StructCtx(ctx, &data); err != nil {
			return nil, fmt.Errorf("validating request payload: %w", err)
		}
	}

	_, err := r.WriteDB.NewInsert().
		Model(&data).
		Returning("*").
		Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("inserting record: %w", err)
	}

	if r.ToResource != nil {
		return r.ToResource(&data), nil
	}
	return nil, nil
}

func (r *RepositoryImpl[TData, TResponse, TRequest, TID]) CreateMany(
	ctx context.Context,
	data []TData,
) ([]*TResponse, error) {
	if len(data) == 0 {
		return []*TResponse{}, nil
	}

	if r.Validator != nil {
		for i := range data {
			if err := r.Validator.StructCtx(ctx, &data[i]); err != nil {
				return nil, fmt.Errorf("validating request payload at index %d: %w", i, err)
			}
		}
	}

	_, err := r.WriteDB.NewInsert().
		Model(&data).
		Returning("*").
		Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("bulk inserting records: %w", err)
	}

	if r.ToResource == nil {
		return nil, nil
	}

	responses := make([]*TResponse, 0, len(data))
	for i := range data {
		if res := r.ToResource(&data[i]); res != nil {
			responses = append(responses, res)
		}
	}
	return responses, nil
}

func (r *RepositoryImpl[TData, TResponse, TRequest, TID]) CreateWithTx(
	ctx context.Context,
	tx bun.Tx,
	data TData,
) (*TResponse, error) {
	if r.Validator != nil {
		if err := r.Validator.StructCtx(ctx, &data); err != nil {
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

	if r.ToResource != nil {
		return r.ToResource(&data), nil
	}
	return nil, nil
}

func (r *RepositoryImpl[TData, TResponse, TRequest, TID]) CreateManyWithTx(
	ctx context.Context,
	tx bun.Tx,
	data []TData,
) ([]*TResponse, error) {
	if len(data) == 0 {
		return []*TResponse{}, nil
	}
	if r.Validator != nil {
		for i := range data {
			if err := r.Validator.StructCtx(ctx, &data[i]); err != nil {
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
	if r.ToResource == nil {
		return nil, nil
	}
	responses := make([]*TResponse, 0, len(data))
	for i := range data {
		if res := r.ToResource(&data[i]); res != nil {
			responses = append(responses, res)
		}
	}
	return responses, nil
}
