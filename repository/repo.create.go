package repository

import (
	"context"
	"errors"
	"fmt"
)

// Create validates request payload, maps to TData, inserts row, and returns TResponse.
func (r *RepositoryImpl[TData, TResponse, TRequest, TID]) Create(
	ctx context.Context,
	request TRequest,
) (*TResponse, error) {
	if r.Validator != nil {
		if err := r.Validator.StructCtx(ctx, request); err != nil {
			return nil, fmt.Errorf("validating request payload: %w", err)
		}
	}
	if r.ToData == nil {
		return nil, errors.New("todata mapper is not configured")
	}
	entity := r.ToData(request)
	if entity == nil {
		return nil, errors.New("todata mapper returned nil entity")
	}
	_, err := r.WriteDB.NewInsert().
		Model(entity).
		Returning("*").
		Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("inserting record: %w", err)
	}
	r.OnCreated(ctx, entity)
	if r.ToResource != nil {
		return r.ToResource(entity), nil
	}
	return nil, nil
}

func (r *RepositoryImpl[TData, TResponse, TRequest, TID]) CreateMany(
	ctx context.Context,
	requests []TRequest,
) ([]*TResponse, error) {
	if len(requests) == 0 {
		return []*TResponse{}, nil
	}
	if r.Validator != nil {
		for i, req := range requests {
			if err := r.Validator.StructCtx(ctx, req); err != nil {
				return nil, fmt.Errorf("validating request payload at index %d: %w", i, err)
			}
		}
	}
	if r.ToData == nil {
		return nil, errors.New("todata mapper is not configured")
	}

	entities := make([]*TData, 0, len(requests))
	for _, req := range requests {
		data := r.ToData(req)
		if data != nil {
			entities = append(entities, data)
		}
	}
	_, err := r.WriteDB.NewInsert().
		Model(&entities).
		Returning("*").
		Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("bulk inserting records: %w", err)
	}
	responses := make([]*TResponse, 0, len(entities))
	for _, entity := range entities {
		r.OnCreated(ctx, entity)
		if r.ToResource != nil {
			if res := r.ToResource(entity); res != nil {
				responses = append(responses, res)
			}
		}
	}
	return responses, nil
}
