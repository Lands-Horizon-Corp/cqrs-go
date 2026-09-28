package repository

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/uptrace/bun"
)

func (r *RepositoryImpl[TData, TResponse, TRequest, TID]) UpdateByID(
	ctx context.Context,
	id TID,
	data TData,
) (*TResponse, error) {
	if r.Validator != nil {
		if err := r.Validator.StructCtx(ctx, &data); err != nil {
			return nil, fmt.Errorf("validating request payload: %w", err)
		}
	}
	res, err := r.WriteDB.NewUpdate().
		Model(&data).
		Where("? = ?", bun.Ident(r.ColumnDefaultID), id).
		Returning("*").
		Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("updating record: %w", err)
	}
	rows, err := res.RowsAffected()
	if err == nil && rows == 0 {
		return nil, sql.ErrNoRows
	}
	if r.ToResource != nil {
		return r.ToResource(&data), nil
	}
	return nil, nil
}

func (r *RepositoryImpl[TData, TResponse, TRequest, TID]) UpdateByIDWithTx(
	ctx context.Context,
	tx bun.Tx,
	id TID,
	data TData,
) (*TResponse, error) {
	if r.Validator != nil {
		if err := r.Validator.StructCtx(ctx, &data); err != nil {
			return nil, fmt.Errorf("validating request payload: %w", err)
		}
	}
	res, err := tx.NewUpdate().
		Model(&data).
		Where("? = ?", bun.Ident(r.ColumnDefaultID), id).
		Returning("*").
		Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("updating record in tx: %w", err)
	}
	rows, err := res.RowsAffected()
	if err == nil && rows == 0 {
		return nil, sql.ErrNoRows
	}
	if r.ToResource != nil {
		return r.ToResource(&data), nil
	}
	return nil, nil
}
