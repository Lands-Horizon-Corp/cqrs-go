package repository

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/uptrace/bun"
)

func (r *RepositoryImpl[TData, TResponse, TRequest, TID]) DeleteByID(
	ctx context.Context,
	id TID,
) error {
	res, err := r.WriteDB.NewDelete().
		Model((*TData)(nil)).
		Where("? = ?", bun.Ident(r.ColumnDefaultID), id).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("deleting record: %w", err)
	}
	rows, err := res.RowsAffected()
	if err == nil && rows == 0 {
		return sql.ErrNoRows
	}
	return nil
}

func (r *RepositoryImpl[TData, TResponse, TRequest, TID]) DeleteByIDWithTx(
	ctx context.Context,
	tx bun.Tx,
	id TID,
) error {
	res, err := tx.NewDelete().
		Model((*TData)(nil)).
		Where("? = ?", bun.Ident(r.ColumnDefaultID), id).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("deleting record in tx: %w", err)
	}
	rows, err := res.RowsAffected()
	if err == nil && rows == 0 {
		return sql.ErrNoRows
	}
	return nil
}

func (r *RepositoryImpl[TData, TResponse, TRequest, TID]) DeleteMany(
	ctx context.Context,
	ids []TID,
) error {
	if len(ids) == 0 {
		return nil
	}
	_, err := r.WriteDB.NewDelete().
		Model((*TData)(nil)).
		Where("? IN (?)", bun.Ident(r.ColumnDefaultID), bun.In(ids)).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("bulk deleting records: %w", err)
	}
	return nil
}

func (r *RepositoryImpl[TData, TResponse, TRequest, TID]) DeleteManyWithTx(
	ctx context.Context,
	tx bun.Tx,
	ids []TID,
) error {
	if len(ids) == 0 {
		return nil
	}
	_, err := tx.NewDelete().
		Model((*TData)(nil)).
		Where("? IN (?)", bun.Ident(r.ColumnDefaultID), bun.In(ids)).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("bulk deleting records in tx: %w", err)
	}
	return nil
}
