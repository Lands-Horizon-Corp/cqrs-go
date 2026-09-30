package cqrs

import (
	"context"
	"fmt"

	"github.com/uptrace/bun"
)

func (c *CQRSImpl[TData, TResponse, TRequest, TID]) Preload(
	preload ...string,
) []string {
	if preload == nil {
		preload = c.Preloads
	}
	if len(preload) == 0 {
		preload = c.Preloads
	}
	if len(preload) == 1 && preload[0] == "" {
		preload = []string{}
	}
	return preload
}

func (c *CQRSImpl[TData, TResponse, TRequest, TID]) applyPreloads(
	ctx context.Context,
	db bun.IDB,
	data *TData,
	preload []string,
) error {
	if len(preload) == 0 {
		return nil
	}
	q := db.NewSelect().Model(data).WherePK()
	for _, rel := range preload {
		q = q.Relation(rel)
	}
	if err := q.Scan(ctx); err != nil {
		return fmt.Errorf("loading preloads %v: %w", preload, err)
	}
	return nil
}
