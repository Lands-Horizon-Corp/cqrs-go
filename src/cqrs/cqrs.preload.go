package cqrs

import (
	"context"
	"fmt"

	"github.com/uptrace/bun"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/utils"
)

func (c *CQRSImpl[TData, TResponse, TRequest, TID]) resolvePreload(preload []string) []string {
	return utils.ResolvePreload(preload, c.Preloads)
}

func (c *CQRSImpl[TData, TResponse, TRequest, TID]) applyPreloads(
	ctx context.Context,
	db bun.IDB,
	data *TData,
	preload ...string,
) error {
	resolved := c.resolvePreload(preload)
	if len(resolved) == 0 {
		return nil
	}
	q := db.NewSelect().Model(data).WherePK()
	for _, rel := range resolved {
		q = q.Relation(rel)
	}
	if err := q.Scan(ctx); err != nil {
		return fmt.Errorf("loading preloads %v: %w", resolved, err)
	}
	return nil
}

func (c *CQRSImpl[TData, TResponse, TRequest, TID]) applyPreloadsMany(
	ctx context.Context,
	db bun.IDB,
	data *[]TData,
	preload ...string,
) error {
	return utils.ApplyPreloadsMany(ctx, db, data, c.Preloads, preload...)
}
