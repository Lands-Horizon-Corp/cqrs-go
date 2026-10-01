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

// warnDroppedPreloads logs one warning per entry in dropped — a preload
// (or a segment of a nested one) that doesn't name a real relation on
// TData, typically because it was mistyped, or because the relation it
// once named was renamed or removed from the model. These are dropped
// rather than failing the read entirely: a stale preload reference
// shouldn't be able to take production traffic down just because a model
// changed shape elsewhere.
func (c *CQRSImpl[TData, TResponse, TRequest, TID]) warnDroppedPreloads(ctx context.Context, dropped []string) {
	for _, d := range dropped {
		c.warn(ctx, fmt.Sprintf("preload: dropping unknown relation %q", d))
	}
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
	valid, dropped := utils.ValidPreloads[TData](db, resolved)
	c.warnDroppedPreloads(ctx, dropped)
	if len(valid) == 0 {
		return nil
	}
	q := db.NewSelect().Model(data).WherePK()
	for _, rel := range valid {
		q = q.Relation(rel)
	}
	if err := q.Scan(ctx); err != nil {
		return fmt.Errorf("loading preloads %v: %w", valid, err)
	}
	return nil
}

func (c *CQRSImpl[TData, TResponse, TRequest, TID]) applyPreloadsMany(
	ctx context.Context,
	db bun.IDB,
	data *[]TData,
	preload ...string,
) error {
	dropped, err := utils.ApplyPreloadsMany(ctx, db, data, c.Preloads, preload...)
	c.warnDroppedPreloads(ctx, dropped)
	return err
}
