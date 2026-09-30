package cqrs

import (
	"context"
	"fmt"

	"github.com/uptrace/bun"
)

// ResolvePreload applies defaultPreloads when preload is empty/nil, and
// treats a single explicit empty string as "load nothing" — overriding
// defaultPreloads even when one is configured. Exported so src/pagination's
// PaginationService can share the exact same preload resolution rules
// CQRSImpl's Create/Update use, without duplicating the logic.
func ResolvePreload(preload []string, defaultPreloads []string) []string {
	if preload == nil {
		preload = defaultPreloads
	}
	if len(preload) == 0 {
		preload = defaultPreloads
	}
	if len(preload) == 1 && preload[0] == "" {
		preload = []string{}
	}
	return preload
}

func (c *CQRSImpl[TData, TResponse, TRequest, TID]) resolvePreload(preload []string) []string {
	return ResolvePreload(preload, c.Preloads)
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
	return ApplyPreloadsMany(ctx, db, data, c.Preloads, preload...)
}

// ApplyPreloadsMany loads relations for every element of data in a single
// query rather than one per element: bun's WherePK() on a slice model
// generates a single "pk IN (...)" match against the slice's current PK
// values, so this is one round trip regardless of how many rows are in
// data. Exported for src/pagination's PaginationService to reuse.
func ApplyPreloadsMany[TData any](
	ctx context.Context,
	db bun.IDB,
	data *[]TData,
	defaultPreloads []string,
	preload ...string,
) error {
	resolved := ResolvePreload(preload, defaultPreloads)
	if len(resolved) == 0 || len(*data) == 0 {
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
