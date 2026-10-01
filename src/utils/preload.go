package utils

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/schema"
)

// ResolvePreload applies defaultPreloads when preload is empty/nil, and
// treats a single explicit empty string as "load nothing" — overriding
// defaultPreloads even when one is configured. Lives in utils (rather than
// on cqrs.CQRSImpl or pagination.PaginationService directly) so both
// packages share the exact same preload resolution rules without either
// one depending on the other just for this.
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

// ValidPreloads splits preload into the entries that name a real relation
// chain on TData (normalized to the exact PascalCase bun's Relation()
// expects, and safe to pass to it) and the entries that don't. Each entry
// may be a dotted, arbitrarily deep path ("author.posts.comments.likes.user"
// resolves the same as "Author.Posts.Comments.Likes.User" — bun itself
// supports nested relation preloading this way), and every individual
// segment is independently case/format-normalized via ToPascalCase before
// being checked, so "author_posts", "authorPosts" and "AuthorPosts" all
// resolve identically.
//
// This exists so a preload referencing a relation that doesn't exist (a
// typo, or a relation that was since renamed or removed from the model) can
// be dropped with a warning instead of hard-failing the entire read — see
// cqrs.CQRSImpl's and pagination.PaginationService's own warn() call sites,
// which is what actually logs each dropped entry; this function only
// reports them back, since the utils package itself has no LogService to
// log through.
func ValidPreloads[TData any](db bun.IDB, preload []string) (valid, dropped []string) {
	table := db.Dialect().Tables().Get(reflect.TypeFor[TData]())
	for _, raw := range preload {
		if raw == "" {
			continue
		}
		resolved, ok := resolveRelationPath(table, raw)
		if !ok {
			dropped = append(dropped, raw)
			continue
		}
		valid = append(valid, resolved)
	}
	return valid, dropped
}

// resolveRelationPath walks raw's dot-separated segments against table's
// own Relations map and then, for each segment after the first, the
// previous segment's own JoinTable — the same way bun's own Relation()
// parses a nested path internally, just checked ahead of time instead of
// discovered as a query-building error. Every segment is normalized via
// ToPascalCase before the lookup. ok is false (and resolved empty) if any
// segment along the path — not just the last one — doesn't name a real
// relation, since a partially-valid chain can't be preloaded at all.
func resolveRelationPath(table *schema.Table, raw string) (resolved string, ok bool) {
	segments := strings.Split(raw, ".")
	names := make([]string, 0, len(segments))
	current := table
	for _, seg := range segments {
		if current == nil {
			return "", false
		}
		name := ToPascalCase(seg)
		if name == "" {
			return "", false
		}
		rel, exists := current.Relations[name]
		if !exists {
			return "", false
		}
		names = append(names, name)
		current = rel.JoinTable
	}
	return strings.Join(names, "."), true
}

// ApplyPreloadsMany loads relations for every element of data in a single
// query rather than one per element: bun's WherePK() on a slice model
// generates a single "pk IN (...)" match against the slice's current PK
// values, so this is one round trip regardless of how many rows are in
// data. Any requested relation (or any segment of a nested one) that
// doesn't actually exist on TData is silently excluded from the query and
// reported back via dropped, rather than failing the whole load — see
// ValidPreloads.
func ApplyPreloadsMany[TData any](
	ctx context.Context,
	db bun.IDB,
	data *[]TData,
	defaultPreloads []string,
	preload ...string,
) (dropped []string, err error) {
	resolved := ResolvePreload(preload, defaultPreloads)
	if len(resolved) == 0 || len(*data) == 0 {
		return nil, nil
	}
	valid, dropped := ValidPreloads[TData](db, resolved)
	if len(valid) == 0 {
		return dropped, nil
	}
	q := db.NewSelect().Model(data).WherePK()
	for _, rel := range valid {
		q = q.Relation(rel)
	}
	if err := q.Scan(ctx); err != nil {
		return dropped, fmt.Errorf("loading preloads %v: %w", valid, err)
	}
	return dropped, nil
}
