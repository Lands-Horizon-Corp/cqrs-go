package pagination

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"strings"

	"github.com/uptrace/bun"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/utils"
)

// EnablePartitioning is a one-time setup call for pg_partman-backed range
// partitioning on TData's table — it is deliberately NOT called from
// Pagination() itself. partman.create_parent is DDL that takes locks and
// registers the table once in partman.part_config; running any version of
// that check on every paginated read would be wasteful at best and a real
// footgun at worst (every replica racing to register the same table on
// boot). Call this once, from a migration or deploy step, against
// ReadSQLService (the same database Pagination() queries) — never from a
// request-handling path.
//
// This only covers creating TData's table fresh, partitioned from the
// start (CREATE TABLE IF NOT EXISTS ... PARTITION BY RANGE, so it's safe to
// call again on redeploy). It does not — and safely cannot — retrofit
// partitioning onto an existing table that already has data: pg_partman's
// create_parent requires the parent table to already be declared
// PARTITION BY RANGE, and converting a live, populated plain table into a
// partitioned one is a data-migration problem (new table, copy, swap) that
// depends on downtime/locking tradeoffs specific to your deployment — not
// something safe to automate generically here.
//
// control must name a real column on TData (validated the same way
// resolveSortFields/applyFilters validate field names, since a typo here
// would otherwise silently misconfigure partitioning rather than error).
// interval is pg_partman's own interval string, e.g. "1 month" or "1 day".
//
// control must ALSO be tagged `,pk` alongside TData's existing primary key
// column(s) — Postgres requires that every unique constraint on a
// partitioned table, including its primary key, include the partitioning
// column (confirmed directly: CREATE TABLE ... PARTITION BY RANGE fails
// outright with a single-column id-only PK). This means a partitioned
// table's id is only unique in combination with control, not on its own —
// id values still need to be globally unique in practice (e.g. a UUID/ULID
// generator), since the database no longer enforces that alone.
//
// Note: a table that's actively CDC-synced by something like cqrs.CQRSImpl's
// Run() can't cleanly use this today either — that upsert path hardcodes
// "ON CONFLICT (ColumnDefaultID) DO UPDATE", which needs a matching
// single-column unique constraint that a partitioned table with a
// composite PK won't have. EnablePartitioning is intended for read-side
// tables populated some other way (a reporting/analytics projection, a
// batch job, ...), not ones a CDC sync also writes into.
func (c *PaginationService[TData, TID]) EnablePartitioning(
	ctx context.Context, control string, interval string,
) error {
	if c.ReadSQLService == nil {
		return fmt.Errorf("enabling partitioning requires ReadSQLService to be set")
	}
	if utils.BunColumnFieldIndex[TData](control) == -1 {
		return fmt.Errorf("unknown partition control column %q", control)
	}
	if !bunFieldIsPK[TData](control) {
		return fmt.Errorf(
			"partition control column %q must also be tagged as part of TData's "+
				"primary key (bun:\"%s,pk,...\") — Postgres requires a partitioned "+
				"table's primary key to include the partitioning column",
			control, control,
		)
	}

	db := c.ReadSQLService.Client()
	table := db.Table(reflect.TypeFor[TData]())

	// bun's static table registry only knows a schema if the model's `bun`
	// tag names one explicitly — it has no idea what search_path resolves
	// to at connection time, which is exactly what an unqualified
	// CREATE TABLE below actually targets. Asking Postgres directly is what
	// keeps this correct for a caller using a non-"public" search_path
	// (e.g. per-tenant schemas), rather than silently assuming "public".
	var schemaName string
	if err := db.NewRaw("SELECT current_schema()").Scan(ctx, &schemaName); err != nil {
		return fmt.Errorf("resolving current schema: %w", err)
	}
	if schemaName == "" {
		schemaName = "public"
	}
	qualifiedTable := schemaName + "." + table.Name

	if _, err := db.NewCreateTable().
		Model((*TData)(nil)).
		PartitionBy("RANGE (?)", bun.Ident(control)).
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating partitioned table: %w", err)
	}

	// create_parent is not idempotent on its own (a second call on an
	// already-registered table fails on part_config's primary key,
	// verified directly against a real pg_partman instance) — this
	// existence check is what makes calling EnablePartitioning again (e.g.
	// on every redeploy) safe.
	registered, err := db.NewSelect().
		TableExpr("partman.part_config").
		Where("parent_table = ?", qualifiedTable).
		Exists(ctx)
	if err != nil {
		return fmt.Errorf("checking pg_partman registration: %w", err)
	}
	if registered {
		return nil
	}

	if _, err := db.NewRaw(
		"SELECT partman.create_parent(p_parent_table => ?, p_control => ?, p_type => 'range', p_interval => ?)",
		qualifiedTable, control, interval,
	).Exec(ctx); err != nil {
		return fmt.Errorf("registering with pg_partman: %w", err)
	}
	return nil
}

// bunFieldIsPK reports whether T's exported field tagged with the given bun
// column name also carries the "pk" option — same tag-walking approach as
// utils.BunColumnFieldIndex, just checking for a different option instead
// of resolving a field index.
func bunFieldIsPK[T any](column string) bool {
	t := reflect.TypeFor[T]()
	if t.Kind() != reflect.Struct {
		return false
	}
	for field := range t.Fields() {
		if !field.IsExported() {
			continue
		}
		tag := field.Tag.Get("bun")
		if tag == "" || tag == "-" {
			continue
		}
		parts := strings.Split(tag, ",")
		if parts[0] != column {
			continue
		}
		return slices.Contains(parts[1:], "pk")
	}
	return false
}
