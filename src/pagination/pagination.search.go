package pagination

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/uptrace/bun"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/utils"
)

// EnableSearchIndex is a one-time setup call for a ParadeDB (pg_search)
// BM25 index over the given TData columns, enabling ModeSearch filters
// against them — mirrors EnablePartitioning's own rationale exactly: it is
// deliberately NOT called from Pagination() itself (index creation is DDL,
// not something to check/run on every paginated read), and should be
// called once from a migration/deploy step against ReadSQLService, never
// from a request-handling path.
//
// Unlike EnablePartitioning, this needs no separate existence check for
// idempotency — plain "CREATE INDEX IF NOT EXISTS" is enough on its own,
// so calling this again (e.g. on every redeploy, or to add it after the
// fact to an existing table) is always safe.
//
// fields must each name a real column on TData (validated the same way
// resolveSortFields/applyFilters/EnablePartitioning's own control column
// validate field names). ColumnDefaultID is always included as the index's
// key_field, matching pg_search's own requirement that every BM25 index
// have one — the same column Pagination already uses as its keyset
// tiebreaker.
//
// Requires the pg_search extension already created (CREATE EXTENSION
// pg_search CASCADE) and shared_preload_libraries=pg_search set on the
// target Postgres instance — confirmed directly: without either, this
// fails with Postgres's own clear error rather than something this method
// needs to special-case.
func (c *PaginationService[TData, TID]) EnableSearchIndex(
	ctx context.Context, fields ...string,
) error {
	if c.ReadSQLService == nil {
		return fmt.Errorf("enabling search index requires ReadSQLService to be set")
	}
	for _, field := range fields {
		if utils.BunColumnFieldIndex[TData](field) == -1 {
			return fmt.Errorf("unknown search index column %q", field)
		}
	}

	db := c.ReadSQLService.Client()
	table := db.Table(reflect.TypeFor[TData]())

	// key_field (ColumnDefaultID) always leads the column list — pg_search
	// requires every BM25 index to declare one, and it's also what
	// applyFilterTerm's whole-index ModeSearch case queries against.
	columnArgs := make([]any, 0, len(fields)+1)
	columnArgs = append(columnArgs, bun.Ident(c.ColumnDefaultID))
	for _, field := range fields {
		columnArgs = append(columnArgs, bun.Ident(field))
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?, ", len(columnArgs)), ", ")

	indexName := table.Name + "_search_idx"
	args := make([]any, 0, len(columnArgs)+3)
	args = append(args, bun.Ident(indexName), bun.Ident(table.Name))
	args = append(args, columnArgs...)
	args = append(args, c.ColumnDefaultID) // key_field is a string reloption, not an identifier reference

	query := fmt.Sprintf("CREATE INDEX IF NOT EXISTS ? ON ? USING bm25 (%s) WITH (key_field = ?)", placeholders)
	if _, err := db.NewRaw(query, args...).Exec(ctx); err != nil {
		return fmt.Errorf("creating search index: %w", err)
	}
	return nil
}
