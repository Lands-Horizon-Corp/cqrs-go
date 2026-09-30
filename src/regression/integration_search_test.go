//go:build integration

package regression

import (
	"context"
	"testing"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/domains"
	"github.com/Lands-Horizon-Corp/cqrs-go/src/pagination"
)

// ModeSearch (BM25 full-text search via ParadeDB's pg_search extension, the
// @@@ operator) is real Postgres-only DDL/query syntax — there's no SQLite
// equivalent to unit-test this against, same as EnablePartitioning in
// integration_partition_test.go. Reuses the shared widget fixture (already
// has Name/Notes text columns) rather than a dedicated one — nothing here
// needs a constraint the widget fixture doesn't already satisfy.

func newSearchTestCQRS(t *testing.T) (*pagination.PaginationService[widget, string], *fakeSQLService) {
	t.Helper()
	skipUnlessInfraReachable(t)
	read := newPostgresSQLService(t, itReadDSN)
	p := pagination.NewPaginationService(pagination.PaginationService[widget, string]{
		ReadSQLService: read,
	})
	return p, read
}

func TestIntegration_HappyPath_EnableSearchIndexCreatesBM25IndexAndIsIdempotent(t *testing.T) {
	t.Parallel()
	c, read := newSearchTestCQRS(t)
	ctx := context.Background()

	if err := c.EnableSearchIndex(ctx, "name", "notes"); err != nil {
		t.Fatalf("EnableSearchIndex returned error: %v", err)
	}

	var relkind string
	if err := read.Client().NewRaw(
		"SELECT relkind FROM pg_class WHERE relname = 'widgets_search_idx' AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = current_schema())",
	).Scan(ctx, &relkind); err != nil {
		t.Fatalf("checking widgets_search_idx's relkind: %v", err)
	}
	if relkind != "i" {
		t.Fatalf("expected widgets_search_idx to exist as an index (relkind 'i'), got %q", relkind)
	}

	// Plain "CREATE INDEX IF NOT EXISTS" is idempotent on its own (unlike
	// pg_partman's create_parent, see EnablePartitioning's own comment on
	// why partitioning needed a separate existence check) — calling this
	// again must not error.
	if err := c.EnableSearchIndex(ctx, "name", "notes"); err != nil {
		t.Fatalf("second EnableSearchIndex call returned error: %v", err)
	}
}

func TestIntegration_SadPath_EnableSearchIndexUnknownColumnReturnsError(t *testing.T) {
	t.Parallel()
	c, _ := newSearchTestCQRS(t)
	if err := c.EnableSearchIndex(context.Background(), "not_a_real_column"); err == nil {
		t.Fatal("expected an error for an unknown search index column, got nil")
	}
}

func TestIntegration_SadPath_EnableSearchIndexReturnsErrorWhenReadSQLServiceIsNil(t *testing.T) {
	t.Parallel()
	raw := pagination.PaginationService[widget, string]{}
	if err := raw.EnableSearchIndex(context.Background(), "name"); err == nil {
		t.Fatal("expected an error when ReadSQLService is nil, got nil")
	}
}

func TestIntegration_HappyPath_ModeSearchScopedToOneColumnMatchesViaBM25(t *testing.T) {
	t.Parallel()
	c, read := newSearchTestCQRS(t)
	ctx := context.Background()

	if err := c.EnableSearchIndex(ctx, "name", "notes"); err != nil {
		t.Fatalf("EnableSearchIndex returned error: %v", err)
	}
	seedWidgets(t, read,
		widget{ID: "w1", Name: "running shoes", Notes: new("a standard component")},
		widget{ID: "w2", Name: "blue socks", Notes: new("nothing unusual here")},
	)

	result, err := c.Pagination(ctx, domains.Pagination{
		Filter: domains.StructuredFilter{Filters: []domains.Filter{
			{Field: "name", Mode: domains.ModeSearch, Value: "running"},
		}},
	})
	if err != nil {
		t.Fatalf("Pagination returned error: %v", err)
	}
	if len(result.Data) != 1 || result.Data[0].ID != "w1" {
		t.Fatalf("expected only [w1] (BM25 match on \"running\" in name), got %+v", result.Data)
	}
}

// TestIntegration_HappyPath_ModeSearchWholeIndexMatchesAcrossMultipleColumns
// is the "multiple column search filter" case: an empty Field searches
// every column EnableSearchIndex indexed, not just one — proven here by a
// term that only appears in notes, not name.
func TestIntegration_HappyPath_ModeSearchWholeIndexMatchesAcrossMultipleColumns(t *testing.T) {
	t.Parallel()
	c, read := newSearchTestCQRS(t)
	ctx := context.Background()

	if err := c.EnableSearchIndex(ctx, "name", "notes"); err != nil {
		t.Fatalf("EnableSearchIndex returned error: %v", err)
	}
	seedWidgets(t, read,
		widget{ID: "w1", Name: "Alpha Widget", Notes: new("a standard component")},
		widget{ID: "w2", Name: "Beta Widget", Notes: new("contains a special coating")},
		widget{ID: "w3", Name: "Gamma Gadget", Notes: new("nothing unusual here")},
	)

	// "coating" only appears in w2's Notes, never in any Name.
	byNotesTerm, err := c.Pagination(ctx, domains.Pagination{
		Filter: domains.StructuredFilter{Filters: []domains.Filter{
			{Field: "", Mode: domains.ModeSearch, Value: "coating"},
		}},
	})
	if err != nil {
		t.Fatalf("Pagination returned error: %v", err)
	}
	if len(byNotesTerm.Data) != 1 || byNotesTerm.Data[0].ID != "w2" {
		t.Fatalf("expected only [w2] (whole-index match on \"coating\" in notes), got %+v", byNotesTerm.Data)
	}

	// "widget" appears in both w1 and w2's Name.
	byNameTerm, err := c.Pagination(ctx, domains.Pagination{
		Filter: domains.StructuredFilter{
			SortFields: []domains.SortField{{Field: "id", Order: domains.SortOrderAsc}},
			Filters:    []domains.Filter{{Field: "", Mode: domains.ModeSearch, Value: "widget"}},
		},
	})
	if err != nil {
		t.Fatalf("Pagination returned error: %v", err)
	}
	if len(byNameTerm.Data) != 2 || byNameTerm.Data[0].ID != "w1" || byNameTerm.Data[1].ID != "w2" {
		t.Fatalf("expected [w1, w2] (whole-index match on \"widget\" in name), got %+v", byNameTerm.Data)
	}
}
