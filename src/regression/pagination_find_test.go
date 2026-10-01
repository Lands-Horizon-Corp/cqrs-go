package regression

// This file verifies pagination.PaginationService's Find/FindWithTx — the
// multi-row counterpart to FindOne (pagination_find_one_test.go), which
// returns only the first match. Find returns every row matching filter,
// the same as Filter/FilterWithTx (pagination_service_interface_test.go),
// but additionally supports an optional preloads override the way
// FindOne/Count/Exists do, which plain Filter does not.

import (
	"context"
	"testing"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/domains"
)

// TestPaginationFind_HappyPath_ReturnsEveryMatchingRow deliberately doesn't
// set SortFields on filter: filter is passed through as paginate's
// extraFilter (the same hardcoded/trusted slot PaginateFilter's filter
// argument uses), and extraFilter.SortFields is never read by paginate —
// only pagination.Filter.SortFields is (see paginate's own doc comment).
// So row order here is whatever paginate's own default-sort fallback
// produces, not something this test controls — hence the set-based
// comparison instead of a positional one.
func TestPaginationFind_HappyPath_ReturnsEveryMatchingRow(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	seedWidgets(t, read,
		widget{ID: "w1", Name: "Alpha", Active: true},
		widget{ID: "w2", Name: "Beta", Active: false},
		widget{ID: "w3", Name: "Gamma", Active: true},
	)

	got, err := c.Find(context.Background(), domains.StructuredFilter{
		Filters: []domains.Filter{{Field: "active", Mode: domains.ModeEqual, DataType: domains.DataTypeBool, Value: true}},
	})
	if err != nil {
		t.Fatalf("Find returned error: %v", err)
	}
	gotIDs := map[string]bool{}
	for _, r := range got {
		gotIDs[r.ID] = true
	}
	if len(got) != 2 || !gotIDs["w1"] || !gotIDs["w3"] {
		t.Fatalf("expected exactly {w1, w3} in some order, got %+v", got)
	}
}

func TestPaginationFind_HappyPath_NoMatchReturnsEmptySliceNotError(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	seedWidgets(t, read, widget{ID: "w1", Name: "Alpha"})

	got, err := c.Find(context.Background(), domains.StructuredFilter{
		Filters: []domains.Filter{{Field: "name", Mode: domains.ModeEqual, Value: "NoSuchName"}},
	})
	if err != nil {
		t.Fatalf("Find returned error: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected no rows, got %+v", got)
	}
}

func TestPaginationFind_SadPath_UnknownFilterFieldErrorsRatherThanDropping(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	seedWidgets(t, read, widget{ID: "w1", Name: "Alpha"})

	_, err := c.Find(context.Background(), domains.StructuredFilter{
		Filters: []domains.Filter{{Field: "not_a_real_column", Mode: domains.ModeEqual, Value: "x"}},
	})
	if err == nil {
		t.Fatal("expected an error for an unknown field in Find's filter, got nil")
	}
}

func TestPaginationFind_PoisonPill_UnsupportedFilterModeReturnsError(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	seedWidgets(t, read, widget{ID: "w1", Name: "Alpha"})

	_, err := c.Find(context.Background(), domains.StructuredFilter{
		Filters: []domains.Filter{{Field: "name", Mode: "bogusMode", Value: "x"}},
	})
	if err == nil {
		t.Fatal("expected an error for an unsupported filter mode, got nil")
	}
}

// TestPaginationFind_HappyPath_PreloadOverrideAppliesPerCall confirms the
// one real behavioral difference from plain Filter: Find lets a caller
// override the configured default preloads per call.
func TestPaginationFind_HappyPath_PreloadOverrideAppliesPerCall(t *testing.T) {
	t.Parallel()
	c, db := newPreloadTestCQRS(t)
	ctx := context.Background()
	seedPreloadAuthor(t, db, "a1", "Ada")
	if _, err := c.CreateManyFormat(ctx, []preloadPost{
		{ID: "p1", Title: "One", AuthorID: "a1"},
		{ID: "p2", Title: "Two", AuthorID: "a1"},
	}); err != nil {
		t.Fatalf("seed CreateMany returned error: %v", err)
	}

	pc := newPaginationPreloadService(db)
	got, err := pc.Find(ctx, domains.StructuredFilter{}, "Author")
	if err != nil {
		t.Fatalf("Find returned error: %v", err)
	}
	if len(got) != 2 || got[0].Author == nil || got[0].Author.Name != "Ada" || got[1].Author == nil || got[1].Author.Name != "Ada" {
		t.Fatalf("expected both rows with Author preloaded, got %+v", got)
	}
}

func TestPaginationFind_HappyPath_FindWithTxSeesUncommittedWritesInTheSameTx(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	ctx := context.Background()

	// Same single-connection caveat as
	// TestPagination_HappyPath_FilterWithTxSeesUncommittedWritesInTheSameTx:
	// only assert visibility inside the tx and after it's gone, never
	// concurrently with it open.
	tx, err := read.Client().BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx returned error: %v", err)
	}
	if _, err := tx.NewInsert().Model(&widget{ID: "w1", Name: "InTx"}).Exec(ctx); err != nil {
		t.Fatalf("inserting inside the transaction: %v", err)
	}

	inTx, err := c.FindWithTx(ctx, &tx, domains.StructuredFilter{})
	if err != nil {
		t.Fatalf("FindWithTx returned error: %v", err)
	}
	if len(inTx) != 1 || inTx[0].ID != "w1" {
		t.Fatalf("expected FindWithTx to see the uncommitted row, got %+v", inTx)
	}

	if err := tx.Rollback(); err != nil {
		t.Fatalf("rolling back: %v", err)
	}

	after, err := c.Find(ctx, domains.StructuredFilter{})
	if err != nil {
		t.Fatalf("Find (after rollback) returned error: %v", err)
	}
	if len(after) != 0 {
		t.Fatalf("expected no rows after rollback, got %+v", after)
	}
}
