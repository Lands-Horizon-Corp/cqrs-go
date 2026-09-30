package regression

import (
	"context"
	"testing"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/domains"
)

// Smoke tests: minimal, single-assertion, fast sanity checks that the core
// capabilities work at all — distinct from the deeper scenario coverage in
// pagination_query_test.go's Happy Path tests. These are the tests to run
// first / fail fast on, not the tests that prove edge cases.

func TestPagination_Smoke_ConstructAndFetchFirstPage(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	seedWidgets(t, read, widget{ID: "w1", Name: "Alpha"})

	result, err := c.Pagination(context.Background(), domains.Pagination{})
	if err != nil {
		t.Fatalf("Pagination returned error: %v", err)
	}
	if len(result.Data) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Data))
	}
}

func TestPagination_Smoke_BackwardCursorFetchesPreviousPage(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	ctx := context.Background()
	seedWidgets(t, read, widget{ID: "a", Name: "A"}, widget{ID: "b", Name: "B"})
	sortFields := []domains.SortField{{Field: "id", Order: domains.SortOrderAsc}}

	page1, err := c.Pagination(ctx, domains.Pagination{Filter: domains.StructuredFilter{SortFields: sortFields}, PageSize: 1})
	if err != nil {
		t.Fatalf("page1 error: %v", err)
	}
	page2, err := c.Pagination(ctx, domains.Pagination{Filter: domains.StructuredFilter{SortFields: sortFields}, PageSize: 1, Cursor: page1.NextCursor})
	if err != nil {
		t.Fatalf("page2 error: %v", err)
	}
	back, err := c.Pagination(ctx, domains.Pagination{Filter: domains.StructuredFilter{SortFields: sortFields}, PageSize: 1, Cursor: page2.PreviousCursor})
	if err != nil {
		t.Fatalf("backward error: %v", err)
	}
	if len(back.Data) != 1 || back.Data[0].ID != "a" {
		t.Fatalf("expected backward navigation to return [a], got %+v", back.Data)
	}
}

func TestPagination_Smoke_FilterNarrowsResults(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	seedWidgets(t, read, widget{ID: "w1", Name: "Alpha"}, widget{ID: "w2", Name: "Beta"})

	result, err := c.Pagination(context.Background(), domains.Pagination{
		Filter: domains.StructuredFilter{Filters: []domains.Filter{{Field: "name", Mode: domains.ModeEqual, Value: "Alpha"}}},
	})
	if err != nil {
		t.Fatalf("Pagination returned error: %v", err)
	}
	if len(result.Data) != 1 || result.Data[0].ID != "w1" {
		t.Fatalf("expected the filter to narrow to [w1], got %+v", result.Data)
	}
}

func TestPagination_Smoke_PreloadPopulatesRelation(t *testing.T) {
	t.Parallel()
	c, db := newPreloadTestCQRS(t)
	ctx := context.Background()
	seedPreloadAuthor(t, db, "a1", "Ada")
	if _, err := c.CreateFormat(ctx, preloadPost{ID: "p1", Title: "One", AuthorID: "a1"}); err != nil {
		t.Fatalf("seed Create returned error: %v", err)
	}

	pc := newPaginationPreloadService(db)
	result, err := pc.Pagination(ctx, domains.Pagination{}, "Author")
	if err != nil {
		t.Fatalf("Pagination returned error: %v", err)
	}
	if len(result.Data) != 1 || result.Data[0].Author == nil || result.Data[0].Author.Name != "Ada" {
		t.Fatalf("expected the preloaded author name 'Ada', got %+v", result.Data)
	}
}
