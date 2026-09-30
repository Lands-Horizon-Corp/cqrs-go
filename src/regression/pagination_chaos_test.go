package regression

import (
	"context"
	"testing"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/domains"
)

// This file injects real faults at the SQLite/bun boundary (closing the DB,
// dropping the table mid-walk) rather than reasoning about them from
// reading the code — no Docker needed, fast, but still a genuine failure
// being exercised, not a mock returning a canned error. The real-Postgres
// equivalent (a container restart mid-walk) lives in
// integration_pagination_chaos_test.go.

func TestPagination_Chaos_DBClosedBetweenPagesReturnsErrorOnNextPage(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	ctx := context.Background()
	seedWidgets(t, read,
		widget{ID: "a", Name: "A"}, widget{ID: "b", Name: "B"}, widget{ID: "c", Name: "C"},
	)
	sortFields := []domains.SortField{{Field: "id", Order: domains.SortOrderAsc}}

	page1, err := c.Pagination(ctx, domains.Pagination{
		Filter: domains.StructuredFilter{SortFields: sortFields}, PageSize: 1,
	})
	if err != nil {
		t.Fatalf("page1 returned error: %v", err)
	}
	if page1.NextCursor == nil {
		t.Fatal("expected a NextCursor after page1")
	}

	if err := read.Client().Close(); err != nil {
		t.Fatalf("closing db: %v", err)
	}

	_, err = c.Pagination(ctx, domains.Pagination{
		Filter: domains.StructuredFilter{SortFields: sortFields}, PageSize: 1, Cursor: page1.NextCursor,
	})
	if err == nil {
		t.Fatal("expected page2 to error once the underlying DB is closed, got nil")
	}
}

func TestPagination_Chaos_TableDroppedMidWalkReturnsErrorNotPanic(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	ctx := context.Background()
	seedWidgets(t, read,
		widget{ID: "a", Name: "A"}, widget{ID: "b", Name: "B"}, widget{ID: "c", Name: "C"},
	)
	sortFields := []domains.SortField{{Field: "id", Order: domains.SortOrderAsc}}

	page1, err := c.Pagination(ctx, domains.Pagination{
		Filter: domains.StructuredFilter{SortFields: sortFields}, PageSize: 1,
	})
	if err != nil {
		t.Fatalf("page1 returned error: %v", err)
	}

	if _, err := read.Client().NewDropTable().Model((*widget)(nil)).Exec(ctx); err != nil {
		t.Fatalf("dropping widgets table: %v", err)
	}

	_, err = c.Pagination(ctx, domains.Pagination{
		Filter: domains.StructuredFilter{SortFields: sortFields}, PageSize: 1, Cursor: page1.NextCursor,
	})
	if err == nil {
		t.Fatal("expected page2 to error once the table is dropped, got nil")
	}
}
