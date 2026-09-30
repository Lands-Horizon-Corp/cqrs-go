package regression

import (
	"context"
	"testing"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/domains"
)

// This file proves the fix for a confirmed, serious bug: before it,
// cursoring/sorting by a nullable column could silently drop rows or end
// the walk early, because encodeCursor collapsed a NULL boundary value
// into "" (empty string) and the keyset >/< comparison then compared that
// against the real typed column with no NULL-awareness at all. The fix
// makes every ORDER BY explicit "NULLS LAST" and makes appendCursorTerm
// NULL-aware (see its own doc comment in pagination.cursor.go for the
// exact WHERE shape), forcing the mixed-direction per-term path whenever
// any sort column is nullable (anyNullableSortField) since the uniform
// row-value comparison has no well-defined meaning once a component can be
// NULL.

// TestPagination_HappyPath_CursorForwardWalkReachesRowWithNullSortValue is
// the direct regression test for the bug: before the fix, this returned
// only 2 of the 3 seeded rows (SQLite: the null-priority row's page
// returned correctly, but every row *after* it in the walk silently
// vanished; confirmed separately on Postgres: the null-priority row itself
// never appeared at all, terminating the walk one page early). Now every
// row must be reachable exactly once, all rows in NULLS-LAST order.
func TestPagination_HappyPath_CursorForwardWalkReachesRowWithNullSortValue(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	ctx := context.Background()
	seedWidgets(t, read,
		widget{ID: "w1", Name: "Alpha", Priority: nil},
		widget{ID: "w2", Name: "Beta", Priority: new(10)},
		widget{ID: "w3", Name: "Gamma", Priority: new(20)},
	)
	sortFields := []domains.SortField{{Field: "priority", Order: domains.SortOrderAsc}}

	var walked []string
	var cursor *string
	for pages := 0; ; pages++ {
		if pages > 3 {
			t.Fatal("walked more pages than there are rows — likely an infinite loop")
		}
		page, err := c.Pagination(ctx, domains.Pagination{
			Filter: domains.StructuredFilter{SortFields: sortFields}, PageSize: 1, Cursor: cursor,
		})
		if err != nil {
			t.Fatalf("page %d: Pagination returned error: %v", pages, err)
		}
		for _, w := range page.Data {
			walked = append(walked, w.ID)
		}
		if page.NextCursor == nil {
			break
		}
		cursor = page.NextCursor
	}

	// NULLS LAST, ascending: 10, 20, then the null row.
	want := []string{"w2", "w3", "w1"}
	if len(walked) != len(want) {
		t.Fatalf("expected all 3 rows walked (nulls last: %v), got %v", want, walked)
	}
	for i := range want {
		if walked[i] != want[i] {
			t.Fatalf("expected walk order %v, got %v", want, walked)
		}
	}
}

// TestPagination_HappyPath_CursorBackwardWalkFromNullSortValueReachesStart
// is the symmetric backward-walk check: starting from the trailing NULL
// row and walking PreviousCursor back to the start must reproduce every
// non-null row in reverse, ending with a nil PreviousCursor.
func TestPagination_HappyPath_CursorBackwardWalkFromNullSortValueReachesStart(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	ctx := context.Background()
	seedWidgets(t, read,
		widget{ID: "w1", Name: "Alpha", Priority: nil},
		widget{ID: "w2", Name: "Beta", Priority: new(10)},
		widget{ID: "w3", Name: "Gamma", Priority: new(20)},
	)
	sortFields := []domains.SortField{{Field: "priority", Order: domains.SortOrderAsc}}

	// Walk to the last page (the null row, w1) first.
	last, err := c.Pagination(ctx, domains.Pagination{Filter: domains.StructuredFilter{SortFields: sortFields}, PageSize: 3})
	if err != nil {
		t.Fatalf("fetching all 3 in one page: %v", err)
	}
	if len(last.Data) != 3 || last.Data[2].ID != "w1" {
		t.Fatalf("expected [w2,w3,w1] with w1 (null priority) last, got %+v", last.Data)
	}

	// Re-fetch one at a time so PreviousCursor is meaningful, then walk
	// backward from the null row (w1, the third page) to the start.
	page1, err := c.Pagination(ctx, domains.Pagination{Filter: domains.StructuredFilter{SortFields: sortFields}, PageSize: 1})
	if err != nil {
		t.Fatalf("fetching page1: %v", err)
	}
	page2, err := c.Pagination(ctx, domains.Pagination{
		Filter: domains.StructuredFilter{SortFields: sortFields}, PageSize: 1, Cursor: page1.NextCursor,
	})
	if err != nil {
		t.Fatalf("fetching page2: %v", err)
	}
	page3, err := c.Pagination(ctx, domains.Pagination{
		Filter: domains.StructuredFilter{SortFields: sortFields}, PageSize: 1, Cursor: page2.NextCursor,
	})
	if err != nil {
		t.Fatalf("fetching page3: %v", err)
	}
	if len(page3.Data) != 1 || page3.Data[0].ID != "w1" {
		t.Fatalf("expected page3 to be [w1] (the null-priority row), got %+v", page3.Data)
	}

	var walkedBack []string
	backCursor := page3.PreviousCursor
	for i := range 3 {
		if backCursor == nil {
			break
		}
		page, err := c.Pagination(ctx, domains.Pagination{
			Filter: domains.StructuredFilter{SortFields: sortFields}, PageSize: 1, Cursor: backCursor,
		})
		if err != nil {
			t.Fatalf("backward step %d: Pagination returned error: %v", i, err)
		}
		for _, w := range page.Data {
			walkedBack = append(walkedBack, w.ID)
		}
		backCursor = page.PreviousCursor
	}
	if backCursor != nil {
		t.Errorf("expected a nil PreviousCursor once the backward walk reaches the start, got %v", *backCursor)
	}
	// Backward from w1: w3, then w2.
	want := []string{"w3", "w2"}
	if len(walkedBack) != len(want) || walkedBack[0] != want[0] || walkedBack[1] != want[1] {
		t.Fatalf("expected backward walk %v, got %v", want, walkedBack)
	}
}

// TestPagination_HappyPath_MultiColumnSortWithNullableFirstColumnStillCorrect
// covers the multi-column case: a nullable first sort column forces the
// mixed-direction path (anyNullableSortField), and the non-null second
// column plus the ColumnDefaultID tiebreaker must still correctly order
// and tie-break rows that share a (possibly null) first-column value.
func TestPagination_HappyPath_MultiColumnSortWithNullableFirstColumnStillCorrect(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	ctx := context.Background()
	seedWidgets(t, read,
		widget{ID: "w1", Name: "Charlie", Priority: nil},
		widget{ID: "w2", Name: "Alpha", Priority: nil},
		widget{ID: "w3", Name: "Bravo", Priority: new(5)},
	)
	sortFields := []domains.SortField{
		{Field: "priority", Order: domains.SortOrderAsc},
		{Field: "name", Order: domains.SortOrderAsc},
	}

	result, err := c.Pagination(ctx, domains.Pagination{Filter: domains.StructuredFilter{SortFields: sortFields}, PageSize: 10})
	if err != nil {
		t.Fatalf("Pagination returned error: %v", err)
	}
	// priority=5 first (not null), then the two null-priority rows broken
	// by name ASC: Alpha (w2) before Charlie (w1).
	want := []string{"w3", "w2", "w1"}
	if len(result.Data) != len(want) {
		t.Fatalf("expected %d rows, got %d: %+v", len(want), len(result.Data), result.Data)
	}
	for i := range want {
		if result.Data[i].ID != want[i] {
			t.Fatalf("expected order %v, got %v", want, func() []string {
				ids := make([]string, len(result.Data))
				for j, w := range result.Data {
					ids[j] = w.ID
				}
				return ids
			}())
		}
	}
}
