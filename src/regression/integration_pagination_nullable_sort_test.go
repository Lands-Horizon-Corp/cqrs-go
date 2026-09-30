//go:build integration

package regression

import (
	"context"
	"testing"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/domains"
	"github.com/Lands-Horizon-Corp/cqrs-go/src/pagination"
)

// TestIntegration_HappyPath_CursorForwardWalkReachesRowWithNullSortValue is
// the real-Postgres regression test for the nullable-sort-column bug (see
// pagination_nullable_sort_test.go's own doc comment for the full story).
// This is the dialect where the bug's failure mode was most severe:
// confirmed directly, before the fix, the null-priority row never appeared
// at all — the walk silently terminated one page early instead of erroring
// or including it.
func TestIntegration_HappyPath_CursorForwardWalkReachesRowWithNullSortValue(t *testing.T) {
	t.Parallel()
	skipUnlessInfraReachable(t)
	read := newPostgresSQLService(t, itReadDSN)
	c := pagination.NewPaginationService(pagination.PaginationService[widget, string]{ReadSQLService: read})
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

	want := []string{"w2", "w3", "w1"} // NULLS LAST, ascending
	if len(walked) != len(want) {
		t.Fatalf("expected all 3 rows walked (nulls last: %v), got %v", want, walked)
	}
	for i := range want {
		if walked[i] != want[i] {
			t.Fatalf("expected walk order %v, got %v", want, walked)
		}
	}
}
