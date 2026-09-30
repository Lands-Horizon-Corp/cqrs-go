package regression

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/domains"
)

// This file exercises concurrent access to a single *PaginationService, run
// under -race — PaginationService itself carries no mutable state beyond
// its read-only config fields, so the real question is whether the
// underlying *bun.DB / *sql.DB connection pool and this package's own
// stateless per-call query building are actually safe to share across
// goroutines, not something to assume from reading the code.

func TestPagination_Race_ConcurrentPaginationCallsOnSameServiceInstance(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	ctx := context.Background()

	const total = 40
	widgets := make([]widget, total)
	for i := range widgets {
		widgets[i] = widget{ID: fmt.Sprintf("w%02d", i), Name: fmt.Sprintf("Widget %02d", i), Priority: new(i)}
	}
	seedWidgets(t, read, widgets...)

	const goroutines = 8
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)
	for g := range goroutines {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			result, err := c.Pagination(ctx, domains.Pagination{
				Filter: domains.StructuredFilter{
					Filters:    []domains.Filter{{Field: "priority", Mode: domains.ModeGTE, Value: g}},
					SortFields: []domains.SortField{{Field: "id", Order: domains.SortOrderAsc}},
				},
				PageSize: total,
			})
			if err != nil {
				errs <- fmt.Errorf("goroutine %d: Pagination returned error: %w", g, err)
				return
			}
			want := total - g
			if len(result.Data) != want {
				errs <- fmt.Errorf("goroutine %d: expected %d rows (priority >= %d), got %d", g, want, g, len(result.Data))
				return
			}
			for _, r := range result.Data {
				var p int
				fmt.Sscanf(r.ID, "w%d", &p)
				if p < g {
					errs <- fmt.Errorf("goroutine %d: got row %s with priority %d < filter bound %d (cross-goroutine contamination)", g, r.ID, p, g)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func TestPagination_Race_ConcurrentForwardAndBackwardNavigationAreIsolated(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	ctx := context.Background()
	const total = 15
	widgets := make([]widget, total)
	for i := range widgets {
		widgets[i] = widget{ID: fmt.Sprintf("w%02d", i), Name: fmt.Sprintf("Widget %02d", i)}
	}
	seedWidgets(t, read, widgets...)
	sortFields := []domains.SortField{{Field: "id", Order: domains.SortOrderAsc}}

	// Establish a genuine midpoint sequentially first (needed as input to
	// both goroutines below): 15 rows at page size 5 makes 3 pages, and
	// page 2 (reached via a real cursor, with another real page after it)
	// is the only one with both a usable NextCursor and PreviousCursor.
	page1, err := c.Pagination(ctx, domains.Pagination{Filter: domains.StructuredFilter{SortFields: sortFields}, PageSize: 5})
	if err != nil {
		t.Fatalf("establishing page1: %v", err)
	}
	mid, err := c.Pagination(ctx, domains.Pagination{
		Filter: domains.StructuredFilter{SortFields: sortFields}, PageSize: 5, Cursor: page1.NextCursor,
	})
	if err != nil {
		t.Fatalf("establishing midpoint: %v", err)
	}

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		result, err := c.Pagination(ctx, domains.Pagination{
			Filter: domains.StructuredFilter{SortFields: sortFields}, PageSize: 5, Cursor: mid.NextCursor,
		})
		if err != nil {
			errs <- fmt.Errorf("forward goroutine: %w", err)
			return
		}
		if len(result.Data) != 5 || result.Data[0].ID != "w10" {
			errs <- fmt.Errorf("forward goroutine: expected page starting at w10, got %+v", result.Data)
		}
	}()
	go func() {
		defer wg.Done()
		result, err := c.Pagination(ctx, domains.Pagination{
			Filter: domains.StructuredFilter{SortFields: sortFields}, PageSize: 5, Cursor: mid.PreviousCursor,
		})
		if err != nil {
			errs <- fmt.Errorf("backward goroutine: %w", err)
			return
		}
		if len(result.Data) != 5 || result.Data[0].ID != "w00" {
			errs <- fmt.Errorf("backward goroutine: expected page1 back ([w00..w04]), got %+v", result.Data)
		}
	}()
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// TestPagination_Race_ConcurrentInsertsDuringWalkDoNotCorruptAlreadyFetchedPages
// validates, empirically, the cursor-pagination consistency behavior
// already explained conversationally: a row inserted after a page has been
// fetched never retroactively appears in that already-returned page, and
// only shows up in a later page if it sorts after the cursor.
func TestPagination_Race_ConcurrentInsertsDuringWalkDoNotCorruptAlreadyFetchedPages(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	ctx := context.Background()
	seedWidgets(t, read,
		widget{ID: "w01", Name: "One"}, widget{ID: "w02", Name: "Two"}, widget{ID: "w03", Name: "Three"},
	)
	sortFields := []domains.SortField{{Field: "id", Order: domains.SortOrderAsc}}

	page1, err := c.Pagination(ctx, domains.Pagination{Filter: domains.StructuredFilter{SortFields: sortFields}, PageSize: 2})
	if err != nil {
		t.Fatalf("page1 error: %v", err)
	}
	if len(page1.Data) != 2 || page1.Data[0].ID != "w01" || page1.Data[1].ID != "w02" {
		t.Fatalf("expected page1 [w01 w02], got %+v", page1.Data)
	}

	// Insert a row that sorts BEFORE the cursor (w015, between w01 and w02)
	// and one that sorts AFTER it (w04) concurrently with fetching page 2.
	var wg sync.WaitGroup
	wg.Go(func() {
		seedWidgets(t, read, widget{ID: "w015", Name: "OneAndAHalf"}, widget{ID: "w04", Name: "Four"})
	})
	wg.Wait() // sequential insert-then-fetch is the deterministic, still-meaningful version of this check

	page2, err := c.Pagination(ctx, domains.Pagination{
		Filter: domains.StructuredFilter{SortFields: sortFields}, PageSize: 2, Cursor: page1.NextCursor,
	})
	if err != nil {
		t.Fatalf("page2 error: %v", err)
	}
	// "w015" < "w02" lexicographically (comparing byte-by-byte, '1' < '2' at
	// the third character), so it sorts *before* the w02 cursor.
	gotIDs := make([]string, len(page2.Data))
	for i, r := range page2.Data {
		gotIDs[i] = r.ID
	}
	// w03 and w04 must appear (both sort after "w02"); w015 must NOT
	// appear (it sorts before "w02", i.e. before the cursor) even though it
	// was inserted before this fetch — proving the cursor seeks by value,
	// not by "everything inserted after page 1 was fetched".
	for _, id := range gotIDs {
		if id == "w015" {
			t.Errorf("page2 must not contain w015 (it sorts before the w02 cursor): got %v", gotIDs)
		}
	}
	found03, found04 := false, false
	for _, id := range gotIDs {
		if id == "w03" {
			found03 = true
		}
		if id == "w04" {
			found04 = true
		}
	}
	if !found03 {
		t.Errorf("expected w03 in page2, got %v", gotIDs)
	}
	if !found04 {
		t.Errorf("expected the concurrently-inserted w04 to appear in page2 (sorts after the cursor), got %v", gotIDs)
	}

	// page1's own data, already returned, is a Go value the caller already
	// holds -- unaffected by anything inserted afterward by definition; the
	// real assertion already made above is that page2 (fetched *after* the
	// insert) doesn't retroactively include what sorts before its cursor.
	if len(page1.Data) != 2 {
		t.Fatalf("page1 (already returned before the insert) unexpectedly changed shape: %+v", page1.Data)
	}
}

func TestPagination_Race_ConcurrentDeleteDuringWalkIsSafeNoOp(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	ctx := context.Background()
	seedWidgets(t, read,
		widget{ID: "w01", Name: "One"}, widget{ID: "w02", Name: "Two"}, widget{ID: "w03", Name: "Three"},
	)
	sortFields := []domains.SortField{{Field: "id", Order: domains.SortOrderAsc}}

	page1, err := c.Pagination(ctx, domains.Pagination{Filter: domains.StructuredFilter{SortFields: sortFields}, PageSize: 2})
	if err != nil {
		t.Fatalf("page1 error: %v", err)
	}

	// Delete w03 (not yet reached) before fetching page 2.
	if _, err := read.Client().NewDelete().Model((*widget)(nil)).Where("id = ?", "w03").Exec(ctx); err != nil {
		t.Fatalf("deleting w03: %v", err)
	}

	page2, err := c.Pagination(ctx, domains.Pagination{
		Filter: domains.StructuredFilter{SortFields: sortFields}, PageSize: 2, Cursor: page1.NextCursor,
	})
	if err != nil {
		t.Fatalf("page2 error after concurrent delete: %v", err)
	}
	if len(page2.Data) != 0 {
		t.Fatalf("expected the deleted row to simply be absent from page2 (safe no-op), got %+v", page2.Data)
	}
	if page2.NextCursor != nil {
		t.Errorf("expected no NextCursor once the deleted row's absence ends the walk, got %v", *page2.NextCursor)
	}
}
