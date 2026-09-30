package regression

import (
	"context"
	"testing"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/domains"
)

// This file verifies *pagination.PaginationService satisfies
// domains.PaginationService[TData, TRequest, TID] — an interface a consumer
// can depend on instead of the concrete struct — and that each of its four
// methods (Paginate/PaginateFilter/Filter/FilterWithTx) behaves as
// documented, not just that they compile. Both the concrete
// pagination.PaginationService and this interface hand back raw TData
// (widget here), never a ToResource-converted view.

// asPaginationServiceInterface fails to compile if *pagination.PaginationService
// ever stops satisfying domains.PaginationService[widget,  string] — a
// static assertion, exercised through an actual variable of the interface
// type so every test below also proves the assignment works, not just the
// type.
func asPaginationServiceInterface(t *testing.T) (domains.PaginationService[widget, string], *fakeSQLService) {
	t.Helper()
	c, read := newPaginationQueryTestCQRS(t)
	var iface domains.PaginationService[widget, string] = c
	return iface, read
}

func TestPagination_HappyPath_PaginateReturnsFirstPageByValue(t *testing.T) {
	t.Parallel()
	c, read := asPaginationServiceInterface(t)
	seedWidgets(t, read, widget{ID: "w1", Name: "Alpha"}, widget{ID: "w2", Name: "Beta"})

	result, err := c.Paginate(context.Background(), domains.Pagination{})
	if err != nil {
		t.Fatalf("Paginate returned error: %v", err)
	}
	if len(result.Data) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Data))
	}
}

func TestPagination_SadPath_PaginateReturnsErrorNotPanicOnUnknownSortField(t *testing.T) {
	t.Parallel()
	c, read := asPaginationServiceInterface(t)
	seedWidgets(t, read, widget{ID: "w1", Name: "Alpha"})

	_, err := c.Paginate(context.Background(), domains.Pagination{
		Filter: domains.StructuredFilter{SortFields: []domains.SortField{{Field: "not_a_real_column"}}},
	})
	if err == nil {
		t.Fatal("expected an error for an unknown sort field, got nil")
	}
}

// TestPagination_HappyPath_PaginateFilterCombinesBothFiltersWithAnd is the
// whole reason PaginateFilter takes filter as a separate parameter from
// pagination: filter is typically backend-hardcoded (e.g. tenant scoping),
// pagination.Filter is typically frontend-supplied — neither should
// override the other, both must apply together as
// "(filter) AND (pagination.Filter)".
func TestPagination_HappyPath_PaginateFilterCombinesBothFiltersWithAnd(t *testing.T) {
	t.Parallel()
	c, read := asPaginationServiceInterface(t)
	seedWidgets(t, read,
		widget{ID: "w1", Name: "Alpha", Priority: new(1)},
		widget{ID: "w2", Name: "Alpha", Priority: new(2)},
		widget{ID: "w3", Name: "Beta", Priority: new(1)},
	)

	// filter (hardcoded): name = "Alpha". pagination.Filter (frontend):
	// priority = 1. Only w1 satisfies both.
	result, err := c.PaginateFilter(
		context.Background(),
		domains.StructuredFilter{Filters: []domains.Filter{{Field: "name", Mode: domains.ModeEqual, Value: "Alpha"}}},
		domains.Pagination{
			Filter: domains.StructuredFilter{Filters: []domains.Filter{{Field: "priority", Mode: domains.ModeEqual, Value: 1}}},
		},
	)
	if err != nil {
		t.Fatalf("PaginateFilter returned error: %v", err)
	}
	if len(result.Data) != 1 || result.Data[0].ID != "w1" {
		t.Fatalf("expected only [w1] (name=Alpha AND priority=1), got %+v", result.Data)
	}
}

// TestPagination_HappyPath_PaginateFilterCombinesFiltersOnMixedDirectionPath
// is the same "(filter) AND (pagination.Filter)" guarantee, but on
// paginateMixedDirection's own code path (a genuinely mixed ascending/
// descending multi-column sort, walked backward via a real cursor) rather
// than the uniform row-value-comparison path the test above exercises —
// each UNION ALL branch needs the hardcoded filter applied too, or a
// backward walk on a mixed-direction sort would silently bypass it.
func TestPagination_HappyPath_PaginateFilterCombinesFiltersOnMixedDirectionPath(t *testing.T) {
	t.Parallel()
	c, read := asPaginationServiceInterface(t)
	ctx := context.Background()
	seedWidgets(t, read,
		widget{ID: "b", Name: "Bravo", Priority: new(5)},
		widget{ID: "c", Name: "Charlie", Priority: new(5)},
		widget{ID: "a", Name: "Alpha", Priority: new(9)},
	)
	sortFields := []domains.SortField{
		{Field: "priority", Order: domains.SortOrderDesc},
		{Field: "name", Order: domains.SortOrderAsc},
	}

	page1, err := c.Paginate(ctx, domains.Pagination{Filter: domains.StructuredFilter{SortFields: sortFields}, PageSize: 1})
	if err != nil {
		t.Fatalf("page1 error: %v", err)
	}
	page2, err := c.Paginate(ctx, domains.Pagination{
		Filter: domains.StructuredFilter{SortFields: sortFields}, PageSize: 1, Cursor: page1.NextCursor,
	})
	if err != nil {
		t.Fatalf("page2 error: %v", err)
	}
	if page2.Data[0].ID != "b" {
		t.Fatalf("expected page2 to be [b], got %+v", page2.Data)
	}

	// Walking backward from here without any hardcoded filter reproduces
	// page1's row [a] (same as TestPagination_HappyPath_BackwardDirectionWithMixedDirectionSort).
	// A hardcoded filter excluding "a" must still suppress it here too.
	back, err := c.PaginateFilter(ctx,
		domains.StructuredFilter{Filters: []domains.Filter{{Field: "id", Mode: domains.ModeNotEqual, Value: "a"}}},
		domains.Pagination{Filter: domains.StructuredFilter{SortFields: sortFields}, PageSize: 1, Cursor: page2.PreviousCursor},
	)
	if err != nil {
		t.Fatalf("backward PaginateFilter error: %v", err)
	}
	if len(back.Data) != 0 {
		t.Fatalf("expected the hardcoded filter to exclude \"a\" on the mixed-direction backward path, got %+v", back.Data)
	}
}

func TestPagination_HappyPath_FilterUsesDefaultPagination(t *testing.T) {
	t.Parallel()
	c, read := asPaginationServiceInterface(t)
	seedWidgets(t, read, widget{ID: "w1", Name: "Alpha"}, widget{ID: "w2", Name: "Beta"})

	result, err := c.Filter(context.Background(), domains.StructuredFilter{
		Filters: []domains.Filter{{Field: "name", Mode: domains.ModeEqual, Value: "Alpha"}},
	})
	if err != nil {
		t.Fatalf("Filter returned error: %v", err)
	}
	if len(result.Data) != 1 || result.Data[0].ID != "w1" {
		t.Fatalf("expected [w1], got %+v", result.Data)
	}
	if result.PageSize != 30 {
		t.Fatalf("expected Filter to fall back to the default PageSize 30, got %d", result.PageSize)
	}
}

func TestPagination_HappyPath_FilterWithTxSeesUncommittedWritesInTheSameTx(t *testing.T) {
	t.Parallel()
	c, read := asPaginationServiceInterface(t)
	ctx := context.Background()

	// fakeSQLService's underlying *sql.DB is capped at a single connection
	// (SetMaxOpenConns(1), so every query in this suite runs against the
	// same in-memory SQLite instance) — a non-tx query issued while this tx
	// is still open would block forever waiting for a connection the open
	// tx is holding, so this test only asserts visibility inside the tx
	// (FilterWithTx) and after it's gone (rolled back), never concurrently
	// with it open.
	tx, err := read.Client().BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx returned error: %v", err)
	}
	if _, err := tx.NewInsert().Model(&widget{ID: "w1", Name: "InTx"}).Exec(ctx); err != nil {
		t.Fatalf("inserting inside the transaction: %v", err)
	}

	// FilterWithTx, given the same tx, must see the uncommitted row.
	inTx, err := c.FilterWithTx(ctx, &tx, domains.StructuredFilter{})
	if err != nil {
		t.Fatalf("FilterWithTx returned error: %v", err)
	}
	if len(inTx.Data) != 1 || inTx.Data[0].ID != "w1" {
		t.Fatalf("expected FilterWithTx to see the uncommitted row, got %+v", inTx.Data)
	}

	if err := tx.Rollback(); err != nil {
		t.Fatalf("rolling back: %v", err)
	}

	// Now that the tx is gone, a normal (non-tx) read must not see the
	// rolled-back row.
	after, err := c.Filter(ctx, domains.StructuredFilter{})
	if err != nil {
		t.Fatalf("Filter (after rollback) returned error: %v", err)
	}
	if len(after.Data) != 0 {
		t.Fatalf("expected the rolled-back row to be gone, got %+v", after.Data)
	}
}

// TestPagination_SadPath_FilterWithTxUnknownFieldErrorsRatherThanDropping
// documents a deliberate asymmetry: FilterWithTx's filter argument is the
// backend-hardcoded side of "(filter) AND (pagination.Filter)" (see
// PaginateFilter), so it's trusted code, not untrusted client input — an
// unknown field in it is a real bug worth a hard error, not something to
// silently drop-and-warn the way normalizeFilters treats a frontend
// filter's unknown field.
func TestPagination_SadPath_FilterWithTxUnknownFieldErrorsRatherThanDropping(t *testing.T) {
	t.Parallel()
	c, read := asPaginationServiceInterface(t)
	ctx := context.Background()
	seedWidgets(t, read, widget{ID: "w1", Name: "Alpha"})

	tx, err := read.Client().BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx returned error: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	_, err = c.FilterWithTx(ctx, &tx,
		domains.StructuredFilter{Filters: []domains.Filter{{Field: "not_a_real_column", Mode: domains.ModeEqual, Value: "x"}}},
	)
	if err == nil {
		t.Fatal("expected an error for an unknown field in the hardcoded filter, got nil")
	}
}

func TestPagination_SadPath_FilterWithTxReturnsErrorWhenColumnDefaultIDIsEmpty(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	ctx := context.Background()

	tx, err := read.Client().BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx returned error: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Bypass the constructor's ColumnDefaultID default, same class of setup
	// error checkReady guards Pagination against.
	c.ColumnDefaultID = ""
	if _, err := c.FilterWithTx(ctx, &tx, domains.StructuredFilter{}); err == nil {
		t.Fatal("expected an error for an empty ColumnDefaultID, got nil")
	}
}
