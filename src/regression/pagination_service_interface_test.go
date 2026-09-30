package regression

import (
	"context"
	"testing"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/domains"
)

// This file verifies *pagination.PaginationService satisfies
// domains.PaginationService[T] — the single-type-parameter interface a
// consumer can depend on without knowing about TData/TRequest/TID — and
// that each of its four methods (Paginate/PaginateFilter/Filter/
// FilterWithTx) behaves as documented, not just that they compile.

// asPaginationServiceInterface fails to compile if *pagination.PaginationService
// ever stops satisfying domains.PaginationService[widgetResource] — a static
// assertion, exercised through an actual variable of the interface type so
// every test below also proves the assignment works, not just the type.
func asPaginationServiceInterface(t *testing.T) (domains.PaginationService[widgetResource], *fakeSQLService) {
	t.Helper()
	c, read := newPaginationQueryTestCQRS(t)
	var iface domains.PaginationService[widgetResource] = c
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

func TestPagination_HappyPath_PaginateFilterOverridesPaginationFilterField(t *testing.T) {
	t.Parallel()
	c, read := asPaginationServiceInterface(t)
	seedWidgets(t, read, widget{ID: "w1", Name: "Alpha"}, widget{ID: "w2", Name: "Beta"})

	// pagination.Filter narrows to "Alpha", but the explicit filter argument
	// (narrowing to "Beta") must be what actually applies.
	result, err := c.PaginateFilter(
		context.Background(),
		domains.StructuredFilter{Filters: []domains.Filter{{Field: "name", Mode: domains.ModeEqual, Value: "Beta"}}},
		domains.Pagination{
			Filter: domains.StructuredFilter{Filters: []domains.Filter{{Field: "name", Mode: domains.ModeEqual, Value: "Alpha"}}},
		},
	)
	if err != nil {
		t.Fatalf("PaginateFilter returned error: %v", err)
	}
	if len(result.Data) != 1 || result.Data[0].ID != "w2" {
		t.Fatalf("expected the explicit filter argument to win, got %+v", result.Data)
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
	inTx, err := c.FilterWithTx(ctx, &tx, domains.StructuredFilter{}, domains.Pagination{})
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

func TestPagination_SadPath_FilterWithTxReturnsErrorNotPanicOnUnknownFilterField(t *testing.T) {
	t.Parallel()
	c, read := asPaginationServiceInterface(t)
	ctx := context.Background()

	tx, err := read.Client().BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx returned error: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	_, err = c.FilterWithTx(ctx, &tx,
		domains.StructuredFilter{Filters: []domains.Filter{{Field: "not_a_real_column", Mode: domains.ModeEqual, Value: "x"}}},
		domains.Pagination{},
	)
	if err == nil {
		t.Fatal("expected an error for an unknown filter field, got nil")
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
	if _, err := c.FilterWithTx(ctx, &tx, domains.StructuredFilter{}, domains.Pagination{}); err == nil {
		t.Fatal("expected an error for an empty ColumnDefaultID, got nil")
	}
}
