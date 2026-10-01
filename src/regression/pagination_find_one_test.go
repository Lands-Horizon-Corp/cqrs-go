package regression

// This file verifies pagination.PaginationService's FindOne/FindOneWithTx —
// the single-row counterpart to Filter/FilterWithTx tested in
// pagination_query_test.go and pagination_service_interface_test.go. Each
// takes a domains.StructuredFilter and returns the first matching row (or
// sql.ErrNoRows), reusing paginate's own filtering/sorting/preload
// machinery under a PageSize of 1. Lookup by primary key specifically is
// GetByID (see pagination_get_by_id_test.go), which is FindOne scoped to a
// single "ColumnDefaultID = id" filter term — not a separate code path
// here.

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/domains"
)

func TestPaginationFindOne_HappyPath_ReturnsTheOnlyMatchingRow(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	seedWidgets(t, read,
		widget{ID: "w1", Name: "Alpha", Priority: new(1)},
		widget{ID: "w2", Name: "Beta", Priority: new(2)},
	)

	got, err := c.FindOne(context.Background(), domains.StructuredFilter{
		Filters: []domains.Filter{{Field: "name", Mode: domains.ModeEqual, Value: "Beta"}},
	})
	if err != nil {
		t.Fatalf("FindOne returned error: %v", err)
	}
	if got == nil || got.ID != "w2" {
		t.Fatalf("expected w2, got %+v", got)
	}
}

// TestPaginationFindOne_HappyPath_MultipleMatchesReturnsDeterministicFirstRow
// confirms "first" isn't an arbitrary row: FindOne shares paginate's
// default sort fallback (ColumnDefaultSort "updated_at DESC", tiebroken by
// ColumnDefaultID DESC — see pagination_count_test.go's sibling note and
// resolveSortFields/defaultSortField in pagination.cursor.go), so ties on
// UpdatedAt fall back to id DESC: "w2" sorts before "w1".
func TestPaginationFindOne_HappyPath_MultipleMatchesReturnsDeterministicFirstRow(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	seedWidgets(t, read,
		widget{ID: "w1", Name: "Alpha", Priority: new(5)},
		widget{ID: "w2", Name: "Beta", Priority: new(5)},
	)

	got, err := c.FindOne(context.Background(), domains.StructuredFilter{
		Filters: []domains.Filter{{Field: "priority", Mode: domains.ModeEqual, Value: 5}},
	})
	if err != nil {
		t.Fatalf("FindOne returned error: %v", err)
	}
	if got == nil || got.ID != "w2" {
		t.Fatalf("expected w2 (id DESC tiebreaker), got %+v", got)
	}
}

func TestPaginationFindOne_SadPath_NoMatchReturnsErrNoRows(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	seedWidgets(t, read, widget{ID: "w1", Name: "Alpha"})

	_, err := c.FindOne(context.Background(), domains.StructuredFilter{
		Filters: []domains.Filter{{Field: "name", Mode: domains.ModeEqual, Value: "NoSuchName"}},
	})
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected sql.ErrNoRows, got %v", err)
	}
}

func TestPaginationFindOne_SadPath_EmptyTableReturnsErrNoRows(t *testing.T) {
	t.Parallel()
	c, _ := newPaginationQueryTestCQRS(t)
	_, err := c.FindOne(context.Background(), domains.StructuredFilter{})
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected sql.ErrNoRows, got %v", err)
	}
}

func TestPaginationFindOne_SadPath_ReturnsErrorWhenColumnDefaultIDIsEmpty(t *testing.T) {
	t.Parallel()
	c, _ := newPaginationQueryTestCQRS(t)
	c.ColumnDefaultID = "" // bypasses the constructor's default, same class of setup error checkReady guards elsewhere
	if _, err := c.FindOne(context.Background(), domains.StructuredFilter{}); err == nil {
		t.Fatal("expected an error for an empty ColumnDefaultID, got nil")
	}
}

// TestPaginationFindOne_SadPath_UnknownFilterFieldErrorsRatherThanDropping
// mirrors TestPagination_SadPath_FilterWithTxUnknownFieldErrorsRatherThanDropping:
// FindOne's filter argument goes through paginate as the hardcoded/trusted
// slot (the same one Filter/FilterWithTx use), not the frontend-lenient
// pagination.Filter slot Pagination/Paginate use — so an unknown field in
// it is a real error, not something to silently drop.
func TestPaginationFindOne_SadPath_UnknownFilterFieldErrorsRatherThanDropping(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	seedWidgets(t, read, widget{ID: "w1", Name: "Alpha"})

	_, err := c.FindOne(context.Background(), domains.StructuredFilter{
		Filters: []domains.Filter{{Field: "not_a_real_column", Mode: domains.ModeEqual, Value: "x"}},
	})
	if err == nil {
		t.Fatal("expected an error for an unknown field in FindOne's filter, got nil")
	}
}

func TestPaginationFindOne_PoisonPill_UnsupportedFilterModeReturnsError(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	seedWidgets(t, read, widget{ID: "w1", Name: "Alpha"})

	_, err := c.FindOne(context.Background(), domains.StructuredFilter{
		Filters: []domains.Filter{{Field: "name", Mode: "bogusMode", Value: "x"}},
	})
	if err == nil {
		t.Fatal("expected an error for an unsupported filter mode, got nil")
	}
}

func TestPaginationFindOne_HappyPath_PreloadIsApplied(t *testing.T) {
	t.Parallel()
	c, db := newPreloadTestCQRS(t)
	ctx := context.Background()
	seedPreloadAuthor(t, db, "a1", "Ada")
	if _, err := c.CreateManyFormat(ctx, []preloadPost{{ID: "p1", Title: "One", AuthorID: "a1"}}); err != nil {
		t.Fatalf("seed CreateMany returned error: %v", err)
	}

	pc := newPaginationPreloadService(db)
	got, err := pc.FindOne(ctx, domains.StructuredFilter{
		Filters: []domains.Filter{{Field: "id", Mode: domains.ModeEqual, Value: "p1"}},
	}, "Author")
	if err != nil {
		t.Fatalf("FindOne returned error: %v", err)
	}
	if got == nil || got.Author == nil || got.Author.Name != "Ada" {
		t.Fatalf("expected Author preloaded with Name 'Ada', got %+v", got)
	}
}

func TestPaginationFindOne_HappyPath_FindOneWithTxSeesUncommittedWritesInTheSameTx(t *testing.T) {
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

	inTx, err := c.FindOneWithTx(ctx, &tx, domains.StructuredFilter{})
	if err != nil {
		t.Fatalf("FindOneWithTx returned error: %v", err)
	}
	if inTx == nil || inTx.ID != "w1" {
		t.Fatalf("expected FindOneWithTx to see the uncommitted row, got %+v", inTx)
	}

	if err := tx.Rollback(); err != nil {
		t.Fatalf("rolling back: %v", err)
	}

	_, err = c.FindOne(ctx, domains.StructuredFilter{})
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected sql.ErrNoRows after rollback, got %v", err)
	}
}
