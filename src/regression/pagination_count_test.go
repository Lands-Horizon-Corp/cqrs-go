package regression

// This file verifies pagination.PaginationService's Count/CountWithTx — the
// counting counterparts to Paginate/Filter/FilterWithTx tested in
// pagination_query_test.go and pagination_service_interface_test.go. Each
// takes a domains.StructuredFilter (a zero-value one counts every row) and
// returns just a row count, without ever scanning the matched rows back.

import (
	"context"
	"testing"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/domains"
)

func TestPaginationCount_HappyPath_CountsAllRowsIgnoringNoFilter(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	seedWidgets(t, read, widget{ID: "w1", Name: "Alpha"}, widget{ID: "w2", Name: "Beta"}, widget{ID: "w3", Name: "Gamma"})

	got, err := c.Count(context.Background(), domains.StructuredFilter{})
	if err != nil {
		t.Fatalf("Count returned error: %v", err)
	}
	if got != 3 {
		t.Fatalf("expected 3, got %d", got)
	}
}

func TestPaginationCount_HappyPath_EmptyTableCountsZero(t *testing.T) {
	t.Parallel()
	c, _ := newPaginationQueryTestCQRS(t)
	got, err := c.Count(context.Background(), domains.StructuredFilter{})
	if err != nil {
		t.Fatalf("Count returned error: %v", err)
	}
	if got != 0 {
		t.Fatalf("expected 0, got %d", got)
	}
}

func TestPaginationCount_SadPath_ReturnsErrorWhenColumnDefaultIDIsEmpty(t *testing.T) {
	t.Parallel()
	c, _ := newPaginationQueryTestCQRS(t)
	c.ColumnDefaultID = "" // bypasses the constructor's default, same class of setup error checkReady guards elsewhere
	if _, err := c.Count(context.Background(), domains.StructuredFilter{}); err == nil {
		t.Fatal("expected an error for an empty ColumnDefaultID, got nil")
	}
}

func TestPaginationCount_HappyPath_CountMatchesLenOfEquivalentFilterCall(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	seedWidgets(t, read,
		widget{ID: "w1", Name: "Alpha", Priority: new(5)},
		widget{ID: "w2", Name: "Beta", Priority: new(15)},
		widget{ID: "w3", Name: "Gamma", Priority: new(25)},
	)
	filter := domains.StructuredFilter{Filters: []domains.Filter{{Field: "priority", Mode: domains.ModeGT, Value: 10}}}

	count, err := c.Count(context.Background(), filter)
	if err != nil {
		t.Fatalf("Count returned error: %v", err)
	}
	rows, err := c.Filter(context.Background(), filter)
	if err != nil {
		t.Fatalf("Filter returned error: %v", err)
	}
	if count != int64(len(rows)) {
		t.Fatalf("expected Count (%d) to match len(Filter result) (%d)", count, len(rows))
	}
	if count != 2 {
		t.Fatalf("expected 2 rows with priority > 10, got %d", count)
	}
}

func TestPaginationCount_HappyPath_ZeroMatchesReturnsZeroNotError(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	seedWidgets(t, read, widget{ID: "w1", Name: "Alpha", Priority: new(1)})

	got, err := c.Count(context.Background(), domains.StructuredFilter{
		Filters: []domains.Filter{{Field: "priority", Mode: domains.ModeGT, Value: 100}},
	})
	if err != nil {
		t.Fatalf("Count returned error: %v", err)
	}
	if got != 0 {
		t.Fatalf("expected 0, got %d", got)
	}
}

// TestPaginationCount_SadPath_UnknownFilterFieldErrorsRatherThanDropping
// mirrors TestPagination_SadPath_FilterWithTxUnknownFieldErrorsRatherThanDropping
// in pagination_service_interface_test.go: Count's filter argument is
// treated the same trusted/hardcoded way Filter/FilterWithTx treat theirs,
// so an unknown field in it is a real error, not something to drop.
func TestPaginationCount_SadPath_UnknownFilterFieldErrorsRatherThanDropping(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	seedWidgets(t, read, widget{ID: "w1", Name: "Alpha"}, widget{ID: "w2", Name: "Beta"})

	_, err := c.Count(context.Background(), domains.StructuredFilter{
		Filters: []domains.Filter{{Field: "not_a_real_column", Mode: domains.ModeEqual, Value: "x"}},
	})
	if err == nil {
		t.Fatal("expected an error for an unknown field in Count's filter, got nil")
	}
}

func TestPaginationCount_PoisonPill_UnsupportedFilterModeReturnsError(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	seedWidgets(t, read, widget{ID: "w1", Name: "Alpha"})

	_, err := c.Count(context.Background(), domains.StructuredFilter{
		Filters: []domains.Filter{{Field: "name", Mode: "bogusMode", Value: "x"}},
	})
	if err == nil {
		t.Fatal("expected an error for an unsupported filter mode, got nil")
	}
}

func TestPaginationCount_HappyPath_CountWithTxSeesUncommittedWritesInTheSameTx(t *testing.T) {
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

	inTx, err := c.CountWithTx(ctx, &tx, domains.StructuredFilter{})
	if err != nil {
		t.Fatalf("CountWithTx returned error: %v", err)
	}
	if inTx != 1 {
		t.Fatalf("expected CountWithTx to see the uncommitted row, got %d", inTx)
	}

	if err := tx.Rollback(); err != nil {
		t.Fatalf("rolling back: %v", err)
	}

	after, err := c.Count(ctx, domains.StructuredFilter{})
	if err != nil {
		t.Fatalf("Count (after rollback) returned error: %v", err)
	}
	if after != 0 {
		t.Fatalf("expected the rolled-back row to be gone, got %d", after)
	}
}
