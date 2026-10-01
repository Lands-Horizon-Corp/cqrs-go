package regression

// This file verifies pagination.PaginationService's Exists/ExistsWithTx —
// the boolean counterpart to Count/CountWithTx tested in
// pagination_count_test.go. Each takes a domains.StructuredFilter (a
// zero-value one asks "does this table have any rows at all") and returns
// just a bool, without ever scanning a matched row back.

import (
	"context"
	"testing"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/domains"
)

func TestPaginationExists_HappyPath_TrueWhenAnyRowExistsIgnoringNoFilter(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	seedWidgets(t, read, widget{ID: "w1", Name: "Alpha"})

	got, err := c.Exists(context.Background(), domains.StructuredFilter{})
	if err != nil {
		t.Fatalf("Exists returned error: %v", err)
	}
	if !got {
		t.Fatal("expected true, got false")
	}
}

func TestPaginationExists_HappyPath_FalseWhenTableIsEmpty(t *testing.T) {
	t.Parallel()
	c, _ := newPaginationQueryTestCQRS(t)
	got, err := c.Exists(context.Background(), domains.StructuredFilter{})
	if err != nil {
		t.Fatalf("Exists returned error: %v", err)
	}
	if got {
		t.Fatal("expected false on an empty table, got true")
	}
}

func TestPaginationExists_SadPath_ReturnsErrorWhenColumnDefaultIDIsEmpty(t *testing.T) {
	t.Parallel()
	c, _ := newPaginationQueryTestCQRS(t)
	c.ColumnDefaultID = "" // bypasses the constructor's default, same class of setup error checkReady guards elsewhere
	if _, err := c.Exists(context.Background(), domains.StructuredFilter{}); err == nil {
		t.Fatal("expected an error for an empty ColumnDefaultID, got nil")
	}
}

func TestPaginationExists_HappyPath_MatchesWhetherFilterFindsAnyRow(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	seedWidgets(t, read,
		widget{ID: "w1", Name: "Alpha", Priority: new(5)},
		widget{ID: "w2", Name: "Beta", Priority: new(15)},
	)
	found := domains.StructuredFilter{Filters: []domains.Filter{{Field: "priority", Mode: domains.ModeGT, Value: 10}}}
	notFound := domains.StructuredFilter{Filters: []domains.Filter{{Field: "priority", Mode: domains.ModeGT, Value: 100}}}

	got, err := c.Exists(context.Background(), found)
	if err != nil {
		t.Fatalf("Exists returned error: %v", err)
	}
	if !got {
		t.Fatal("expected true for a filter matching w2, got false")
	}

	got, err = c.Exists(context.Background(), notFound)
	if err != nil {
		t.Fatalf("Exists returned error: %v", err)
	}
	if got {
		t.Fatal("expected false for a filter matching no rows, got true")
	}
}

// TestPaginationExists_SadPath_UnknownFilterFieldErrorsRatherThanDropping
// mirrors TestPaginationCount_SadPath_UnknownFilterFieldErrorsRatherThanDropping:
// Exists' filter argument is treated the same trusted/hardcoded way
// Filter/FilterWithTx/Count treat theirs, so an unknown field in it is a
// real error, not something to drop.
func TestPaginationExists_SadPath_UnknownFilterFieldErrorsRatherThanDropping(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	seedWidgets(t, read, widget{ID: "w1", Name: "Alpha"})

	_, err := c.Exists(context.Background(), domains.StructuredFilter{
		Filters: []domains.Filter{{Field: "not_a_real_column", Mode: domains.ModeEqual, Value: "x"}},
	})
	if err == nil {
		t.Fatal("expected an error for an unknown field in Exists' filter, got nil")
	}
}

func TestPaginationExists_PoisonPill_UnsupportedFilterModeReturnsError(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	seedWidgets(t, read, widget{ID: "w1", Name: "Alpha"})

	_, err := c.Exists(context.Background(), domains.StructuredFilter{
		Filters: []domains.Filter{{Field: "name", Mode: "bogusMode", Value: "x"}},
	})
	if err == nil {
		t.Fatal("expected an error for an unsupported filter mode, got nil")
	}
}

func TestPaginationExists_HappyPath_ExistsWithTxSeesUncommittedWritesInTheSameTx(t *testing.T) {
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

	inTx, err := c.ExistsWithTx(ctx, &tx, domains.StructuredFilter{})
	if err != nil {
		t.Fatalf("ExistsWithTx returned error: %v", err)
	}
	if !inTx {
		t.Fatal("expected ExistsWithTx to see the uncommitted row, got false")
	}

	if err := tx.Rollback(); err != nil {
		t.Fatalf("rolling back: %v", err)
	}

	after, err := c.Exists(ctx, domains.StructuredFilter{})
	if err != nil {
		t.Fatalf("Exists (after rollback) returned error: %v", err)
	}
	if after {
		t.Fatal("expected the rolled-back row to be gone, got true")
	}
}
