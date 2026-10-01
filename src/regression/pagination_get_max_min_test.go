package regression

// This file verifies CQRSImpl's GetMax/GetMin/GetMaxWithTx/GetMinWithTx —
// the row matching filter whose field holds the highest/lowest value,
// built on pagination.PaginationService's GetMax/GetMin (an explicit
// SortFields override on top of paginate, see
// pagination.get_max_min.go's extreme helper).

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/domains"
)

func TestGetMax_HappyPath_ReturnsTheRowWithTheHighestFieldValue(t *testing.T) {
	t.Parallel()
	c, write := newTestCQRS(t)
	seedWidgets(t, write,
		widget{ID: "w1", Name: "Alpha", Priority: new(5)},
		widget{ID: "w2", Name: "Beta", Priority: new(20)},
		widget{ID: "w3", Name: "Gamma", Priority: new(10)},
	)

	got, err := c.Max(context.Background(), "priority", domains.StructuredFilter{})
	if err != nil {
		t.Fatalf("GetMax returned error: %v", err)
	}
	if got == nil || got.ID != "w2" {
		t.Fatalf("expected w2 (priority 20, the highest), got %+v", got)
	}
}

func TestGetMin_HappyPath_ReturnsTheRowWithTheLowestFieldValue(t *testing.T) {
	t.Parallel()
	c, write := newTestCQRS(t)
	seedWidgets(t, write,
		widget{ID: "w1", Name: "Alpha", Priority: new(5)},
		widget{ID: "w2", Name: "Beta", Priority: new(20)},
		widget{ID: "w3", Name: "Gamma", Priority: new(10)},
	)

	got, err := c.Min(context.Background(), "priority", domains.StructuredFilter{})
	if err != nil {
		t.Fatalf("GetMin returned error: %v", err)
	}
	if got == nil || got.ID != "w1" {
		t.Fatalf("expected w1 (priority 5, the lowest), got %+v", got)
	}
}

// TestGetMax_HappyPath_FilterScopesWhichRowsAreConsidered confirms the
// max/min search only considers rows matching filter, not the whole table:
// the globally-highest-priority row is excluded by the filter, so the
// highest *matching* row wins instead.
func TestGetMax_HappyPath_FilterScopesWhichRowsAreConsidered(t *testing.T) {
	t.Parallel()
	c, write := newTestCQRS(t)
	seedWidgets(t, write,
		widget{ID: "w1", Name: "Alpha", Active: true, Priority: new(5)},
		widget{ID: "w2", Name: "Beta", Active: false, Priority: new(20)}, // globally highest, but Active=false
		widget{ID: "w3", Name: "Gamma", Active: true, Priority: new(10)},
	)

	got, err := c.Max(context.Background(), "priority", domains.StructuredFilter{
		Filters: []domains.Filter{{Field: "active", Mode: domains.ModeEqual, DataType: domains.DataTypeBool, Value: true}},
	})
	if err != nil {
		t.Fatalf("GetMax returned error: %v", err)
	}
	if got == nil || got.ID != "w3" {
		t.Fatalf("expected w3 (priority 10, highest among Active=true rows), got %+v", got)
	}
}

func TestGetMax_SadPath_NoMatchReturnsErrNoRows(t *testing.T) {
	t.Parallel()
	c, write := newTestCQRS(t)
	seedWidgets(t, write, widget{ID: "w1", Name: "Alpha", Priority: new(1)})

	_, err := c.Max(context.Background(), "priority", domains.StructuredFilter{
		Filters: []domains.Filter{{Field: "name", Mode: domains.ModeEqual, Value: "NoSuchName"}},
	})
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected sql.ErrNoRows, got %v", err)
	}
}

func TestGetMax_PoisonPill_UnknownFieldReturnsError(t *testing.T) {
	t.Parallel()
	c, write := newTestCQRS(t)
	seedWidgets(t, write, widget{ID: "w1", Name: "Alpha", Priority: new(1)})

	_, err := c.Max(context.Background(), "not_a_real_column", domains.StructuredFilter{})
	if err == nil {
		t.Fatal("expected an error for an unknown field, got nil")
	}
}

func TestGetMaxFormat_HappyPath_ReturnsToResourceConvertedView(t *testing.T) {
	t.Parallel()
	c, write := newTestCQRS(t)
	seedWidgets(t, write,
		widget{ID: "w1", Name: "Alpha", Priority: new(1)},
		widget{ID: "w2", Name: "Beta", Priority: new(9)},
	)

	got, err := c.MaxFormat(context.Background(), "priority", domains.StructuredFilter{})
	if err != nil {
		t.Fatalf("GetMaxFormat returned error: %v", err)
	}
	if got == nil || got.ID != "w2" || got.Name != "Beta" {
		t.Fatalf("expected a widgetResource for w2, got %+v", got)
	}
}

func TestGetMaxWithTx_HappyPath_SeesUncommittedWritesInTheSameTx(t *testing.T) {
	t.Parallel()
	c, write := newTestCQRS(t)
	ctx := context.Background()

	tx, err := write.Client().BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx returned error: %v", err)
	}
	if _, err := tx.NewInsert().Model(&[]widget{
		{ID: "w1", Name: "Alpha", Priority: new(1)},
		{ID: "w2", Name: "Beta", Priority: new(9)},
	}).Exec(ctx); err != nil {
		t.Fatalf("inserting inside the transaction: %v", err)
	}

	inTx, err := c.MaxWithTx(ctx, &tx, "priority", domains.StructuredFilter{})
	if err != nil {
		t.Fatalf("GetMaxWithTx returned error: %v", err)
	}
	if inTx == nil || inTx.ID != "w2" {
		t.Fatalf("expected GetMaxWithTx to see the uncommitted rows and pick w2, got %+v", inTx)
	}

	if err := tx.Rollback(); err != nil {
		t.Fatalf("rolling back: %v", err)
	}

	_, err = c.Max(ctx, "priority", domains.StructuredFilter{})
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected sql.ErrNoRows after rollback, got %v", err)
	}
}

func TestGetMinWithTx_HappyPath_SeesUncommittedWritesInTheSameTx(t *testing.T) {
	t.Parallel()
	c, write := newTestCQRS(t)
	ctx := context.Background()

	tx, err := write.Client().BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx returned error: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.NewInsert().Model(&[]widget{
		{ID: "w1", Name: "Alpha", Priority: new(1)},
		{ID: "w2", Name: "Beta", Priority: new(9)},
	}).Exec(ctx); err != nil {
		t.Fatalf("inserting inside the transaction: %v", err)
	}

	inTx, err := c.MinWithTx(ctx, &tx, "priority", domains.StructuredFilter{})
	if err != nil {
		t.Fatalf("GetMinWithTx returned error: %v", err)
	}
	if inTx == nil || inTx.ID != "w1" {
		t.Fatalf("expected GetMinWithTx to see the uncommitted rows and pick w1, got %+v", inTx)
	}
}
