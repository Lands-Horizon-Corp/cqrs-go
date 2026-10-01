package regression

// This file verifies CQRSImpl's GetByID/GetByIDFormat/GetByIDWithTx/
// GetByIDWithTxFormat — thin aliases over FindOne/FindOneFormat/
// FindOneWithTx/FindOneWithTxFormat (see pagination_find_one_test.go),
// named to match this package's existing UpdateByID/DeleteByID convention.
// These tests confirm the alias actually resolves ColumnDefaultID and
// behaves identically, not just that it compiles. Unlike
// pagination_find_one_test.go (which exercises *pagination.PaginationService
// directly), GetByID only exists on *cqrs.CQRSImpl, so every test here goes
// through newTestCQRS instead of newPaginationQueryTestCQRS.

import (
	"context"
	"database/sql"
	"errors"
	"testing"
)

func TestGetByID_HappyPath_ReturnsTheRowWithThatID(t *testing.T) {
	t.Parallel()
	c, write := newTestCQRS(t)
	seedWidgets(t, write,
		widget{ID: "w1", Name: "Alpha"},
		widget{ID: "w2", Name: "Beta"},
	)

	got, err := c.GetByID(context.Background(), "w2")
	if err != nil {
		t.Fatalf("GetByID returned error: %v", err)
	}
	if got == nil || got.ID != "w2" || got.Name != "Beta" {
		t.Fatalf("expected w2/Beta, got %+v", got)
	}
}

func TestGetByID_SadPath_UnknownIDReturnsErrNoRows(t *testing.T) {
	t.Parallel()
	c, write := newTestCQRS(t)
	seedWidgets(t, write, widget{ID: "w1", Name: "Alpha"})

	_, err := c.GetByID(context.Background(), "not-a-real-id")
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected sql.ErrNoRows, got %v", err)
	}
}

// TestGetByID_HappyPath_UsesConfiguredColumnDefaultID confirms GetByID
// resolves against ColumnDefaultID (an operator-configurable setting that
// defaults to "id" — see NewCQRS), not some other hard-coded column name.
func TestGetByID_HappyPath_UsesConfiguredColumnDefaultID(t *testing.T) {
	t.Parallel()
	c, write := newTestCQRS(t)
	if c.ColumnDefaultID != "id" {
		t.Fatalf("test fixture assumption broken: expected ColumnDefaultID \"id\", got %q", c.ColumnDefaultID)
	}
	seedWidgets(t, write, widget{ID: "w1", Name: "Alpha"})

	got, err := c.GetByID(context.Background(), "w1")
	if err != nil {
		t.Fatalf("GetByID returned error: %v", err)
	}
	if got == nil || got.ID != "w1" {
		t.Fatalf("expected w1, got %+v", got)
	}
}

func TestGetByIDFormat_HappyPath_ReturnsToResourceConvertedView(t *testing.T) {
	t.Parallel()
	c, write := newTestCQRS(t)
	seedWidgets(t, write, widget{ID: "w1", Name: "Alpha", Active: true})

	got, err := c.GetByIDFormat(context.Background(), "w1")
	if err != nil {
		t.Fatalf("GetByIDFormat returned error: %v", err)
	}
	if got == nil || got.ID != "w1" || got.Name != "Alpha" || !got.Active {
		t.Fatalf("expected a widgetResource for w1, got %+v", got)
	}
}

func TestGetByIDWithTx_HappyPath_SeesUncommittedWritesInTheSameTx(t *testing.T) {
	t.Parallel()
	c, write := newTestCQRS(t)
	ctx := context.Background()

	// Same single-connection caveat as
	// TestPagination_HappyPath_FilterWithTxSeesUncommittedWritesInTheSameTx:
	// only assert visibility inside the tx and after it's gone, never
	// concurrently with it open.
	tx, err := write.Client().BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx returned error: %v", err)
	}
	if _, err := tx.NewInsert().Model(&widget{ID: "w1", Name: "InTx"}).Exec(ctx); err != nil {
		t.Fatalf("inserting inside the transaction: %v", err)
	}

	inTx, err := c.GetByIDWithTx(ctx, &tx, "w1")
	if err != nil {
		t.Fatalf("GetByIDWithTx returned error: %v", err)
	}
	if inTx == nil || inTx.ID != "w1" {
		t.Fatalf("expected GetByIDWithTx to see the uncommitted row, got %+v", inTx)
	}

	if err := tx.Rollback(); err != nil {
		t.Fatalf("rolling back: %v", err)
	}

	_, err = c.GetByID(ctx, "w1")
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected sql.ErrNoRows after rollback, got %v", err)
	}
}

func TestGetByIDWithTxFormat_HappyPath_ReturnsToResourceConvertedView(t *testing.T) {
	t.Parallel()
	c, write := newTestCQRS(t)
	ctx := context.Background()
	seedWidgets(t, write, widget{ID: "w1", Name: "Alpha", Active: true})

	tx, err := write.Client().BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx returned error: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	got, err := c.GetByIDWithTxFormat(ctx, &tx, "w1")
	if err != nil {
		t.Fatalf("GetByIDWithTxFormat returned error: %v", err)
	}
	if got == nil || got.ID != "w1" || got.Name != "Alpha" || !got.Active {
		t.Fatalf("expected a widgetResource for w1, got %+v", got)
	}
}
