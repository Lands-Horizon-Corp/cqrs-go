package regression

import (
	"context"
	"database/sql"
	"errors"
	"testing"
)

func TestDeleteByID_HappyPath_RemovesRow(t *testing.T) {
	c, write := newTestCQRS(t)
	ctx := context.Background()
	seedWidget(t, c, widget{ID: "w1", Name: "n"})

	if err := c.DeleteByID(ctx, "w1"); err != nil {
		t.Fatalf("DeleteByID returned error: %v", err)
	}

	if _, ok := readWidgetFrom(t, write, "w1"); ok {
		t.Error("expected row to be deleted")
	}
}

func TestDeleteMany_HappyPath_RemovesOnlyGivenIDs(t *testing.T) {
	c, write := newTestCQRS(t)
	ctx := context.Background()
	seedWidget(t, c, widget{ID: "a", Name: "n"})
	seedWidget(t, c, widget{ID: "b", Name: "n"})
	seedWidget(t, c, widget{ID: "c", Name: "n"})

	if err := c.DeleteMany(ctx, []string{"a", "c"}); err != nil {
		t.Fatalf("DeleteMany returned error: %v", err)
	}

	count, err := write.db.NewSelect().Model((*widget)(nil)).Count(ctx)
	if err != nil {
		t.Fatalf("counting rows: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 row remaining, got %d", count)
	}
	if got, ok := readWidgetFrom(t, write, "b"); !ok || got.ID != "b" {
		t.Errorf("expected widget 'b' to survive, got %+v (found=%v)", got, ok)
	}
}

func TestDeleteByID_SadPath_NonexistentIDReturnsErrNoRows(t *testing.T) {
	c, _ := newTestCQRS(t)
	err := c.DeleteByID(context.Background(), "missing")
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected sql.ErrNoRows, got %v", err)
	}
}

func TestDeleteByID_SadPath_DBErrorIsWrapped(t *testing.T) {
	c, write := newTestCQRS(t)
	seedWidget(t, c, widget{ID: "w1", Name: "n"})
	dropWidgetsTable(t, write)

	err := c.DeleteByID(context.Background(), "w1")
	if err == nil {
		t.Fatal("expected a DB error once the table is gone, got nil")
	}
	if errors.Is(err, sql.ErrNoRows) {
		t.Fatal("expected a generic DB error, not ErrNoRows")
	}
}

func TestDeleteMany_SadPath_DBErrorIsWrapped(t *testing.T) {
	c, write := newTestCQRS(t)
	seedWidget(t, c, widget{ID: "w1", Name: "n"})
	dropWidgetsTable(t, write)

	if err := c.DeleteMany(context.Background(), []string{"w1"}); err == nil {
		t.Fatal("expected a DB error once the table is gone, got nil")
	}
}

func TestDeleteMany_SadPath_EmptyInputIsANoOp(t *testing.T) {
	c, write := newTestCQRS(t)
	ctx := context.Background()
	seedWidget(t, c, widget{ID: "w1", Name: "n"})

	if err := c.DeleteMany(ctx, nil); err != nil {
		t.Fatalf("expected no error for empty id list, got %v", err)
	}

	count, err := write.db.NewSelect().Model((*widget)(nil)).Count(ctx)
	if err != nil {
		t.Fatalf("counting rows: %v", err)
	}
	if count != 1 {
		t.Errorf("expected the seeded row to be untouched, got count %d", count)
	}
}

func TestDeleteByID_PoisonPill_AdversarialIDIsSafeNoOp(t *testing.T) {
	c, write := newTestCQRS(t)
	ctx := context.Background()
	seedWidget(t, c, widget{ID: "w1", Name: "n"})

	err := c.DeleteByID(ctx, "'; DROP TABLE widgets; --")
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected sql.ErrNoRows for adversarial nonexistent id, got %v", err)
	}

	// If the adversarial string had broken out of parameterization, this
	// table would no longer exist and the count below would error instead
	// of returning 1.
	count, err := write.db.NewSelect().Model((*widget)(nil)).Count(ctx)
	if err != nil {
		t.Fatalf("widgets table was affected by adversarial id (parameterization failed?): %v", err)
	}
	if count != 1 {
		t.Errorf("expected the seeded row to be untouched, got count %d", count)
	}
}
