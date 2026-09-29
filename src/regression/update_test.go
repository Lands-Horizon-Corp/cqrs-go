package regression

import (
	"context"
	"database/sql"
	"errors"
	"testing"
)

func TestUpdateByID_HappyPath_ChangesPersist(t *testing.T) {
	c, write := newTestCQRS(t)
	ctx := context.Background()
	seedWidget(t, c, widget{ID: "w1", Name: "old", Active: true, Featured: new(true)})

	res, err := c.UpdateByID(ctx, "w1", widget{ID: "w1", Name: "new", Active: true, Featured: new(true)})
	if err != nil {
		t.Fatalf("UpdateByID returned error: %v", err)
	}
	if res.Name != "new" {
		t.Errorf("expected resource name 'new', got %q", res.Name)
	}
	if got, ok := readWidgetFrom(t, write, "w1"); !ok || got.Name != "new" {
		t.Errorf("expected db name 'new', got %q (found=%v)", got.Name, ok)
	}
}

// TestUpdateByID_NonPointerBool_FalseActuallyPersists is the direct test
// for the "can't update a bool to false" symptom. UpdateByID does a
// full-struct update (bun writes every column from the TData you hand it),
// so setting Active: false in the struct you pass in DOES persist as
// false here.
func TestUpdateByID_NonPointerBool_FalseActuallyPersists(t *testing.T) {
	c, write := newTestCQRS(t)
	ctx := context.Background()
	seedWidget(t, c, widget{ID: "w1", Name: "n", Active: true})

	if got, ok := readWidgetFrom(t, write, "w1"); !ok || !got.Active {
		t.Fatalf("setup invariant broken: expected seeded row to have active=true")
	}

	if _, err := c.UpdateByID(ctx, "w1", widget{ID: "w1", Name: "n", Active: false}); err != nil {
		t.Fatalf("UpdateByID returned error: %v", err)
	}

	if got, ok := readWidgetFrom(t, write, "w1"); !ok || got.Active {
		t.Errorf("expected active=false to persist after explicit update, got %v (found=%v)", got.Active, ok)
	}
}

// TestUpdateByID_PointerBool_NilOverwritesToNULL demonstrates the sharpest
// edge of the pointer/non-pointer bug: UpdateByID takes a full TData
// struct, not a partial patch. There is no "leave this column alone"
// semantics here — if a *bool field is nil when you call UpdateByID, the
// column is overwritten with NULL, even though a nil pointer could
// conceptually have meant "I didn't set this."
func TestUpdateByID_PointerBool_NilOverwritesToNULL(t *testing.T) {
	c, write := newTestCQRS(t)
	ctx := context.Background()
	seedWidget(t, c, widget{ID: "w1", Name: "n", Featured: new(true), Notes: new("hi"), Priority: new(5)})

	if _, err := c.UpdateByID(ctx, "w1", widget{ID: "w1", Name: "n"}); err != nil {
		t.Fatalf("UpdateByID returned error: %v", err)
	}

	got, ok := readWidgetFrom(t, write, "w1")
	if !ok {
		t.Fatal("expected row to still exist")
	}
	if got.Featured != nil {
		t.Errorf("expected featured overwritten to NULL by a nil pointer, got %v", *got.Featured)
	}
	if got.Notes != nil {
		t.Errorf("expected notes overwritten to NULL by a nil pointer, got %v", *got.Notes)
	}
	if got.Priority != nil {
		t.Errorf("expected priority overwritten to NULL by a nil pointer, got %v", *got.Priority)
	}
}

// TestUpdateByID_PointerBool_ExplicitFalseDistinctFromNil is the
// counterpart: a *bool explicitly set to &false is distinguishable from a
// nil *bool (see zerovalue_test.go), and it persists as false here, not
// NULL.
func TestUpdateByID_PointerBool_ExplicitFalseDistinctFromNil(t *testing.T) {
	c, write := newTestCQRS(t)
	ctx := context.Background()
	seedWidget(t, c, widget{ID: "w1", Name: "n", Featured: new(true)})

	if _, err := c.UpdateByID(ctx, "w1", widget{ID: "w1", Name: "n", Featured: new(false)}); err != nil {
		t.Fatalf("UpdateByID returned error: %v", err)
	}

	got, ok := readWidgetFrom(t, write, "w1")
	if !ok {
		t.Fatal("expected row to still exist")
	}
	if got.Featured == nil {
		t.Fatal("expected featured to be false (not NULL), got NULL")
	}
	if *got.Featured {
		t.Errorf("expected featured=false, got true")
	}
}

func TestUpdateByID_SadPath_NonexistentIDAndValidation(t *testing.T) {
	t.Run("Nonexistent ID Returns ErrNoRows", func(t *testing.T) {
		c, _ := newTestCQRS(t)
		_, err := c.UpdateByID(context.Background(), "missing", widget{ID: "missing", Name: "n"})
		if !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("expected sql.ErrNoRows, got %v", err)
		}
	})

	t.Run("Validation Failure Blocks Update", func(t *testing.T) {
		c, write := newTestCQRS(t)
		ctx := context.Background()
		seedWidget(t, c, widget{ID: "w1", Name: "original"})

		_, err := c.UpdateByID(ctx, "w1", widget{ID: "w1", Name: ""}) // Name is required
		if err == nil {
			t.Fatal("expected validation error for empty Name, got nil")
		}
		if got, ok := readWidgetFrom(t, write, "w1"); !ok || got.Name != "original" {
			t.Errorf("expected row untouched after validation failure, got name %q", got.Name)
		}
	})

	t.Run("DB Error Is Wrapped And Returned", func(t *testing.T) {
		c, write := newTestCQRS(t)
		ctx := context.Background()
		seedWidget(t, c, widget{ID: "w1", Name: "n"})
		dropWidgetsTable(t, write)

		_, err := c.UpdateByID(ctx, "w1", widget{ID: "w1", Name: "n2"})
		if err == nil {
			t.Fatal("expected a DB error once the table is gone, got nil")
		}
	})
}

func TestUpdateByID_HappyPath_NilToResourceReturnsNilWithoutError(t *testing.T) {
	write := newFakeSQLService(t)
	c := newCQRSNoResource(t, write)
	ctx := context.Background()
	seedWidget(t, c, widget{ID: "w1", Name: "n"})

	res, err := c.UpdateByID(ctx, "w1", widget{ID: "w1", Name: "n2"})
	if err != nil {
		t.Fatalf("UpdateByID returned error: %v", err)
	}
	if res != nil {
		t.Errorf("expected nil resource when ToResource is unset, got %+v", res)
	}
	if got, ok := readWidgetFrom(t, write, "w1"); !ok || got.Name != "n2" {
		t.Errorf("expected the update to still persist, got %q (found=%v)", got.Name, ok)
	}
}

// TestUpdateByID_PoisonPill_ForgottenFieldsGetClobbered is the scenario
// most likely to bite in real handler code: a caller builds a widget from
// only the fields they meant to change (e.g. just the Name, from a partial
// PATCH-style request body) and forgets that UpdateByID is a full-struct
// write. Every column not explicitly set gets reset to its zero value.
func TestUpdateByID_PoisonPill_ForgottenFieldsGetClobbered(t *testing.T) {
	c, write := newTestCQRS(t)
	ctx := context.Background()
	seedWidget(t, c, widget{
		ID: "w1", Name: "original", Active: true,
		Featured: new(true), Notes: new("hi"), Priority: new(5),
	})

	// Caller only intended to rename the widget, but built a fresh
	// zero-value struct and only set ID/Name.
	if _, err := c.UpdateByID(ctx, "w1", widget{ID: "w1", Name: "renamed"}); err != nil {
		t.Fatalf("UpdateByID returned error: %v", err)
	}

	got, ok := readWidgetFrom(t, write, "w1")
	if !ok {
		t.Fatal("expected row to still exist")
	}
	if got.Name != "renamed" {
		t.Fatalf("expected name to change to 'renamed', got %q", got.Name)
	}
	if got.Active {
		t.Errorf("Active was silently clobbered to false (expected engine behavior, not a test bug)")
	}
	if got.Featured != nil || got.Notes != nil || got.Priority != nil {
		t.Errorf("expected every unset pointer field to be clobbered to NULL, got featured=%v notes=%v priority=%v",
			got.Featured, got.Notes, got.Priority)
	}
}

func TestUpdateMany_HappyPath_UpdatesAllRowsInOneStatement(t *testing.T) {
	c, write := newTestCQRS(t)
	ctx := context.Background()
	seedWidget(t, c, widget{ID: "a", Name: "old-a", Active: false})
	seedWidget(t, c, widget{ID: "b", Name: "old-b", Active: false})

	res, err := c.UpdateMany(ctx, []widget{
		{ID: "a", Name: "new-a", Active: true},
		{ID: "b", Name: "new-b", Active: true},
	})
	if err != nil {
		t.Fatalf("UpdateMany returned error: %v", err)
	}
	if len(res) != 2 {
		t.Fatalf("expected 2 resources, got %d", len(res))
	}

	for _, id := range []string{"a", "b"} {
		got, ok := readWidgetFrom(t, write, id)
		if !ok || got.Name != "new-"+id || !got.Active {
			t.Errorf("expected %s updated to new-%s/active=true, got %+v (found=%v)", id, id, got, ok)
		}
	}
}

func TestUpdateMany_SadPath_EmptyInputIsANoOp(t *testing.T) {
	c, _ := newTestCQRS(t)
	res, err := c.UpdateMany(context.Background(), nil)
	if err != nil {
		t.Fatalf("expected no error for empty input, got %v", err)
	}
	if len(res) != 0 {
		t.Errorf("expected empty result, got %d entries", len(res))
	}
}

func TestUpdateMany_SadPath_ValidationFailureBlocksWholeBatch(t *testing.T) {
	c, write := newTestCQRS(t)
	ctx := context.Background()
	seedWidget(t, c, widget{ID: "a", Name: "original"})

	_, err := c.UpdateMany(ctx, []widget{
		{ID: "a", Name: "ok"},
		{ID: "b", Name: ""}, // Name required
	})
	if err == nil {
		t.Fatal("expected validation error, got nil")
	}
	if got, ok := readWidgetFrom(t, write, "a"); !ok || got.Name != "original" {
		t.Errorf("expected row untouched after validation failure, got name %q", got.Name)
	}
}

// TestUpdateMany_PoisonPill_MissingIDsAreSilentlyIgnored documents the bulk
// semantics explicitly: unlike UpdateByID, a bulk update doesn't surface a
// not-found signal for IDs that don't exist — consistent with DeleteMany's
// existing behavior for the same reason (no individual per-row result).
func TestUpdateMany_PoisonPill_MissingIDsAreSilentlyIgnored(t *testing.T) {
	c, write := newTestCQRS(t)
	ctx := context.Background()
	seedWidget(t, c, widget{ID: "a", Name: "original"})

	_, err := c.UpdateMany(ctx, []widget{
		{ID: "a", Name: "updated"},
		{ID: "ghost", Name: "updated"},
	})
	if err != nil {
		t.Fatalf("expected no error for a mix of existing and missing IDs, got %v", err)
	}
	if got, ok := readWidgetFrom(t, write, "a"); !ok || got.Name != "updated" {
		t.Errorf("expected 'a' to be updated, got %+v (found=%v)", got, ok)
	}
	if _, ok := readWidgetFrom(t, write, "ghost"); ok {
		t.Error("expected 'ghost' to not have been created by the update")
	}
}

func TestUpdateMany_HappyPath_NilToResourceReturnsNilResponses(t *testing.T) {
	write := newFakeSQLService(t)
	c := newCQRSNoResource(t, write)
	ctx := context.Background()
	seedWidget(t, c, widget{ID: "a", Name: "old"})

	res, err := c.UpdateMany(ctx, []widget{{ID: "a", Name: "new"}})
	if err != nil {
		t.Fatalf("UpdateMany returned error: %v", err)
	}
	if res != nil {
		t.Errorf("expected nil responses when ToResource is unset, got %+v", res)
	}
	if got, ok := readWidgetFrom(t, write, "a"); !ok || got.Name != "new" {
		t.Errorf("expected the update to still persist, got %q (found=%v)", got.Name, ok)
	}
}
