package regression

import (
	"context"
	"strings"
	"testing"
)

// TestCreate_HappyPath_ReturnsTDataDirectlyNotResource verifies Create's
// return type is *widget (TData), not *widgetResource (TResponse) — the
// `var _ *widget = res` line is a compile-time pin on that, since Go would
// simply fail to build this file if Create's signature ever regressed back
// to returning *TResponse. CreateFormat (tested elsewhere in this file) is
// the one that goes through ToResource; Create never does, even though
// ToResource is configured here via newTestCQRS.
func TestCreate_HappyPath_ReturnsTDataDirectlyNotResource(t *testing.T) {
	t.Parallel()
	c, write := newTestCQRS(t)
	ctx := context.Background()

	res, err := c.Create(ctx, widget{ID: "w1", Name: "gadget", Active: true})
	if err != nil {
		t.Fatalf("Create returned error: %v", err)
	}
	var _ *widget = res
	if res == nil || res.ID != "w1" || res.Name != "gadget" || !res.Active {
		t.Fatalf("unexpected TData: %+v", res)
	}
	if got, ok := readWidgetFrom(t, write, "w1"); !ok || got.Name != "gadget" {
		t.Fatalf("expected row to exist in write db, got %+v (found=%v)", got, ok)
	}
}

func TestCreateMany_HappyPath_ReturnsTDataDirectlyNotResource(t *testing.T) {
	t.Parallel()
	c, _ := newTestCQRS(t)
	ctx := context.Background()

	res, err := c.CreateMany(ctx, []widget{{ID: "a", Name: "one"}, {ID: "b", Name: "two"}})
	if err != nil {
		t.Fatalf("CreateMany returned error: %v", err)
	}
	var _ []*widget = res
	if len(res) != 2 || res[0].Name != "one" || res[1].Name != "two" {
		t.Fatalf("unexpected TData slice: %+v", res)
	}
}

func TestCreate_HappyPath_InsertsAndReturnsResource(t *testing.T) {
	t.Parallel()
	c, write := newTestCQRS(t)
	ctx := context.Background()

	res, err := c.CreateFormat(ctx, widget{
		ID:       "w1",
		Name:     "gadget",
		Active:   true,
		Featured: new(true),
	})
	if err != nil {
		t.Fatalf("Create returned error: %v", err)
	}
	if res == nil || res.ID != "w1" || res.Name != "gadget" {
		t.Fatalf("unexpected resource: %+v", res)
	}
	if !res.Active {
		t.Errorf("expected Active=true in resource")
	}
	if res.Featured == nil || !*res.Featured {
		t.Errorf("expected Featured=&true in resource, got %v", res.Featured)
	}

	got, ok := readWidgetFrom(t, write, "w1")
	if !ok {
		t.Fatal("expected row to exist in write db")
	}
	if !got.Active {
		t.Errorf("expected active=true in db, got false")
	}
	if got.Featured == nil || !*got.Featured {
		t.Errorf("expected featured=true in db, got %v", got.Featured)
	}
}

func TestCreate_HappyPath_NilToResourceReturnsNilWithoutError(t *testing.T) {
	t.Parallel()
	write := newFakeSQLService(t)
	c := newCQRSNoResource(t, write)
	res, err := c.CreateFormat(context.Background(), widget{ID: "w1", Name: "gadget"})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if res != nil {
		t.Errorf("expected nil resource when ToResource is unset, got %+v", res)
	}
}

func TestCreate_SadPath_ValidationAndConstraintFailures(t *testing.T) {
	t.Parallel()
	t.Run("Validation Failure Blocks Insert", func(t *testing.T) {
		c, write := newTestCQRS(t)
		ctx := context.Background()

		_, err := c.CreateFormat(ctx, widget{ID: "w1"}) // Name is required
		if err == nil {
			t.Fatal("expected validation error for empty Name, got nil")
		}

		count, err := write.db.NewSelect().Model((*widget)(nil)).Count(ctx)
		if err != nil {
			t.Fatalf("counting rows: %v", err)
		}
		if count != 0 {
			t.Errorf("expected no row inserted after validation failure, got %d", count)
		}
	})

	t.Run("Duplicate Primary Key Fails", func(t *testing.T) {
		c, _ := newTestCQRS(t)
		ctx := context.Background()

		if _, err := c.CreateFormat(ctx, widget{ID: "dup", Name: "first"}); err != nil {
			t.Fatalf("first insert failed: %v", err)
		}
		_, err := c.CreateFormat(ctx, widget{ID: "dup", Name: "second"})
		if err == nil {
			t.Fatal("expected error inserting duplicate primary key, got nil")
		}
	})
}

func TestCreate_PoisonPill_ZeroAndAbsentValues(t *testing.T) {
	t.Parallel()
	t.Run("Zero Value Bool Persists As False, Not Dropped", func(t *testing.T) {
		c, write := newTestCQRS(t)
		ctx := context.Background()

		// Active left at its zero value (false) and every pointer field
		// left nil ("absent"): confirm these are actually written, not
		// silently skipped or coerced into something else.
		if _, err := c.CreateFormat(ctx, widget{ID: "w1", Name: "n"}); err != nil {
			t.Fatalf("Create returned error: %v", err)
		}

		got, ok := readWidgetFrom(t, write, "w1")
		if !ok {
			t.Fatal("expected row to exist")
		}
		if got.Active {
			t.Errorf("expected active=false (zero value) to persist, got true")
		}
		if got.Featured != nil {
			t.Errorf("expected featured=nil (absent) to persist as NULL, got %v", *got.Featured)
		}
		if got.Notes != nil {
			t.Errorf("expected notes=nil to persist as NULL, got %v", *got.Notes)
		}
		if got.Priority != nil {
			t.Errorf("expected priority=nil to persist as NULL, got %v", *got.Priority)
		}
		if got.ExpiresAt != nil {
			t.Errorf("expected expires_at=nil to persist as NULL, got %v", *got.ExpiresAt)
		}
	})

	t.Run("All Pointer Fields Populated Persist Correctly", func(t *testing.T) {
		c, write := newTestCQRS(t)
		ctx := context.Background()

		if _, err := c.CreateFormat(ctx, widget{
			ID: "w1", Name: "n",
			Featured: new(false), // explicitly false, not nil
			Notes:    new(""),    // explicitly empty string, not nil
			Priority: new(0),     // explicitly zero, not nil
		}); err != nil {
			t.Fatalf("Create returned error: %v", err)
		}

		got, ok := readWidgetFrom(t, write, "w1")
		if !ok {
			t.Fatal("expected row to exist")
		}
		if got.Featured == nil || *got.Featured {
			t.Errorf("expected featured=&false (not NULL), got %v", got.Featured)
		}
		if got.Notes == nil || *got.Notes != "" {
			t.Errorf("expected notes=&\"\" (not NULL), got %v", got.Notes)
		}
		if got.Priority == nil || *got.Priority != 0 {
			t.Errorf("expected priority=&0 (not NULL), got %v", got.Priority)
		}
	})

	t.Run("Adversarial String Content Is Stored Literally", func(t *testing.T) {
		c, write := newTestCQRS(t)
		ctx := context.Background()

		poison := "'; DROP TABLE widgets; --"
		if _, err := c.CreateFormat(ctx, widget{ID: "w1", Name: poison}); err != nil {
			t.Fatalf("Create returned error: %v", err)
		}

		got, ok := readWidgetFrom(t, write, "w1")
		if !ok {
			t.Fatal("table was affected by adversarial input (parameterization failed?)")
		}
		if got.Name != poison {
			t.Errorf("expected adversarial string stored verbatim, got %q", got.Name)
		}
		if !strings.Contains(got.Name, "DROP TABLE") {
			t.Fatal("sanity check on test itself failed")
		}
	})
}

func TestCreateMany_HappyPath_InsertsAllAndReturnsResources(t *testing.T) {
	t.Parallel()
	c, write := newTestCQRS(t)
	ctx := context.Background()

	res, err := c.CreateManyFormat(ctx, []widget{
		{ID: "a", Name: "one", Active: true},
		{ID: "b", Name: "two", Featured: new(false)},
	})
	if err != nil {
		t.Fatalf("CreateMany returned error: %v", err)
	}
	if len(res) != 2 {
		t.Fatalf("expected 2 resources, got %d", len(res))
	}

	count, err := write.db.NewSelect().Model((*widget)(nil)).Count(ctx)
	if err != nil {
		t.Fatalf("counting rows: %v", err)
	}
	if count != 2 {
		t.Errorf("expected 2 rows in db, got %d", count)
	}
}

func TestCreateMany_SadPath_EmptyInputIsANoOp(t *testing.T) {
	t.Parallel()
	c, _ := newTestCQRS(t)
	res, err := c.CreateManyFormat(context.Background(), nil)
	if err != nil {
		t.Fatalf("expected no error for empty input, got %v", err)
	}
	if len(res) != 0 {
		t.Errorf("expected empty result, got %d entries", len(res))
	}
}

func TestCreateMany_SadPath_ValidationFailureBlocksWholeBatch(t *testing.T) {
	t.Parallel()
	c, write := newTestCQRS(t)
	ctx := context.Background()

	// The second item is invalid; nothing in the batch should be inserted.
	_, err := c.CreateManyFormat(ctx, []widget{
		{ID: "a", Name: "ok"},
		{ID: "b", Name: ""}, // Name required
	})
	if err == nil {
		t.Fatal("expected validation error, got nil")
	}
	count, err := write.db.NewSelect().Model((*widget)(nil)).Count(ctx)
	if err != nil {
		t.Fatalf("counting rows: %v", err)
	}
	if count != 0 {
		t.Errorf("expected no rows inserted after validation failure, got %d", count)
	}
}

func TestCreateMany_SadPath_DBErrorOnDuplicateID(t *testing.T) {
	t.Parallel()
	c, _ := newTestCQRS(t)
	ctx := context.Background()

	_, err := c.CreateManyFormat(ctx, []widget{
		{ID: "dup", Name: "one"},
		{ID: "dup", Name: "two"},
	})
	if err == nil {
		t.Fatal("expected a DB error for duplicate primary keys in the same batch, got nil")
	}
}

func TestCreateMany_HappyPath_NilToResourceReturnsNilResponses(t *testing.T) {
	t.Parallel()
	write := newFakeSQLService(t)
	c := newCQRSNoResource(t, write)

	res, err := c.CreateManyFormat(context.Background(), []widget{{ID: "a", Name: "n"}})
	if err != nil {
		t.Fatalf("CreateMany returned error: %v", err)
	}
	if res != nil {
		t.Errorf("expected nil responses when ToResource is unset, got %+v", res)
	}
}
