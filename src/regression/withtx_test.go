package regression

import (
	"context"
	"testing"

	"github.com/uptrace/bun"
)

// These cover the *WithTx variants of Create/Update/Delete, which mirror
// their non-tx counterparts exactly except for running against a caller-
// supplied bun.Tx instead of the service's own client.

func withTx(t *testing.T, write *fakeSQLService, fn func(ctx context.Context, tx bun.Tx) error) {
	t.Helper()
	ctx := context.Background()
	if err := write.db.RunInTx(ctx, nil, fn); err != nil {
		t.Fatalf("transaction failed: %v", err)
	}
}

func TestCreateWithTx_HappyPath_InsertsWithinTransaction(t *testing.T) {
	c, write := newTestCQRS(t)

	withTx(t, write, func(ctx context.Context, tx bun.Tx) error {
		res, err := c.CreateWithTx(ctx, tx, widget{ID: "w1", Name: "n"})
		if err != nil {
			return err
		}
		if res == nil || res.ID != "w1" {
			t.Fatalf("unexpected resource: %+v", res)
		}
		return nil
	})

	if _, ok := readWidgetFrom(t, write, "w1"); !ok {
		t.Fatal("expected row to exist after commit")
	}
}

func TestCreateWithTx_SadPath_ValidationFailureBlocksInsert(t *testing.T) {
	c, write := newTestCQRS(t)

	err := write.db.RunInTx(context.Background(), nil, func(ctx context.Context, tx bun.Tx) error {
		_, err := c.CreateWithTx(ctx, tx, widget{ID: "w1"}) // Name required
		return err
	})
	if err == nil {
		t.Fatal("expected validation error, got nil")
	}
	if _, ok := readWidgetFrom(t, write, "w1"); ok {
		t.Error("expected no row inserted after validation failure")
	}
}

func TestCreateWithTx_SadPath_DBErrorIsWrapped(t *testing.T) {
	c, write := newTestCQRS(t)
	dropWidgetsTable(t, write)

	err := write.db.RunInTx(context.Background(), nil, func(ctx context.Context, tx bun.Tx) error {
		_, err := c.CreateWithTx(ctx, tx, widget{ID: "w1", Name: "n"})
		return err
	})
	if err == nil {
		t.Fatal("expected a DB error once the table is gone, got nil")
	}
}

func TestCreateWithTx_HappyPath_NilToResourceReturnsNilWithoutError(t *testing.T) {
	write := newFakeSQLService(t)
	c := newCQRSNoResource(t, write)

	withTx(t, write, func(ctx context.Context, tx bun.Tx) error {
		res, err := c.CreateWithTx(ctx, tx, widget{ID: "w1", Name: "n"})
		if err != nil {
			return err
		}
		if res != nil {
			t.Errorf("expected nil resource when ToResource is unset, got %+v", res)
		}
		return nil
	})
}

func TestCreateManyWithTx_HappyPath_InsertsAllWithinTransaction(t *testing.T) {
	c, write := newTestCQRS(t)

	withTx(t, write, func(ctx context.Context, tx bun.Tx) error {
		res, err := c.CreateManyWithTx(ctx, tx, []widget{{ID: "a", Name: "n"}, {ID: "b", Name: "n"}})
		if err != nil {
			return err
		}
		if len(res) != 2 {
			t.Fatalf("expected 2 resources, got %d", len(res))
		}
		return nil
	})

	count, err := write.db.NewSelect().Model((*widget)(nil)).Count(context.Background())
	if err != nil {
		t.Fatalf("counting rows: %v", err)
	}
	if count != 2 {
		t.Errorf("expected 2 rows, got %d", count)
	}
}

func TestCreateManyWithTx_SadPath_EmptyInputIsANoOp(t *testing.T) {
	c, write := newTestCQRS(t)
	withTx(t, write, func(ctx context.Context, tx bun.Tx) error {
		res, err := c.CreateManyWithTx(ctx, tx, nil)
		if err != nil {
			t.Fatalf("expected no error for empty input, got %v", err)
		}
		if len(res) != 0 {
			t.Errorf("expected empty result, got %d", len(res))
		}
		return nil
	})
}

func TestCreateManyWithTx_SadPath_ValidationFailureBlocksWholeBatch(t *testing.T) {
	c, write := newTestCQRS(t)
	err := write.db.RunInTx(context.Background(), nil, func(ctx context.Context, tx bun.Tx) error {
		_, err := c.CreateManyWithTx(ctx, tx, []widget{{ID: "a", Name: "ok"}, {ID: "b", Name: ""}})
		return err
	})
	if err == nil {
		t.Fatal("expected validation error, got nil")
	}
}

func TestCreateManyWithTx_SadPath_DBErrorOnDuplicateID(t *testing.T) {
	c, write := newTestCQRS(t)
	err := write.db.RunInTx(context.Background(), nil, func(ctx context.Context, tx bun.Tx) error {
		_, err := c.CreateManyWithTx(ctx, tx, []widget{{ID: "dup", Name: "one"}, {ID: "dup", Name: "two"}})
		return err
	})
	if err == nil {
		t.Fatal("expected a DB error for duplicate primary keys in the same batch, got nil")
	}
}

func TestCreateManyWithTx_HappyPath_NilToResourceReturnsNilResponses(t *testing.T) {
	write := newFakeSQLService(t)
	c := newCQRSNoResource(t, write)

	withTx(t, write, func(ctx context.Context, tx bun.Tx) error {
		res, err := c.CreateManyWithTx(ctx, tx, []widget{{ID: "a", Name: "n"}})
		if err != nil {
			return err
		}
		if res != nil {
			t.Errorf("expected nil responses when ToResource is unset, got %+v", res)
		}
		return nil
	})
}

func TestUpdateByIDWithTx_HappyPath_ChangesPersist(t *testing.T) {
	c, write := newTestCQRS(t)
	seedWidget(t, c, widget{ID: "w1", Name: "old"})

	withTx(t, write, func(ctx context.Context, tx bun.Tx) error {
		res, err := c.UpdateByIDWithTx(ctx, tx, "w1", widget{ID: "w1", Name: "new"})
		if err != nil {
			return err
		}
		if res.Name != "new" {
			t.Fatalf("expected resource name 'new', got %q", res.Name)
		}
		return nil
	})

	if got, ok := readWidgetFrom(t, write, "w1"); !ok || got.Name != "new" {
		t.Errorf("expected db name 'new', got %q (found=%v)", got.Name, ok)
	}
}

func TestUpdateByIDWithTx_SadPath_NonexistentIDReturnsErrNoRows(t *testing.T) {
	c, write := newTestCQRS(t)
	err := write.db.RunInTx(context.Background(), nil, func(ctx context.Context, tx bun.Tx) error {
		_, err := c.UpdateByIDWithTx(ctx, tx, "missing", widget{ID: "missing", Name: "n"})
		return err
	})
	if err == nil {
		t.Fatal("expected an error for a nonexistent id, got nil")
	}
}

func TestUpdateByIDWithTx_SadPath_ValidationFailureBlocksUpdate(t *testing.T) {
	c, write := newTestCQRS(t)
	seedWidget(t, c, widget{ID: "w1", Name: "original"})

	err := write.db.RunInTx(context.Background(), nil, func(ctx context.Context, tx bun.Tx) error {
		_, err := c.UpdateByIDWithTx(ctx, tx, "w1", widget{ID: "w1", Name: ""})
		return err
	})
	if err == nil {
		t.Fatal("expected validation error, got nil")
	}
	if got, ok := readWidgetFrom(t, write, "w1"); !ok || got.Name != "original" {
		t.Errorf("expected row untouched after validation failure, got name %q", got.Name)
	}
}

func TestUpdateByIDWithTx_SadPath_DBErrorIsWrapped(t *testing.T) {
	c, write := newTestCQRS(t)
	seedWidget(t, c, widget{ID: "w1", Name: "n"})
	dropWidgetsTable(t, write)

	err := write.db.RunInTx(context.Background(), nil, func(ctx context.Context, tx bun.Tx) error {
		_, err := c.UpdateByIDWithTx(ctx, tx, "w1", widget{ID: "w1", Name: "n2"})
		return err
	})
	if err == nil {
		t.Fatal("expected a DB error once the table is gone, got nil")
	}
}

func TestUpdateByIDWithTx_HappyPath_NilToResourceReturnsNilWithoutError(t *testing.T) {
	write := newFakeSQLService(t)
	c := newCQRSNoResource(t, write)
	seedWidget(t, c, widget{ID: "w1", Name: "n"})

	withTx(t, write, func(ctx context.Context, tx bun.Tx) error {
		res, err := c.UpdateByIDWithTx(ctx, tx, "w1", widget{ID: "w1", Name: "n2"})
		if err != nil {
			return err
		}
		if res != nil {
			t.Errorf("expected nil resource when ToResource is unset, got %+v", res)
		}
		return nil
	})
}

func TestDeleteByIDWithTx_HappyPath_RemovesRow(t *testing.T) {
	c, write := newTestCQRS(t)
	seedWidget(t, c, widget{ID: "w1", Name: "n"})

	withTx(t, write, func(ctx context.Context, tx bun.Tx) error {
		return c.DeleteByIDWithTx(ctx, tx, "w1")
	})

	if _, ok := readWidgetFrom(t, write, "w1"); ok {
		t.Error("expected row to be deleted")
	}
}

func TestDeleteByIDWithTx_SadPath_NonexistentIDReturnsErrNoRows(t *testing.T) {
	c, write := newTestCQRS(t)
	err := write.db.RunInTx(context.Background(), nil, func(ctx context.Context, tx bun.Tx) error {
		return c.DeleteByIDWithTx(ctx, tx, "missing")
	})
	if err == nil {
		t.Fatal("expected an error for a nonexistent id, got nil")
	}
}

func TestDeleteManyWithTx_HappyPath_RemovesGivenIDs(t *testing.T) {
	c, write := newTestCQRS(t)
	seedWidget(t, c, widget{ID: "a", Name: "n"})
	seedWidget(t, c, widget{ID: "b", Name: "n"})

	withTx(t, write, func(ctx context.Context, tx bun.Tx) error {
		return c.DeleteManyWithTx(ctx, tx, []string{"a"})
	})

	if _, ok := readWidgetFrom(t, write, "a"); ok {
		t.Error("expected widget 'a' to be deleted")
	}
	if _, ok := readWidgetFrom(t, write, "b"); !ok {
		t.Error("expected widget 'b' to survive")
	}
}

func TestDeleteManyWithTx_SadPath_EmptyInputIsANoOp(t *testing.T) {
	c, write := newTestCQRS(t)
	seedWidget(t, c, widget{ID: "a", Name: "n"})

	withTx(t, write, func(ctx context.Context, tx bun.Tx) error {
		if err := c.DeleteManyWithTx(ctx, tx, nil); err != nil {
			t.Fatalf("expected no error for empty id list, got %v", err)
		}
		return nil
	})

	if _, ok := readWidgetFrom(t, write, "a"); !ok {
		t.Error("expected widget 'a' to be untouched")
	}
}

func TestDeleteByIDWithTx_SadPath_DBErrorIsWrapped(t *testing.T) {
	c, write := newTestCQRS(t)
	seedWidget(t, c, widget{ID: "w1", Name: "n"})
	dropWidgetsTable(t, write)

	err := write.db.RunInTx(context.Background(), nil, func(ctx context.Context, tx bun.Tx) error {
		return c.DeleteByIDWithTx(ctx, tx, "w1")
	})
	if err == nil {
		t.Fatal("expected a DB error once the table is gone, got nil")
	}
}

func TestDeleteManyWithTx_SadPath_DBErrorIsWrapped(t *testing.T) {
	c, write := newTestCQRS(t)
	seedWidget(t, c, widget{ID: "w1", Name: "n"})
	dropWidgetsTable(t, write)

	err := write.db.RunInTx(context.Background(), nil, func(ctx context.Context, tx bun.Tx) error {
		return c.DeleteManyWithTx(ctx, tx, []string{"w1"})
	})
	if err == nil {
		t.Fatal("expected a DB error once the table is gone, got nil")
	}
}
