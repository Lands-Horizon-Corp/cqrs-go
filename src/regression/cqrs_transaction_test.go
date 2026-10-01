package regression

// This file verifies CQRSImpl's Start/End transaction lifecycle: Start
// begins a transaction on WriteSQLService and hands it back to the caller
// (rather than storing it on the instance, which would be unsafe — one
// CQRSImpl is typically shared across concurrent requests), and End
// commits it if the caller's err is nil, or rolls it back and returns that
// err unchanged otherwise.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/domains"
)

func TestCQRSTransaction_HappyPath_EndCommitsOnNilErr(t *testing.T) {
	t.Parallel()
	c, _ := newTestCQRS(t)
	ctx := context.Background()

	tx, err := c.Start(ctx)
	if err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	if _, err := c.CreateWithTx(ctx, tx, widget{ID: "w1", Name: "Alpha"}); err != nil {
		t.Fatalf("CreateWithTx returned error: %v", err)
	}
	if err := c.End(ctx, tx, nil); err != nil {
		t.Fatalf("End returned error: %v", err)
	}

	// Committed: a plain (non-tx) read must now see it.
	got, err := c.GetByID(ctx, "w1")
	if err != nil {
		t.Fatalf("GetByID after commit returned error: %v", err)
	}
	if got.Name != "Alpha" {
		t.Fatalf("expected the committed row back, got %+v", got)
	}

	// The tx is done: committing it again must error, not hang or
	// silently no-op — proving End actually called Commit rather than
	// skipping it.
	if err := tx.Commit(); err == nil {
		t.Error("expected committing an already-committed tx to error")
	}
}

func TestCQRSTransaction_HappyPath_EndRollsBackOnNonNilErr(t *testing.T) {
	t.Parallel()
	c, _ := newTestCQRS(t)
	ctx := context.Background()

	tx, err := c.Start(ctx)
	if err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	if _, err := c.CreateWithTx(ctx, tx, widget{ID: "w1", Name: "Alpha"}); err != nil {
		t.Fatalf("CreateWithTx returned error: %v", err)
	}

	sentinel := errors.New("something failed after the create")
	gotErr := c.End(ctx, tx, sentinel)
	if !errors.Is(gotErr, sentinel) {
		t.Fatalf("expected End to return the original error unchanged, got: %v", gotErr)
	}

	// Rolled back: the row must not exist.
	if _, err := c.GetByID(ctx, "w1"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected sql.ErrNoRows (rolled back), got %v", err)
	}
}

// TestCQRSTransaction_HappyPath_FullCreateUpdateDeleteFindGetByIDLifecycleUnderStartEnd
// is the realistic usage shape: Start, every ...WithTx method in turn
// (CreateWithTx, FindWithTx, GetByIDWithTx, UpdateByIDWithTx, DeleteByIDWithTx),
// then a deferred End assigning back to the same named err — proving
// Start/End interoperates with the full ...WithTx family, not just a
// couple of them. Every read here deliberately goes through a ...WithTx
// variant (FindWithTx/GetByIDWithTx against tx), never the plain
// Find/GetByID: those go through ReadSQLService, which is for
// read-heavy, replica-tolerant lookups like pagination — not for reading
// back work this same transaction hasn't committed yet, which only the
// writer's own connection can see at all.
func TestCQRSTransaction_HappyPath_FullCreateUpdateDeleteFindGetByIDLifecycleUnderStartEnd(t *testing.T) {
	t.Parallel()
	c, _ := newTestCQRS(t)
	ctx := context.Background()
	byID := domains.StructuredFilter{Filters: []domains.Filter{{Field: "id", Mode: domains.ModeEqual, Value: "w1"}}}

	err := func() (err error) {
		tx, err := c.Start(ctx)
		if err != nil {
			return err
		}
		defer func() { err = c.End(ctx, tx, err) }()

		// Create.
		if _, err = c.CreateWithTx(ctx, tx, widget{ID: "w1", Name: "Alpha", Priority: new(1)}); err != nil {
			return err
		}

		// Find/GetByID must see it immediately, inside the same still-open tx.
		foundByFilter, err := c.FindWithTx(ctx, &tx, byID)
		if err != nil {
			return err
		}
		if len(foundByFilter) != 1 || foundByFilter[0].Name != "Alpha" {
			return fmt.Errorf("expected [Alpha] from FindWithTx after create, got %+v", foundByFilter)
		}
		foundByID, err := c.GetByIDWithTx(ctx, &tx, "w1")
		if err != nil {
			return err
		}
		if foundByID.Name != "Alpha" {
			return fmt.Errorf("expected Name=Alpha from GetByIDWithTx after create, got %q", foundByID.Name)
		}

		// Update.
		if _, err = c.UpdateByIDWithTx(ctx, tx, "w1", widget{ID: "w1", Name: "Beta", Priority: new(2)}); err != nil {
			return err
		}
		if foundByID, err = c.GetByIDWithTx(ctx, &tx, "w1"); err != nil {
			return err
		}
		if foundByID.Name != "Beta" {
			return fmt.Errorf("expected Name=Beta from GetByIDWithTx after update, got %q", foundByID.Name)
		}

		// Delete.
		if err = c.DeleteByIDWithTx(ctx, tx, "w1"); err != nil {
			return err
		}
		if _, err = c.GetByIDWithTx(ctx, &tx, "w1"); !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("expected sql.ErrNoRows from GetByIDWithTx after delete, got %v", err)
		}
		if foundByFilter, err = c.FindWithTx(ctx, &tx, byID); err != nil {
			return err
		}
		if len(foundByFilter) != 0 {
			return fmt.Errorf("expected no rows from FindWithTx after delete, got %+v", foundByFilter)
		}
		return nil
	}()
	if err != nil {
		t.Fatalf("transaction lifecycle returned error: %v", err)
	}

	// Committed: the net effect (create, update, then delete) is really
	// durable, not just visible while the transaction was still open.
	if _, err := c.GetByID(ctx, "w1"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected sql.ErrNoRows after commit (net effect of create+update+delete), got %v", err)
	}
}

func TestCQRSTransaction_SadPath_StartReturnsErrorOnClosedDB(t *testing.T) {
	t.Parallel()
	c, write := newTestCQRS(t)
	if err := write.Client().Close(); err != nil {
		t.Fatalf("closing db: %v", err)
	}

	if _, err := c.Start(context.Background()); err == nil {
		t.Fatal("expected an error starting a transaction on a closed DB, got nil")
	}
}
