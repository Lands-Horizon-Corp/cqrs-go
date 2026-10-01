package regression

// This file demonstrates Create/Update/Delete interoperating with
// Find/GetByID inside one real transaction. The transaction is always
// begun on WriteSQLService (the writer) — never ReadSQLService: a
// transaction only shows its own uncommitted work to callers sharing that
// same connection, and in a real deployment ReadSQLService may point at a
// read replica that doesn't even share the writer's connection, let alone
// an open transaction on it. See CreateWithTx/UpdateByIDWithTx/
// DeleteByIDWithTx/FindOneWithTx/GetByIDWithTx's own doc comments for the same
// point.

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/domains"
)

// TestTxCRUD_HappyPath_CreateUpdateDeleteFindGetRoundTripThenCommit walks a
// single row through Create -> Find/Get -> Update -> Find/Get -> Delete ->
// Find/Get, all inside one transaction, asserting read-your-writes at every
// step. It then commits and confirms the net effect (created, updated, then
// deleted — nothing left) is durable from outside the transaction too, not
// just visible while it was still open.
func TestTxCRUD_HappyPath_CreateUpdateDeleteFindGetRoundTripThenCommit(t *testing.T) {
	t.Parallel()
	c, write := newTestCQRS(t)
	ctx := context.Background()

	tx, err := write.Client().BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx returned error: %v", err)
	}
	defer func() { _ = tx.Rollback() }() // no-op once Commit below succeeds

	// --- Create ---
	created, err := c.CreateWithTx(ctx, tx, widget{ID: "w1", Name: "Alpha", Priority: new(1)})
	if err != nil {
		t.Fatalf("CreateWithTx returned error: %v", err)
	}
	if created.ID != "w1" || created.Name != "Alpha" {
		t.Fatalf("expected the created row back, got %+v", created)
	}

	// Find/Get must see it immediately, inside the same still-open tx.
	foundByFilter, err := c.FindOneWithTx(ctx, &tx, domains.StructuredFilter{
		Filters: []domains.Filter{{Field: "id", Mode: domains.ModeEqual, Value: "w1"}},
	})
	if err != nil {
		t.Fatalf("FindOneWithTx (after create) returned error: %v", err)
	}
	if foundByFilter.Name != "Alpha" {
		t.Fatalf("expected Name=Alpha after create, got %+v", foundByFilter)
	}
	foundByID, err := c.GetByIDWithTx(ctx, &tx, "w1")
	if err != nil {
		t.Fatalf("GetByIDWithTx (after create) returned error: %v", err)
	}
	if foundByID.Name != "Alpha" {
		t.Fatalf("expected Name=Alpha after create, got %+v", foundByID)
	}

	// --- Update ---
	updated, err := c.UpdateByIDWithTx(ctx, tx, "w1", widget{ID: "w1", Name: "Beta", Priority: new(2)})
	if err != nil {
		t.Fatalf("UpdateByIDWithTx returned error: %v", err)
	}
	if updated.Name != "Beta" {
		t.Fatalf("expected the updated row back with Name=Beta, got %+v", updated)
	}

	// Find/Get must see the update immediately too.
	foundByID, err = c.GetByIDWithTx(ctx, &tx, "w1")
	if err != nil {
		t.Fatalf("GetByIDWithTx (after update) returned error: %v", err)
	}
	if foundByID.Name != "Beta" {
		t.Fatalf("expected Name=Beta after update, got %+v", foundByID)
	}
	foundByFilter, err = c.FindOneWithTx(ctx, &tx, domains.StructuredFilter{
		Filters: []domains.Filter{{Field: "name", Mode: domains.ModeEqual, Value: "Beta"}},
	})
	if err != nil {
		t.Fatalf("FindOneWithTx (after update) returned error: %v", err)
	}
	if foundByFilter.ID != "w1" {
		t.Fatalf("expected to find w1 by its updated Name, got %+v", foundByFilter)
	}

	// --- Delete ---
	if err := c.DeleteByIDWithTx(ctx, tx, "w1"); err != nil {
		t.Fatalf("DeleteByIDWithTx returned error: %v", err)
	}

	// Find/Get must see the deletion immediately, still inside the same tx.
	if _, err := c.GetByIDWithTx(ctx, &tx, "w1"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected sql.ErrNoRows from GetByIDWithTx after delete, got %v", err)
	}
	if _, err := c.FindOneWithTx(ctx, &tx, domains.StructuredFilter{
		Filters: []domains.Filter{{Field: "id", Mode: domains.ModeEqual, Value: "w1"}},
	}); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected sql.ErrNoRows from FindOneWithTx after delete, got %v", err)
	}

	// --- Commit, then prove the net effect is real from outside the tx ---
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit returned error: %v", err)
	}
	if _, err := c.GetByID(ctx, "w1"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected sql.ErrNoRows after commit (net effect of create+update+delete), got %v", err)
	}
}

// TestTxCRUD_HappyPath_RollbackDiscardsEverythingDoneInsideTheTransaction
// confirms the other half of the same guarantee: work that's visible inside
// a still-open transaction (read-your-writes) is entirely discarded on
// Rollback — nothing leaks out to a caller reading without that tx.
func TestTxCRUD_HappyPath_RollbackDiscardsEverythingDoneInsideTheTransaction(t *testing.T) {
	t.Parallel()
	c, write := newTestCQRS(t)
	ctx := context.Background()

	tx, err := write.Client().BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx returned error: %v", err)
	}

	if _, err := c.CreateWithTx(ctx, tx, widget{ID: "w1", Name: "Alpha"}); err != nil {
		t.Fatalf("CreateWithTx returned error: %v", err)
	}
	if _, err := c.UpdateByIDWithTx(ctx, tx, "w1", widget{ID: "w1", Name: "Beta"}); err != nil {
		t.Fatalf("UpdateByIDWithTx returned error: %v", err)
	}

	// Visible inside the still-open tx...
	found, err := c.GetByIDWithTx(ctx, &tx, "w1")
	if err != nil {
		t.Fatalf("GetByIDWithTx (before rollback) returned error: %v", err)
	}
	if found.Name != "Beta" {
		t.Fatalf("expected Name=Beta visible inside the tx before rollback, got %+v", found)
	}

	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback returned error: %v", err)
	}

	// ...but gone entirely once rolled back, whether looked up by filter or by ID.
	if _, err := c.GetByID(ctx, "w1"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected sql.ErrNoRows from GetByID after rollback, got %v", err)
	}
	if _, err := c.FindOne(ctx, domains.StructuredFilter{
		Filters: []domains.Filter{{Field: "id", Mode: domains.ModeEqual, Value: "w1"}},
	}); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected sql.ErrNoRows from Find after rollback, got %v", err)
	}
}
