package regression

// This file verifies CQRSImpl's IncrementByID/IncrementByIDWithTx (atomic
// "SET field = field + delta") at the functional level. Row-locking itself
// — every *WithTx single/multi-row fetch (GetByIDWithTx, FindWithTx,
// FindOneWithTx, MaxWithTx, MinWithTx) issues "SELECT ... FOR UPDATE", see
// paginate's own doc comment in pagination.structured.go — has no
// meaningful test here: SQLite's query planner doesn't support that clause
// at all (confirmed directly: it's a syntax error against
// modernc.org/sqlite), and its single-writer-lock model has no row-level
// contention for a row lock to demonstrate anyway, even if it did. Real
// concurrency/locking behavior is covered separately against real
// Postgres — see integration_ledger_*_test.go.

import (
	"context"
	"database/sql"
	"errors"
	"testing"
)

func TestIncrementByID_HappyPath_AddsDeltaToField(t *testing.T) {
	t.Parallel()
	c, write := newTestCQRS(t)
	seedWidgets(t, write, widget{ID: "w1", Name: "Alpha", Priority: new(10)})

	got, err := c.IncrementByID(context.Background(), "w1", "priority", 5)
	if err != nil {
		t.Fatalf("IncrementByID returned error: %v", err)
	}
	if got.Priority == nil || *got.Priority != 15 {
		t.Fatalf("expected priority 15, got %+v", got)
	}
}

func TestIncrementByID_HappyPath_NegativeDeltaSubtracts(t *testing.T) {
	t.Parallel()
	c, write := newTestCQRS(t)
	seedWidgets(t, write, widget{ID: "w1", Name: "Alpha", Priority: new(10)})

	got, err := c.IncrementByID(context.Background(), "w1", "priority", -3)
	if err != nil {
		t.Fatalf("IncrementByID returned error: %v", err)
	}
	if got.Priority == nil || *got.Priority != 7 {
		t.Fatalf("expected priority 7, got %+v", got)
	}
}

func TestIncrementByID_SadPath_UnknownFieldReturnsError(t *testing.T) {
	t.Parallel()
	c, write := newTestCQRS(t)
	seedWidgets(t, write, widget{ID: "w1", Name: "Alpha", Priority: new(10)})

	if _, err := c.IncrementByID(context.Background(), "w1", "not_a_real_column", 1); err == nil {
		t.Fatal("expected an error for an unknown field, got nil")
	}
}

func TestIncrementByID_SadPath_UnknownIDReturnsErrNoRows(t *testing.T) {
	t.Parallel()
	c, _ := newTestCQRS(t)

	_, err := c.IncrementByID(context.Background(), "not-a-real-id", "priority", 1)
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected sql.ErrNoRows, got %v", err)
	}
}

func TestIncrementByIDWithTx_HappyPath_SeesUncommittedWritesInTheSameTx(t *testing.T) {
	t.Parallel()
	c, write := newTestCQRS(t)
	ctx := context.Background()

	tx, err := write.Client().BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx returned error: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.NewInsert().Model(&widget{ID: "w1", Name: "Alpha", Priority: new(10)}).Exec(ctx); err != nil {
		t.Fatalf("inserting inside the transaction: %v", err)
	}

	got, err := c.IncrementByIDWithTx(ctx, tx, "w1", "priority", 5)
	if err != nil {
		t.Fatalf("IncrementByIDWithTx returned error: %v", err)
	}
	if got.Priority == nil || *got.Priority != 15 {
		t.Fatalf("expected priority 15 inside the tx, got %+v", got)
	}
}

// TestIncrementByID_HappyPath_ConcurrentIncrementsAllLand is a weaker,
// SQLite-level sanity check that each individual IncrementByID call is
// atomic and additive (serialized writes still land in full) — SQLite's
// own single-writer lock means this can't actually exercise row-level
// contention the way the Postgres-backed ledger tests do, but it still
// catches a regression where IncrementByID stopped being additive (e.g.
// accidentally overwriting instead of adding).
func TestIncrementByID_HappyPath_ConcurrentIncrementsAllLand(t *testing.T) {
	t.Parallel()
	c, write := newTestCQRS(t)
	seedWidgets(t, write, widget{ID: "w1", Name: "Alpha", Priority: new(0)})

	const n = 20
	errs := make(chan error, n)
	for range n {
		go func() {
			_, err := c.IncrementByID(context.Background(), "w1", "priority", 1)
			errs <- err
		}()
	}
	for range n {
		if err := <-errs; err != nil {
			t.Fatalf("IncrementByID returned error: %v", err)
		}
	}

	got, err := c.GetByID(context.Background(), "w1")
	if err != nil {
		t.Fatalf("GetByID returned error: %v", err)
	}
	if got.Priority == nil || *got.Priority != n {
		t.Fatalf("expected priority %d (every increment landed), got %+v", n, got)
	}
}

// TestIncrementByID_HappyPath_LargeExistingValuePlusSmallDeltaStaysExact
// pins down IncrementByID's own doc comment precisely: delta's float64
// precision limit (2^53) bounds delta itself, not the column's stored
// value — the database does the actual addition against its own exact
// integer type once delta is formatted into the query, not against a
// float64 accumulator. A column already well beyond 2^53, incremented by
// an ordinary small delta, must land exactly.
func TestIncrementByID_HappyPath_LargeExistingValuePlusSmallDeltaStaysExact(t *testing.T) {
	t.Parallel()
	c, write := newTestCQRS(t)
	const big = 1 << 60 // far beyond 2^53 (9,007,199,254,740,992), the float64 exact-integer limit
	seedWidgets(t, write, widget{ID: "w1", Name: "Alpha", Priority: new(big)})

	got, err := c.IncrementByID(context.Background(), "w1", "priority", 5)
	if err != nil {
		t.Fatalf("IncrementByID returned error: %v", err)
	}
	if got.Priority == nil || *got.Priority != big+5 {
		t.Fatalf("expected %d (exact, no precision loss), got %+v", big+5, got)
	}
}

// TestIncrementByID_HappyPath_DeltaAtThePrecisionBoundaryStaysExact confirms
// a delta argument exactly at 2^53 — the largest integer float64 can still
// represent exactly — round-trips correctly.
func TestIncrementByID_HappyPath_DeltaAtThePrecisionBoundaryStaysExact(t *testing.T) {
	t.Parallel()
	c, write := newTestCQRS(t)
	const maxSafeDelta = 1 << 53
	seedWidgets(t, write, widget{ID: "w1", Name: "Alpha", Priority: new(0)})

	got, err := c.IncrementByID(context.Background(), "w1", "priority", maxSafeDelta)
	if err != nil {
		t.Fatalf("IncrementByID returned error: %v", err)
	}
	if got.Priority == nil || *got.Priority != maxSafeDelta {
		t.Fatalf("expected exactly %d at the precision boundary, got %+v", maxSafeDelta, got)
	}
}

// TestIncrementByID_PoisonPill_DeltaBeyondThePrecisionBoundaryLosesPrecision
// documents the one real limit IncrementByID's doc comment describes: a
// delta argument whose magnitude exceeds 2^53 cannot be represented
// exactly as float64 and silently rounds before it ever reaches SQL. This
// is confirmed, expected behavior for an out-of-realistic-range input
// (2^53 cents is about $90 trillion in one single call), locked in here so
// a future change to IncrementByID's signature (e.g. a bigint-safe delta
// type) is a deliberate decision, not an unnoticed behavior change.
func TestIncrementByID_PoisonPill_DeltaBeyondThePrecisionBoundaryLosesPrecision(t *testing.T) {
	t.Parallel()
	c, write := newTestCQRS(t)
	// A variable, explicitly converted at runtime — not an untyped Go
	// constant passed directly as the float64 argument, which the compiler
	// would round at compile time instead, proving nothing about
	// IncrementByID's own runtime behavior.
	unsafeDelta := int64(1<<53) + 1 // smallest integer float64 cannot represent exactly
	seedWidgets(t, write, widget{ID: "w1", Name: "Alpha", Priority: new(0)})

	got, err := c.IncrementByID(context.Background(), "w1", "priority", float64(unsafeDelta))
	if err != nil {
		t.Fatalf("IncrementByID returned error: %v", err)
	}
	if got.Priority == nil {
		t.Fatal("expected a non-nil Priority")
	}
	gotPriority := int64(*got.Priority)
	if gotPriority == unsafeDelta {
		t.Fatalf("expected the known float64 precision limit to round this delta down to %d, "+
			"but it landed exactly at %d instead — either Go's float64 behavior changed, or this "+
			"platform/column type isn't hitting the boundary this test assumes", unsafeDelta-1, gotPriority)
	}
	if gotPriority != unsafeDelta-1 {
		t.Fatalf("expected the delta to round down to exactly %d (the nearest float64-representable "+
			"integer), got %d", unsafeDelta-1, gotPriority)
	}
}
