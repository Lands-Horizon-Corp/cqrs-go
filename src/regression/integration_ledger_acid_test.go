//go:build integration

// ACID & Fault Injection Testing (Hard Crash / Mid-Commit Failures), one
// test per ACID property plus a real crash scenario, against real
// Postgres — see integration_ledger_helpers_test.go for why.
package regression

import (
	"context"
	"fmt"
	"testing"
)

// TestLedgerACID_Atomicity_FailedCreditRollsBackTheAlreadyAppliedDebit
// forces the credit half of a transfer to fail (crediting a nonexistent
// account) after the debit half has already been applied inside the same
// transaction, and confirms End's rollback undoes the debit too — the two
// writes either both happen or neither does, never just one.
func TestLedgerACID_Atomicity_FailedCreditRollsBackTheAlreadyAppliedDebit(t *testing.T) {
	t.Parallel()
	c, write := newLedgerCQRS(t)
	seedLedgerAccounts(t, write, ledgerAccount{ID: "a1", Name: "Alice", BalanceCents: 1000})
	ctx := context.Background()

	err := func() (err error) {
		tx, err := c.Start(ctx)
		if err != nil {
			return err
		}
		defer func() { err = c.End(ctx, tx, err) }()

		if _, err = c.GetByIDWithTx(ctx, &tx, "a1"); err != nil {
			return err
		}
		if _, err = c.IncrementByIDWithTx(ctx, tx, "a1", "balance_cents", -400); err != nil {
			return fmt.Errorf("debit: %w", err)
		}
		// "does-not-exist" has no row to credit — IncrementByIDWithTx
		// returns sql.ErrNoRows, simulating a failure partway through the
		// transfer after the debit has already been applied in-tx.
		if _, err = c.IncrementByIDWithTx(ctx, tx, "does-not-exist", "balance_cents", 400); err != nil {
			return fmt.Errorf("credit: %w", err)
		}
		return nil
	}()
	if err == nil {
		t.Fatal("expected the forced credit failure to produce an error, got nil")
	}

	got, getErr := c.GetByID(ctx, "a1")
	if getErr != nil {
		t.Fatalf("GetByID returned error: %v", getErr)
	}
	if got.BalanceCents != 1000 {
		t.Fatalf("expected the debit to have been rolled back (balance still 1000), got %d", got.BalanceCents)
	}
}

// TestLedgerACID_Consistency_InsufficientFundsAbortsBeforeAnyWrite checks
// the business-rule invariant transferFunds itself enforces (no account
// may go negative): attempting to transfer more than the source account
// holds must leave both accounts completely untouched, not partially
// debited.
func TestLedgerACID_Consistency_InsufficientFundsAbortsBeforeAnyWrite(t *testing.T) {
	t.Parallel()
	c, write := newLedgerCQRS(t)
	seedLedgerAccounts(t, write,
		ledgerAccount{ID: "a1", Name: "Alice", BalanceCents: 500},
		ledgerAccount{ID: "a2", Name: "Bob", BalanceCents: 1000},
	)
	ctx := context.Background()

	err := transferFunds(ctx, c, "a1", "a2", 600) // more than a1 has
	if err == nil {
		t.Fatal("expected an insufficient-funds error, got nil")
	}

	a1, err := c.GetByID(ctx, "a1")
	if err != nil {
		t.Fatalf("GetByID a1 returned error: %v", err)
	}
	a2, err := c.GetByID(ctx, "a2")
	if err != nil {
		t.Fatalf("GetByID a2 returned error: %v", err)
	}
	if a1.BalanceCents != 500 || a2.BalanceCents != 1000 {
		t.Fatalf("expected both accounts unchanged after the rejected transfer, got a1=%d a2=%d", a1.BalanceCents, a2.BalanceCents)
	}
}

// TestLedgerACID_Isolation_ConcurrentTransfersOnOverlappingAccountsStayCorrect
// runs two transfers that share one account (A->B and B->C, both touching
// B) concurrently and confirms the final balances are exactly what serial
// execution in either order would produce — Postgres's row locking under
// GetByIDWithTx serializes the two transactions' access to B, so neither
// can observe the other's half-finished state.
func TestLedgerACID_Isolation_ConcurrentTransfersOnOverlappingAccountsStayCorrect(t *testing.T) {
	t.Parallel()
	c, write := newLedgerCQRSWithPool(t, 8)
	seedLedgerAccounts(t, write,
		ledgerAccount{ID: "a1", Name: "Alice", BalanceCents: 10000},
		ledgerAccount{ID: "a2", Name: "Bob", BalanceCents: 10000},
		ledgerAccount{ID: "a3", Name: "Carol", BalanceCents: 10000},
	)
	ctx := context.Background()

	errs := make(chan error, 2)
	go func() { errs <- transferFunds(ctx, c, "a1", "a2", 3000) }()
	go func() { errs <- transferFunds(ctx, c, "a2", "a3", 2000) }()
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("transferFunds returned error: %v", err)
		}
	}

	a1, _ := c.GetByID(ctx, "a1")
	a2, _ := c.GetByID(ctx, "a2")
	a3, _ := c.GetByID(ctx, "a3")
	if a1.BalanceCents != 7000 {
		t.Errorf("expected a1=7000, got %d", a1.BalanceCents)
	}
	if a2.BalanceCents != 11000 { // +3000 from a1, -2000 to a3, regardless of order
		t.Errorf("expected a2=11000, got %d", a2.BalanceCents)
	}
	if a3.BalanceCents != 12000 {
		t.Errorf("expected a3=12000, got %d", a3.BalanceCents)
	}
	if got := totalLedgerBalance(t, write); got != 30000 {
		t.Errorf("expected total 30000, got %d", got)
	}
}

// TestLedgerACID_Durability_CommittedTransferSurvivesANewConnection commits
// a transfer, then opens a completely fresh connection pool (not reusing
// anything the committing connection might have cached) to the same
// Postgres instance and confirms the change is there — durability means
// the data outlives the specific connection that wrote it, not just that
// Go's in-process struct still looks right.
func TestLedgerACID_Durability_CommittedTransferSurvivesANewConnection(t *testing.T) {
	t.Parallel()
	c, write := newLedgerCQRS(t)
	seedLedgerAccounts(t, write,
		ledgerAccount{ID: "a1", Name: "Alice", BalanceCents: 5000},
		ledgerAccount{ID: "a2", Name: "Bob", BalanceCents: 5000},
	)
	ctx := context.Background()

	if err := transferFunds(ctx, c, "a1", "a2", 1500); err != nil {
		t.Fatalf("transferFunds returned error: %v", err)
	}

	// A brand new client, reusing only the schema write.Client() set up —
	// not the same *sql.DB connection, a fresh one from this test's own
	// Postgres connection.
	fresh := newLedgerFreshConnectionToSameSchema(t, write)
	a1, err := fresh.GetByID(ctx, "a1")
	if err != nil {
		t.Fatalf("GetByID (fresh connection) a1 returned error: %v", err)
	}
	a2, err := fresh.GetByID(ctx, "a2")
	if err != nil {
		t.Fatalf("GetByID (fresh connection) a2 returned error: %v", err)
	}
	if a1.BalanceCents != 3500 || a2.BalanceCents != 6500 {
		t.Fatalf("expected the committed transfer visible from a fresh connection (a1=3500, a2=6500), got a1=%d a2=%d",
			a1.BalanceCents, a2.BalanceCents)
	}
}

// TestLedgerACID_FaultInjection_PostgresContainerRestartLosesOnlyUncommittedWork
// is the real "hard crash mid-commit" scenario: an uncommitted transfer is
// left open on one connection while the actual Postgres container is
// killed and restarted (reusing restartContainer from
// integration_chaos_test.go — a real `docker restart`, not a simulated
// failure), alongside a committed transfer that happened first. After
// Postgres comes back, a fresh connection must see the committed transfer
// and must NOT see any trace of the uncommitted one — a real crash during
// an open transaction is exactly what WAL-based crash recovery exists to
// guarantee, and this proves this package's transaction layer doesn't
// accidentally defeat that guarantee (e.g. by not actually opening a real
// transaction, or by buffering writes somewhere that survives differently
// than Postgres's own durability does).
func TestLedgerACID_FaultInjection_PostgresContainerRestartLosesOnlyUncommittedWork(t *testing.T) {
	c, write := newLedgerCQRS(t) // not t.Parallel(): restarts a shared container
	ctx := context.Background()
	seedLedgerAccounts(t, write,
		ledgerAccount{ID: "a1", Name: "Alice", BalanceCents: 10000},
		ledgerAccount{ID: "a2", Name: "Bob", BalanceCents: 10000},
	)

	// Committed before the crash: must survive.
	if err := transferFunds(ctx, c, "a1", "a2", 1000); err != nil {
		t.Fatalf("pre-crash committed transferFunds returned error: %v", err)
	}

	// Left open, never committed: must NOT survive.
	tx, err := c.Start(ctx)
	if err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	if _, err := c.GetByIDWithTx(ctx, &tx, "a1"); err != nil {
		t.Fatalf("GetByIDWithTx returned error: %v", err)
	}
	if _, err := c.IncrementByIDWithTx(ctx, tx, "a1", "balance_cents", -5000); err != nil {
		t.Fatalf("IncrementByIDWithTx (uncommitted debit) returned error: %v", err)
	}
	if _, err := c.IncrementByIDWithTx(ctx, tx, "a2", "balance_cents", 5000); err != nil {
		t.Fatalf("IncrementByIDWithTx (uncommitted credit) returned error: %v", err)
	}
	// Deliberately never call c.End — the restart below kills the
	// connection (and the server process) out from under this open tx,
	// which is the "hard crash" itself.

	restartContainer(t, "cqrs-postgres-write")

	fresh := newLedgerFreshConnectionToSameSchema(t, write)
	a1, err := fresh.GetByID(ctx, "a1")
	if err != nil {
		t.Fatalf("GetByID (post-restart) a1 returned error: %v", err)
	}
	a2, err := fresh.GetByID(ctx, "a2")
	if err != nil {
		t.Fatalf("GetByID (post-restart) a2 returned error: %v", err)
	}
	if a1.BalanceCents != 9000 || a2.BalanceCents != 11000 {
		t.Fatalf("expected only the pre-crash committed transfer to have survived (a1=9000, a2=11000), got a1=%d a2=%d — "+
			"either the committed work was lost (durability failure) or the uncommitted work leaked through (atomicity failure)",
			a1.BalanceCents, a2.BalanceCents)
	}
}
