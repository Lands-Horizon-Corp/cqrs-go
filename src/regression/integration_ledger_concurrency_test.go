//go:build integration

// Concurrency & Isolation Testing (Locking & Race Conditions), plus Lock
// Contention & Deadlocks, against real Postgres — see
// integration_ledger_helpers_test.go for why SQLite can't stand in here.
package regression

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestLedgerConcurrency_HappyPath_ManyConcurrentTransfersPreserveTotalBalance
// is the core correctness invariant for any ledger: no matter how many
// transfers run concurrently, or in what order Postgres actually
// serializes their row locks, money is only ever moved between accounts
// that already exist in this test — never created or destroyed. The total
// across every account must be exactly what it started as.
func TestLedgerConcurrency_HappyPath_ManyConcurrentTransfersPreserveTotalBalance(t *testing.T) {
	t.Parallel()
	c, write := newLedgerCQRSWithPool(t, 16)
	const numAccounts = 8
	const startingBalance = 100000 // cents
	ids := make([]string, numAccounts)
	accounts := make([]ledgerAccount, numAccounts)
	for i := range ids {
		ids[i] = fmt.Sprintf("acc%02d", i)
		accounts[i] = ledgerAccount{ID: ids[i], Name: ids[i], BalanceCents: startingBalance}
	}
	seedLedgerAccounts(t, write, accounts...)
	wantTotal := int64(numAccounts * startingBalance)

	const numTransfers = 200
	rng := rand.New(rand.NewSource(1))
	var wg sync.WaitGroup
	for i := 0; i < numTransfers; i++ {
		from := ids[rng.Intn(numAccounts)]
		to := ids[rng.Intn(numAccounts)]
		if from == to {
			continue
		}
		amount := int64(1 + rng.Intn(500))
		wg.Add(1)
		go func(from, to string, amount int64) {
			defer wg.Done()
			// Insufficient-funds failures are expected and fine here — the
			// invariant under test is conservation of the total, not that
			// every randomly generated transfer succeeds.
			_ = transferFunds(context.Background(), c, from, to, amount)
		}(from, to, amount)
	}
	wg.Wait()

	if got := totalLedgerBalance(t, write); got != wantTotal {
		t.Fatalf("expected total balance to stay %d after %d concurrent transfers, got %d", wantTotal, numTransfers, got)
	}

	// Consistency, not just conservation: nothing should have gone negative
	// (transferFunds checks this under lock, but confirm no check was
	// silently bypassed by a race).
	var accs []ledgerAccount
	if err := write.Client().NewSelect().Model(&accs).Scan(context.Background()); err != nil {
		t.Fatalf("scanning accounts: %v", err)
	}
	for _, a := range accs {
		if a.BalanceCents < 0 {
			t.Errorf("account %s went negative: %d", a.ID, a.BalanceCents)
		}
	}
}

// TestLedgerConcurrency_HappyPath_AtomicIncrementPreventsLostUpdates is the
// direct regression test for the bug a naive "GetByID, add in Go,
// UpdateByID" implementation would have: many goroutines crediting the
// same single account at once. If IncrementByIDWithTx's arithmetic ever
// stopped being a single atomic SQL statement (e.g. someone "simplifies"
// it back into a read-modify-write), this is what would catch it — the
// final balance would come up short.
func TestLedgerConcurrency_HappyPath_AtomicIncrementPreventsLostUpdates(t *testing.T) {
	t.Parallel()
	c, write := newLedgerCQRSWithPool(t, 16)
	seedLedgerAccounts(t, write, ledgerAccount{ID: "a1", Name: "Alice", BalanceCents: 0})

	const n = 100
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := context.Background()
			tx, err := c.Start(ctx)
			if err != nil {
				t.Errorf("Start returned error: %v", err)
				return
			}
			defer func() { _ = c.End(ctx, tx, err) }()
			if _, err = c.IncrementByIDWithTx(ctx, tx, "a1", "balance_cents", 100); err != nil {
				t.Errorf("IncrementByIDWithTx returned error: %v", err)
			}
		}()
	}
	wg.Wait()

	got, err := c.GetByID(context.Background(), "a1")
	if err != nil {
		t.Fatalf("GetByID returned error: %v", err)
	}
	if want := int64(n * 100); got.BalanceCents != want {
		t.Fatalf("expected balance %d (every +100 landed, none lost), got %d", want, got.BalanceCents)
	}
}

// TestLedgerConcurrency_HappyPath_ForUpdateSerializesConcurrentLockersOfSameRow
// proves the row lock GetByIDWithTx now takes is real against Postgres,
// not a no-op: a second transaction's GetByIDWithTx on the same row must
// block until the first one ends. (See cqrs.increment.go/pagination's
// doc comments for why every *WithTx fetch locks.)
func TestLedgerConcurrency_HappyPath_ForUpdateSerializesConcurrentLockersOfSameRow(t *testing.T) {
	t.Parallel()
	c, write := newLedgerCQRSWithPool(t, 4)
	seedLedgerAccounts(t, write, ledgerAccount{ID: "a1", Name: "Alice", BalanceCents: 1000})
	ctx := context.Background()

	tx1, err := c.Start(ctx)
	if err != nil {
		t.Fatalf("Start tx1 returned error: %v", err)
	}
	if _, err := c.GetByIDWithTx(ctx, &tx1, "a1"); err != nil {
		t.Fatalf("GetByIDWithTx (tx1) returned error: %v", err)
	}

	unblocked := make(chan struct{})
	go func() {
		tx2, err := c.Start(ctx)
		if err != nil {
			t.Errorf("Start tx2 returned error: %v", err)
			return
		}
		defer func() { _ = c.End(ctx, tx2, nil) }()
		if _, err := c.GetByIDWithTx(ctx, &tx2, "a1"); err != nil {
			t.Errorf("GetByIDWithTx (tx2) returned error: %v", err)
		}
		close(unblocked)
	}()

	select {
	case <-unblocked:
		t.Fatal("expected tx2's GetByIDWithTx to block while tx1 holds the lock, but it returned immediately")
	case <-time.After(500 * time.Millisecond):
		// still blocked, as expected
	}

	if err := c.End(ctx, tx1, nil); err != nil {
		t.Fatalf("End tx1 returned error: %v", err)
	}

	select {
	case <-unblocked:
	case <-time.After(5 * time.Second):
		t.Fatal("expected tx2 to unblock after tx1 ended, but it never did")
	}
}

// TestLedgerConcurrency_HappyPath_ReadCommittedHidesUncommittedTransfer is
// the dirty-read-prevention isolation check: while a transfer is debited
// but not yet committed inside tx1, a completely separate, ordinary
// (non-tx) GetByID must still see the pre-transfer balance — Postgres's
// default READ COMMITTED isolation guarantees this, but it's the kind of
// guarantee worth pinning down with an actual test rather than trusting by
// assumption, given what's riding on it here.
func TestLedgerConcurrency_HappyPath_ReadCommittedHidesUncommittedTransfer(t *testing.T) {
	t.Parallel()
	c, write := newLedgerCQRSWithPool(t, 4)
	seedLedgerAccounts(t, write,
		ledgerAccount{ID: "a1", Name: "Alice", BalanceCents: 1000},
		ledgerAccount{ID: "a2", Name: "Bob", BalanceCents: 1000},
	)
	ctx := context.Background()

	tx, err := c.Start(ctx)
	if err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	if _, err := c.GetByIDWithTx(ctx, &tx, "a1"); err != nil {
		t.Fatalf("GetByIDWithTx returned error: %v", err)
	}
	if _, err := c.IncrementByIDWithTx(ctx, tx, "a1", "balance_cents", -400); err != nil {
		t.Fatalf("IncrementByIDWithTx (debit) returned error: %v", err)
	}
	if _, err := c.IncrementByIDWithTx(ctx, tx, "a2", "balance_cents", 400); err != nil {
		t.Fatalf("IncrementByIDWithTx (credit) returned error: %v", err)
	}

	// Still uncommitted: a plain read from a separate connection must see
	// the original balance, not the in-flight debit.
	outside, err := c.GetByID(ctx, "a1")
	if err != nil {
		t.Fatalf("GetByID (outside tx) returned error: %v", err)
	}
	if outside.BalanceCents != 1000 {
		t.Fatalf("expected the uncommitted debit to be invisible outside the tx (balance still 1000), got %d", outside.BalanceCents)
	}

	if err := c.End(ctx, tx, nil); err != nil {
		t.Fatalf("End returned error: %v", err)
	}

	after, err := c.GetByID(ctx, "a1")
	if err != nil {
		t.Fatalf("GetByID (after commit) returned error: %v", err)
	}
	if after.BalanceCents != 600 {
		t.Fatalf("expected the committed debit to now be visible (balance 600), got %d", after.BalanceCents)
	}
}

// TestLedgerConcurrency_HappyPath_ConcurrentAccountCreationRaceOnSameIDOnlyOneWins
// confirms the primary-key constraint still protects account creation
// itself under real concurrent load, the same "load balancer double
// request" scenario integration_concurrency_test.go already covers for
// widgets — repeated here because a ledger specifically cannot tolerate
// two different "open this account" requests silently producing two
// different starting balances for the same account ID.
func TestLedgerConcurrency_HappyPath_ConcurrentAccountCreationRaceOnSameIDOnlyOneWins(t *testing.T) {
	t.Parallel()
	c, write := newLedgerCQRSWithPool(t, 16)
	const racers = 8

	var succeeded int32
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, err := c.Create(context.Background(), ledgerAccount{ID: "race1", Name: fmt.Sprintf("attempt-%d", i), BalanceCents: int64(i * 1000)})
			if err == nil {
				succeeded++
			}
		}(i)
	}
	close(start)
	wg.Wait()

	if succeeded != 1 {
		t.Fatalf("expected exactly 1 account creation to win the race, got %d", succeeded)
	}
	if got := totalLedgerBalance(t, write); got < 0 {
		t.Errorf("unexpected negative total after the race: %d", got)
	}
}

// TestLedgerConcurrency_LockContentionAndDeadlocks_OppositeOrderTransfersAreDetectedAndRecovered
// is the dedicated lock-contention/deadlock test: transferFundsUnsafeLockOrder
// always locks fromID before toID, so two goroutines transferring in
// opposite directions (A->B and B->A) repeatedly, concurrently, reliably
// construct a real Postgres deadlock (SQLSTATE 40P01) often enough across
// many attempts — not simulated, not mocked. Postgres's own deadlock
// detector picks one transaction to abort; this asserts that abort
// surfaces as a clean Go error (not a hang) and that End still cleanly
// rolls that side back, and that simply retrying the loser lets both
// sides eventually succeed with the mathematically correct final
// balances — proving real lock contention doesn't corrupt state or wedge
// the application, even though it isn't prevented here.
func TestLedgerConcurrency_LockContentionAndDeadlocks_OppositeOrderTransfersAreDetectedAndRecovered(t *testing.T) {
	t.Parallel()
	c, write := newLedgerCQRSWithPool(t, 16)
	seedLedgerAccounts(t, write,
		ledgerAccount{ID: "a1", Name: "Alice", BalanceCents: 1000000},
		ledgerAccount{ID: "a2", Name: "Bob", BalanceCents: 1000000},
	)

	isDeadlock := func(err error) bool {
		return err != nil && strings.Contains(err.Error(), "deadlock detected")
	}

	const rounds = 40
	var sawADeadlock bool
	var mu sync.Mutex
	for i := 0; i < rounds; i++ {
		var wg sync.WaitGroup
		start := make(chan struct{})
		var errAB, errBA error
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			errAB = transferFundsUnsafeLockOrder(context.Background(), c, "a1", "a2", 10)
		}()
		go func() {
			defer wg.Done()
			<-start
			errBA = transferFundsUnsafeLockOrder(context.Background(), c, "a2", "a1", 10)
		}()
		close(start)
		wg.Wait()

		for _, err := range []error{errAB, errBA} {
			if isDeadlock(err) {
				mu.Lock()
				sawADeadlock = true
				mu.Unlock()
			} else if err != nil {
				t.Fatalf("round %d: unexpected non-deadlock error: %v", i, err)
			}
		}

		// Retry whichever side lost to the deadlock (or both, if neither
		// did) — mirroring how a real caller would react to a transient
		// 40P01: just try the same operation again.
		if isDeadlock(errAB) {
			if err := transferFundsUnsafeLockOrder(context.Background(), c, "a1", "a2", 10); err != nil {
				t.Fatalf("round %d: retry of A->B returned error: %v", i, err)
			}
		}
		if isDeadlock(errBA) {
			if err := transferFundsUnsafeLockOrder(context.Background(), c, "a2", "a1", 10); err != nil {
				t.Fatalf("round %d: retry of B->A returned error: %v", i, err)
			}
		}
	}

	if !sawADeadlock {
		t.Skip("never actually reproduced a real deadlock across all rounds (timing-dependent) — the recovery/retry logic above was still exercised on whichever errors did occur, but this run can't confirm the deadlock-detection path specifically")
	}

	// Net effect regardless of how many rounds deadlocked: every attempted
	// transfer that counts (post-retry) moved exactly 10 cents each way,
	// which nets to zero — both accounts should be back to their starting
	// balance.
	a1, err := c.GetByID(context.Background(), "a1")
	if err != nil {
		t.Fatalf("GetByID a1 returned error: %v", err)
	}
	a2, err := c.GetByID(context.Background(), "a2")
	if err != nil {
		t.Fatalf("GetByID a2 returned error: %v", err)
	}
	if a1.BalanceCents != 1000000 || a2.BalanceCents != 1000000 {
		t.Fatalf("expected both accounts back at their starting balance (equal transfers each way), got a1=%d a2=%d", a1.BalanceCents, a2.BalanceCents)
	}
}
