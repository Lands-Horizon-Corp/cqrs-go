//go:build integration

package regression

import (
	"context"
	"testing"
	"time"
)

func TestLedgerSmoke_HappyPath_SingleTransferMovesTheRightAmount(t *testing.T) {
	t.Parallel()
	c, write := newLedgerCQRS(t)
	seedLedgerAccounts(t, write,
		ledgerAccount{ID: "a1", Name: "Alice", BalanceCents: 10000},
		ledgerAccount{ID: "a2", Name: "Bob", BalanceCents: 5000},
	)

	if err := transferFunds(context.Background(), c, "a1", "a2", 3000); err != nil {
		t.Fatalf("transferFunds returned error: %v", err)
	}

	a1, err := c.GetByID(context.Background(), "a1")
	if err != nil {
		t.Fatalf("GetByID a1 returned error: %v", err)
	}
	a2, err := c.GetByID(context.Background(), "a2")
	if err != nil {
		t.Fatalf("GetByID a2 returned error: %v", err)
	}
	if a1.BalanceCents != 7000 {
		t.Errorf("expected a1 balance 7000, got %d", a1.BalanceCents)
	}
	if a2.BalanceCents != 8000 {
		t.Errorf("expected a2 balance 8000, got %d", a2.BalanceCents)
	}
	if got := totalLedgerBalance(t, write); got != 15000 {
		t.Errorf("expected total balance 15000, got %d", got)
	}
}

// TestLedgerSmoke_HappyPath_GetByIDWithTxReallyBlocksASecondLocker proves
// GetByIDWithTx's row lock is real against Postgres, not a no-op: a second
// transaction's GetByIDWithTx on the same row must block until the first
// one ends.
func TestLedgerSmoke_HappyPath_GetByIDWithTxReallyBlocksASecondLocker(t *testing.T) {
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
