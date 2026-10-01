//go:build integration && load

// Load, Stress, and Soak Testing (Resource Exhaustion) for the ledger
// transaction layer. Opt-in on top of "integration", same as load_test.go:
// `go test -tags="integration load" ./src/regression/... -run TestLedgerLoad -v`.
package regression

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ledgerLoadDuration reads CQRS_IT_LEDGER_LOAD_SECONDS (default 10). This
// is a sustained-throughput/soak test, not a fixed-N one — it keeps firing
// transfers from a fixed worker pool for a wall-clock duration, which is
// what actually exercises "resource exhaustion" (connection pool held
// under constant pressure for a while) rather than just "a lot of
// operations as fast as possible then stop."
func ledgerLoadDuration() time.Duration {
	s, err := strconv.Atoi(envOr("CQRS_IT_LEDGER_LOAD_SECONDS", "10"))
	if err != nil || s <= 0 {
		s = 10
	}
	return time.Duration(s) * time.Second
}

func TestLedgerLoad_SustainedConcurrentTransferThroughputAndConservation(t *testing.T) {
	duration := ledgerLoadDuration()
	const workers = 50
	const numAccounts = 50
	const startingBalance = 1_000_000
	const maxPoolSize = 60 // >= workers, so no worker ever waits on a free connection just from pool exhaustion itself

	t.Logf("ledger load: %d workers, %d accounts, %v duration (override via CQRS_IT_LEDGER_LOAD_SECONDS), pool=%d",
		workers, numAccounts, duration, maxPoolSize)

	c, write := newLedgerCQRSWithPool(t, maxPoolSize) // not t.Parallel(): wants the pool budget to itself
	ids := make([]string, numAccounts)
	accounts := make([]ledgerAccount, numAccounts)
	for i := range ids {
		ids[i] = fmt.Sprintf("load%03d", i)
		accounts[i] = ledgerAccount{ID: ids[i], Name: ids[i], BalanceCents: startingBalance}
	}
	seedLedgerAccounts(t, write, accounts...)
	startingTotal := int64(numAccounts * startingBalance)

	var attempted, committed, aborted int64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	start := time.Now()
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			ctx := context.Background()
			for {
				select {
				case <-stop:
					return
				default:
				}
				from := ids[rng.Intn(numAccounts)]
				to := ids[rng.Intn(numAccounts)]
				if from == to {
					continue
				}
				amount := int64(1 + rng.Intn(500))
				atomic.AddInt64(&attempted, 1)
				if err := transferFunds(ctx, c, from, to, amount); err != nil {
					atomic.AddInt64(&aborted, 1)
				} else {
					atomic.AddInt64(&committed, 1)
				}
			}
		}(int64(w) + 1)
	}

	time.Sleep(duration)
	close(stop)
	wg.Wait()
	elapsed := time.Since(start)

	tps := float64(committed) / elapsed.Seconds()
	t.Logf("ran %v: %d attempted, %d committed, %d aborted (%.0f committed transfers/sec, %d workers over a %d-connection pool)",
		elapsed, attempted, committed, aborted, tps, workers, maxPoolSize)

	if committed == 0 {
		t.Fatal("expected at least some transfers to commit during the load window")
	}

	// Correctness under sustained load, not just throughput: conservation
	// and non-negativity must still hold after hammering the pool for the
	// whole duration — a connection leak, a lock-handling bug, or a
	// resource-exhaustion-triggered partial write would show up here as a
	// drifted total or a negative balance, not just as a slow run.
	if got := totalLedgerBalance(t, write); got != startingTotal {
		t.Errorf("expected total balance to stay %d after sustained load, got %d", startingTotal, got)
	}
	var accs []ledgerAccount
	if err := write.Client().NewSelect().Model(&accs).Scan(context.Background()); err != nil {
		t.Fatalf("scanning accounts: %v", err)
	}
	for _, a := range accs {
		if a.BalanceCents < 0 {
			t.Errorf("account %s went negative under load: %d", a.ID, a.BalanceCents)
		}
	}
}
