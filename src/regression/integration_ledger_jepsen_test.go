//go:build integration

// "ACID / Jepsen Testing" — named honestly: real Jepsen tests a
// distributed system's linearizability under network partitions and node
// crashes, checked by a dedicated checker (Knossos/Elle) against a
// multi-node cluster. There is no multi-node consensus layer here to
// nemesis-test — this package is a client library over one Postgres
// instance, not a distributed database. What this file actually does,
// honestly labeled, is Jepsen's *methodology* scaled to what's actually
// present: record a complete history of concurrent operations (including
// their real outcomes — committed or aborted), then verify that history
// could only have come from *some* valid serial ordering of the committed
// operations, by checking invariants no valid serialization could ever
// violate (conservation of total balance, no negative balance). That's a
// real, useful correctness property — it just isn't a replacement for
// genuine multi-node Jepsen testing, which doesn't apply here.
package regression

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
)

type ledgerHistoryEntry struct {
	from, to string
	amount   int64
	err      error // nil = committed, non-nil = aborted (insufficient funds, deadlock, ...)
}

// TestLedgerJepsenStyle_ConcurrentHistorySatisfiesInvariantsUnderAnySerialization
// throws a large number of concurrent, randomly-targeted transfers at a
// fixed pool of accounts, records each one's real outcome, and then checks
// the two invariants that must hold under *every* possible serialization
// of the committed subset: total balance is conserved, and no account goes
// negative. It additionally spot-checks that the final total matches what
// summing the recorded committed deltas predicts — tying the "what
// actually got applied" history back to the database's own final state,
// not just asserting the invariant in isolation.
func TestLedgerJepsenStyle_ConcurrentHistorySatisfiesInvariantsUnderAnySerialization(t *testing.T) {
	t.Parallel()
	t.Log("note: this is Jepsen-style invariant checking against a single Postgres instance, " +
		"not real multi-node Jepsen (no distributed consensus layer exists here to nemesis-test) — see this file's own doc comment")
	c, write := newLedgerCQRSWithPool(t, 24)

	const numAccounts = 10
	const startingBalance = 50000
	ids := make([]string, numAccounts)
	accounts := make([]ledgerAccount, numAccounts)
	for i := range ids {
		ids[i] = fmt.Sprintf("jacc%02d", i)
		accounts[i] = ledgerAccount{ID: ids[i], Name: ids[i], BalanceCents: startingBalance}
	}
	seedLedgerAccounts(t, write, accounts...)
	startingTotal := int64(numAccounts * startingBalance)

	const numOps = 500
	history := make([]ledgerHistoryEntry, numOps)
	rng := rand.New(rand.NewSource(42))
	type job struct {
		idx      int
		from, to string
		amount   int64
	}
	jobs := make([]job, 0, numOps)
	for i := 0; i < numOps; i++ {
		from := ids[rng.Intn(numAccounts)]
		to := ids[rng.Intn(numAccounts)]
		for to == from {
			to = ids[rng.Intn(numAccounts)]
		}
		jobs = append(jobs, job{idx: i, from: from, to: to, amount: int64(1 + rng.Intn(2000))})
	}

	var wg sync.WaitGroup
	for _, j := range jobs {
		wg.Add(1)
		go func(j job) {
			defer wg.Done()
			err := transferFunds(context.Background(), c, j.from, j.to, j.amount)
			history[j.idx] = ledgerHistoryEntry{from: j.from, to: j.to, amount: j.amount, err: err}
		}(j)
	}
	wg.Wait()

	// Invariant check 1: predicted total from the recorded, *committed*
	// history must equal startingTotal (transfers only move money between
	// accounts already in this closed set — conservation).
	predictedDelta := map[string]int64{}
	committed := 0
	for _, h := range history {
		if h.err != nil {
			continue // aborted: no effect, correctly excluded from the prediction
		}
		committed++
		predictedDelta[h.from] -= h.amount
		predictedDelta[h.to] += h.amount
	}
	t.Logf("%d/%d transfers committed", committed, numOps)

	var accs []ledgerAccount
	if err := write.Client().NewSelect().Model(&accs).Scan(context.Background()); err != nil {
		t.Fatalf("scanning accounts: %v", err)
	}
	actual := map[string]int64{}
	var actualTotal int64
	for _, a := range accs {
		actual[a.ID] = a.BalanceCents
		actualTotal += a.BalanceCents
		if a.BalanceCents < 0 {
			t.Errorf("invariant violated: account %s went negative (%d) — no valid serialization of a "+
				"balance-checked transfer history could produce this", a.ID, a.BalanceCents)
		}
	}
	if actualTotal != startingTotal {
		t.Errorf("invariant violated: total balance drifted from %d to %d — conservation does not hold "+
			"under any serialization", startingTotal, actualTotal)
	}

	// Invariant check 2: per-account, the database's actual final balance
	// must equal starting balance + the committed history's predicted
	// delta — ties the recorded history directly to the real end state,
	// not just to its own aggregate.
	for _, id := range ids {
		want := int64(startingBalance) + predictedDelta[id]
		if actual[id] != want {
			t.Errorf("account %s: predicted balance %d from recorded committed history, actual %d — "+
				"the real database state is not explained by any serialization of the recorded history",
				id, want, actual[id])
		}
	}
}
