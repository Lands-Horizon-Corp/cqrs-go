//go:build integration

// Transactional Benchmarking. Named honestly: this is inspired by TPC-B's
// core transaction shape (debit one account, credit another, inside one
// ACID transaction, measured as transactions/sec) — it is not a literal
// TPC-B or TPC-C implementation, which specify exact schemas, scale
// factors, terminal/think-time simulation, and an audited methodology
// this file doesn't attempt to reproduce. What it measures honestly: real
// transfer throughput and per-transaction latency against real Postgres,
// using this package's own Start/End + GetByIDWithTx + IncrementByIDWithTx
// transaction layer — not a separate, hand-optimized benchmark harness.
//
// Run with: go test -tags=integration ./src/regression/... -bench Ledger -run '^$' -benchtime=3s
package regression

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
)

// BenchmarkLedgerTransfer measures single-transfer latency/throughput
// against a fixed pool of accounts, b.N transfers total, serially (the
// baseline every concurrency comparison starts from).
func BenchmarkLedgerTransfer(b *testing.B) {
	c, ids := benchmarkLedgerSetup(b)
	rng := rand.New(rand.NewSource(1))
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		from := ids[rng.Intn(len(ids))]
		to := ids[rng.Intn(len(ids))]
		if from == to {
			continue
		}
		if err := transferFunds(ctx, c, from, to, 1); err != nil {
			b.Fatalf("transferFunds returned error: %v", err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "transfers/sec")
}

// BenchmarkLedgerTransferParallel is BenchmarkLedgerTransfer run across
// Go's own parallel benchmark workers (b.SetParallelism / GOMAXPROCS by
// default) against one shared account pool and one shared connection
// pool — the concurrent-throughput counterpart to the serial baseline
// above, closer to how this transaction layer is actually used in
// production (many goroutines, one pool).
func BenchmarkLedgerTransferParallel(b *testing.B) {
	c, ids := benchmarkLedgerSetup(b)
	ctx := context.Background()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		rng := rand.New(rand.NewSource(rand.Int63()))
		for pb.Next() {
			from := ids[rng.Intn(len(ids))]
			to := ids[rng.Intn(len(ids))]
			if from == to {
				continue
			}
			if err := transferFunds(ctx, c, from, to, 1); err != nil {
				b.Fatalf("transferFunds returned error: %v", err)
			}
		}
	})
	b.StopTimer()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "transfers/sec")
}

// BenchmarkLedgerIncrementByID isolates IncrementByIDWithTx's own atomic
// "SET col = col + ?" cost from transferFunds' full two-account-lock
// shape — a single-statement baseline to compare the full transfer
// against.
func BenchmarkLedgerIncrementByID(b *testing.B) {
	c, ids := benchmarkLedgerSetup(b)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := c.IncrementByID(ctx, ids[i%len(ids)], "balance_cents", 1); err != nil {
			b.Fatalf("IncrementByID returned error: %v", err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "increments/sec")
}

// benchmarkLedgerSetup seeds a fixed, well-funded pool of accounts large
// enough that b.N transfers of 1 cent each essentially never hit
// insufficient-funds (which would otherwise make the benchmark measure a
// mix of commit and abort paths instead of a consistent one).
func benchmarkLedgerSetup(b *testing.B) (*ledgerCQRS, []string) {
	b.Helper()
	b.Log("note: inspired by TPC-B's debit/credit transaction shape, not a literal TPC-B/TPC-C " +
		"implementation — see this file's own doc comment")

	const numAccounts = 20
	const startingBalance = 1 << 40 // effectively unlimited at 1-cent-per-op scale
	c, write := newLedgerCQRSWithPool(b, 32)

	ids := make([]string, numAccounts)
	accounts := make([]ledgerAccount, numAccounts)
	for i := range ids {
		ids[i] = fmt.Sprintf("bench%03d", i)
		accounts[i] = ledgerAccount{ID: ids[i], Name: ids[i], BalanceCents: startingBalance}
	}
	if _, err := write.Client().NewInsert().Model(&accounts).Exec(context.Background()); err != nil {
		b.Fatalf("seeding benchmark accounts: %v", err)
	}
	return c, ids
}
