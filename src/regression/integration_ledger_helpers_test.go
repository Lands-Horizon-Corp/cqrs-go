//go:build integration

// Shared fixture for the ledger-style transaction tests
// (integration_ledger_*_test.go, ledger_load_test.go, ledger_bench_test.go):
// a minimal double-entry-ish accounts table and a transferFunds helper
// built entirely on CQRSImpl's own public transaction API (Start/End,
// GetByIDWithTx — which locks the row it reads, see pagination.structured.go
// — and IncrementByIDWithTx) — not a parallel, simplified reimplementation
// of transaction handling. If transferFunds is correct under concurrency,
// that's this package's transaction layer being correct, not a separately
// hand-rolled one.
//
// This all runs against real Postgres (see newLedgerPostgres below), never
// SQLite: SQLite's single-writer-lock model has no row-level contention,
// no real MVCC, and no "SELECT ... FOR UPDATE" support at all (confirmed
// directly — it's a syntax error), so it cannot exhibit the lost-update,
// dirty-read, or lock-contention/deadlock behaviors this suite exists to
// test against.
package regression

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/driver/pgdriver"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/cqrs"
)

// UpdatedAt exists because NewCQRS defaults ColumnDefaultSort to
// "updated_at DESC" (see src/cqrs/index.go) — every GetByID/FindOne/Find
// call orders by it unless told otherwise, so any model used with this
// package needs the column to exist, not just because it's realistic
// audit-trail data for a ledger account (which it also is).
type ledgerAccount struct {
	bun.BaseModel `bun:"table:ledger_accounts"`
	ID            string    `bun:"id,pk"`
	Name          string    `bun:"name"`
	BalanceCents  int64     `bun:"balance_cents,notnull"`
	UpdatedAt     time.Time `bun:"updated_at,nullzero"`
}

type ledgerCQRS = cqrs.CQRSImpl[ledgerAccount, ledgerAccount, any, string]

// newLedgerCQRS builds a CQRSImpl over a real, freshly-schema-isolated
// Postgres connection (same per-test-schema isolation approach as
// newPostgresSQLServiceInSchema in integration_helpers_test.go, kept as
// its own small helper here rather than generalizing that one, since it's
// hardcoded to the widget/ProcessedEvent pair many other tests already
// depend on).
func newLedgerCQRS(t *testing.T) (*ledgerCQRS, *fakeSQLService) {
	return newLedgerCQRSWithPool(t, defaultPoolSize)
}

// newLedgerCQRSWithPool takes testing.TB rather than *testing.T so
// benchmarks (ledger_bench_test.go) can reuse it, the same reason
// skipUnlessInfraReachable does.
func newLedgerCQRSWithPool(t testing.TB, maxConns int) (*ledgerCQRS, *fakeSQLService) {
	t.Helper()
	skipUnlessInfraReachable(t)
	write := newLedgerPostgres(t, maxConns)
	c := cqrs.NewCQRS(cqrs.CQRSImpl[ledgerAccount, ledgerAccount, any, string]{
		WriteSQLService: write,
		ToResource:      func(a *ledgerAccount) *ledgerAccount { return a },
	})
	return c, write
}

// ledgerSchemaDSNs maps a *fakeSQLService built by newLedgerPostgres back to
// the scoped DSN it was opened with, so
// newLedgerFreshConnectionToSameSchema can open a genuinely independent
// connection (a different *sql.DB, not just a different logical
// transaction) to the exact same per-test schema — needed by the
// durability and fault-injection tests, which specifically care that data
// outlives the connection that wrote it, not just that Go's own struct
// still looks right.
var ledgerSchemaDSNs sync.Map // *fakeSQLService -> scoped DSN string

// newLedgerFreshConnectionToSameSchema opens a brand new connection pool —
// and a brand new CQRSImpl over it — pointed at the same schema write
// already uses, sharing nothing with write's own *sql.DB.
func newLedgerFreshConnectionToSameSchema(t *testing.T, write *fakeSQLService) *ledgerCQRS {
	t.Helper()
	v, ok := ledgerSchemaDSNs.Load(write)
	if !ok {
		t.Fatal("newLedgerFreshConnectionToSameSchema: write was not created by newLedgerPostgres")
	}
	scopedDSN := v.(string)

	sqldb := sql.OpenDB(pgdriver.NewConnector(pgdriver.WithDSN(scopedDSN)))
	sqldb.SetMaxOpenConns(defaultPoolSize)
	db := bun.NewDB(sqldb, pgdialect.New())
	t.Cleanup(func() { _ = db.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("pinging fresh connection to the same schema: %v", err)
	}

	return cqrs.NewCQRS(cqrs.CQRSImpl[ledgerAccount, ledgerAccount, any, string]{
		WriteSQLService: &fakeSQLService{db: db},
		ToResource:      func(a *ledgerAccount) *ledgerAccount { return a },
	})
}

func newLedgerPostgres(t testing.TB, maxConns int) *fakeSQLService {
	t.Helper()
	schema := schemaNameRE.ReplaceAllString(t.Name(), "_")
	if len(schema) > 40 {
		schema = schema[:40]
	}
	schema = fmt.Sprintf("ledger_%s_%d", schema, time.Now().UnixNano())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	admin := sql.OpenDB(pgdriver.NewConnector(pgdriver.WithDSN(itWriteDSN)))
	defer admin.Close()
	if _, err := admin.ExecContext(ctx, "CREATE SCHEMA IF NOT EXISTS "+pgQuoteIdent(schema)); err != nil {
		t.Fatalf("creating schema %s: %v", schema, err)
	}
	t.Cleanup(func() {
		admin := sql.OpenDB(pgdriver.NewConnector(pgdriver.WithDSN(itWriteDSN)))
		defer admin.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := admin.ExecContext(ctx, "DROP SCHEMA IF EXISTS "+pgQuoteIdent(schema)+" CASCADE"); err != nil {
			t.Logf("dropping schema %s during cleanup: %v", schema, err)
		}
	})

	scopedDSN, err := withSearchPath(itWriteDSN, schema)
	if err != nil {
		t.Fatalf("building scoped DSN for schema %s: %v", schema, err)
	}
	sqldb := sql.OpenDB(pgdriver.NewConnector(pgdriver.WithDSN(scopedDSN)))
	sqldb.SetMaxOpenConns(maxConns)
	sqldb.SetMaxIdleConns(maxConns)
	db := bun.NewDB(sqldb, pgdialect.New())
	t.Cleanup(func() { _ = db.Close() })

	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("pinging postgres at schema %s: %v", schema, err)
	}
	if _, err := db.NewCreateTable().Model((*ledgerAccount)(nil)).Exec(ctx); err != nil {
		t.Fatalf("creating ledger_accounts table: %v", err)
	}
	svc := &fakeSQLService{db: db}
	ledgerSchemaDSNs.Store(svc, scopedDSN)
	return svc
}

func seedLedgerAccounts(t *testing.T, db *fakeSQLService, accounts ...ledgerAccount) {
	t.Helper()
	if len(accounts) == 0 {
		return
	}
	if _, err := db.Client().NewInsert().Model(&accounts).Exec(context.Background()); err != nil {
		t.Fatalf("seeding ledger accounts: %v", err)
	}
}

func totalLedgerBalance(t *testing.T, db *fakeSQLService) int64 {
	t.Helper()
	var total sql.NullInt64
	if err := db.Client().NewSelect().Model((*ledgerAccount)(nil)).
		ColumnExpr("COALESCE(SUM(balance_cents), 0)").Scan(context.Background(), &total); err != nil {
		t.Fatalf("summing ledger balances: %v", err)
	}
	return total.Int64
}

// transferFunds moves amountCents from fromID to toID inside one Start/End
// transaction. GetByIDWithTx itself locks the row it reads ("SELECT ...
// FOR UPDATE" — see paginate's own doc comment in
// pagination.structured.go), so locking and reading each account's current
// balance is the same call, done in a fixed, consistent order —
// lexicographically by ID — specifically so two transfers running in
// opposite directions concurrently can never deadlock against each other;
// see transferFundsUnsafeLockOrder for the deliberately broken version used
// to construct a real deadlock on purpose. The source balance is checked
// under that lock before either side is touched, and both sides are
// updated via IncrementByIDWithTx's atomic SET-based arithmetic rather
// than a read-modify-write round trip.
func transferFunds(ctx context.Context, c *ledgerCQRS, fromID, toID string, amountCents int64) (err error) {
	tx, err := c.Start(ctx)
	if err != nil {
		return fmt.Errorf("starting transfer tx: %w", err)
	}
	defer func() { err = c.End(ctx, tx, err) }()

	first, second := fromID, toID
	if second < first {
		first, second = second, first
	}
	firstAccount, err := c.GetByIDWithTx(ctx, &tx, first)
	if err != nil {
		return fmt.Errorf("locking %s: %w", first, err)
	}
	secondAccount, err := c.GetByIDWithTx(ctx, &tx, second)
	if err != nil {
		return fmt.Errorf("locking %s: %w", second, err)
	}

	from := firstAccount
	if fromID == second {
		from = secondAccount
	}
	if from.BalanceCents < amountCents {
		return fmt.Errorf("insufficient funds in %s: have %d cents, need %d cents", fromID, from.BalanceCents, amountCents)
	}

	if _, err = c.IncrementByIDWithTx(ctx, tx, fromID, "balance_cents", float64(-amountCents)); err != nil {
		return fmt.Errorf("debiting %s: %w", fromID, err)
	}
	if _, err = c.IncrementByIDWithTx(ctx, tx, toID, "balance_cents", float64(amountCents)); err != nil {
		return fmt.Errorf("crediting %s: %w", toID, err)
	}
	return nil
}

// transferFundsUnsafeLockOrder is transferFunds with the deadlock-avoidant
// lock ordering deliberately removed: it always locks fromID first, then
// toID, regardless of their sort order. Two of these running concurrently
// in opposite directions (A->B and B->A) will, often enough to be useful
// in a test, each hold one lock the other needs — a real Postgres deadlock,
// not a simulated one.
func transferFundsUnsafeLockOrder(ctx context.Context, c *ledgerCQRS, fromID, toID string, amountCents int64) (err error) {
	tx, err := c.Start(ctx)
	if err != nil {
		return fmt.Errorf("starting transfer tx: %w", err)
	}
	defer func() { err = c.End(ctx, tx, err) }()

	from, err := c.GetByIDWithTx(ctx, &tx, fromID)
	if err != nil {
		return fmt.Errorf("locking %s: %w", fromID, err)
	}
	if _, err = c.GetByIDWithTx(ctx, &tx, toID); err != nil {
		return fmt.Errorf("locking %s: %w", toID, err)
	}

	if from.BalanceCents < amountCents {
		return fmt.Errorf("insufficient funds in %s: have %d cents, need %d cents", fromID, from.BalanceCents, amountCents)
	}

	if _, err = c.IncrementByIDWithTx(ctx, tx, fromID, "balance_cents", float64(-amountCents)); err != nil {
		return fmt.Errorf("debiting %s: %w", fromID, err)
	}
	if _, err = c.IncrementByIDWithTx(ctx, tx, toID, "balance_cents", float64(amountCents)); err != nil {
		return fmt.Errorf("crediting %s: %w", toID, err)
	}
	return nil
}
