//go:build integration

package regression

import (
	"context"
	"testing"
	"time"

	"github.com/uptrace/bun"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/cqrs"
	"github.com/Lands-Horizon-Corp/cqrs-go/src/domains"
)

// EnablePartitioning is real DDL against real Postgres (CREATE TABLE ...
// PARTITION BY RANGE, plus registering with the actual pg_partman
// extension) — there's no SQLite equivalent to unit-test this against, so
// it's integration-only, same as the real Kafka/Debezium-dependent tests
// in this package.
//
// This uses its own fixture (partitionedLedgerEntry) rather than the
// shared widget one: pg_partman's create_parent rejects a nullable control
// column outright ("must be set to NOT NULL" — hit this directly against
// the real extension before writing this fixture), and widget's
// UpdatedAt is deliberately nullable (bun:"updated_at,nullzero"). A
// ledger/time-series table with a genuinely NOT NULL timestamp is also
// more representative of what actually gets partitioned in practice.
// CreatedAt is part of the primary key alongside ID (not just NOT NULL):
// Postgres requires a partitioned table's primary key to include the
// partitioning column — confirmed directly against a real instance, where
// a plain single-column "id" PK was rejected outright once PARTITION BY
// RANGE was added. See EnablePartitioning's doc comment for the tradeoff
// this implies (id alone is no longer uniqueness-enforced by the DB).
type partitionedLedgerEntry struct {
	bun.BaseModel `bun:"table:partitioned_ledger_entries"`
	ID            string    `bun:"id,pk"`
	CreatedAt     time.Time `bun:"created_at,pk,notnull"`
	Amount        float64   `bun:"amount"`
}

type partitionedLedgerEntryResource struct {
	ID        string    `json:"id"`
	CreatedAt time.Time `json:"createdAt"`
	Amount    float64   `json:"amount"`
}

func partitionedLedgerEntryToResource(e *partitionedLedgerEntry) *partitionedLedgerEntryResource {
	return &partitionedLedgerEntryResource{ID: e.ID, CreatedAt: e.CreatedAt, Amount: e.Amount}
}

func newPartitionTestCQRS(t *testing.T) (*cqrs.CQRSImpl[partitionedLedgerEntry, partitionedLedgerEntryResource, any, string], *fakeSQLService) {
	t.Helper()
	skipUnlessInfraReachable(t)
	read := newPostgresSQLService(t, itReadDSN)
	c := cqrs.NewCQRS(cqrs.CQRSImpl[partitionedLedgerEntry, partitionedLedgerEntryResource, any, string]{
		WriteSQLService: read,
		ReadSQLService:  read,
		ToResource:      partitionedLedgerEntryToResource,
	})
	return c, read
}

// qualifiedLedgerTable resolves "<current_schema>.partitioned_ledger_entries"
// — the same way EnablePartitioning itself resolves the table it registers
// with pg_partman — so a test can check/clean up exactly its own row rather
// than an unqualified match that could hit a different parallel test's
// table (pg_class/partman.part_config are both database-wide, not
// schema-scoped, and every test here creates its own isolated schema).
func qualifiedLedgerTable(t *testing.T, ctx context.Context, read *fakeSQLService) string {
	t.Helper()
	var schemaName string
	if err := read.Client().NewRaw("SELECT current_schema()").Scan(ctx, &schemaName); err != nil {
		t.Fatalf("resolving current schema: %v", err)
	}
	return schemaName + ".partitioned_ledger_entries"
}

func cleanupPartmanRegistration(t *testing.T, read *fakeSQLService, qualifiedTable string) {
	t.Helper()
	t.Cleanup(func() {
		// part_config isn't cleaned up by the schema DROP ... CASCADE this
		// test's isolated schema already gets (it's a plain text column,
		// not a real FK into the schema Postgres would cascade), so this
		// avoids leaving orphaned rows behind across test runs.
		_, _ = read.Client().NewRaw(
			"DELETE FROM partman.part_config WHERE parent_table = ?", qualifiedTable,
		).Exec(context.Background())
	})
}

func TestIntegration_HappyPath_EnablePartitioningCreatesPartitionedTableAndIsIdempotent(t *testing.T) {
	t.Parallel()
	c, read := newPartitionTestCQRS(t)
	ctx := context.Background()

	if err := c.EnablePartitioning(ctx, "created_at", "1 day"); err != nil {
		t.Fatalf("EnablePartitioning returned error: %v", err)
	}
	qualifiedTable := qualifiedLedgerTable(t, ctx, read)
	cleanupPartmanRegistration(t, read, qualifiedTable)

	var relkind string
	if err := read.Client().NewRaw(
		"SELECT relkind FROM pg_class WHERE relname = 'partitioned_ledger_entries' AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = current_schema())",
	).Scan(ctx, &relkind); err != nil {
		t.Fatalf("checking partitioned_ledger_entries' relkind: %v", err)
	}
	if relkind != "p" {
		t.Fatalf("expected partitioned_ledger_entries to be a partitioned table (relkind 'p'), got %q", relkind)
	}

	registered, err := read.Client().NewSelect().
		TableExpr("partman.part_config").
		Where("parent_table = ?", qualifiedTable).
		Exists(ctx)
	if err != nil {
		t.Fatalf("checking partman registration: %v", err)
	}
	if !registered {
		t.Fatal("expected partitioned_ledger_entries to be registered in partman.part_config")
	}

	// Calling it again must not error (create_parent alone is NOT
	// idempotent — verified directly against a real pg_partman instance
	// before writing EnablePartitioning's existence check).
	if err := c.EnablePartitioning(ctx, "created_at", "1 day"); err != nil {
		t.Fatalf("second EnablePartitioning call returned error: %v", err)
	}
}

func TestIntegration_HappyPath_PaginationWorksAgainstAPartitionedTable(t *testing.T) {
	t.Parallel()
	c, read := newPartitionTestCQRS(t)
	ctx := context.Background()

	if err := c.EnablePartitioning(ctx, "created_at", "1 day"); err != nil {
		t.Fatalf("EnablePartitioning returned error: %v", err)
	}
	cleanupPartmanRegistration(t, read, qualifiedLedgerTable(t, ctx, read))

	entry := partitionedLedgerEntry{ID: "e1", CreatedAt: time.Now()}
	if _, err := read.Client().NewInsert().Model(&entry).Exec(ctx); err != nil {
		t.Fatalf("inserting into partitioned table: %v", err)
	}

	result, err := c.Pagination(ctx, domains.Pagination{
		Filter: domains.StructuredFilter{SortFields: []domains.SortField{{Field: "id", Order: domains.SortOrderAsc}}},
	})
	if err != nil {
		t.Fatalf("Pagination returned error: %v", err)
	}
	if len(result.Data) != 1 || result.Data[0].ID != "e1" {
		t.Fatalf("expected the seeded row back from the partitioned table, got %+v", result.Data)
	}
}

func TestIntegration_SadPath_EnablePartitioningUnknownControlColumnReturnsError(t *testing.T) {
	t.Parallel()
	c, _ := newPartitionTestCQRS(t)
	if err := c.EnablePartitioning(context.Background(), "not_a_real_column", "1 day"); err == nil {
		t.Fatal("expected an error for an unknown control column, got nil")
	}
}

func TestIntegration_SadPath_EnablePartitioningNonPKControlColumnReturnsError(t *testing.T) {
	t.Parallel()
	c, _ := newPartitionTestCQRS(t)
	// "amount" is a real column but isn't part of the primary key — this
	// must be rejected up front with a clear error rather than surfacing
	// Postgres's own cryptic DDL error at CREATE TABLE time.
	if err := c.EnablePartitioning(context.Background(), "amount", "1 day"); err == nil {
		t.Fatal("expected an error for a control column that isn't part of the primary key, got nil")
	}
}

func TestIntegration_SadPath_EnablePartitioningNilReadSQLServiceReturnsError(t *testing.T) {
	t.Parallel()
	skipUnlessInfraReachable(t)
	c := cqrs.NewCQRS(cqrs.CQRSImpl[partitionedLedgerEntry, partitionedLedgerEntryResource, any, string]{
		WriteSQLService: newPostgresSQLService(t, itReadDSN),
		ToResource:      partitionedLedgerEntryToResource,
	})
	if err := c.EnablePartitioning(context.Background(), "created_at", "1 day"); err == nil {
		t.Fatal("expected an error when ReadSQLService is nil, got nil")
	}
}
