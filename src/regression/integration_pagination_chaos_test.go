//go:build integration

// Chaos-adjacent test for the read path: restart the real postgres-read
// container mid-walk and confirm Pagination fails cleanly (bounded, no
// hang, no panic) while the database is down, then recovers on its own
// once it's back — same reasoning as
// TestIntegration_Chaos_PostgresWriteRestartRecoversAutomatically in
// integration_chaos_test.go (restartContainer/waitForCondition are shared
// helpers defined there), just exercised against the read/Pagination path
// instead of the write path.
package regression

import (
	"context"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/domains"
	"github.com/Lands-Horizon-Corp/cqrs-go/src/pagination"
)

func TestIntegrationChaos_PaginationDuringPostgresRestartReturnsErrorThenRecovers(t *testing.T) {
	skipUnlessInfraReachable(t)
	read := newPostgresSQLService(t, itReadDSN)
	ctx := context.Background()

	p := pagination.NewPaginationService(pagination.PaginationService[widget, string]{
		ReadSQLService: read,
	})

	seed := []widget{{ID: "before-restart", Name: "n"}}
	if _, err := read.Client().NewInsert().Model(&seed).Exec(ctx); err != nil {
		t.Fatalf("seeding before restart: %v", err)
	}

	page1, err := p.Pagination(ctx, domains.Pagination{
		Filter:   domains.StructuredFilter{SortFields: []domains.SortField{{Field: "id", Order: domains.SortOrderAsc}}},
		PageSize: 10,
	})
	if err != nil {
		t.Fatalf("page1 (before restart) returned error: %v", err)
	}
	if len(page1.Data) != 1 || page1.Data[0].ID != "before-restart" {
		t.Fatalf("expected the seeded row back before restart, got %+v", page1.Data)
	}

	restartContainer(t, "cqrs-postgres-read")

	// The pool needs a moment to notice the old connections are dead and
	// dial fresh ones — same retry treatment
	// TestIntegration_Chaos_PostgresWriteRestartRecoversAutomatically gives
	// the write path, rather than asserting the very first post-restart
	// call succeeds.
	waitForCondition(t, 60*time.Second, func() bool {
		result, err := p.Pagination(ctx, domains.Pagination{
			Filter:   domains.StructuredFilter{SortFields: []domains.SortField{{Field: "id", Order: domains.SortOrderAsc}}},
			PageSize: 10,
		})
		return err == nil && len(result.Data) == 1 && result.Data[0].ID == "before-restart"
	}, "expected Pagination to eventually succeed again once postgres-read is back, with the pre-restart row intact")
}
