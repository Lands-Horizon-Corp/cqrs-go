package regression

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/domains"
	"github.com/Lands-Horizon-Corp/cqrs-go/src/pagination"
)

// This file verifies pagination's own filter-field normalization end to
// end: a client-supplied Filter.Field in any casing/spacing still resolves
// to the real column (see utils.NormalizeColumnName for the unit-level
// behavior in normalize_test.go), and a field that still doesn't resolve
// after normalization is dropped with a Warn instead of failing the page.

func TestPagination_HappyPath_FilterFieldNormalizesCamelCaseAndMessySpacingVariants(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	seedWidgets(t, read, widget{ID: "w1", Name: "Alpha"}, widget{ID: "w2", Name: "Beta"})

	variants := []string{"name", "Name", "NAME", "  name  "}
	for _, field := range variants {
		t.Run(field, func(t *testing.T) {
			result, err := c.Pagination(context.Background(), domains.Pagination{
				Filter: domains.StructuredFilter{Filters: []domains.Filter{
					{Field: field, Mode: domains.ModeEqual, Value: "Alpha"},
				}},
			})
			if err != nil {
				t.Fatalf("Pagination returned error for field spelling %q: %v", field, err)
			}
			if len(result.Data) != 1 || result.Data[0].ID != "w1" {
				t.Fatalf("field spelling %q: expected only [w1], got %+v", field, result.Data)
			}
		})
	}
}

func TestPagination_HappyPath_UnknownFilterFieldLogsAWarnWhenDropped(t *testing.T) {
	t.Parallel()
	read := newFakeSQLService(t)
	logs := &fakeLogService{}
	c := pagination.NewPaginationService(pagination.PaginationService[widget, string]{
		ReadSQLService: read,
		LogService:     logs,
	})
	seedWidgets(t, read, widget{ID: "w1", Name: "Alpha"})

	result, err := c.Pagination(context.Background(), domains.Pagination{
		Filter: domains.StructuredFilter{Filters: []domains.Filter{
			{Field: "Not A Real Column!!", Mode: domains.ModeEqual, Value: "x"},
		}},
	})
	if err != nil {
		t.Fatalf("expected the unknown filter field to be dropped rather than error, got: %v", err)
	}
	if len(result.Data) != 1 {
		t.Fatalf("expected the seeded row back (unknown filter ignored), got %+v", result.Data)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		for _, call := range logs.snapshot() {
			if call.level == "warn" && strings.Contains(call.msg, "not_a_real_column") {
				return
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("expected a warn log naming the dropped, normalized field; got %+v", logs.snapshot())
}

// TestPagination_HappyPath_WholeIndexSearchFilterSurvivesNormalization
// covers a real bug class: {Mode: ModeSearch, Field: ""} means "search
// every column EnableSearchIndex indexed" (see applyFilterTerm), not "an
// unknown column named \"\"" — normalizeFilters must special-case it
// rather than normalizing "" and dropping it like any other unresolvable
// field.
//
// ModeSearch itself (the "@@@" operator) is ParadeDB/Postgres-only and has
// no SQLite equivalent — genuinely exercising it lives in
// integration_search_test.go against real Postgres. This test only proves
// normalizeFilters doesn't drop the filter, without needing that real
// infra: if it *had* been dropped, Filters would end up empty and this
// call would succeed with the seeded row back. Instead it must reach
// applyFilterTerm and build a query containing "@@@", which SQLite then
// rejects with a syntax error — that error is the proof the filter
// survived normalization intact.
func TestPagination_HappyPath_WholeIndexSearchFilterSurvivesNormalization(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	seedWidgets(t, read, widget{ID: "w1", Name: "Alpha"})

	_, err := c.Pagination(context.Background(), domains.Pagination{
		Filter: domains.StructuredFilter{Filters: []domains.Filter{
			{Field: "", Mode: domains.ModeSearch, Value: "alpha"},
		}},
	})
	if err == nil {
		t.Fatal("expected a SQLite syntax error from the @@@ operator (proving the filter reached query-building instead of being dropped), got nil")
	}
}
