package regression

// ============================================================================
// CATEGORY: Mutation Testing
// ============================================================================
//
// Where filter_placebo_test.go shows tests that can never fail, this file is
// the deliberate opposite: each test is built around one specific, narrow
// boundary in pagination.filter.go so that a single-operator "mutant" —
// the kind of change a mutation-testing tool like go-mutesting or PIT
// introduces automatically (> -> >=, AND -> OR, IN -> NOT IN, OR -> AND
// inside a NULL check, < -> <=) — would flip this test's result.
//
// Every test picks data where the boundary actually matters: at least one
// row sits exactly on the line being tested, so a one-character mutation
// changes which rows come back, not just how many. A test that only ever
// exercises values far from the boundary (e.g. GT 5 on data at 1 and 100)
// would pass identically whether the operator were > or >=, which is just
// a placebo with extra steps — see filter_placebo_test.go's
// TestFilterPlacebo_TautologicalAssertion for that failure mode.

import (
	"context"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/domains"
)

// TestFilterMutation_GTBoundary_KillsGTEMutant pins down ModeGT's strict
// inequality (pagination.filter.go: `q.Where("? > ?", col, f.Value)`). The
// seeded row at exactly the filter value must be excluded — if a mutant
// changed ">" to ">=", that row would incorrectly appear and the length
// check below would catch it immediately.
func TestFilterMutation_GTBoundary_KillsGTEMutant(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	seedWidgets(t, read,
		widget{ID: "w1", Name: "Below", Priority: new(5)},
		widget{ID: "w2", Name: "AtBoundary", Priority: new(10)}, // exactly the GT value — must be excluded
		widget{ID: "w3", Name: "Above", Priority: new(15)},
	)

	result, err := c.Pagination(context.Background(), domains.Pagination{
		Filter: domains.StructuredFilter{
			Filters:    []domains.Filter{{Field: "priority", Mode: domains.ModeGT, Value: 10}},
			SortFields: []domains.SortField{{Field: "id", Order: domains.SortOrderAsc}},
		},
	})
	if err != nil {
		t.Fatalf("Pagination returned error: %v", err)
	}
	if len(result.Data) != 1 || result.Data[0].ID != "w3" {
		t.Fatalf("GT 10: expected exactly [w3] (boundary row w2 excluded), got %+v", result.Data)
	}
}

// TestFilterMutation_LogicAndVsOr_KillsLogicSwapMutant pins down
// applyFilters' AND/OR selection (`sep := "AND"; if filterRoot.Logic ==
// domains.LogicOr { sep = "OR" }`). The data is built so AND and OR give
// different, non-overlapping answers: only one widget satisfies both
// Active=true AND priority>10, but two widgets individually satisfy one
// term or the other, so an accidental AND->OR mutation would return 2 or 3
// rows instead of exactly 1.
func TestFilterMutation_LogicAndVsOr_KillsLogicSwapMutant(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	seedWidgets(t, read,
		widget{ID: "w1", Name: "ActiveOnly", Active: true, Priority: new(5)},     // satisfies Active=true only
		widget{ID: "w2", Name: "Both", Active: true, Priority: new(15)},          // satisfies both terms
		widget{ID: "w3", Name: "PriorityOnly", Active: false, Priority: new(20)}, // satisfies priority>10 only
	)

	// Logic left unset: zero-value "" behaves as AND (see
	// parseFilter/applyFilters), matching how most of this suite's tests
	// build filters directly in Go.
	result, err := c.Pagination(context.Background(), domains.Pagination{
		Filter: domains.StructuredFilter{
			Filters: []domains.Filter{
				{Field: "active", Mode: domains.ModeEqual, DataType: domains.DataTypeBool, Value: true},
				{Field: "priority", Mode: domains.ModeGT, Value: 10},
			},
		},
	})
	if err != nil {
		t.Fatalf("Pagination returned error: %v", err)
	}
	if len(result.Data) != 1 || result.Data[0].ID != "w2" {
		t.Fatalf("AND of active=true, priority>10: expected exactly [w2], got %+v (an AND->OR mutation "+
			"would return w1, w2 and w3 here)", result.Data)
	}
}

// TestFilterMutation_IsEmptyNullVsFalse_KillsOrToAndMutant pins down the
// OR/AND inside ModeIsEmpty's and ModeIsNotEmpty's NULL checks
// (`"(? IS NULL OR ? = ”)"` / `"(? IS NOT NULL AND ? != ”)"`), using the
// nullable boolean column "featured" so the NULL/false/true boundary is a
// real boolean distinction, not just a string one.
//
// Verified directly against this suite's SQLite fixture beforehand: a bool
// column's stored "false" (0) is NOT consumed by the `= ”` comparison —
// only a genuine NULL satisfies ModeIsEmpty on a bool column. If IsEmpty's
// OR were mutated to AND, "(col IS NULL AND col = ”)" could never be true
// simultaneously and IsEmpty would silently match zero rows always; if
// IsNotEmpty's AND were mutated to OR, it would incorrectly swallow the
// NULL row too.
func TestFilterMutation_IsEmptyNullVsFalse_KillsOrToAndMutant(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	seedWidgets(t, read,
		widget{ID: "w1", Name: "NullFeatured", Featured: nil},
		widget{ID: "w2", Name: "FalseFeatured", Featured: boolPtr(false)},
		widget{ID: "w3", Name: "TrueFeatured", Featured: boolPtr(true)},
	)
	sort := []domains.SortField{{Field: "id", Order: domains.SortOrderAsc}}

	isEmpty, err := c.Pagination(context.Background(), domains.Pagination{
		Filter: domains.StructuredFilter{Filters: []domains.Filter{{Field: "featured", Mode: domains.ModeIsEmpty}}, SortFields: sort},
	})
	if err != nil {
		t.Fatalf("IsEmpty: Pagination returned error: %v", err)
	}
	if len(isEmpty.Data) != 1 || isEmpty.Data[0].ID != "w1" {
		t.Fatalf("IsEmpty(featured): expected exactly [w1] (only the NULL row), got %+v", isEmpty.Data)
	}

	isNotEmpty, err := c.Pagination(context.Background(), domains.Pagination{
		Filter: domains.StructuredFilter{Filters: []domains.Filter{{Field: "featured", Mode: domains.ModeIsNotEmpty}}, SortFields: sort},
	})
	if err != nil {
		t.Fatalf("IsNotEmpty: Pagination returned error: %v", err)
	}
	if len(isNotEmpty.Data) != 2 || isNotEmpty.Data[0].ID != "w2" || isNotEmpty.Data[1].ID != "w3" {
		t.Fatalf("IsNotEmpty(featured): expected exactly [w2 w3] (false counts as present, only NULL is empty), got %+v",
			isNotEmpty.Data)
	}
}

// TestFilterMutation_InsideOutsideNegation_KillsINNotINSwapMutant pins down
// ModeInside vs ModeOutside (`"? IN (?)"` vs `"? NOT IN (?)"`) with a list
// that excludes exactly one of three rows, so an IN/NOT IN swap mutation
// flips which single row is excluded rather than changing a count that
// could coincidentally still match.
func TestFilterMutation_InsideOutsideNegation_KillsINNotINSwapMutant(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	seedWidgets(t, read,
		widget{ID: "w1", Name: "Alpha"},
		widget{ID: "w2", Name: "Beta"},
		widget{ID: "w3", Name: "Gamma"},
	)
	sort := []domains.SortField{{Field: "id", Order: domains.SortOrderAsc}}
	list := []any{"w1", "w3"}

	inside, err := c.Pagination(context.Background(), domains.Pagination{
		Filter: domains.StructuredFilter{Filters: []domains.Filter{{Field: "id", Mode: domains.ModeInside, Value: list}}, SortFields: sort},
	})
	if err != nil {
		t.Fatalf("Inside: Pagination returned error: %v", err)
	}
	if len(inside.Data) != 2 || inside.Data[0].ID != "w1" || inside.Data[1].ID != "w3" {
		t.Fatalf("Inside([w1,w3]): expected exactly [w1 w3], got %+v", inside.Data)
	}

	outside, err := c.Pagination(context.Background(), domains.Pagination{
		Filter: domains.StructuredFilter{Filters: []domains.Filter{{Field: "id", Mode: domains.ModeOutside, Value: list}}, SortFields: sort},
	})
	if err != nil {
		t.Fatalf("Outside: Pagination returned error: %v", err)
	}
	if len(outside.Data) != 1 || outside.Data[0].ID != "w2" {
		t.Fatalf("Outside([w1,w3]): expected exactly [w2], got %+v (an IN<->NOT IN swap would instead "+
			"return [w1 w3] here)", outside.Data)
	}
}

// TestFilterMutation_BeforeAfterBoundary_KillsInclusiveAndSwapMutants pins
// down both ModeBefore (`"? < ?"`) and ModeAfter (`"? > ?"`) against a row
// that sits exactly on the comparison instant: that row must be excluded
// from both, which simultaneously catches (a) either mode being made
// inclusive (< -> <=, > -> >=) and (b) the two modes being swapped with
// each other.
func TestFilterMutation_BeforeAfterBoundary_KillsInclusiveAndSwapMutants(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	ctx := context.Background()
	jan1 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	jun1 := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
	dec1 := time.Date(2024, 12, 1, 0, 0, 0, 0, time.UTC)
	seedWidgets(t, read,
		widget{ID: "w1", Name: "Earlier", ExpiresAt: new(jan1)},
		widget{ID: "w2", Name: "AtBoundary", ExpiresAt: new(jun1)}, // exactly the comparison instant
		widget{ID: "w3", Name: "Later", ExpiresAt: new(dec1)},
	)
	sort := []domains.SortField{{Field: "id", Order: domains.SortOrderAsc}}

	before, err := c.Pagination(ctx, domains.Pagination{
		Filter: domains.StructuredFilter{Filters: []domains.Filter{{Field: "expires_at", Mode: domains.ModeBefore, Value: jun1}}, SortFields: sort},
	})
	if err != nil {
		t.Fatalf("Before: Pagination returned error: %v", err)
	}
	if len(before.Data) != 1 || before.Data[0].ID != "w1" {
		t.Fatalf("Before(jun1): expected exactly [w1] (boundary row w2 excluded), got %+v", before.Data)
	}

	after, err := c.Pagination(ctx, domains.Pagination{
		Filter: domains.StructuredFilter{Filters: []domains.Filter{{Field: "expires_at", Mode: domains.ModeAfter, Value: jun1}}, SortFields: sort},
	})
	if err != nil {
		t.Fatalf("After: Pagination returned error: %v", err)
	}
	if len(after.Data) != 1 || after.Data[0].ID != "w3" {
		t.Fatalf("After(jun1): expected exactly [w3] (boundary row w2 excluded), got %+v", after.Data)
	}
}
