package regression

// ============================================================================
// CATEGORY: High Bugs (bugs in the TEST code, not the production code)
// ============================================================================
//
// Every test in this file has real, concrete assertions — unlike
// filter_placebo_test.go, these are not missing checks. The bug in each one
// lives in the test's own logic: a loop bound that's off by one, an
// expected value copy-pasted from the wrong case, a comparison that looks
// meaningful but structurally can never fire, an assertion aimed at the
// wrong variable, or an ordering assumption the API never promised. Each
// currently passes — that's what makes this category dangerous: a suite
// full of tests like these looks exactly as healthy as a suite full of
// correct ones, right up until the day the bug they were supposed to catch
// actually ships.

import (
	"context"
	"testing"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/domains"
)

// TestFilterHighBugs_OffByOneSkipsLastCase builds a 3-case table but the
// loop bound is `i < len(cases)-1`, not `i < len(cases)`. The third case
// (LTE) is built, seeded against, and never actually checked — a
// production regression in ModeLTE specifically would sail through this
// test untouched, while GT and GTE being correct makes the whole function
// report green.
func TestFilterHighBugs_OffByOneSkipsLastCase(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	seedWidgets(t, read,
		widget{ID: "w1", Name: "Alpha", Priority: new(5)},
		widget{ID: "w2", Name: "Beta", Priority: new(10)},
		widget{ID: "w3", Name: "Gamma", Priority: new(15)},
	)

	cases := []struct {
		name   string
		filter domains.Filter
		want   []string
	}{
		{"GT", domains.Filter{Field: "priority", Mode: domains.ModeGT, Value: 10}, []string{"w3"}},
		{"GTE", domains.Filter{Field: "priority", Mode: domains.ModeGTE, Value: 10}, []string{"w2", "w3"}},
		// BUG: this LTE case is defined but the loop below never reaches
		// index 2, so it is never executed. A broken ModeLTE implementation
		// would not make this test fail.
		{"LTE", domains.Filter{Field: "priority", Mode: domains.ModeLTE, Value: 10}, []string{"w1", "w2"}},
	}

	// BUG: should be `i < len(cases)`.
	for i := 0; i < len(cases)-1; i++ {
		tc := cases[i]
		t.Run(tc.name, func(t *testing.T) {
			result, err := c.Pagination(context.Background(), domains.Pagination{
				Filter: domains.StructuredFilter{
					Filters:    []domains.Filter{tc.filter},
					SortFields: []domains.SortField{{Field: "id", Order: domains.SortOrderAsc}},
				},
			})
			if err != nil {
				t.Fatalf("Pagination returned error: %v", err)
			}
			if len(result.Data) != len(tc.want) {
				t.Fatalf("expected %d rows, got %d: %+v", len(tc.want), len(result.Data), result.Data)
			}
		})
	}
}

// TestFilterHighBugs_CopyPasteWrongExpectedSlice_GTAndGTEIndistinguishable
// compares ModeGT and ModeGTE against data that has no row sitting exactly
// on the boundary value — so the two modes produce identical results no
// matter which one is actually implemented correctly, and the test (copied
// from a GT case, then reused for GTE without adjusting the data) can never
// tell a GT/GTE mix-up apart. Contrast with
// TestFilterMutation_GTBoundary_KillsGTEMutant in filter_mutation_test.go,
// which seeds a row exactly at the boundary for exactly this reason.
func TestFilterHighBugs_CopyPasteWrongExpectedSlice_GTAndGTEIndistinguishable(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	seedWidgets(t, read,
		widget{ID: "w1", Name: "Below", Priority: new(5)},
		widget{ID: "w2", Name: "Above", Priority: new(15)},
		// BUG: no widget at Priority == 10 (the threshold below), so GT and
		// GTE cannot be distinguished by this dataset.
	)
	want := []string{"w2"}

	gt, err := c.Pagination(context.Background(), domains.Pagination{
		Filter: domains.StructuredFilter{Filters: []domains.Filter{{Field: "priority", Mode: domains.ModeGT, Value: 10}}},
	})
	if err != nil {
		t.Fatalf("GT: Pagination returned error: %v", err)
	}
	if len(gt.Data) != len(want) || gt.Data[0].ID != want[0] {
		t.Fatalf("GT 10: expected %v, got %+v", want, gt.Data)
	}

	gte, err := c.Pagination(context.Background(), domains.Pagination{
		Filter: domains.StructuredFilter{Filters: []domains.Filter{{Field: "priority", Mode: domains.ModeGTE, Value: 10}}},
	})
	if err != nil {
		t.Fatalf("GTE: Pagination returned error: %v", err)
	}
	// BUG: reuses the exact same `want` as GT instead of an independently
	// chosen boundary dataset — this passes whether ModeGTE is actually
	// inclusive or was accidentally implemented as a second GT.
	if len(gte.Data) != len(want) || gte.Data[0].ID != want[0] {
		t.Fatalf("GTE 10: expected %v, got %+v", want, gte.Data)
	}
}

// TestFilterHighBugs_PointerIdentityComparisonNeverTriggersFailure seeds a
// widget with a non-nil *bool and, after reading it back through
// Pagination, "checks" it by comparing the returned pointer's identity
// against the original seeding pointer. Every row scanned out of the
// database is a freshly allocated Go value, so this pointer comparison is
// always false — the t.Error branch below can never run, regardless of
// whether Featured's actual boolean value came back correct, flipped, or
// nil.
func TestFilterHighBugs_PointerIdentityComparisonNeverTriggersFailure(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	originalFeatured := true
	seedWidgets(t, read, widget{ID: "w1", Name: "Alpha", Featured: &originalFeatured})

	result, err := c.Pagination(context.Background(), domains.Pagination{
		Filter: domains.StructuredFilter{Filters: []domains.Filter{{Field: "id", Mode: domains.ModeEqual, Value: "w1"}}},
	})
	if err != nil {
		t.Fatalf("Pagination returned error: %v", err)
	}
	if len(result.Data) != 1 {
		t.Fatalf("expected exactly 1 row, got %+v", result.Data)
	}
	// BUG: `==` here compares pointer identity, not the pointed-to bool
	// value. result.Data[0].Featured is always a distinct allocation from
	// &originalFeatured (it came back through a database round trip), so
	// this condition is always false and this check can never fail — even
	// if Featured had come back nil, or pointing at false.
	if result.Data[0].Featured == &originalFeatured {
		t.Error("expected a freshly-scanned pointer, got the original seeding pointer back somehow")
	}
}

// TestFilterHighBugs_AssertsOnPreFilterDataInsteadOfQueryResult builds the
// expected row count from the Go slice used to *seed* the database, then
// asserts against that slice's own length instead of result.Data — a
// refactor-era copy-paste mistake that means this test cannot detect any
// bug in Pagination's actual filtering at all: it would pass identically
// even if c.Pagination always returned zero rows, or an error were ignored.
func TestFilterHighBugs_AssertsOnPreFilterDataInsteadOfQueryResult(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	widgets := []widget{
		{ID: "w1", Name: "Alpha", Active: true},
		{ID: "w2", Name: "Beta", Active: true},
		{ID: "w3", Name: "Gamma", Active: false},
	}
	seedWidgets(t, read, widgets...)

	result, err := c.Pagination(context.Background(), domains.Pagination{
		Filter: domains.StructuredFilter{Filters: []domains.Filter{
			{Field: "active", Mode: domains.ModeEqual, DataType: domains.DataTypeBool, Value: true},
		}},
	})
	if err != nil {
		t.Fatalf("Pagination returned error: %v", err)
	}
	_ = result // BUG: the real query result is never inspected below.

	activeCount := 0
	for _, w := range widgets {
		if w.Active {
			activeCount++
		}
	}
	// BUG: this recomputes "how many of the widgets we built happen to have
	// Active=true" from the local `widgets` slice — a fact that was true
	// the moment the slice literal above was written, completely
	// independent of whether c.Pagination's database round trip, filtering,
	// or network/driver layer works at all.
	if activeCount != 2 {
		t.Fatalf("expected 2 active widgets in the seed data, got %d", activeCount)
	}
}

// TestFilterHighBugs_RelyingOnImplicitInsertOrderWithoutSortField asserts a
// specific row order back from Pagination without supplying any
// SortFields, assuming rows come back in insertion order. They don't: with
// no SortFields given, resolveSortFields falls back to
// PaginationService.ColumnDefaultSort (defaulted by NewPaginationService to
// "updated_at DESC" when an operator doesn't set one) plus a "? DESC"
// tiebreaker on ColumnDefaultID — confirmed directly, this is why the
// assertion below is keyed on id DESC, not on the order the two widgets
// were inserted in. The test still passes today, but for a completely
// different reason than its author (and its own missing SortFields)
// implied: it is silently pinned to this service's operator-configured
// default sort, which is invisible here and would change the "expected"
// order the moment someone sets ColumnDefaultSort differently, or the two
// rows' UpdatedAt timestamps stop tying.
func TestFilterHighBugs_RelyingOnImplicitInsertOrderWithoutSortField(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	seedWidgets(t, read,
		widget{ID: "w1", Name: "Alpha", Priority: new(5)},
		widget{ID: "w2", Name: "Beta", Priority: new(5)},
	)

	// BUG: no SortFields set. See
	// TestPagination_HappyPath_MixedAscDescMultiColumnSortWithTiesAndCursor
	// elsewhere in this suite for how a real test pins down order instead —
	// by supplying explicit SortFields, not by relying on whatever default
	// sort happens to be configured.
	result, err := c.Pagination(context.Background(), domains.Pagination{
		Filter: domains.StructuredFilter{Filters: []domains.Filter{{Field: "priority", Mode: domains.ModeEqual, Value: 5}}},
	})
	if err != nil {
		t.Fatalf("Pagination returned error: %v", err)
	}
	// Both rows tie on UpdatedAt (zero value), so the id-DESC tiebreaker
	// alone decides the order: "w2" sorts before "w1" descending — the
	// opposite of insertion order, and not something this test's own code
	// states anywhere.
	if len(result.Data) != 2 || result.Data[0].ID != "w2" || result.Data[1].ID != "w1" {
		ids := make([]string, len(result.Data))
		for i, r := range result.Data {
			ids[i] = r.ID
		}
		t.Fatalf("expected [w2 w1] (today's actual default-sort order), got %v", ids)
	}
}
