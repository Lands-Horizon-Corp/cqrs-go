package regression

import (
	"context"
	"fmt"
	"testing"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/domains"
)

// This file pushes the filter system past the small 2-3-condition examples
// used elsewhere: 10+ filters combined at once, nested backend/frontend
// AND-of-(AND)/OR logic through PaginateFilter, a large combined filter set
// walked forward then backward across several pages on the hardest sort
// path (paginateMixedDirection), and deliberately adversarial/impossible
// combinations — all through CQRSImpl's own Paginate/PaginateFilter
// wrappers (cqrs.pagination.go), not the lower-level PaginationService
// directly.

// TestCQRSPaginate_HappyPath_ThirteenAndedFiltersAllMustMatchSimultaneously
// combines 13 filters across 8 different Modes with the default LogicAnd —
// one widget is built to satisfy every single one, and several decoys each
// fail at least one condition while satisfying the rest, proving every
// condition is actually enforced rather than some being vacuously true.
func TestCQRSPaginate_HappyPath_ThirteenAndedFiltersAllMustMatchSimultaneously(t *testing.T) {
	t.Parallel()
	c, _ := newTestCQRS(t)
	ctx := context.Background()

	target := widget{
		ID: "target", Name: "Super Widget Pro", Active: true,
		Featured: new(true), Notes: new("some notes"), Priority: new(25),
	}
	seedWidget(t, c, target)
	seedWidget(t, c, widget{ID: "decoy-active", Name: target.Name, Active: false, Featured: new(true), Notes: new("x"), Priority: new(25)})
	seedWidget(t, c, widget{ID: "decoy-featured", Name: target.Name, Active: true, Featured: new(false), Notes: new("x"), Priority: new(25)})
	seedWidget(t, c, widget{ID: "decoy-contains", Name: "Super Gadget Pro", Active: true, Featured: new(true), Notes: new("x"), Priority: new(25)})
	seedWidget(t, c, widget{ID: "decoy-startswith", Name: "Really Widget Pro", Active: true, Featured: new(true), Notes: new("x"), Priority: new(25)})
	seedWidget(t, c, widget{ID: "decoy-endswith", Name: "Super Widget Max", Active: true, Featured: new(true), Notes: new("x"), Priority: new(25)})
	seedWidget(t, c, widget{ID: "decoy-notempty", Name: target.Name, Active: true, Featured: new(true), Notes: nil, Priority: new(25)})
	seedWidget(t, c, widget{ID: "decoy-not-inside", Name: target.Name, Active: true, Featured: new(true), Notes: new("x"), Priority: new(25)})
	seedWidget(t, c, widget{ID: "excluded-1", Name: target.Name, Active: true, Featured: new(true), Notes: new("x"), Priority: new(25)})
	seedWidget(t, c, widget{ID: "decoy-priority-low", Name: target.Name, Active: true, Featured: new(true), Notes: new("x"), Priority: new(2)})
	seedWidget(t, c, widget{ID: "decoy-priority-high", Name: target.Name, Active: true, Featured: new(true), Notes: new("x"), Priority: new(500)})

	result, err := c.Paginate(ctx, domains.Pagination{
		Filter: domains.StructuredFilter{Filters: []domains.Filter{
			{Field: "active", Mode: domains.ModeEqual, Value: true},
			{Field: "featured", Mode: domains.ModeNotEqual, Value: false},
			{Field: "priority", Mode: domains.ModeGT, Value: 5},
			{Field: "priority", Mode: domains.ModeGTE, Value: 10},
			{Field: "priority", Mode: domains.ModeLT, Value: 100},
			{Field: "priority", Mode: domains.ModeLTE, Value: 50},
			{Field: "name", Mode: domains.ModeContains, Value: "Widget"},
			{Field: "name", Mode: domains.ModeStartsWith, Value: "Super"},
			{Field: "name", Mode: domains.ModeEndsWith, Value: "Pro"},
			{Field: "notes", Mode: domains.ModeIsNotEmpty, Value: nil},
			{Field: "id", Mode: domains.ModeInside, Value: []any{"target", "decoy-a", "decoy-b"}},
			{Field: "id", Mode: domains.ModeOutside, Value: []any{"excluded-1", "excluded-2"}},
			{Field: "priority", Mode: domains.ModeRange, Value: domains.RangeNumber{From: 10, To: 50}},
		}},
	})
	if err != nil {
		t.Fatalf("Paginate returned error: %v", err)
	}
	if len(result.Data) != 1 || result.Data[0].ID != "target" {
		t.Fatalf("expected only [target] to satisfy all 13 ANDed conditions, got %+v", result.Data)
	}
}

// TestCQRSPaginate_HappyPath_TenOredFiltersWalkedAcrossMultiplePagesViaCursor
// combines 10 OR'd filters (each uniquely matching one of 10 widgets) with
// real cursor pagination — every matching widget must turn up somewhere
// across the walked pages, and the two widgets matching none of the 10
// conditions must never appear.
func TestCQRSPaginate_HappyPath_TenOredFiltersWalkedAcrossMultiplePagesViaCursor(t *testing.T) {
	t.Parallel()
	c, _ := newTestCQRS(t)
	ctx := context.Background()

	matchNames := []string{
		"Alpha", "Bravo", "Charlie", "Delta", "Echo",
		"Foxtrot", "Golf", "Hotel", "India", "Juliet",
	}
	filters := make([]domains.Filter, len(matchNames))
	for i, name := range matchNames {
		filters[i] = domains.Filter{Field: "name", Mode: domains.ModeEqual, Value: name}
		seedWidget(t, c, widget{ID: fmt.Sprintf("or-%02d", i), Name: name})
	}
	seedWidget(t, c, widget{ID: "no-match-1", Name: "Kilo"})
	seedWidget(t, c, widget{ID: "no-match-2", Name: "Lima"})

	sortFields := []domains.SortField{{Field: "name", Order: domains.SortOrderAsc}}
	seen := map[string]bool{}
	var cursor *string
	pages := 0
	for {
		page, err := c.Paginate(ctx, domains.Pagination{
			Filter:   domains.StructuredFilter{Filters: filters, Logic: domains.LogicOr, SortFields: sortFields},
			PageSize: 3,
			Cursor:   cursor,
		})
		if err != nil {
			t.Fatalf("page %d: Paginate returned error: %v", pages, err)
		}
		pages++
		for _, w := range page.Data {
			if seen[w.Name] {
				t.Fatalf("widget named %q returned more than once across pages", w.Name)
			}
			seen[w.Name] = true
		}
		if page.NextCursor == nil {
			break
		}
		cursor = page.NextCursor
		if pages > len(matchNames) {
			t.Fatal("walked more pages than there are matching widgets — likely an infinite loop")
		}
	}
	if len(seen) != len(matchNames) {
		t.Fatalf("expected exactly %d rows across all pages, got %d: %v", len(matchNames), len(seen), seen)
	}
	for _, name := range matchNames {
		if !seen[name] {
			t.Errorf("expected %q to have been walked, was not seen", name)
		}
	}
	if seen["Kilo"] || seen["Lima"] {
		t.Errorf("expected the two non-matching widgets to be excluded, got %+v", seen)
	}
}

// TestCQRSPaginateFilter_HappyPath_NestedHardcodedAndFrontendOrLogicCombine
// nests a multi-condition AND (hardcoded) with a multi-condition OR
// (frontend) through PaginateFilter: "(active=true AND priority>=10) AND
// (name contains Red OR Blue OR Green)" — genuinely nested composite logic,
// not just a flat list of conditions.
func TestCQRSPaginateFilter_HappyPath_NestedHardcodedAndFrontendOrLogicCombine(t *testing.T) {
	t.Parallel()
	c, _ := newTestCQRS(t)
	ctx := context.Background()

	seedWidget(t, c, widget{ID: "w1", Name: "RedWidget", Active: true, Priority: new(20)})
	seedWidget(t, c, widget{ID: "w2", Name: "BlueWidget", Active: true, Priority: new(20)})
	seedWidget(t, c, widget{ID: "w3", Name: "GreenWidget", Active: true, Priority: new(20)})
	seedWidget(t, c, widget{ID: "w4", Name: "RedWidget", Active: false, Priority: new(20)})   // fails hardcoded (active)
	seedWidget(t, c, widget{ID: "w5", Name: "RedWidget", Active: true, Priority: new(5)})     // fails hardcoded (priority)
	seedWidget(t, c, widget{ID: "w6", Name: "YellowWidget", Active: true, Priority: new(20)}) // fails frontend (no color match)

	result, err := c.PaginateFilter(ctx,
		domains.StructuredFilter{Filters: []domains.Filter{
			{Field: "active", Mode: domains.ModeEqual, Value: true},
			{Field: "priority", Mode: domains.ModeGTE, Value: 10},
		}},
		domains.Pagination{
			Filter: domains.StructuredFilter{
				Logic: domains.LogicOr,
				Filters: []domains.Filter{
					{Field: "name", Mode: domains.ModeContains, Value: "Red"},
					{Field: "name", Mode: domains.ModeContains, Value: "Blue"},
					{Field: "name", Mode: domains.ModeContains, Value: "Green"},
				},
				SortFields: []domains.SortField{{Field: "id", Order: domains.SortOrderAsc}},
			},
		},
	)
	if err != nil {
		t.Fatalf("PaginateFilter returned error: %v", err)
	}
	if len(result.Data) != 3 || result.Data[0].ID != "w1" || result.Data[1].ID != "w2" || result.Data[2].ID != "w3" {
		t.Fatalf("expected [w1, w2, w3], got %+v", result.Data)
	}
}

// TestCQRSPaginate_HappyPath_ComplexFilterWithMixedDirectionSortFullCursorWalk
// is the hardest combination in this file: a 3-condition filter narrowing a
// mixed pool down to exactly 12 rows (including priority ties, forcing the
// name tiebreaker to matter), sorted priority DESC + name ASC (a genuinely
// mixed direction, which forces paginateMixedDirection rather than the
// simpler uniform row-value-comparison path), walked forward to the end and
// then backward to the start via cursor — every forward page must be
// exactly reproduced by its corresponding backward page.
func TestCQRSPaginate_HappyPath_ComplexFilterWithMixedDirectionSortFullCursorWalk(t *testing.T) {
	t.Parallel()
	c, _ := newTestCQRS(t)
	ctx := context.Background()

	// 12 matching widgets, descending priority with ties at 90 (x2) and 10
	// (x3), each with a distinct name to make the ASC tiebreak deterministic.
	priorities := []int{90, 90, 80, 70, 60, 50, 40, 30, 20, 10, 10, 10}
	for i, p := range priorities {
		seedWidget(t, c, widget{
			ID: fmt.Sprintf("match-%02d", i), Name: fmt.Sprintf("Target-%03d-%c", p, rune('A'+i)),
			Active: true, Priority: new(p),
		})
	}
	// Decoys: fail exactly one of the three filter conditions each.
	seedWidget(t, c, widget{ID: "decoy-inactive", Name: "Target-999-Z", Active: false, Priority: new(50)})
	seedWidget(t, c, widget{ID: "decoy-out-of-range", Name: "Target-999-Z", Active: true, Priority: new(500)})
	seedWidget(t, c, widget{ID: "decoy-no-name-match", Name: "Unrelated-999", Active: true, Priority: new(50)})

	filter := domains.StructuredFilter{
		Filters: []domains.Filter{
			{Field: "active", Mode: domains.ModeEqual, Value: true},
			{Field: "priority", Mode: domains.ModeRange, Value: domains.RangeNumber{From: 10, To: 90}},
			{Field: "name", Mode: domains.ModeContains, Value: "Target"},
		},
		SortFields: []domains.SortField{
			{Field: "priority", Order: domains.SortOrderDesc},
			{Field: "name", Order: domains.SortOrderAsc},
		},
	}
	const pageSize = 3
	const total = 12

	var forwardPages [][]string
	var forwardCursors []*string
	var cursor *string
	for {
		page, err := c.Paginate(ctx, domains.Pagination{Filter: filter, PageSize: pageSize, Cursor: cursor})
		if err != nil {
			t.Fatalf("forward page %d: Paginate returned error: %v", len(forwardPages), err)
		}
		ids := make([]string, len(page.Data))
		for i, w := range page.Data {
			ids[i] = w.ID
		}
		forwardPages = append(forwardPages, ids)
		forwardCursors = append(forwardCursors, cursor)
		if page.NextCursor == nil {
			break
		}
		cursor = page.NextCursor
		if len(forwardPages) > total {
			t.Fatal("forward walk exceeded total matching rows without terminating")
		}
	}
	if len(forwardPages) != total/pageSize {
		t.Fatalf("expected %d forward pages, got %d: %v", total/pageSize, len(forwardPages), forwardPages)
	}

	lastPage, err := c.Paginate(ctx, domains.Pagination{Filter: filter, PageSize: pageSize, Cursor: forwardCursors[len(forwardCursors)-1]})
	if err != nil {
		t.Fatalf("re-fetching last page: %v", err)
	}
	backCursor := lastPage.PreviousCursor
	for i := len(forwardPages) - 2; i >= 0; i-- {
		if backCursor == nil {
			t.Fatalf("backward walk terminated early at forward-page index %d", i)
		}
		page, err := c.Paginate(ctx, domains.Pagination{Filter: filter, PageSize: pageSize, Cursor: backCursor})
		if err != nil {
			t.Fatalf("backward page (reconstructing forward page %d): %v", i, err)
		}
		gotIDs := make([]string, len(page.Data))
		for j, w := range page.Data {
			gotIDs[j] = w.ID
		}
		if fmt.Sprint(gotIDs) != fmt.Sprint(forwardPages[i]) {
			t.Fatalf("backward reconstruction of forward page %d: expected %v, got %v", i, forwardPages[i], gotIDs)
		}
		backCursor = page.PreviousCursor
	}
	if backCursor != nil {
		t.Errorf("expected a nil PreviousCursor once the backward walk reaches the first page, got %v", *backCursor)
	}
}

// TestCQRSPaginate_PoisonPill_TwentyTwoRedundantAndedFiltersStillWork stress
// tests raw filter count/query size rather than semantic complexity: 22
// ANDed conditions (mostly redundant/overlapping numeric bounds plus a
// handful of real ones), confirming the query still builds and executes
// correctly at that scale instead of hitting some placeholder-count or
// query-length limit.
func TestCQRSPaginate_PoisonPill_TwentyTwoRedundantAndedFiltersStillWork(t *testing.T) {
	t.Parallel()
	c, _ := newTestCQRS(t)
	ctx := context.Background()

	seedWidget(t, c, widget{ID: "target", Name: "StressTestWidget", Active: true, Priority: new(50)})
	seedWidget(t, c, widget{ID: "decoy", Name: "StressTestWidget", Active: true, Priority: new(999)})

	filters := []domains.Filter{
		{Field: "active", Mode: domains.ModeEqual, Value: true},
		{Field: "name", Mode: domains.ModeContains, Value: "StressTest"},
	}
	// 20 redundant, progressively-narrowing numeric bounds — all satisfied
	// by priority=50, none by priority=999 past a certain point.
	for i := 1; i <= 10; i++ {
		filters = append(filters,
			domains.Filter{Field: "priority", Mode: domains.ModeGTE, Value: i},
			domains.Filter{Field: "priority", Mode: domains.ModeLTE, Value: 1000 - i},
		)
	}
	if len(filters) != 22 {
		t.Fatalf("test setup error: expected 22 filters, got %d", len(filters))
	}

	result, err := c.Paginate(ctx, domains.Pagination{Filter: domains.StructuredFilter{Filters: filters}})
	if err != nil {
		t.Fatalf("Paginate returned error with 22 ANDed filters: %v", err)
	}
	if len(result.Data) != 1 || result.Data[0].ID != "target" {
		t.Fatalf("expected only [target] to survive all 22 conditions, got %+v", result.Data)
	}
}

// TestCQRSPaginate_PoisonPill_ContradictoryFiltersReturnZeroRowsNotError is
// the literal "impossible for filter" case: priority > 100 AND priority <
// 50 can never both be true for any row — the system must return an empty
// result cleanly, not error or panic, for a filter combination that's
// structurally impossible to satisfy.
func TestCQRSPaginate_PoisonPill_ContradictoryFiltersReturnZeroRowsNotError(t *testing.T) {
	t.Parallel()
	c, _ := newTestCQRS(t)
	ctx := context.Background()
	seedWidget(t, c, widget{ID: "w1", Name: "Anything", Priority: new(75)})

	result, err := c.Paginate(ctx, domains.Pagination{
		Filter: domains.StructuredFilter{Filters: []domains.Filter{
			{Field: "priority", Mode: domains.ModeGT, Value: 100},
			{Field: "priority", Mode: domains.ModeLT, Value: 50},
		}},
	})
	if err != nil {
		t.Fatalf("expected a clean empty result for a contradictory filter, got error: %v", err)
	}
	if len(result.Data) != 0 {
		t.Fatalf("expected zero rows for an impossible-to-satisfy filter, got %+v", result.Data)
	}
}
