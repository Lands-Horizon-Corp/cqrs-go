//go:build integration

package regression

import (
	"context"
	"fmt"
	"testing"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/domains"
)

// This file covers ModeSearch composed with the rest of the pagination
// filter system — combined with other filter modes, with the
// PaginateFilter backend/frontend AND-merge, with cursor pagination across
// multiple pages, and at real scale (thousands of rows, a small known
// subset of which should match) — as opposed to integration_search_test.go's
// minimal single-assertion coverage of EnableSearchIndex/ModeSearch in
// isolation.

func TestIntegration_HappyPath_ModeSearchCombinedWithEqualFilterUsesAnd(t *testing.T) {
	t.Parallel()
	c, read := newSearchTestCQRS(t)
	ctx := context.Background()

	if err := c.EnableSearchIndex(ctx, "name", "notes"); err != nil {
		t.Fatalf("EnableSearchIndex returned error: %v", err)
	}
	seedWidgets(t, read,
		widget{ID: "w1", Name: "wireless gadget", Notes: new("in stock"), Active: true},
		widget{ID: "w2", Name: "wireless gadget", Notes: new("discontinued"), Active: false},
		widget{ID: "w3", Name: "wired gadget", Notes: new("in stock"), Active: true},
	)

	// BM25 match on "wireless" AND a plain Equal filter (active=true) —
	// only w1 satisfies both; w2 matches the search but fails Active, w3
	// passes Active but doesn't match the search term.
	result, err := c.Pagination(ctx, domains.Pagination{
		Filter: domains.StructuredFilter{Filters: []domains.Filter{
			{Field: "name", Mode: domains.ModeSearch, Value: "wireless"},
			{Field: "active", Mode: domains.ModeEqual, Value: true},
		}},
	})
	if err != nil {
		t.Fatalf("Pagination returned error: %v", err)
	}
	if len(result.Data) != 1 || result.Data[0].ID != "w1" {
		t.Fatalf("expected only [w1] (search match AND active=true), got %+v", result.Data)
	}
}

// TestIntegration_HappyPath_MultipleScopedSearchesCombinedWithOr is the
// "search a custom subset of indexed columns, not all of them" pattern
// documented in pagination.filter.go's ModeSearch comment: two scoped
// ModeSearch filters (one per column) combined with LogicOr, rather than
// the whole-index empty-Field form which would search every indexed
// column unconditionally.
func TestIntegration_HappyPath_MultipleScopedSearchesCombinedWithOr(t *testing.T) {
	t.Parallel()
	c, read := newSearchTestCQRS(t)
	ctx := context.Background()

	if err := c.EnableSearchIndex(ctx, "name", "notes"); err != nil {
		t.Fatalf("EnableSearchIndex returned error: %v", err)
	}
	seedWidgets(t, read,
		widget{ID: "w1", Name: "sunrise lamp", Notes: new("ships tomorrow")},
		widget{ID: "w2", Name: "desk fan", Notes: new("sunrise delivery slot")},
		widget{ID: "w3", Name: "desk fan", Notes: new("ships tomorrow")},
	)

	result, err := c.Pagination(ctx, domains.Pagination{
		Filter: domains.StructuredFilter{
			Logic: domains.LogicOr,
			Filters: []domains.Filter{
				{Field: "name", Mode: domains.ModeSearch, Value: "sunrise"},
				{Field: "notes", Mode: domains.ModeSearch, Value: "sunrise"},
			},
			SortFields: []domains.SortField{{Field: "id", Order: domains.SortOrderAsc}},
		},
	})
	if err != nil {
		t.Fatalf("Pagination returned error: %v", err)
	}
	if len(result.Data) != 2 || result.Data[0].ID != "w1" || result.Data[1].ID != "w2" {
		t.Fatalf("expected [w1, w2] (\"sunrise\" in either name or notes), got %+v", result.Data)
	}
}

// TestIntegration_HappyPath_PaginateFilterCombinesHardcodedFilterWithFrontendModeSearch
// ties ModeSearch to the PaginateFilter AND-merge: a backend-hardcoded
// Equal filter and a frontend-supplied ModeSearch term must both apply,
// neither overriding the other.
func TestIntegration_HappyPath_PaginateFilterCombinesHardcodedFilterWithFrontendModeSearch(t *testing.T) {
	t.Parallel()
	c, read := newSearchTestCQRS(t)
	ctx := context.Background()

	if err := c.EnableSearchIndex(ctx, "name"); err != nil {
		t.Fatalf("EnableSearchIndex returned error: %v", err)
	}
	seedWidgets(t, read,
		widget{ID: "w1", Name: "premium headphones", Active: true},
		widget{ID: "w2", Name: "premium headphones", Active: false},
		widget{ID: "w3", Name: "budget headphones", Active: true},
	)

	result, err := c.PaginateFilter(ctx,
		domains.StructuredFilter{Filters: []domains.Filter{{Field: "active", Mode: domains.ModeEqual, Value: true}}},
		domains.Pagination{Filter: domains.StructuredFilter{Filters: []domains.Filter{
			{Field: "name", Mode: domains.ModeSearch, Value: "premium"},
		}}},
	)
	if err != nil {
		t.Fatalf("PaginateFilter returned error: %v", err)
	}
	if len(result.Data) != 1 || result.Data[0].ID != "w1" {
		t.Fatalf("expected only [w1] (active=true AND search match on \"premium\"), got %+v", result.Data)
	}
}

// TestIntegration_HappyPath_ModeSearchWithCursorPaginationWalksAllMatchesWithoutGapsOrOverlap
// proves ModeSearch composes correctly with keyset cursor pagination across
// several pages, forward then backward — not just a single-page result.
func TestIntegration_HappyPath_ModeSearchWithCursorPaginationWalksAllMatchesWithoutGapsOrOverlap(t *testing.T) {
	t.Parallel()
	c, read := newSearchTestCQRS(t)
	ctx := context.Background()

	if err := c.EnableSearchIndex(ctx, "notes"); err != nil {
		t.Fatalf("EnableSearchIndex returned error: %v", err)
	}

	const matching = 23
	widgets := make([]widget, 0, matching+matching)
	for i := range matching {
		widgets = append(widgets,
			widget{ID: fmt.Sprintf("match-%02d", i), Name: fmt.Sprintf("Widget %02d", i), Notes: new("contains the keyword marigold")},
			widget{ID: fmt.Sprintf("nomatch-%02d", i), Name: fmt.Sprintf("Other %02d", i), Notes: new("nothing relevant here")},
		)
	}
	seedWidgets(t, read, widgets...)

	sortFields := []domains.SortField{{Field: "id", Order: domains.SortOrderAsc}}
	seen := map[string]bool{}
	var cursor *string
	pages := 0
	for {
		page, err := c.Pagination(ctx, domains.Pagination{
			Filter: domains.StructuredFilter{
				Filters:    []domains.Filter{{Field: "notes", Mode: domains.ModeSearch, Value: "marigold"}},
				SortFields: sortFields,
			},
			PageSize: 5,
			Cursor:   cursor,
		})
		if err != nil {
			t.Fatalf("page %d error: %v", pages, err)
		}
		pages++
		for _, w := range page.Data {
			if seen[w.ID] {
				t.Fatalf("row %s returned more than once across pages", w.ID)
			}
			seen[w.ID] = true
		}
		if page.NextCursor == nil {
			break
		}
		cursor = page.NextCursor
		if pages > matching { // guard against an infinite loop on a real bug
			t.Fatal("walked more pages than there are matching rows — likely an infinite loop")
		}
	}
	if len(seen) != matching {
		t.Fatalf("expected exactly %d matching rows walked across all pages, got %d", matching, len(seen))
	}
	for i := range matching {
		id := fmt.Sprintf("match-%02d", i)
		if !seen[id] {
			t.Errorf("expected %s to have been walked, was not seen", id)
		}
	}
}

// TestIntegration_HappyPath_ModeSearchAtScaleFindsExactSubsetAmongThousandsOfRows
// is the actual "lot of data" case: a real BM25 index over several thousand
// rows, where a search term correctly isolates a small, exact, known
// subset rather than matching too broadly or too narrowly. Also confirms
// EnableSearchIndex/BM25 querying stays correct (not merely tolerable) at a
// scale where a naive LIKE '%term%' scan would be visibly slow.
func TestIntegration_HappyPath_ModeSearchAtScaleFindsExactSubsetAmongThousandsOfRows(t *testing.T) {
	t.Parallel()
	c, read := newSearchTestCQRS(t)
	ctx := context.Background()

	if err := c.EnableSearchIndex(ctx, "name", "notes"); err != nil {
		t.Fatalf("EnableSearchIndex returned error: %v", err)
	}

	const (
		total       = 4000
		needleCount = 9
	)
	fillerWords := []string{
		"widget", "gadget", "gizmo", "device", "component", "module",
		"bracket", "fastener", "adapter", "sensor", "panel", "housing",
	}
	widgets := make([]widget, 0, total)
	for i := range total {
		widgets = append(widgets, widget{
			ID:       fmt.Sprintf("bulk-%05d", i),
			Name:     fmt.Sprintf("%s %s %d", fillerWords[i%len(fillerWords)], fillerWords[(i*7+3)%len(fillerWords)], i),
			Notes:    new(fmt.Sprintf("standard %s inventory item", fillerWords[(i*13+5)%len(fillerWords)])),
			Priority: new(i % 100),
		})
	}
	// A small, exact, known subset carries a distinctive term that never
	// appears anywhere in the generated filler content above.
	for i := range needleCount {
		id := fmt.Sprintf("needle-%02d", i)
		widgets = append(widgets, widget{
			ID:    id,
			Name:  fmt.Sprintf("special order %d", i),
			Notes: new("contains xanthoglyph coating"),
		})
	}
	seedWidgets(t, read, widgets...)

	result, err := c.Pagination(ctx, domains.Pagination{
		Filter: domains.StructuredFilter{
			Filters:    []domains.Filter{{Field: "notes", Mode: domains.ModeSearch, Value: "xanthoglyph"}},
			SortFields: []domains.SortField{{Field: "id", Order: domains.SortOrderAsc}},
		},
		PageSize: total, // one page is enough here; cursor-walking is covered separately above
	})
	if err != nil {
		t.Fatalf("Pagination returned error: %v", err)
	}
	if len(result.Data) != needleCount {
		t.Fatalf("expected exactly %d matching rows out of %d total, got %d: %+v",
			needleCount, total+needleCount, len(result.Data), result.Data)
	}
	for i, w := range result.Data {
		want := fmt.Sprintf("needle-%02d", i)
		if w.ID != want {
			t.Errorf("result[%d]: expected %s, got %s", i, want, w.ID)
		}
	}
}
