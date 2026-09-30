package regression

import (
	"context"
	"testing"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/domains"
)

// This file proves the fixes for the other zero-value/absence-value flaws
// found in the filter system: a nil Filter.Value used to silently compile
// into a query that could never match anything (comparison modes), search
// for the literal string "<nil>" (Contains/StartsWith/EndsWith/ModeSearch),
// or produce a raw, confusing driver-level SQL syntax error
// (Inside/Outside) — none of which ever returned a Go error a caller could
// act on. Every case here now returns a clear error instead.

func TestPagination_SadPath_ComparisonModesWithNilValueReturnClearError(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	seedWidgets(t, read, widget{ID: "w1", Name: "Alpha"})

	modes := []domains.Mode{
		domains.ModeEqual, domains.ModeNotEqual,
		domains.ModeGT, domains.ModeGTE, domains.ModeLT, domains.ModeLTE,
		domains.ModeBefore, domains.ModeAfter,
		domains.ModeContains, domains.ModeNotContains,
		domains.ModeStartsWith, domains.ModeEndsWith,
		domains.ModeSearch, domains.ModeRange,
	}
	for _, mode := range modes {
		t.Run(string(mode), func(t *testing.T) {
			_, err := c.Pagination(context.Background(), domains.Pagination{
				Filter: domains.StructuredFilter{Filters: []domains.Filter{{Field: "name", Mode: mode, Value: nil}}},
			})
			if err == nil {
				t.Fatalf("expected an error for mode %q with a nil value, got nil", mode)
			}
		})
	}
}

// TestPagination_HappyPath_IsEmptyModesIgnoreValueEvenWhenNil confirms
// ModeIsEmpty/ModeIsNotEmpty are deliberately excluded from the nil-value
// check above — they never look at Value at all, by design.
func TestPagination_HappyPath_IsEmptyModesIgnoreValueEvenWhenNil(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	seedWidgets(t, read, widget{ID: "w1", Name: "Alpha", Notes: nil})

	result, err := c.Pagination(context.Background(), domains.Pagination{
		Filter: domains.StructuredFilter{Filters: []domains.Filter{{Field: "notes", Mode: domains.ModeIsEmpty, Value: nil}}},
	})
	if err != nil {
		t.Fatalf("ModeIsEmpty with nil Value returned error: %v", err)
	}
	if len(result.Data) != 1 || result.Data[0].ID != "w1" {
		t.Fatalf("expected [w1], got %+v", result.Data)
	}
}

func TestPagination_SadPath_InsideOutsideWithNilValueReturnClearError(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	seedWidgets(t, read, widget{ID: "w1", Name: "Alpha"})

	for _, mode := range []domains.Mode{domains.ModeInside, domains.ModeOutside} {
		t.Run(string(mode), func(t *testing.T) {
			_, err := c.Pagination(context.Background(), domains.Pagination{
				Filter: domains.StructuredFilter{Filters: []domains.Filter{{Field: "id", Mode: mode, Value: nil}}},
			})
			if err == nil {
				t.Fatalf("expected an error for mode %q with a nil value, got nil", mode)
			}
		})
	}
}

// TestPagination_SadPath_InsideWithNonListValueReturnsClearError covers a
// caller passing a plain scalar instead of a list by mistake — confirmed
// this used to reach bun.In and produce a confusing driver-level error
// rather than a clear "requires a list" message.
func TestPagination_SadPath_InsideWithNonListValueReturnsClearError(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	seedWidgets(t, read, widget{ID: "w1", Name: "Alpha"})

	_, err := c.Pagination(context.Background(), domains.Pagination{
		Filter: domains.StructuredFilter{Filters: []domains.Filter{{Field: "id", Mode: domains.ModeInside, Value: "w1"}}},
	})
	if err == nil {
		t.Fatal("expected an error for a non-list Inside value, got nil")
	}
}

// TestPagination_HappyPath_InsideWithEmptyNonNilListStillWorks confirms the
// fix didn't over-correct: a real, empty, non-nil list ([]any{}) is a
// legitimate value (confirmed separately to work cleanly pre-fix too,
// matching zero rows on both SQLite and Postgres) and must still be
// accepted without error.
func TestPagination_HappyPath_InsideWithEmptyNonNilListStillWorks(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	seedWidgets(t, read, widget{ID: "w1", Name: "Alpha"})

	result, err := c.Pagination(context.Background(), domains.Pagination{
		Filter: domains.StructuredFilter{Filters: []domains.Filter{{Field: "id", Mode: domains.ModeInside, Value: []any{}}}},
	})
	if err != nil {
		t.Fatalf("expected an empty, non-nil Inside list to work cleanly, got error: %v", err)
	}
	if len(result.Data) != 0 {
		t.Fatalf("expected zero rows for an empty Inside list, got %+v", result.Data)
	}
}

// TestPagination_SadPath_RangeWithNullBoundReturnsClearError covers the
// JSON-shaped {"from": null, "to": 100} case — a value a client can easily
// send — confirmed this used to silently match zero rows ("col BETWEEN
// NULL AND 100" is never true) instead of erroring.
func TestPagination_SadPath_RangeWithNullBoundReturnsClearError(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	seedWidgets(t, read, widget{ID: "w1", Name: "Alpha", Priority: new(50)})

	cases := map[string]map[string]any{
		"null from": {"from": nil, "to": 100},
		"null to":   {"from": 0, "to": nil},
	}
	for name, value := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := c.Pagination(context.Background(), domains.Pagination{
				Filter: domains.StructuredFilter{Filters: []domains.Filter{{Field: "priority", Mode: domains.ModeRange, Value: value}}},
			})
			if err == nil {
				t.Fatalf("expected an error for a range with a null bound, got nil")
			}
		})
	}
}
