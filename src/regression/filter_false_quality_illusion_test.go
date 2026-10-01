package regression

// ============================================================================
// CATEGORY: False Quality Illusion
// ============================================================================
//
// Every test in this file genuinely executes pagination.filter.go's code —
// loops over every filter mode, runs against real nested/object/boolean
// values, scans real rows back out of SQLite. If you only looked at *code
// coverage*, this file looks excellent. The illusion is in what gets
// checked once that code has run: err == nil, a length, a type, a
// previously-accepted snapshot string — never whether the actual returned
// data is the data a caller asked for. High line/branch coverage and
// correctness are different axes; this file maximizes the first while
// contributing almost nothing to the second.

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/domains"
)

// TestFilterFalseQuality_OnlyChecksErrIsNil runs every comparison filter
// mode against real seeded data, including a boolean field and a
// nested/object Range value — wide execution coverage — but the only
// assertion anywhere in the loop is "err == nil". Whether any of these
// modes returned the right rows, the wrong rows, or zero rows is never
// looked at.
func TestFilterFalseQuality_OnlyChecksErrIsNil(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	seedWidgets(t, read,
		widget{ID: "w1", Name: "Alpha", Active: true, Priority: new(5)},
		widget{ID: "w2", Name: "Beta", Active: false, Priority: new(15)},
	)

	filters := []domains.Filter{
		{Field: "active", Mode: domains.ModeEqual, DataType: domains.DataTypeBool, Value: true},
		{Field: "name", Mode: domains.ModeContains, Value: "Alpha"},
		{Field: "priority", Mode: domains.ModeGT, Value: 10},
		{Field: "priority", Mode: domains.ModeRange, Value: map[string]any{"from": 0, "to": 20}}, // object/nested shape
		{Field: "id", Mode: domains.ModeInside, Value: []any{"w1", "w2"}},
	}
	for _, f := range filters {
		t.Run(string(f.Mode), func(t *testing.T) {
			_, err := c.Pagination(context.Background(), domains.Pagination{
				Filter: domains.StructuredFilter{Filters: []domains.Filter{f}},
			})
			// ILLUSION: this is the only check. A filter that always
			// returned every row, or always zero rows, or the wrong rows
			// entirely, would pass this test exactly as well as a correct
			// one, as long as it didn't also return a Go error.
			if err != nil {
				t.Fatalf("Pagination returned error for mode %q: %v", f.Mode, err)
			}
		})
	}
}

// TestFilterFalseQuality_OnlyChecksLengthNotContent checks that the right
// *number* of rows came back, never which ones. Two completely different —
// one correct, one silently wrong — result sets of the same size would
// both satisfy this test.
func TestFilterFalseQuality_OnlyChecksLengthNotContent(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	seedWidgets(t, read,
		widget{ID: "w1", Name: "Alpha", Active: true},
		widget{ID: "w2", Name: "Beta", Active: true},
		widget{ID: "w3", Name: "Gamma", Active: false},
	)

	result, err := c.Pagination(context.Background(), domains.Pagination{
		Filter: domains.StructuredFilter{Filters: []domains.Filter{
			{Field: "active", Mode: domains.ModeEqual, DataType: domains.DataTypeBool, Value: true},
		}},
	})
	if err != nil {
		t.Fatalf("Pagination returned error: %v", err)
	}
	// ILLUSION: this would pass identically if applyFilterTerm's ModeEqual
	// were silently negated (returning w3 and one of w1/w2 instead of w1
	// and w2) — any 2-row result satisfies this check, correct or not. A
	// real version additionally checks the IDs, the way every table-driven
	// test in pagination_query_test.go does.
	if len(result.Data) != 2 {
		t.Fatalf("expected 2 active widgets, got %d", len(result.Data))
	}
}

// TestFilterFalseQuality_OnlyChecksTypeNotValue sweeps boolean, flat
// object, and nested object Filter.Value shapes through Pagination and
// asserts only that a *domains.PaginationResult[widget] came back non-nil
// — real "coverage" of every shape the task asked for, zero verification
// that any of them actually matched (or didn't match) the right data.
func TestFilterFalseQuality_OnlyChecksTypeNotValue(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	seedWidgets(t, read, widget{ID: "w1", Name: "Alpha", Active: true})

	shapes := []any{
		true,                          // boolean
		map[string]any{"city": "NYC"}, // flat object
		map[string]any{"address": map[string]any{"city": "NYC"}}, // nested object
		[]any{map[string]any{"nested": true}, "w1"},              // list containing a nested object
	}
	for i, value := range shapes {
		t.Run(fmt.Sprintf("shape_%d", i), func(t *testing.T) {
			result, err := c.Pagination(context.Background(), domains.Pagination{
				Filter: domains.StructuredFilter{Filters: []domains.Filter{
					{Field: "name", Mode: domains.ModeEqual, Value: value},
				}},
			})
			if err != nil {
				t.Fatalf("Pagination returned error: %v", err)
			}
			// ILLUSION: a nil check on the result wrapper, not on its
			// Data. Pagination never returns a nil *PaginationResult
			// without a non-nil error, so this can never fail — it looks
			// like it's verifying each value shape is "handled", but it
			// would pass just as well if result.Data were always empty,
			// always wrong, or always every row in the table.
			if result == nil {
				t.Fatalf("expected a non-nil result for shape %v", value)
			}
		})
	}
}

// TestFilterFalseQuality_SnapshotAcceptedWithoutReview is a "golden file"
// style test: it formats the result of a filter query to a string and
// compares it against a hard-coded constant that was captured from a
// single successful run and never independently verified against the
// actual business requirement (what should "active widgets, sorted by id"
// even return?). If the underlying query logic is wrong in a way that
// still produces a stable, repeatable string, this snapshot will match
// forever.
func TestFilterFalseQuality_SnapshotAcceptedWithoutReview(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	seedWidgets(t, read,
		widget{ID: "w1", Name: "Alpha", Active: true},
		widget{ID: "w2", Name: "Beta", Active: false},
		widget{ID: "w3", Name: "Gamma", Active: true},
	)

	result, err := c.Pagination(context.Background(), domains.Pagination{
		Filter: domains.StructuredFilter{
			Filters:    []domains.Filter{{Field: "active", Mode: domains.ModeEqual, DataType: domains.DataTypeBool, Value: true}},
			SortFields: []domains.SortField{{Field: "id", Order: domains.SortOrderAsc}},
		},
	})
	if err != nil {
		t.Fatalf("Pagination returned error: %v", err)
	}
	var ids []string
	for _, r := range result.Data {
		ids = append(ids, r.ID)
	}
	got := fmt.Sprint(ids)
	// ILLUSION: "[w1 w3]" was pasted in from whatever the code produced
	// when this test was first written, not derived from an independent
	// statement of what "active=true" should mean. Nobody re-derives or
	// questions this string on every run — it just has to keep matching
	// itself. A regression that happened to also change this snapshot in
	// one shot (e.g. the filter and the formatting broke together) would
	// still pass.
	want := "[w1 w3]"
	if got != want {
		t.Fatalf("snapshot mismatch: want %q, got %q", want, got)
	}
}

// TestFilterFalseQuality_BenchmarkMistakenForCorrectnessTest runs a filter
// across a larger dataset and times it, treating "completed without
// panicking or timing out" as if it were evidence the filter is *correct* —
// it is only evidence the filter is *fast enough* and didn't crash. These
// are unrelated properties: a filter that silently returns the wrong rows
// is just as fast as one that returns the right ones.
func TestFilterFalseQuality_BenchmarkMistakenForCorrectnessTest(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	widgets := make([]widget, 200)
	for i := range widgets {
		widgets[i] = widget{ID: fmt.Sprintf("w%03d", i), Name: fmt.Sprintf("Widget %03d", i), Priority: new(i)}
	}
	seedWidgets(t, read, widgets...)

	start := time.Now()
	result, err := c.Pagination(context.Background(), domains.Pagination{
		Filter:   domains.StructuredFilter{Filters: []domains.Filter{{Field: "priority", Mode: domains.ModeGT, Value: 100}}},
		PageSize: 200,
	})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("Pagination returned error: %v", err)
	}
	// ILLUSION: "ran in under a second across 200 rows and didn't error"
	// says nothing about whether GT 100 returned the 99 rows it should
	// have, or some other number. Timing and row count are reported, but
	// the row count is only logged, never compared against the expected
	// 99 — so this reads as a correctness test while only actually being a
	// performance smoke test.
	t.Logf("GT filter over 200 rows took %s and returned %d rows (not verified against the expected 99)",
		elapsed, len(result.Data))
	if elapsed > 5*time.Second {
		t.Fatalf("filter took too long: %s", elapsed)
	}
}
