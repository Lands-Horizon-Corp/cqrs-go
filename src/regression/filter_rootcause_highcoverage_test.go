package regression

// ============================================================================
// CATEGORY: Problem / Root Cause / Solution, paired with High Coverage
// ============================================================================
//
// PROBLEM 1 — wildcard characters never match under SQLite.
//
//   A row whose Name literally contains a "%" or "_" character can never be
//   found via ModeContains/StartsWith/EndsWith/NotContains, even with the
//   exact literal text as the search value.
//
// ROOT CAUSE 1:
//
//   escapeLike (pagination.filter.go) backslash-escapes "%"/"_" before
//   building the LIKE pattern, but applyFilterTerm never appends an
//   `ESCAPE '\'` clause to the generated SQL (`"? LIKE ?"`). PostgreSQL
//   happens to treat backslash as the LIKE escape character by default, so
//   this works there — but SQLite (confirmed directly against the same
//   modernc.org/sqlite driver this suite's fakeSQLService uses, see
//   TestFilterRootCause_Problem_WildcardCharacterNeverMatchesUnderSQLite
//   below) gives backslash no special meaning in LIKE without an explicit
//   ESCAPE clause. The backslashes escapeLike inserts become literal
//   characters the pattern then requires in the data — which isn't there —
//   so the query silently returns zero rows instead of erroring or
//   matching.
//
// PROBLEM 2 — Equal and Contains serialize the exact same object value two
// different, incompatible ways.
//
//   A caller who filters a JSON/object-shaped value with ModeEqual and
//   later tries ModeContains against that same value gets no results, even
//   though the data hasn't changed.
//
// ROOT CAUSE 2:
//
//   ModeEqual passes f.Value straight to bun (`"? = ?"`), and bun's own
//   value formatter JSON-marshals a non-scalar value when inlining it into
//   the query — confirmed directly: map[string]any{"city":"NYC"} becomes
//   the literal text `{"city":"NYC"}`. ModeContains instead calls
//   fmt.Sprint(f.Value) directly, which uses Go's %v syntax instead of
//   JSON — the exact same map becomes `map[city:NYC]`. Two filter modes
//   that look interchangeable for any other DataType silently disagree
//   about what text an object value even means.
//
// PROBLEM 3 — one malformed element in an Inside/Outside list can take
// down the entire filter, instead of being dropped or rejected cleanly.
//
// ROOT CAUSE 3:
//
//   applyFilterTerm's reflect-based check for ModeInside/ModeOutside only
//   confirms f.Value is *some* slice or array — it never looks at what's
//   inside it. A nested slice-of-slices element reaches bun.In() and
//   produces a SQL row-value expression next to plain scalars in the same
//   IN(...) list, which every element must then match the shape of —
//   confirmed directly this surfaces as a raw driver-level "row value
//   misused" error, failing the *whole* query, including the other,
//   perfectly valid literal elements in the same list.
//
// SOLUTION (as currently shipped): these are characterization tests, not a
// code fix — the brief here was to add filter test coverage, not change
// pagination.filter.go. Each PROBLEM test below locks in today's exact,
// verified (if surprising) behavior as a visible regression: the moment
// someone adds dialect-aware ESCAPE handling, unifies Equal/Contains object
// serialization, or validates Inside/Outside list elements, these tests
// will fail and have to be updated to match the fix — which is the point.
//
// HIGH COVERAGE — TestFilterHighCoverage_BooleanObjectNestedAndDateTimeShapes
// below is the "cover everything" counterpart: a single table-driven test
// sweeping plain bool, nullable bool (NULL vs false vs true), flat object,
// nested (object-of-objects) values, a list containing a nested element,
// and date/time bounds — the full breadth the three PROBLEM tests above
// only sample individually.

import (
	"context"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/domains"
)

// TestFilterRootCause_Problem_WildcardCharacterNeverMatchesUnderSQLite
// documents ROOT CAUSE 1. A widget literally named "100%_off" exists in the
// data; searching Contains for that exact literal string currently returns
// zero rows against this suite's SQLite-backed fixture.
func TestFilterRootCause_Problem_WildcardCharacterNeverMatchesUnderSQLite(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	seedWidgets(t, read,
		widget{ID: "w1", Name: "100%_off"},
		widget{ID: "w2", Name: "unrelated"},
	)

	result, err := c.Pagination(context.Background(), domains.Pagination{
		Filter: domains.StructuredFilter{Filters: []domains.Filter{
			{Field: "name", Mode: domains.ModeContains, Value: "100%_off"},
		}},
	})
	if err != nil {
		t.Fatalf("Pagination returned error: %v", err)
	}
	// This is the confirmed bug, not the desired behavior: a literal,
	// exact-text Contains search for data that is verbatim present in the
	// table currently matches nothing, because escapeLike's backslashes
	// have no effect without an ESCAPE clause under SQLite. If this starts
	// failing (i.e. len(result.Data) becomes 1), ROOT CAUSE 1 has been
	// fixed and this test should be updated to assert the row IS found.
	if len(result.Data) != 0 {
		t.Fatalf("expected the known wildcard-escaping bug to still reproduce (0 rows), got %+v — "+
			"if this now finds w1, ESCAPE-clause handling was fixed and this test needs updating", result.Data)
	}
}

// TestFilterRootCause_Problem_EqualVsContainsSerializeObjectValuesDifferently
// documents ROOT CAUSE 2. The same map value is compared through ModeEqual
// (JSON-serialized by bun) and ModeContains (Go-%v-serialized by
// fmt.Sprint) against rows seeded with each exact representation, proving
// neither mode finds the other's text.
func TestFilterRootCause_Problem_EqualVsContainsSerializeObjectValuesDifferently(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	value := map[string]any{"city": "NYC"}
	jsonForm := `{"city":"NYC"}`    // what ModeEqual actually compares against
	goSyntaxForm := "map[city:NYC]" // what ModeContains actually searches for (fmt.Sprint(value))
	seedWidgets(t, read,
		widget{ID: "json-row", Name: jsonForm},
		widget{ID: "go-syntax-row", Name: goSyntaxForm},
	)

	eq, err := c.Pagination(context.Background(), domains.Pagination{
		Filter: domains.StructuredFilter{Filters: []domains.Filter{{Field: "name", Mode: domains.ModeEqual, Value: value}}},
	})
	if err != nil {
		t.Fatalf("Equal: Pagination returned error: %v", err)
	}
	if len(eq.Data) != 1 || eq.Data[0].ID != "json-row" {
		t.Fatalf("Equal(object): expected to match only the JSON-form row, got %+v", eq.Data)
	}

	contains, err := c.Pagination(context.Background(), domains.Pagination{
		Filter: domains.StructuredFilter{Filters: []domains.Filter{{Field: "name", Mode: domains.ModeContains, Value: value}}},
	})
	if err != nil {
		t.Fatalf("Contains: Pagination returned error: %v", err)
	}
	if len(contains.Data) != 1 || contains.Data[0].ID != "go-syntax-row" {
		t.Fatalf("Contains(object): expected to match only the Go-%%v-form row (not the JSON-form row "+
			"Equal matched), got %+v", contains.Data)
	}
}

// TestFilterRootCause_Problem_NestedSliceInsideInsideListBreaksEntireFilter
// documents ROOT CAUSE 3. Confirmed directly beforehand: a map element
// inside an Inside/Outside list is silently ignored (no match, no error),
// but a *nested slice* element in that same list makes bun emit a SQL
// row-value expression that SQLite rejects outright — taking the whole
// query down with it, including the valid literal element in the same
// list that would otherwise have matched.
func TestFilterRootCause_Problem_NestedSliceInsideInsideListBreaksEntireFilter(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	seedWidgets(t, read, widget{ID: "w1", Name: "Alpha"})

	_, err := c.Pagination(context.Background(), domains.Pagination{
		Filter: domains.StructuredFilter{Filters: []domains.Filter{
			// "w1" alone would match cleanly; the nested []any element is
			// what breaks the whole query.
			{Field: "id", Mode: domains.ModeInside, Value: []any{[]any{"nested", "slice"}, "w1"}},
		}},
	})
	// This is the confirmed bug: the entire filter errors out instead of
	// either rejecting the bad element up front or matching "w1" anyway. If
	// this starts returning err == nil, element-level validation (or
	// filtering) was added for Inside/Outside and this test should be
	// updated to assert on the resulting data instead.
	if err == nil {
		t.Fatal("expected a nested-slice element inside an Inside list to still error today " +
			"(a real driver-level error, not a clean validation error) — if this now succeeds, " +
			"ROOT CAUSE 3 has been fixed and this test needs updating")
	}
}

// TestFilterHighCoverage_BooleanObjectNestedAndDateTimeShapes is the "cover
// everything" sibling to the three PROBLEM tests above: one table sweeping
// every Filter.Value *shape* this suite's other filter tests only sample
// individually — plain bool, nullable bool (all three NULL/false/true
// states), a flat object, a nested object-of-objects, and a date/time
// range — run through modes where each shape is well-behaved today, so
// this table's job is breadth of coverage, not bug-hunting.
func TestFilterHighCoverage_BooleanObjectNestedAndDateTimeShapes(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	ctx := context.Background()

	jan1 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	dec1 := time.Date(2024, 12, 1, 0, 0, 0, 0, time.UTC)
	// Every widget not specifically exercising the nullable-bool cases
	// below gets an explicit, non-nil Featured value: *bool's Go zero value
	// is nil, so leaving it unset would make every one of them an
	// unintended extra match for the IsEmpty/IsNotEmpty cases.
	seedWidgets(t, read,
		widget{ID: "plain-bool-true", Active: true, Featured: boolPtr(true)},
		widget{ID: "plain-bool-false", Active: false, Featured: boolPtr(true)},
		widget{ID: "nullable-bool-null", Featured: nil},
		widget{ID: "nullable-bool-false", Featured: boolPtr(false)},
		widget{ID: "nullable-bool-true", Featured: boolPtr(true)},
		widget{ID: "flat-object", Name: `{"city":"NYC"}`, Featured: boolPtr(true)},
		widget{ID: "nested-object", Name: `{"address":{"city":"NYC"}}`, Featured: boolPtr(true)},
		widget{ID: "dated", ExpiresAt: new(jan1), Featured: boolPtr(true)},
		widget{ID: "dated-later", ExpiresAt: new(dec1), Featured: boolPtr(true)},
	)

	cases := []struct {
		name   string
		filter domains.Filter
		want   []string
	}{
		{
			"plain bool Equal true",
			domains.Filter{Field: "active", Mode: domains.ModeEqual, DataType: domains.DataTypeBool, Value: true},
			[]string{"plain-bool-true"},
		},
		{
			"plain bool Equal false",
			domains.Filter{Field: "active", Mode: domains.ModeEqual, DataType: domains.DataTypeBool, Value: false},
			// every other seeded widget defaults Active to its zero value (false)
			[]string{"plain-bool-false", "nullable-bool-null", "nullable-bool-false", "nullable-bool-true",
				"flat-object", "nested-object", "dated", "dated-later"},
		},
		{
			"nullable bool IsEmpty matches only NULL, not false",
			domains.Filter{Field: "featured", Mode: domains.ModeIsEmpty},
			[]string{"nullable-bool-null"},
		},
		{
			"nullable bool IsNotEmpty matches every non-NULL Featured value, not just this case's two",
			domains.Filter{Field: "featured", Mode: domains.ModeIsNotEmpty},
			// Every widget above except nullable-bool-null was seeded with
			// an explicit, non-nil Featured (see the seeding comment) so
			// IsEmpty/IsNotEmpty's NULL-vs-not split would be unambiguous —
			// which means IsNotEmpty's true complement is "every other row".
			[]string{"plain-bool-true", "plain-bool-false", "nullable-bool-false", "nullable-bool-true",
				"flat-object", "nested-object", "dated", "dated-later"},
		},
		{
			"flat object Equal matches its exact JSON serialization",
			domains.Filter{Field: "name", Mode: domains.ModeEqual, Value: map[string]any{"city": "NYC"}},
			[]string{"flat-object"},
		},
		{
			"nested object-of-objects Equal matches its exact JSON serialization",
			domains.Filter{Field: "name", Mode: domains.ModeEqual, Value: map[string]any{"address": map[string]any{"city": "NYC"}}},
			[]string{"nested-object"},
		},
		{
			"Inside list containing a nested map element alongside a literal: the map is silently " +
				"ignored (no error), the literal still matches",
			domains.Filter{Field: "id", Mode: domains.ModeInside, Value: []any{
				map[string]any{"unreachable": "nested value"}, "dated",
			}},
			[]string{"dated"},
		},
		{
			"date Range via the map[string]any JSON shape covers both bounds inclusively",
			domains.Filter{Field: "expires_at", Mode: domains.ModeRange, DataType: domains.DataTypeDate, Value: map[string]any{
				"from": "2024-01-01T00:00:00Z", "to": "2024-12-01T00:00:00Z",
			}},
			[]string{"dated", "dated-later"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := c.Pagination(ctx, domains.Pagination{
				Filter: domains.StructuredFilter{
					Filters:    []domains.Filter{tc.filter},
					SortFields: []domains.SortField{{Field: "id", Order: domains.SortOrderAsc}},
				},
				PageSize: 20,
			})
			if err != nil {
				t.Fatalf("Pagination returned error: %v", err)
			}
			got := make([]string, len(result.Data))
			for i, r := range result.Data {
				got[i] = r.ID
			}
			if len(got) != len(tc.want) {
				t.Fatalf("expected %d rows %v, got %d: %v", len(tc.want), tc.want, len(got), got)
			}
			wantSet := make(map[string]bool, len(tc.want))
			for _, id := range tc.want {
				wantSet[id] = true
			}
			for _, id := range got {
				if !wantSet[id] {
					t.Errorf("unexpected row %q in result: %v (want %v)", id, got, tc.want)
				}
			}
		})
	}
}
