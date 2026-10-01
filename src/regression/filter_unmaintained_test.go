package regression

// ============================================================================
// CATEGORY: Unmaintained Test Suites
// ============================================================================
//
// These tests aren't missing assertions (filter_placebo_test.go) or hiding
// bugs in their own logic (filter_high_bugs_test.go) — they're examples of
// a test suite that was correct once and then nobody came back to it as the
// surrounding code moved on. Each one is annotated with what changed around
// it and why it was never updated to match: a feature flag/ticket that
// outlived its own skip reason, a test cloned during a refactor and never
// pointed at anything new, a doc comment nobody updated when the behavior
// it described changed, a hard-coded "this is every mode we support" list
// that wasn't extended when a new mode shipped, and a filter field name
// that only still resolves because of a leniency layer nobody remembers is
// there.

import (
	"context"
	"testing"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/domains"
)

// TestFilterUnmaintained_SkippedSinceTicketClosedYearsAgo has been skipped
// since this suite was written, citing a ticket as the blocker. Whatever
// PAGN-114 was, it is not tracked anywhere in this repository — there is no
// way for a future reader to tell whether it shipped, was abandoned, or
// never existed in the first place. A skipped test costs nothing in CI
// time and reports nothing as broken, which is exactly why suites
// accumulate these indefinitely: nothing forces anyone to revisit them.
func TestFilterUnmaintained_SkippedSinceTicketClosedYearsAgo(t *testing.T) {
	t.Skip("skipping until PAGN-114 (multi-field composite object filters) ships — filed 2019-03-01")

	c, read := newPaginationQueryTestCQRS(t)
	seedWidgets(t, read, widget{ID: "w1", Name: "Alpha"})
	_, _ = c.Pagination(context.Background(), domains.Pagination{})
}

// TestFilterUnmaintained_DuplicateClonedDuringARefactor was copy-pasted
// from TestPagination_HappyPath_FilterModesEqualGTContainsRange (see
// pagination_query_test.go) during an earlier refactor of this suite, with
// the clear intent of adapting it to cover something new — but whatever
// that "something new" was supposed to be, the clone was never actually
// changed, so it now runs the identical Equal/GT/Contains cases the
// original already covers. It inflates the test count and CI runtime
// without adding any coverage a mutation or regression would actually need.
func TestFilterUnmaintained_DuplicateClonedDuringARefactor(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	seedWidgets(t, read,
		widget{ID: "w1", Name: "Alpha Widget", Priority: new(5)},
		widget{ID: "w2", Name: "Beta Widget", Priority: new(10)},
	)
	result, err := c.Pagination(context.Background(), domains.Pagination{
		Filter: domains.StructuredFilter{Filters: []domains.Filter{
			{Field: "name", Mode: domains.ModeEqual, Value: "Beta Widget"},
		}},
	})
	if err != nil {
		t.Fatalf("Pagination returned error: %v", err)
	}
	if len(result.Data) != 1 || result.Data[0].ID != "w2" {
		t.Fatalf("expected [w2], got %+v", result.Data)
	}
}

// TestFilterUnmaintained_CommentContradictsActualAssertedBehavior documents
// (per its own name and the paragraph below) that Pagination must reject a
// request containing a field name that doesn't match any real column.
//
// That was true once. normalizeFilters was later changed to treat an
// unknown filter field as untrusted client input to silently drop rather
// than reject outright (see TestPagination_SadPath_UnknownFilterFieldIsDroppedNotAnError
// in pagination_query_test.go, which documents the *current* behavior
// deliberately). The assertions below were updated to match that change
// at the time — they correctly expect no error today — but this comment
// was not: a reader skimming only the prose above would come away
// believing the opposite of what the code beneath it actually checks.
func TestFilterUnmaintained_CommentContradictsActualAssertedBehavior(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	seedWidgets(t, read, widget{ID: "w1", Name: "Alpha"})

	_, err := c.Pagination(context.Background(), domains.Pagination{
		Filter: domains.StructuredFilter{Filters: []domains.Filter{
			{Field: "not_a_real_column", Mode: domains.ModeEqual, Value: "x"},
		}},
	})
	if err != nil {
		t.Fatalf("expected an unknown filter field to be dropped rather than error (the comment above "+
			"is the stale part, not this assertion), got: %v", err)
	}
}

// TestFilterUnmaintained_HardcodedModeChecklistMissingNewerModes keeps its
// own private list of "every filter mode this suite exercises" — written
// back when the Mode constants in domains/domain.pagination.go were just
// Equal/NotEqual/Contains/GT/LT. Range, Inside/Outside, Before/After,
// IsEmpty/IsNotEmpty and Search were all added to the real domains.Mode set
// later (see pagination_query_test.go's
// TestPagination_HappyPath_RemainingFilterModes, which exists precisely
// because this older list was never grown to match). This test still
// passes — every mode it knows about still works — which is exactly the
// problem: it reports "all known modes still work" in a way that reads
// like "all modes work", silently going stale every time domains.Mode
// grows and nobody circles back to add the new one here too.
func TestFilterUnmaintained_HardcodedModeChecklistMissingNewerModes(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	seedWidgets(t, read, widget{ID: "w1", Name: "Alpha", Priority: new(5)})

	// This checklist was accurate when it was written; it was never
	// extended when ModeRange/ModeInside/ModeOutside/ModeBefore/ModeAfter/
	// ModeIsEmpty/ModeIsNotEmpty/ModeSearch were added.
	supportedModesChecklist := []domains.Mode{
		domains.ModeEqual,
		domains.ModeNotEqual,
		domains.ModeContains,
		domains.ModeGT,
		domains.ModeLT,
	}
	for _, mode := range supportedModesChecklist {
		t.Run(string(mode), func(t *testing.T) {
			_, err := c.Pagination(context.Background(), domains.Pagination{
				Filter: domains.StructuredFilter{Filters: []domains.Filter{{Field: "priority", Mode: mode, Value: 1}}},
			})
			if err != nil {
				t.Fatalf("mode %q returned error: %v", mode, err)
			}
		})
	}
}

// TestFilterUnmaintained_StaleUppercaseFieldNameSurvivesOnlyViaNormalization
// filters on "Priority" — capitalized, matching a naming convention the
// rest of this suite stopped using once every other filter test switched
// to lowercase snake_case field names (see "priority" throughout
// pagination_query_test.go). This test was never updated to match, and
// nobody noticed, because utils.NormalizeColumnName's leniency (built for
// tolerating real client input, not for masking stale test fixtures)
// quietly papers over the mismatch. It passes for a reason that has
// nothing to do with what it looks like it's testing.
func TestFilterUnmaintained_StaleUppercaseFieldNameSurvivesOnlyViaNormalization(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	seedWidgets(t, read,
		widget{ID: "w1", Name: "Alpha", Priority: new(1)},
		widget{ID: "w2", Name: "Beta", Priority: new(2)},
	)

	result, err := c.Pagination(context.Background(), domains.Pagination{
		// "Priority" (capitalized) only still works because normalizeFilters
		// runs every incoming Field through NormalizeColumnName before
		// validating it against the real bun column name "priority".
		Filter: domains.StructuredFilter{Filters: []domains.Filter{{Field: "Priority", Mode: domains.ModeEqual, Value: 1}}},
	})
	if err != nil {
		t.Fatalf("Pagination returned error: %v", err)
	}
	if len(result.Data) != 1 || result.Data[0].ID != "w1" {
		t.Fatalf("expected [w1], got %+v", result.Data)
	}
}
