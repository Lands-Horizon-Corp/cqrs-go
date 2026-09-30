package regression

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/cqrs"
	"github.com/Lands-Horizon-Corp/cqrs-go/src/domains"
)

// This file verifies CQRSImpl.Pagination — cursor (keyset) pagination
// against ReadSQLService only. Unlike the write-path tests elsewhere in
// this suite, seeding here goes straight through bun (db.NewInsert()),
// bypassing Create/CDC entirely: Pagination only ever reads, so there's
// nothing write-path-specific to exercise.

// newPaginationQueryTestCQRS points both ReadSQLService and WriteSQLService
// at the same in-memory SQLite db (there's only one real database in this
// unit test; the point being verified is that Pagination *only* issues
// reads against ReadSQLService, not that a second physical database exists)
// using the same fakeSQLService/widget fixture as the rest of this suite.
func newPaginationQueryTestCQRS(t *testing.T) (*cqrs.CQRSImpl[widget, widgetResource, any, string], *fakeSQLService) {
	t.Helper()
	read := newFakeSQLService(t)
	c := cqrs.NewCQRS(cqrs.CQRSImpl[widget, widgetResource, any, string]{
		WriteSQLService: read,
		ReadSQLService:  read,
		ToResource:      widgetToResource,
	})
	return c, read
}

func seedWidgets(t *testing.T, read *fakeSQLService, widgets ...widget) {
	t.Helper()
	if len(widgets) == 0 {
		return
	}
	if _, err := read.Client().NewInsert().Model(&widgets).Exec(context.Background()); err != nil {
		t.Fatalf("seeding widgets: %v", err)
	}
}

func TestPagination_HappyPath_CursorPagesThroughAllRowsWithoutOverlapOrGaps(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	ctx := context.Background()

	const total = 25
	widgets := make([]widget, total)
	for i := range widgets {
		widgets[i] = widget{ID: fmt.Sprintf("w%02d", i), Name: fmt.Sprintf("Widget %02d", i), Priority: new(i)}
	}
	seedWidgets(t, read, widgets...)

	pagination := domains.Pagination{
		Filter:   domains.StructuredFilter{SortFields: []domains.SortField{{Field: "id", Order: domains.SortOrderAsc}}},
		PageSize: 10,
	}

	seen := map[string]bool{}
	pages := 0
	for {
		result, err := c.Pagination(ctx, pagination)
		if err != nil {
			t.Fatalf("Pagination returned error: %v", err)
		}
		pages++
		if pages > total { // safety net against an infinite loop on a bug
			t.Fatalf("paged more than %d times without exhausting %d rows", pages, total)
		}
		for _, r := range result.Data {
			if seen[r.ID] {
				t.Fatalf("row %q returned more than once across pages", r.ID)
			}
			seen[r.ID] = true
		}
		if result.NextCursor == nil {
			if len(result.Data) == 0 {
				t.Fatal("last page returned zero rows")
			}
			break
		}
		pagination.Cursor = result.NextCursor
	}

	if len(seen) != total {
		t.Fatalf("expected all %d rows to be seen exactly once across pages, got %d", total, len(seen))
	}
	if pages != 3 { // 10 + 10 + 5
		t.Errorf("expected 3 pages for %d rows at page size 10, got %d", total, pages)
	}
}

func TestPagination_HappyPath_FilterModesEqualGTContainsRange(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	ctx := context.Background()
	seedWidgets(t, read,
		widget{ID: "w1", Name: "Alpha Widget", Priority: new(5)},
		widget{ID: "w2", Name: "Beta Widget", Priority: new(10)},
		widget{ID: "w3", Name: "Gamma Gadget", Priority: new(15)},
	)

	cases := []struct {
		name   string
		filter domains.Filter
		want   []string
	}{
		{"Equal", domains.Filter{Field: "name", Mode: domains.ModeEqual, DataType: domains.DataTypeText, Value: "Beta Widget"}, []string{"w2"}},
		{"GT", domains.Filter{Field: "priority", Mode: domains.ModeGT, DataType: domains.DataTypeNumber, Value: 5}, []string{"w2", "w3"}},
		{"Contains", domains.Filter{Field: "name", Mode: domains.ModeContains, DataType: domains.DataTypeText, Value: "Widget"}, []string{"w1", "w2"}},
		{"Range", domains.Filter{Field: "priority", Mode: domains.ModeRange, DataType: domains.DataTypeNumber, Value: domains.RangeNumber{From: 6, To: 15}}, []string{"w2", "w3"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := c.Pagination(ctx, domains.Pagination{
				Filter: domains.StructuredFilter{
					Filters:    []domains.Filter{tc.filter},
					SortFields: []domains.SortField{{Field: "id", Order: domains.SortOrderAsc}},
				},
				PageSize: 10,
			})
			if err != nil {
				t.Fatalf("Pagination returned error: %v", err)
			}
			if len(result.Data) != len(tc.want) {
				t.Fatalf("expected %d rows, got %d: %+v", len(tc.want), len(result.Data), result.Data)
			}
			for i, id := range tc.want {
				if result.Data[i].ID != id {
					t.Errorf("row %d: expected ID %q, got %q", i, id, result.Data[i].ID)
				}
			}
		})
	}
}

func TestPagination_HappyPath_MixedAscDescMultiColumnSortWithTiesAndCursor(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	ctx := context.Background()
	// Two ties on Priority=5 (b, c) to actually exercise the tiebreaker
	// column, plus a higher-priority row (a) that must sort first despite
	// coming alphabetically last by name.
	seedWidgets(t, read,
		widget{ID: "b", Name: "Bravo", Priority: new(5)},
		widget{ID: "c", Name: "Charlie", Priority: new(5)},
		widget{ID: "a", Name: "Alpha", Priority: new(9)},
	)

	sortFields := []domains.SortField{
		{Field: "priority", Order: domains.SortOrderDesc},
		{Field: "name", Order: domains.SortOrderAsc},
	}

	var gotIDs []string
	var cursor *string
	for i := range 3 {
		result, err := c.Pagination(ctx, domains.Pagination{
			Filter:   domains.StructuredFilter{SortFields: sortFields},
			PageSize: 1,
			Cursor:   cursor,
		})
		if err != nil {
			t.Fatalf("Pagination returned error: %v", err)
		}
		if len(result.Data) != 1 {
			t.Fatalf("page %d: expected exactly 1 row, got %d", i, len(result.Data))
		}
		gotIDs = append(gotIDs, result.Data[0].ID)
		cursor = result.NextCursor
	}
	if cursor != nil {
		t.Error("expected no next page after 3 rows")
	}

	want := []string{"a", "b", "c"} // priority 9 first, then priority-5 ties broken by name asc
	for i, id := range want {
		if gotIDs[i] != id {
			t.Errorf("position %d: expected %q, got %q (full order: %v)", i, id, gotIDs[i], gotIDs)
		}
	}
}

func TestPagination_HappyPath_PreloadIntegration(t *testing.T) {
	t.Parallel()
	c, db := newPreloadTestCQRS(t)
	ctx := context.Background()
	seedPreloadAuthor(t, db, "a1", "Ada")
	if _, err := c.CreateMany(ctx, []preloadPost{
		{ID: "p1", Title: "One", AuthorID: "a1"},
		{ID: "p2", Title: "Two", AuthorID: "a1"},
	}); err != nil {
		t.Fatalf("seed CreateMany returned error: %v", err)
	}

	pc := cqrs.NewCQRS(cqrs.CQRSImpl[preloadPost, preloadPostResource, any, string]{
		WriteSQLService: &fakeSQLService{db: db},
		ReadSQLService:  &fakeSQLService{db: db},
		ToResource:      preloadPostToResource,
	})

	result, err := pc.Pagination(ctx, domains.Pagination{
		Filter:   domains.StructuredFilter{SortFields: []domains.SortField{{Field: "id", Order: domains.SortOrderAsc}}},
		PageSize: 10,
	}, "Author")
	if err != nil {
		t.Fatalf("Pagination returned error: %v", err)
	}
	if len(result.Data) != 2 || result.Data[0].AuthorName != "Ada" || result.Data[1].AuthorName != "Ada" {
		t.Fatalf("expected both rows to have Author preloaded, got %+v, %+v", result.Data[0], result.Data[1])
	}
}

func TestPagination_SadPath_UnknownSortFieldReturnsError(t *testing.T) {
	t.Parallel()
	c, _ := newPaginationQueryTestCQRS(t)
	_, err := c.Pagination(context.Background(), domains.Pagination{
		Filter: domains.StructuredFilter{SortFields: []domains.SortField{{Field: "not_a_real_column", Order: domains.SortOrderAsc}}},
	})
	if err == nil {
		t.Fatal("expected an error for an unknown sort field, got nil")
	}
}

func TestPagination_SadPath_UnknownFilterFieldReturnsError(t *testing.T) {
	t.Parallel()
	c, _ := newPaginationQueryTestCQRS(t)
	_, err := c.Pagination(context.Background(), domains.Pagination{
		Filter: domains.StructuredFilter{Filters: []domains.Filter{{Field: "not_a_real_column", Mode: domains.ModeEqual, Value: "x"}}},
	})
	if err == nil {
		t.Fatal("expected an error for an unknown filter field, got nil")
	}
}

func TestPagination_SadPath_NilReadSQLServiceReturnsErrorNotPanic(t *testing.T) {
	t.Parallel()
	c := cqrs.NewCQRS(cqrs.CQRSImpl[widget, widgetResource, any, string]{
		WriteSQLService: newFakeSQLService(t),
		ToResource:      widgetToResource,
		// ReadSQLService deliberately left nil.
	})
	_, err := c.Pagination(context.Background(), domains.Pagination{})
	if err == nil {
		t.Fatal("expected an error when ReadSQLService is nil, got nil")
	}
}

func TestPagination_PoisonPill_UnsupportedFilterModeReturnsError(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	seedWidgets(t, read, widget{ID: "w1", Name: "Alpha", Priority: new(1)})

	_, err := c.Pagination(context.Background(), domains.Pagination{
		Filter: domains.StructuredFilter{Filters: []domains.Filter{{Field: "name", Mode: "bogusMode", Value: "x"}}},
	})
	if err == nil {
		t.Fatal("expected an error for an unsupported filter mode, got nil")
	}
}

func TestPagination_PoisonPill_MalformedCursorReturnsError(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	seedWidgets(t, read, widget{ID: "w1", Name: "Alpha", Priority: new(1)})

	bogus := "not-a-real-cursor"
	_, err := c.Pagination(context.Background(), domains.Pagination{Cursor: &bogus})
	if err == nil {
		t.Fatal("expected an error for a malformed cursor, got nil")
	}
}

//go:fix inline
func strPtr(s string) *string { return new(s) }

// TestPagination_HappyPath_RemainingFilterModes covers every Mode not
// already exercised by TestPagination_HappyPath_FilterModesEqualGTContainsRange:
// the comparison/negation/wildcard siblings of what's already tested, plus
// Inside/Outside, Before/After, IsEmpty/IsNotEmpty, and both shapes a
// ModeRange Value can arrive in (domains.RangeDate directly, or a
// map[string]any the way it decodes off real JSON — see extractRangeBounds).
func TestPagination_HappyPath_RemainingFilterModes(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	ctx := context.Background()

	jan1 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	jun1 := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
	dec1 := time.Date(2024, 12, 1, 0, 0, 0, 0, time.UTC)
	seedWidgets(t, read,
		widget{ID: "w1", Name: "Alpha Widget", Priority: new(5), Notes: nil, ExpiresAt: new(jan1)},
		widget{ID: "w2", Name: "Beta Widget", Priority: new(10), Notes: new(""), ExpiresAt: new(jun1)},
		widget{ID: "w3", Name: "Gamma Gadget", Priority: new(15), Notes: new("has notes"), ExpiresAt: new(dec1)},
	)

	cases := []struct {
		name   string
		filter domains.Filter
		want   []string
	}{
		{"NotEqual", domains.Filter{Field: "name", Mode: domains.ModeNotEqual, Value: "Beta Widget"}, []string{"w1", "w3"}},
		{"LT", domains.Filter{Field: "priority", Mode: domains.ModeLT, Value: 10}, []string{"w1"}},
		{"GTE", domains.Filter{Field: "priority", Mode: domains.ModeGTE, Value: 10}, []string{"w2", "w3"}},
		{"LTE", domains.Filter{Field: "priority", Mode: domains.ModeLTE, Value: 10}, []string{"w1", "w2"}},
		{"NotContains", domains.Filter{Field: "name", Mode: domains.ModeNotContains, Value: "Widget"}, []string{"w3"}},
		{"StartsWith", domains.Filter{Field: "name", Mode: domains.ModeStartsWith, Value: "Gamma"}, []string{"w3"}},
		{"EndsWith", domains.Filter{Field: "name", Mode: domains.ModeEndsWith, Value: "Gadget"}, []string{"w3"}},
		{"Inside", domains.Filter{Field: "id", Mode: domains.ModeInside, Value: []any{"w1", "w3"}}, []string{"w1", "w3"}},
		{"Outside", domains.Filter{Field: "id", Mode: domains.ModeOutside, Value: []any{"w1", "w3"}}, []string{"w2"}},
		{"Before", domains.Filter{Field: "expires_at", Mode: domains.ModeBefore, Value: jun1}, []string{"w1"}},
		{"After", domains.Filter{Field: "expires_at", Mode: domains.ModeAfter, Value: jun1}, []string{"w3"}},
		{"IsEmpty", domains.Filter{Field: "notes", Mode: domains.ModeIsEmpty}, []string{"w1", "w2"}},
		{"IsNotEmpty", domains.Filter{Field: "notes", Mode: domains.ModeIsNotEmpty}, []string{"w3"}},
		{"RangeTypedStruct", domains.Filter{Field: "expires_at", Mode: domains.ModeRange, Value: domains.RangeDate{From: jan1, To: jun1}}, []string{"w1", "w2"}},
		{"RangeJSONShape", domains.Filter{Field: "priority", Mode: domains.ModeRange, Value: map[string]any{"from": 10, "to": 15}}, []string{"w2", "w3"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := c.Pagination(ctx, domains.Pagination{
				Filter: domains.StructuredFilter{
					Filters:    []domains.Filter{tc.filter},
					SortFields: []domains.SortField{{Field: "id", Order: domains.SortOrderAsc}},
				},
				PageSize: 10,
			})
			if err != nil {
				t.Fatalf("Pagination returned error: %v", err)
			}
			if len(result.Data) != len(tc.want) {
				t.Fatalf("expected %d rows, got %d: %+v", len(tc.want), len(result.Data), result.Data)
			}
			for i, id := range tc.want {
				if result.Data[i].ID != id {
					t.Errorf("row %d: expected ID %q, got %q", i, id, result.Data[i].ID)
				}
			}
		})
	}
}

func TestPagination_SadPath_UnsupportedRangeValueTypeReturnsError(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	seedWidgets(t, read, widget{ID: "w1", Name: "Alpha", Priority: new(1)})

	_, err := c.Pagination(context.Background(), domains.Pagination{
		Filter: domains.StructuredFilter{Filters: []domains.Filter{{Field: "priority", Mode: domains.ModeRange, Value: "not-a-range"}}},
	})
	if err == nil {
		t.Fatal("expected an error for an unsupported range value type, got nil")
	}
}

func TestPagination_SadPath_RangeValueMissingFromToReturnsError(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	seedWidgets(t, read, widget{ID: "w1", Name: "Alpha", Priority: new(1)})

	_, err := c.Pagination(context.Background(), domains.Pagination{
		Filter: domains.StructuredFilter{Filters: []domains.Filter{{Field: "priority", Mode: domains.ModeRange, Value: map[string]any{"from": 1}}}},
	})
	if err == nil {
		t.Fatal("expected an error for a range value missing \"to\", got nil")
	}
}

func TestPagination_HappyPath_LogicOrCombinesMultipleFilters(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	seedWidgets(t, read,
		widget{ID: "w1", Name: "Alpha", Priority: new(1)},
		widget{ID: "w2", Name: "Beta", Priority: new(2)},
		widget{ID: "w3", Name: "Gamma", Priority: new(3)},
	)

	result, err := c.Pagination(context.Background(), domains.Pagination{
		Filter: domains.StructuredFilter{
			Logic: domains.LogicOr,
			Filters: []domains.Filter{
				{Field: "name", Mode: domains.ModeEqual, Value: "Alpha"},
				{Field: "name", Mode: domains.ModeEqual, Value: "Gamma"},
			},
			SortFields: []domains.SortField{{Field: "id", Order: domains.SortOrderAsc}},
		},
	})
	if err != nil {
		t.Fatalf("Pagination returned error: %v", err)
	}
	if len(result.Data) != 2 || result.Data[0].ID != "w1" || result.Data[1].ID != "w3" {
		t.Fatalf("expected w1 and w3 via OR logic, got %+v", result.Data)
	}
}

func TestPagination_SadPath_LaterFilterFieldStillValidatedAfterAnEarlierError(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	seedWidgets(t, read, widget{ID: "w1", Name: "Alpha", Priority: new(1)})

	// Three filters so the loop's "stop processing once an error is set"
	// branch (skipping the third once the second fails) actually executes.
	_, err := c.Pagination(context.Background(), domains.Pagination{
		Filter: domains.StructuredFilter{Filters: []domains.Filter{
			{Field: "name", Mode: domains.ModeEqual, Value: "Alpha"},
			{Field: "not_a_real_column", Mode: domains.ModeEqual, Value: "x"},
			{Field: "priority", Mode: domains.ModeEqual, Value: 1},
		}},
	})
	if err == nil {
		t.Fatal("expected an error for the unknown middle filter field, got nil")
	}
}

func TestPagination_PoisonPill_InvalidExplicitSortOrderNormalizesToAscending(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	seedWidgets(t, read,
		widget{ID: "w1", Name: "Alpha", Priority: new(1)},
		widget{ID: "w2", Name: "Beta", Priority: new(2)},
	)

	result, err := c.Pagination(context.Background(), domains.Pagination{
		Filter: domains.StructuredFilter{SortFields: []domains.SortField{{Field: "priority", Order: "sideways"}}},
	})
	if err != nil {
		t.Fatalf("Pagination returned error: %v", err)
	}
	if len(result.Data) != 2 || result.Data[0].ID != "w1" || result.Data[1].ID != "w2" {
		t.Fatalf("expected an invalid Order to normalize to ascending (w1, w2), got %+v", result.Data)
	}
}

func TestPagination_HappyPath_ColumnDefaultSortAscendingIsUsedWhenNoSortFieldsGiven(t *testing.T) {
	t.Parallel()
	read := newFakeSQLService(t)
	c := cqrs.NewCQRS(cqrs.CQRSImpl[widget, widgetResource, any, string]{
		WriteSQLService:   read,
		ReadSQLService:    read,
		ToResource:        widgetToResource,
		ColumnDefaultSort: "priority asc",
	})
	seedWidgets(t, read,
		widget{ID: "w1", Name: "Alpha", Priority: new(2)},
		widget{ID: "w2", Name: "Beta", Priority: new(1)},
	)

	result, err := c.Pagination(context.Background(), domains.Pagination{})
	if err != nil {
		t.Fatalf("Pagination returned error: %v", err)
	}
	if len(result.Data) != 2 || result.Data[0].ID != "w2" || result.Data[1].ID != "w1" {
		t.Fatalf("expected ColumnDefaultSort \"priority asc\" to sort w2 (priority 1) before w1 (priority 2), got %+v", result.Data)
	}
}

func TestPagination_SadPath_CursorFromDifferentSortShapeIsRejected(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	seedWidgets(t, read,
		widget{ID: "w1", Name: "Alpha", Priority: new(1)},
		widget{ID: "w2", Name: "Beta", Priority: new(2)},
	)

	// Get a real cursor built for a single-column sort ([id]).
	first, err := c.Pagination(context.Background(), domains.Pagination{
		Filter:   domains.StructuredFilter{SortFields: []domains.SortField{{Field: "id", Order: domains.SortOrderAsc}}},
		PageSize: 1,
	})
	if err != nil {
		t.Fatalf("first page returned error: %v", err)
	}
	if first.NextCursor == nil {
		t.Fatal("expected a next cursor after the first page")
	}

	// Reuse that cursor with a two-column sort — the cursor's value tuple no
	// longer matches the number of active sort columns.
	_, err = c.Pagination(context.Background(), domains.Pagination{
		Filter: domains.StructuredFilter{SortFields: []domains.SortField{
			{Field: "priority", Order: domains.SortOrderAsc},
			{Field: "id", Order: domains.SortOrderAsc},
		}},
		Cursor: first.NextCursor,
	})
	if err == nil {
		t.Fatal("expected an error when a cursor built for a different sort shape is reused, got nil")
	}
}

func TestPagination_SadPath_UnknownPreloadRelationReturnsError(t *testing.T) {
	t.Parallel()
	c, db := newPreloadTestCQRS(t)
	ctx := context.Background()
	seedPreloadAuthor(t, db, "a1", "Ada")
	if _, err := c.CreateMany(ctx, []preloadPost{{ID: "p1", Title: "One", AuthorID: "a1"}}); err != nil {
		t.Fatalf("seed CreateMany returned error: %v", err)
	}

	pc := cqrs.NewCQRS(cqrs.CQRSImpl[preloadPost, preloadPostResource, any, string]{
		WriteSQLService: &fakeSQLService{db: db},
		ReadSQLService:  &fakeSQLService{db: db},
		ToResource:      preloadPostToResource,
	})

	_, err := pc.Pagination(ctx, domains.Pagination{}, "NotARealRelation")
	if err == nil {
		t.Fatal("expected an error for an unknown preload relation, got nil")
	}
}

func TestPagination_SadPath_CursorWithInvalidPercentEncodingIsRejected(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	seedWidgets(t, read, widget{ID: "w1", Name: "Alpha", Priority: new(1)})

	bogus := "%zz" // invalid percent-encoding: url.QueryUnescape itself fails, before base64 is even attempted
	_, err := c.Pagination(context.Background(), domains.Pagination{Cursor: &bogus})
	if err == nil {
		t.Fatal("expected an error for a cursor with invalid percent-encoding, got nil")
	}
}

func TestPagination_HappyPath_CurrentAndPreviousCursorsAreSetCorrectly(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	ctx := context.Background()
	seedWidgets(t, read,
		widget{ID: "a", Name: "A"}, widget{ID: "b", Name: "B"}, widget{ID: "c", Name: "C"},
		widget{ID: "d", Name: "D"}, widget{ID: "e", Name: "E"},
	)
	sortFields := []domains.SortField{{Field: "id", Order: domains.SortOrderAsc}}

	page1, err := c.Pagination(ctx, domains.Pagination{
		Filter: domains.StructuredFilter{SortFields: sortFields}, PageSize: 2,
	})
	if err != nil {
		t.Fatalf("page1: Pagination returned error: %v", err)
	}
	if page1.CurrentCursor != nil {
		t.Errorf("page1: expected nil CurrentCursor (no cursor was given), got %v", *page1.CurrentCursor)
	}
	if page1.PreviousCursor != nil {
		t.Errorf("page1: expected nil PreviousCursor (nothing given, nothing before it), got %v", *page1.PreviousCursor)
	}
	if page1.NextCursor == nil {
		t.Fatal("page1: expected a NextCursor")
	}

	page2, err := c.Pagination(ctx, domains.Pagination{
		Filter: domains.StructuredFilter{SortFields: sortFields}, PageSize: 2, Cursor: page1.NextCursor,
	})
	if err != nil {
		t.Fatalf("page2: Pagination returned error: %v", err)
	}
	if page2.CurrentCursor == nil || *page2.CurrentCursor != *page1.NextCursor {
		t.Errorf("page2: expected CurrentCursor to echo the cursor sent (%v), got %v", *page1.NextCursor, page2.CurrentCursor)
	}
	if page2.PreviousCursor == nil {
		t.Fatal("page2: expected a non-nil PreviousCursor (a cursor was given to reach this page)")
	}
	if len(page2.Data) != 2 || page2.Data[0].ID != "c" || page2.Data[1].ID != "d" {
		t.Fatalf("page2: expected [c d], got %+v", page2.Data)
	}
}

func TestPagination_HappyPath_BackwardDirectionReturnsTheExactPreviousPage(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	ctx := context.Background()
	seedWidgets(t, read,
		widget{ID: "a", Name: "A"}, widget{ID: "b", Name: "B"}, widget{ID: "c", Name: "C"},
		widget{ID: "d", Name: "D"}, widget{ID: "e", Name: "E"},
	)
	sortFields := []domains.SortField{{Field: "id", Order: domains.SortOrderAsc}}

	page1, err := c.Pagination(ctx, domains.Pagination{
		Filter: domains.StructuredFilter{SortFields: sortFields}, PageSize: 2,
	})
	if err != nil {
		t.Fatalf("page1: Pagination returned error: %v", err)
	}
	page2, err := c.Pagination(ctx, domains.Pagination{
		Filter: domains.StructuredFilter{SortFields: sortFields}, PageSize: 2, Cursor: page1.NextCursor,
	})
	if err != nil {
		t.Fatalf("page2: Pagination returned error: %v", err)
	}

	back, err := c.Pagination(ctx, domains.Pagination{
		Filter:   domains.StructuredFilter{SortFields: sortFields},
		PageSize: 2,
		Cursor:   page2.PreviousCursor,
	})
	if err != nil {
		t.Fatalf("backward page: Pagination returned error: %v", err)
	}

	if len(back.Data) != 2 || back.Data[0].ID != "a" || back.Data[1].ID != "b" {
		t.Fatalf("expected backward navigation from page2 to reproduce page1's rows [a b], got %+v", back.Data)
	}
	if back.PreviousCursor != nil {
		t.Errorf("expected a nil PreviousCursor on the true first page (reached via backward), got %v", *back.PreviousCursor)
	}
	if back.NextCursor == nil || *back.NextCursor != *page1.NextCursor {
		t.Errorf("expected NextCursor from the backward page to exactly match the original forward cursor %v, got %v",
			*page1.NextCursor, back.NextCursor)
	}
}

func TestPagination_HappyPath_BackwardThenForwardRoundTripsToTheSamePage(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	ctx := context.Background()
	seedWidgets(t, read,
		widget{ID: "a", Name: "A"}, widget{ID: "b", Name: "B"}, widget{ID: "c", Name: "C"},
		widget{ID: "d", Name: "D"}, widget{ID: "e", Name: "E"},
	)
	sortFields := []domains.SortField{{Field: "id", Order: domains.SortOrderAsc}}
	req := func(cursor *string) domains.Pagination {
		return domains.Pagination{
			Filter: domains.StructuredFilter{SortFields: sortFields}, PageSize: 2, Cursor: cursor,
		}
	}

	page1, err := c.Pagination(ctx, req(nil))
	if err != nil {
		t.Fatalf("page1 error: %v", err)
	}
	page2, err := c.Pagination(ctx, req(page1.NextCursor))
	if err != nil {
		t.Fatalf("page2 error: %v", err)
	}
	// page2.PreviousCursor already carries "backward" baked into the token
	// itself (see cursorPayload) — no separate Direction field needed.
	back, err := c.Pagination(ctx, req(page2.PreviousCursor))
	if err != nil {
		t.Fatalf("backward error: %v", err)
	}
	forwardAgain, err := c.Pagination(ctx, req(back.NextCursor))
	if err != nil {
		t.Fatalf("forwardAgain error: %v", err)
	}

	if len(forwardAgain.Data) != len(page2.Data) {
		t.Fatalf("expected round-tripping back to page2's row count, got %d vs %d", len(forwardAgain.Data), len(page2.Data))
	}
	for i := range page2.Data {
		if forwardAgain.Data[i].ID != page2.Data[i].ID {
			t.Errorf("row %d: expected %q (page2), got %q (forwardAgain) after a backward+forward round trip",
				i, page2.Data[i].ID, forwardAgain.Data[i].ID)
		}
	}
}

// TestPagination_HappyPath_BackwardDirectionWithDescendingSort exercises
// reverseSortFields' DESC->ASC branch, which the other backward tests never
// hit (they all sort ascending) — this pins down that backward navigation
// works correctly on a descending sort too, not just ascending.
func TestPagination_HappyPath_BackwardDirectionWithDescendingSort(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	ctx := context.Background()
	seedWidgets(t, read,
		widget{ID: "a", Name: "A", Priority: new(5)},
		widget{ID: "b", Name: "B", Priority: new(4)},
		widget{ID: "c", Name: "C", Priority: new(3)},
		widget{ID: "d", Name: "D", Priority: new(2)},
		widget{ID: "e", Name: "E", Priority: new(1)},
	)
	sortFields := []domains.SortField{{Field: "priority", Order: domains.SortOrderDesc}}

	page1, err := c.Pagination(ctx, domains.Pagination{
		Filter: domains.StructuredFilter{SortFields: sortFields}, PageSize: 2,
	})
	if err != nil {
		t.Fatalf("page1 error: %v", err)
	}
	page2, err := c.Pagination(ctx, domains.Pagination{
		Filter: domains.StructuredFilter{SortFields: sortFields}, PageSize: 2, Cursor: page1.NextCursor,
	})
	if err != nil {
		t.Fatalf("page2 error: %v", err)
	}
	if len(page2.Data) != 2 || page2.Data[0].ID != "c" || page2.Data[1].ID != "d" {
		t.Fatalf("expected page2 [c d] (priority 3, 2), got %+v", page2.Data)
	}

	back, err := c.Pagination(ctx, domains.Pagination{
		Filter:   domains.StructuredFilter{SortFields: sortFields},
		PageSize: 2,
		Cursor:   page2.PreviousCursor,
	})
	if err != nil {
		t.Fatalf("backward error: %v", err)
	}
	if len(back.Data) != 2 || back.Data[0].ID != "a" || back.Data[1].ID != "b" {
		t.Fatalf("expected backward navigation to reproduce page1's rows [a b], got %+v", back.Data)
	}
}
