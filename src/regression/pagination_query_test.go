package regression

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/uptrace/bun"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/domains"
	"github.com/Lands-Horizon-Corp/cqrs-go/src/pagination"
)

// This file verifies pagination.PaginationService.Pagination — cursor
// (keyset) pagination against ReadSQLService only. Unlike the write-path
// tests elsewhere in this suite, seeding here goes straight through bun
// (db.NewInsert()), bypassing Create/CDC entirely: Pagination only ever
// reads, so there's nothing write-path-specific to exercise.

// newPaginationQueryTestCQRS points ReadSQLService at an in-memory SQLite db
// using the same fakeSQLService/widget fixture as the rest of this suite.
func newPaginationQueryTestCQRS(t *testing.T) (*pagination.PaginationService[widget, string], *fakeSQLService) {
	t.Helper()
	read := newFakeSQLService(t)
	p := pagination.NewPaginationService(pagination.PaginationService[widget, string]{
		ReadSQLService: read,
	})
	return p, read
}

// newPaginationPreloadService builds a PaginationService over the same
// preloadPost/preloadAuthor fixture pair preload_test.go uses for the
// write-path preload tests, sharing db so both sides see the same data.
func newPaginationPreloadService(db *bun.DB) *pagination.PaginationService[preloadPost, string] {
	return pagination.NewPaginationService(pagination.PaginationService[preloadPost, string]{
		ReadSQLService: &fakeSQLService{db: db},
	})
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

	p := domains.Pagination{
		Filter:   domains.StructuredFilter{SortFields: []domains.SortField{{Field: "id", Order: domains.SortOrderAsc}}},
		PageSize: 10,
	}

	seen := map[string]bool{}
	pages := 0
	for {
		result, err := c.Pagination(ctx, p)
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
		p.Cursor = result.NextCursor
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

	want := []string{"a", "b", "c"}
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
	if _, err := c.CreateManyFormat(ctx, []preloadPost{
		{ID: "p1", Title: "One", AuthorID: "a1"},
		{ID: "p2", Title: "Two", AuthorID: "a1"},
	}); err != nil {
		t.Fatalf("seed CreateMany returned error: %v", err)
	}

	pc := pagination.NewPaginationService(pagination.PaginationService[preloadPost, string]{
		ReadSQLService: &fakeSQLService{db: db},
	})

	result, err := pc.Pagination(ctx, domains.Pagination{
		Filter:   domains.StructuredFilter{SortFields: []domains.SortField{{Field: "id", Order: domains.SortOrderAsc}}},
		PageSize: 10,
	}, "Author")
	if err != nil {
		t.Fatalf("Pagination returned error: %v", err)
	}
	if len(result.Data) != 2 ||
		result.Data[0].Author == nil || result.Data[0].Author.Name != "Ada" ||
		result.Data[1].Author == nil || result.Data[1].Author.Name != "Ada" {
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

// TestPagination_SadPath_UnknownFilterFieldIsDroppedNotAnError documents the
// current, deliberate behavior: normalizeFilters treats an unknown filter
// field as untrusted client input to ignore, not a request to reject — it's
// dropped (with a Warn, see TestPagination_HappyPath_UnknownFilterFieldLogsAWarnWhenDropped)
// and the rest of the request proceeds as if that term was never sent.
func TestPagination_SadPath_UnknownFilterFieldIsDroppedNotAnError(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	seedWidgets(t, read, widget{ID: "w1", Name: "Alpha"}, widget{ID: "w2", Name: "Beta"})

	result, err := c.Pagination(context.Background(), domains.Pagination{
		Filter: domains.StructuredFilter{Filters: []domains.Filter{{Field: "not_a_real_column", Mode: domains.ModeEqual, Value: "x"}}},
	})
	if err != nil {
		t.Fatalf("expected the unknown filter field to be dropped rather than error, got: %v", err)
	}
	if len(result.Data) != 2 {
		t.Fatalf("expected both rows back (unknown filter ignored), got %+v", result.Data)
	}
}

func TestPagination_SadPath_NewPaginationServicePanicsWhenNeitherReadNorWriteSQLServiceIsSet(t *testing.T) {
	t.Parallel()
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected NewPaginationService to panic when neither ReadSQLService nor WriteSQLService is set, got no panic")
		}
	}()
	pagination.NewPaginationService(pagination.PaginationService[widget, string]{
		// ReadSQLService and WriteSQLService both deliberately left nil.
	})
}

// TestPagination_HappyPath_NewPaginationServiceFallsBackToWriteSQLServiceWhenReadIsNil
// covers the fallback chain itself: a smaller deployment with no dedicated
// read replica can set only WriteSQLService and pagination still works,
// reading through it exactly as it would through ReadSQLService.
func TestPagination_HappyPath_NewPaginationServiceFallsBackToWriteSQLServiceWhenReadIsNil(t *testing.T) {
	t.Parallel()
	write := newFakeSQLService(t)
	seedWidgets(t, write, widget{ID: "w1", Name: "Alpha"})

	c := pagination.NewPaginationService(pagination.PaginationService[widget, string]{
		WriteSQLService: write,
		// ReadSQLService deliberately left nil — WriteSQLService must be
		// used as the fallback instead of panicking.
	})
	result, err := c.Pagination(context.Background(), domains.Pagination{})
	if err != nil {
		t.Fatalf("Pagination returned error: %v", err)
	}
	if len(result.Data) != 1 || result.Data[0].ID != "w1" {
		t.Fatalf("expected the seeded row back via the WriteSQLService fallback, got %+v", result.Data)
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

// TestPagination_HappyPath_UnknownMiddleFilterFieldIsDroppedOthersStillApply
// confirms normalizeFilters drops exactly the one bad term (regardless of
// its position in the slice) while the surrounding valid filters still
// combine normally.
func TestPagination_HappyPath_UnknownMiddleFilterFieldIsDroppedOthersStillApply(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	seedWidgets(t, read,
		widget{ID: "w1", Name: "Alpha", Priority: new(1)},
		widget{ID: "w2", Name: "Alpha", Priority: new(2)},
	)
	result, err := c.Pagination(context.Background(), domains.Pagination{
		Filter: domains.StructuredFilter{Filters: []domains.Filter{
			{Field: "name", Mode: domains.ModeEqual, Value: "Alpha"},
			{Field: "not_a_real_column", Mode: domains.ModeEqual, Value: "x"},
			{Field: "priority", Mode: domains.ModeEqual, Value: 1},
		}},
	})
	if err != nil {
		t.Fatalf("expected the unknown middle filter to be dropped rather than error, got: %v", err)
	}
	if len(result.Data) != 1 || result.Data[0].ID != "w1" {
		t.Fatalf("expected only [w1] (name=Alpha AND priority=1 survives), got %+v", result.Data)
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
	c := pagination.NewPaginationService(pagination.PaginationService[widget, string]{
		ReadSQLService:    read,
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
	if _, err := c.CreateManyFormat(ctx, []preloadPost{{ID: "p1", Title: "One", AuthorID: "a1"}}); err != nil {
		t.Fatalf("seed CreateMany returned error: %v", err)
	}

	pc := pagination.NewPaginationService(pagination.PaginationService[preloadPost, string]{
		ReadSQLService: &fakeSQLService{db: db},
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

// TestPagination_HappyPath_BackwardDirectionWithMixedDirectionSort exercises
// paginateMixedDirection's own backward branch (reverseSortFields applied
// to each UNION ALL branch's ordering) — the other backward tests only use
// a single-column (therefore always-uniform) sort, so they never touch the
// mixed-direction code path at all, only the row-value-comparison one.
func TestPagination_HappyPath_BackwardDirectionWithMixedDirectionSort(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	ctx := context.Background()
	// Ties on priority=5 (b, c) force the tiebreaker (name ASC) to matter,
	// same as the forward-direction mixed-sort test.
	seedWidgets(t, read,
		widget{ID: "b", Name: "Bravo", Priority: new(5)},
		widget{ID: "c", Name: "Charlie", Priority: new(5)},
		widget{ID: "a", Name: "Alpha", Priority: new(9)},
	)
	sortFields := []domains.SortField{
		{Field: "priority", Order: domains.SortOrderDesc},
		{Field: "name", Order: domains.SortOrderAsc},
	}

	page1, err := c.Pagination(ctx, domains.Pagination{Filter: domains.StructuredFilter{SortFields: sortFields}, PageSize: 1})
	if err != nil {
		t.Fatalf("page1 error: %v", err)
	}
	page2, err := c.Pagination(ctx, domains.Pagination{
		Filter: domains.StructuredFilter{SortFields: sortFields}, PageSize: 1, Cursor: page1.NextCursor,
	})
	if err != nil {
		t.Fatalf("page2 error: %v", err)
	}
	if page2.Data[0].ID != "b" {
		t.Fatalf("expected page2 to be [b], got %+v", page2.Data)
	}

	back, err := c.Pagination(ctx, domains.Pagination{
		Filter: domains.StructuredFilter{SortFields: sortFields}, PageSize: 1, Cursor: page2.PreviousCursor,
	})
	if err != nil {
		t.Fatalf("backward error: %v", err)
	}
	if len(back.Data) != 1 || back.Data[0].ID != "a" {
		t.Fatalf("expected backward navigation to reproduce page1's row [a], got %+v", back.Data)
	}
}

func TestPagination_HappyPath_UnknownFilterFieldWithMixedDirectionSortIsDroppedNotAnError(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	ctx := context.Background()
	seedWidgets(t, read,
		widget{ID: "a", Name: "Alpha", Priority: new(1)},
		widget{ID: "b", Name: "Beta", Priority: new(2)},
	)
	sortFields := []domains.SortField{
		{Field: "priority", Order: domains.SortOrderDesc},
		{Field: "name", Order: domains.SortOrderAsc},
	}
	page1, err := c.Pagination(ctx, domains.Pagination{Filter: domains.StructuredFilter{SortFields: sortFields}, PageSize: 1})
	if err != nil {
		t.Fatalf("page1 error: %v", err)
	}

	page2, err := c.Pagination(ctx, domains.Pagination{
		Filter: domains.StructuredFilter{
			SortFields: sortFields,
			Filters:    []domains.Filter{{Field: "not_a_real_column", Mode: domains.ModeEqual, Value: "x"}},
		},
		PageSize: 1,
		Cursor:   page1.NextCursor,
	})
	if err != nil {
		t.Fatalf("expected the unknown filter field to be dropped rather than error on the mixed-direction path, got: %v", err)
	}
	if len(page2.Data) != 1 {
		t.Fatalf("expected page2 to still return a row (unknown filter ignored), got %+v", page2.Data)
	}
}

// ============================================================================
// Happy Path: full round trip + complex filter search
// ============================================================================

// TestPagination_HappyPath_FullForwardWalkThenFullBackwardWalkReturnsToStart
// walks every page from the first to the last (NextCursor goes nil), then
// walks backward from the last page all the way back to the first
// (PreviousCursor goes nil), asserting every backward page reproduces its
// corresponding forward page exactly.
func TestPagination_HappyPath_FullForwardWalkThenFullBackwardWalkReturnsToStart(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	ctx := context.Background()

	const total = 12
	const pageSize = 3
	widgets := make([]widget, total)
	for i := range widgets {
		widgets[i] = widget{ID: fmt.Sprintf("w%02d", i), Name: fmt.Sprintf("Widget %02d", i)}
	}
	seedWidgets(t, read, widgets...)
	sortFields := []domains.SortField{{Field: "id", Order: domains.SortOrderAsc}}

	// Forward walk: collect every page's rows and the cursor used to reach it.
	var forwardPages [][]string
	var forwardCursors []*string // forwardCursors[i] is the cursor that produced forwardPages[i]
	var cursor *string
	for {
		result, err := c.Pagination(ctx, domains.Pagination{
			Filter: domains.StructuredFilter{SortFields: sortFields}, PageSize: pageSize, Cursor: cursor,
		})
		if err != nil {
			t.Fatalf("forward page %d: Pagination returned error: %v", len(forwardPages), err)
		}
		ids := make([]string, len(result.Data))
		for i, r := range result.Data {
			ids[i] = r.ID
		}
		forwardPages = append(forwardPages, ids)
		forwardCursors = append(forwardCursors, cursor)
		if result.NextCursor == nil {
			break
		}
		cursor = result.NextCursor
		if len(forwardPages) > total { // safety net
			t.Fatalf("forward walk exceeded %d pages without terminating", total)
		}
	}
	if len(forwardPages) != total/pageSize {
		t.Fatalf("expected %d forward pages, got %d: %v", total/pageSize, len(forwardPages), forwardPages)
	}

	// Backward walk: starting from the last page, ask for PreviousCursor
	// repeatedly until it's nil, and confirm each step reproduces the
	// matching forward page in reverse order.
	lastPage, err := c.Pagination(ctx, domains.Pagination{
		Filter: domains.StructuredFilter{SortFields: sortFields}, PageSize: pageSize, Cursor: forwardCursors[len(forwardCursors)-1],
	})
	if err != nil {
		t.Fatalf("re-fetching last page: %v", err)
	}
	backCursor := lastPage.PreviousCursor
	for i := len(forwardPages) - 2; i >= 0; i-- {
		if backCursor == nil {
			t.Fatalf("backward walk terminated early at forward-page index %d", i)
		}
		result, err := c.Pagination(ctx, domains.Pagination{
			Filter: domains.StructuredFilter{SortFields: sortFields}, PageSize: pageSize, Cursor: backCursor,
		})
		if err != nil {
			t.Fatalf("backward page (reconstructing forward page %d): %v", i, err)
		}
		gotIDs := make([]string, len(result.Data))
		for j, r := range result.Data {
			gotIDs[j] = r.ID
		}
		if fmt.Sprint(gotIDs) != fmt.Sprint(forwardPages[i]) {
			t.Fatalf("backward reconstruction of forward page %d: expected %v, got %v", i, forwardPages[i], gotIDs)
		}
		backCursor = result.PreviousCursor
	}
	if backCursor != nil {
		t.Errorf("expected a nil PreviousCursor once the backward walk reaches the first page, got %v", *backCursor)
	}
}

// TestPagination_HappyPath_ComplexFilterSearchAndLogicAcrossPagesWithCursor
// is the "complex filter search" case: multiple filter modes combined with
// LogicAnd, a two-column sort, and preload-equivalent data, walked across
// more than one page via cursor.
func TestPagination_HappyPath_ComplexFilterSearchAndLogicAcrossPagesWithCursor(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	ctx := context.Background()
	// Widgets priced (Priority) 10..29; only even IDs get "Gadget" in the
	// name. Filter: priority in [12,26] (range) AND name contains "Gadget"
	// AND priority > 12 (gt) -- narrows to evens in (12, 26]: 14,16,...,26.
	for i := range 20 {
		p := 10 + i
		name := fmt.Sprintf("Item %02d", p)
		if p%2 == 0 {
			name = fmt.Sprintf("Gadget %02d", p)
		}
		seedWidgets(t, read, widget{ID: fmt.Sprintf("w%02d", p), Name: name, Priority: new(p)})
	}

	filter := domains.StructuredFilter{
		Logic: domains.LogicAnd,
		Filters: []domains.Filter{
			{Field: "priority", Mode: domains.ModeRange, Value: domains.RangeNumber{From: 12, To: 26}},
			{Field: "name", Mode: domains.ModeContains, Value: "Gadget"},
			{Field: "priority", Mode: domains.ModeGT, Value: 12},
		},
		SortFields: []domains.SortField{{Field: "priority", Order: domains.SortOrderAsc}},
	}
	want := []int{14, 16, 18, 20, 22, 24, 26}

	var got []int
	var cursor *string
	for {
		result, err := c.Pagination(ctx, domains.Pagination{Filter: filter, PageSize: 3, Cursor: cursor})
		if err != nil {
			t.Fatalf("Pagination returned error: %v", err)
		}
		for _, r := range result.Data {
			var p int
			if _, err := fmt.Sscanf(r.ID, "w%d", &p); err != nil {
				t.Fatalf("parsing id %q: %v", r.ID, err)
			}
			got = append(got, p)
		}
		if result.NextCursor == nil {
			break
		}
		cursor = result.NextCursor
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("expected priorities %v across all pages, got %v", want, got)
	}
}

func TestPagination_HappyPath_ComplexFilterSearchWithLogicOrAcrossPages(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	ctx := context.Background()
	seedWidgets(t, read,
		widget{ID: "w1", Name: "Alpha", Priority: new(1)},
		widget{ID: "w2", Name: "Beta", Priority: new(2)},
		widget{ID: "w3", Name: "Gamma", Priority: new(3)},
		widget{ID: "w4", Name: "Delta", Priority: new(4)},
		widget{ID: "w5", Name: "Epsilon", Priority: new(5)},
	)
	filter := domains.StructuredFilter{
		Logic: domains.LogicOr,
		Filters: []domains.Filter{
			{Field: "name", Mode: domains.ModeEqual, Value: "Alpha"},
			{Field: "name", Mode: domains.ModeEqual, Value: "Gamma"},
			{Field: "name", Mode: domains.ModeEqual, Value: "Epsilon"},
		},
		SortFields: []domains.SortField{{Field: "id", Order: domains.SortOrderAsc}},
	}

	var got []string
	var cursor *string
	for {
		result, err := c.Pagination(ctx, domains.Pagination{Filter: filter, PageSize: 1, Cursor: cursor})
		if err != nil {
			t.Fatalf("Pagination returned error: %v", err)
		}
		for _, r := range result.Data {
			got = append(got, r.ID)
		}
		if result.NextCursor == nil {
			break
		}
		cursor = result.NextCursor
	}
	want := []string{"w1", "w3", "w5"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("expected %v across all pages, got %v", want, got)
	}
}

func TestPagination_HappyPath_PreloadAppliesOnEveryPageOfAMultiPageWalk(t *testing.T) {
	t.Parallel()
	c, db := newPreloadTestCQRS(t)
	ctx := context.Background()
	seedPreloadAuthor(t, db, "a1", "Ada")
	posts := make([]preloadPost, 6)
	for i := range posts {
		posts[i] = preloadPost{ID: fmt.Sprintf("p%d", i), Title: fmt.Sprintf("Post %d", i), AuthorID: "a1"}
	}
	if _, err := c.CreateManyFormat(ctx, posts); err != nil {
		t.Fatalf("seed CreateMany returned error: %v", err)
	}

	pc := pagination.NewPaginationService(pagination.PaginationService[preloadPost, string]{
		ReadSQLService: &fakeSQLService{db: db},
	})

	var cursor *string
	pages := 0
	for {
		result, err := pc.Pagination(ctx, domains.Pagination{
			Filter:   domains.StructuredFilter{SortFields: []domains.SortField{{Field: "id", Order: domains.SortOrderAsc}}},
			PageSize: 2,
			Cursor:   cursor,
		}, "Author")
		if err != nil {
			t.Fatalf("Pagination returned error: %v", err)
		}
		pages++
		for _, r := range result.Data {
			if r.Author == nil || r.Author.Name != "Ada" {
				t.Errorf("page %d: expected Author.Name 'Ada' on every row, got %+v for %s", pages, r.Author, r.ID)
			}
		}
		if result.NextCursor == nil {
			break
		}
		cursor = result.NextCursor
		if pages > 6 {
			t.Fatal("walk did not terminate")
		}
	}
	if pages != 3 {
		t.Errorf("expected 3 pages for 6 rows at page size 2, got %d", pages)
	}
}

// ============================================================================
// Sad Path: canceled context, closed DB
// ============================================================================

func TestPagination_SadPath_CanceledContextReturnsErrorNotPanic_UniformPath(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	seedWidgets(t, read, widget{ID: "w1", Name: "Alpha"})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := c.Pagination(ctx, domains.Pagination{
		Filter: domains.StructuredFilter{SortFields: []domains.SortField{{Field: "id", Order: domains.SortOrderAsc}}},
	})
	if err == nil {
		t.Fatal("expected an error for a pre-canceled context on the uniform path, got nil")
	}
}

func TestPagination_SadPath_CanceledContextReturnsErrorNotPanic_MixedPath(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	seedWidgets(t, read,
		widget{ID: "a", Name: "Alpha", Priority: new(1)},
		widget{ID: "b", Name: "Beta", Priority: new(2)},
	)
	sortFields := []domains.SortField{
		{Field: "priority", Order: domains.SortOrderDesc},
		{Field: "name", Order: domains.SortOrderAsc},
	}
	// A real cursor is required to reach the mixed-direction path at all
	// (cursorIsUniform is trivially true for a nil cursor).
	page1, err := c.Pagination(context.Background(), domains.Pagination{Filter: domains.StructuredFilter{SortFields: sortFields}, PageSize: 1})
	if err != nil {
		t.Fatalf("page1 error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = c.Pagination(ctx, domains.Pagination{
		Filter: domains.StructuredFilter{SortFields: sortFields}, PageSize: 1, Cursor: page1.NextCursor,
	})
	if err == nil {
		t.Fatal("expected an error for a pre-canceled context on the mixed-direction path, got nil")
	}
}

func TestPagination_SadPath_ClosedUnderlyingDBReturnsErrorNotPanic(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	seedWidgets(t, read, widget{ID: "w1", Name: "Alpha"})

	if err := read.Client().Close(); err != nil {
		t.Fatalf("closing db: %v", err)
	}
	_, err := c.Pagination(context.Background(), domains.Pagination{})
	if err == nil {
		t.Fatal("expected an error when the underlying DB is closed, got nil")
	}
}

// ============================================================================
// Poison Pill: overflow, injection, unicode, structural stress
// ============================================================================

func TestPagination_PoisonPill_MaxIntPageSizeDoesNotOverflowOrMisbehave(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	seedWidgets(t, read, widget{ID: "w1", Name: "Alpha"}, widget{ID: "w2", Name: "Beta"})

	result, err := c.Pagination(context.Background(), domains.Pagination{
		Filter:   domains.StructuredFilter{SortFields: []domains.SortField{{Field: "id", Order: domains.SortOrderAsc}}},
		PageSize: math.MaxInt,
	})
	if err != nil {
		t.Fatalf("Pagination returned error for a math.MaxInt PageSize: %v", err)
	}
	if len(result.Data) != 2 {
		t.Fatalf("expected both seeded rows back, got %d: %+v", len(result.Data), result.Data)
	}
	if result.NextCursor != nil {
		t.Errorf("expected a nil NextCursor (only 2 rows exist), got %v", *result.NextCursor)
	}
}

func TestPagination_PoisonPill_SQLInjectionAttemptInFilterValueIsTreatedAsLiteral(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	seedWidgets(t, read, widget{ID: "w1", Name: "Alpha"}, widget{ID: "w2", Name: "Beta"})

	result, err := c.Pagination(context.Background(), domains.Pagination{
		Filter: domains.StructuredFilter{Filters: []domains.Filter{
			{Field: "name", Mode: domains.ModeEqual, Value: "'; DROP TABLE widgets; --"},
		}},
	})
	if err != nil {
		t.Fatalf("Pagination returned error: %v", err)
	}
	if len(result.Data) != 0 {
		t.Fatalf("expected zero matches for the injection-attempt literal, got %+v", result.Data)
	}

	// The table must still exist and be queryable — a real injection would
	// have dropped it.
	again, err := c.Pagination(context.Background(), domains.Pagination{})
	if err != nil {
		t.Fatalf("table appears to have been affected by the injection attempt: %v", err)
	}
	if len(again.Data) != 2 {
		t.Fatalf("expected both original rows to still be present, got %d", len(again.Data))
	}
}

func TestPagination_PoisonPill_UnicodeAndSpecialCharactersRoundTripThroughCursorAndFilter(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	seedWidgets(t, read,
		widget{ID: "w1", Name: "日本語 emoji 🎉 name"},
		widget{ID: "w2", Name: "plain"},
	)

	// Filter match on the unicode value.
	result, err := c.Pagination(context.Background(), domains.Pagination{
		Filter:   domains.StructuredFilter{Filters: []domains.Filter{{Field: "name", Mode: domains.ModeEqual, Value: "日本語 emoji 🎉 name"}}},
		PageSize: 1,
	})
	if err != nil {
		t.Fatalf("Pagination returned error: %v", err)
	}
	if len(result.Data) != 1 || result.Data[0].ID != "w1" {
		t.Fatalf("expected to match the unicode-named row, got %+v", result.Data)
	}

	// Cursor round-trip: page through both rows sorted by id.
	page1, err := c.Pagination(context.Background(), domains.Pagination{
		Filter:   domains.StructuredFilter{SortFields: []domains.SortField{{Field: "id", Order: domains.SortOrderAsc}}},
		PageSize: 1,
	})
	if err != nil {
		t.Fatalf("page1 error: %v", err)
	}
	if page1.NextCursor == nil {
		t.Fatal("expected a NextCursor after page1")
	}
	page2, err := c.Pagination(context.Background(), domains.Pagination{
		Filter:   domains.StructuredFilter{SortFields: []domains.SortField{{Field: "id", Order: domains.SortOrderAsc}}},
		PageSize: 1,
		Cursor:   page1.NextCursor,
	})
	if err != nil {
		t.Fatalf("page2 error: %v", err)
	}
	if len(page2.Data) != 1 || page2.Data[0].ID != "w2" {
		t.Fatalf("expected page2 to be [w2], got %+v", page2.Data)
	}
}

func TestPagination_PoisonPill_ManyFiltersCombinedStructurallyStillWorks(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	widgets := make([]widget, 20)
	for i := range widgets {
		widgets[i] = widget{ID: fmt.Sprintf("w%02d", i), Name: fmt.Sprintf("Widget %02d", i), Priority: new(i)}
	}
	seedWidgets(t, read, widgets...)

	// 20 OR'd equality filters, one per row -- structural stress on the
	// nested WhereGroup building, not just correctness of 2-3 filters.
	filters := make([]domains.Filter, 20)
	for i := range filters {
		filters[i] = domains.Filter{Field: "id", Mode: domains.ModeEqual, Value: fmt.Sprintf("w%02d", i)}
	}
	result, err := c.Pagination(context.Background(), domains.Pagination{
		Filter: domains.StructuredFilter{
			Logic:      domains.LogicOr,
			Filters:    filters,
			SortFields: []domains.SortField{{Field: "id", Order: domains.SortOrderAsc}},
		},
		PageSize: 100,
	})
	if err != nil {
		t.Fatalf("Pagination returned error: %v", err)
	}
	if len(result.Data) != 20 {
		t.Fatalf("expected all 20 rows to match their own OR'd equality filter, got %d", len(result.Data))
	}
}
