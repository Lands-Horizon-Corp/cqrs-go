package regression

import (
	"context"
	"testing"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/domains"
	"github.com/Lands-Horizon-Corp/cqrs-go/src/pagination"
)

// TestPagination_ZeroValue_BypassingConstructorWithEmptyColumnDefaultID
// builds PaginationService as a raw struct literal instead of going
// through NewPaginationService — every field it doesn't set (ColumnDefaultID,
// ColumnDefaultSort) stays at its Go zero value (""), since nothing stops a
// caller from skipping the constructor (all fields are exported). Before the
// explicit ColumnDefaultID=="" guard was added to Pagination, this produced
// a confusing raw driver error ("no such column: DESC" from bun.Ident("")
// leaking into the generated ORDER BY) instead of a clear setup error — the
// same class of problem NewPaginationService's own ReadSQLService nil-check
// exists to avoid, just previously unguarded for callers who bypass the
// constructor entirely.
func TestPagination_ZeroValue_BypassingConstructorWithEmptyColumnDefaultID(t *testing.T) {
	t.Parallel()
	read := newFakeSQLService(t)
	seedWidgets(t, read, widget{ID: "w1", Name: "Alpha"})

	// Deliberately NOT using pagination.NewPaginationService.
	raw := pagination.PaginationService[widget, any, string]{
		ReadSQLService: read,
	}
	_, err := raw.Pagination(context.Background(), domains.Pagination{})
	if err == nil {
		t.Fatal("expected a clear setup error for an empty ColumnDefaultID, got nil")
	}
	if got, want := err.Error(), "pagination requires ColumnDefaultID to be set"; got != want {
		t.Fatalf("expected a clear ColumnDefaultID setup error, got %q", got)
	}
}

// TestPagination_ZeroValue_NilReadSQLServiceReturnsErrorWhenCallingPaginationDirectly
// hits the c.ReadSQLService==nil branch inside Pagination itself, which
// NewPaginationService's own nil-check (tested separately via its panic)
// makes otherwise unreachable through the normal constructor path — only a
// raw struct literal that bypasses the constructor entirely can reach it.
func TestPagination_ZeroValue_NilReadSQLServiceReturnsErrorWhenCallingPaginationDirectly(t *testing.T) {
	t.Parallel()
	raw := pagination.PaginationService[widget, any, string]{
		// ReadSQLService deliberately left nil.
	}
	_, err := raw.Pagination(context.Background(), domains.Pagination{})
	if got, want := err, "pagination requires ReadSQLService to be set"; got == nil || got.Error() != want {
		t.Fatalf("expected error %q, got %v", want, got)
	}
}

// TestPagination_ZeroValue_EmptyColumnDefaultSortWithNonEmptyColumnDefaultIDFallsBackSafely
// bypasses the constructor with ColumnDefaultID set (satisfying the guard
// that TestPagination_ZeroValue_BypassingConstructorWithEmptyColumnDefaultID
// covers) but ColumnDefaultSort left empty — defaultSortField's documented
// fallback for this case is safe (it just sorts by ColumnDefaultID
// descending), unlike the empty-ColumnDefaultID case, so this should succeed
// rather than error.
func TestPagination_ZeroValue_EmptyColumnDefaultSortWithNonEmptyColumnDefaultIDFallsBackSafely(t *testing.T) {
	t.Parallel()
	read := newFakeSQLService(t)
	seedWidgets(t, read, widget{ID: "w1", Name: "Alpha"}, widget{ID: "w2", Name: "Beta"})

	raw := pagination.PaginationService[widget, any, string]{
		ReadSQLService:  read,
		ColumnDefaultID: "id",
		// ColumnDefaultSort deliberately left empty.
	}
	result, err := raw.Pagination(context.Background(), domains.Pagination{})
	if err != nil {
		t.Fatalf("expected a safe fallback to sorting by ColumnDefaultID, got error: %v", err)
	}
	if len(result.Data) != 2 {
		t.Fatalf("expected both rows back, got %d", len(result.Data))
	}
}

func TestPagination_ZeroValue_EmptyDomainsPaginationStructUsesAllDefaults(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	seedWidgets(t, read, widget{ID: "w1", Name: "Alpha"}, widget{ID: "w2", Name: "Beta"})

	result, err := c.Pagination(context.Background(), domains.Pagination{})
	if err != nil {
		t.Fatalf("Pagination returned error for a fully zero-value domains.Pagination: %v", err)
	}
	if result.PageSize != 30 {
		t.Errorf("expected the default PageSize 30, got %d", result.PageSize)
	}
	if len(result.Data) != 2 {
		t.Fatalf("expected both rows back, got %d", len(result.Data))
	}
}

func TestPagination_ZeroValue_NilVsEmptySliceForFiltersSortFieldsPreloadsAreEquivalent(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	seedWidgets(t, read, widget{ID: "w1", Name: "Alpha"}, widget{ID: "w2", Name: "Beta"})
	ctx := context.Background()

	nilResult, err := c.Pagination(ctx, domains.Pagination{
		Filter: domains.StructuredFilter{Filters: nil, SortFields: nil, Preload: nil},
	})
	if err != nil {
		t.Fatalf("nil-slices call returned error: %v", err)
	}
	emptyResult, err := c.Pagination(ctx, domains.Pagination{
		Filter: domains.StructuredFilter{Filters: []domains.Filter{}, SortFields: []domains.SortField{}, Preload: []string{}},
	})
	if err != nil {
		t.Fatalf("empty-slices call returned error: %v", err)
	}
	if len(nilResult.Data) != len(emptyResult.Data) {
		t.Fatalf("expected nil and empty slices to behave identically, got %d vs %d rows", len(nilResult.Data), len(emptyResult.Data))
	}
}

// TestPagination_AbsenceValue_NilPointerFieldsInRowsDoNotPanicAcrossFilterSortPreload
// seeds rows with nil *int/*string/*time.Time fields and exercises them
// through filtering, sorting, and preloading together — ties to
// utils.FieldValueAt's documented nil-safety (a nil pointer field reads as
// an absent "" value, not a literal "<nil>" or a panic).
func TestPagination_AbsenceValue_NilPointerFieldsInRowsDoNotPanicAcrossFilterSortPreload(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	ctx := context.Background()
	seedWidgets(t, read,
		widget{ID: "w1", Name: "HasValues", Priority: new(5), Notes: new("note")},
		widget{ID: "w2", Name: "AllNil", Priority: nil, Notes: nil, Featured: nil, ExpiresAt: nil},
	)

	// Sort by the nullable column itself.
	result, err := c.Pagination(ctx, domains.Pagination{
		Filter: domains.StructuredFilter{SortFields: []domains.SortField{{Field: "priority", Order: domains.SortOrderAsc}}},
	})
	if err != nil {
		t.Fatalf("sorting by a nullable column returned error: %v", err)
	}
	if len(result.Data) != 2 {
		t.Fatalf("expected both rows back, got %d", len(result.Data))
	}

	// Filter on the nullable column with a real value present.
	result, err = c.Pagination(ctx, domains.Pagination{
		Filter: domains.StructuredFilter{Filters: []domains.Filter{{Field: "priority", Mode: domains.ModeIsEmpty}}},
	})
	if err != nil {
		t.Fatalf("IsEmpty filter on a nullable column returned error: %v", err)
	}
	if len(result.Data) != 1 || result.Data[0].ID != "w2" {
		t.Fatalf("expected IsEmpty to match the nil-priority row [w2], got %+v", result.Data)
	}

	// Cursor continuation past a row with nil fields must not panic either.
	page1, err := c.Pagination(ctx, domains.Pagination{
		Filter: domains.StructuredFilter{SortFields: []domains.SortField{{Field: "id", Order: domains.SortOrderAsc}}}, PageSize: 1,
	})
	if err != nil {
		t.Fatalf("page1 error: %v", err)
	}
	if _, err := c.Pagination(ctx, domains.Pagination{
		Filter: domains.StructuredFilter{SortFields: []domains.SortField{{Field: "id", Order: domains.SortOrderAsc}}}, PageSize: 1, Cursor: page1.NextCursor,
	}); err != nil {
		t.Fatalf("page2 (continuing past a row with nil fields) returned error: %v", err)
	}
}

// TestPagination_DanglingPointer_ReturnedCursorPointersAreIndependentAcrossCalls
// rules out an accidental shared-buffer/aliasing bug: two separate calls
// with equal inputs must return distinct *string values for NextCursor,
// even though the strings they point to are equal.
func TestPagination_DanglingPointer_ReturnedCursorPointersAreIndependentAcrossCalls(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	ctx := context.Background()
	seedWidgets(t, read, widget{ID: "w1", Name: "A"}, widget{ID: "w2", Name: "B"})
	req := domains.Pagination{
		Filter: domains.StructuredFilter{SortFields: []domains.SortField{{Field: "id", Order: domains.SortOrderAsc}}}, PageSize: 1,
	}

	r1, err := c.Pagination(ctx, req)
	if err != nil {
		t.Fatalf("first call returned error: %v", err)
	}
	r2, err := c.Pagination(ctx, req)
	if err != nil {
		t.Fatalf("second call returned error: %v", err)
	}
	if r1.NextCursor == nil || r2.NextCursor == nil {
		t.Fatal("expected both calls to return a NextCursor")
	}
	if r1.NextCursor == r2.NextCursor {
		t.Error("expected the two calls' NextCursor pointers to be distinct addresses (no shared-buffer aliasing)")
	}
	if *r1.NextCursor != *r2.NextCursor {
		t.Errorf("expected the two calls' NextCursor values to be equal, got %q vs %q", *r1.NextCursor, *r2.NextCursor)
	}
}

// TestPagination_DanglingPointer_ResultCurrentCursorAliasesCallerInputPointer
// documents the real, current behavior: PaginationResult.CurrentCursor is
// literally the same *string as the input domains.Pagination.Cursor (no
// defensive copy). This means if a caller mutates *inputCursor after the
// call returns, the already-returned result's CurrentCursor reads the
// mutated value too — worth knowing, not necessarily a bug (Go callers
// almost never mutate through a *string in place), but exactly the
// aliased/dangling-pointer class of behavior worth pinning down explicitly.
func TestPagination_DanglingPointer_ResultCurrentCursorAliasesCallerInputPointer(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	ctx := context.Background()
	seedWidgets(t, read, widget{ID: "w1", Name: "A"}, widget{ID: "w2", Name: "B"})
	sortFields := []domains.SortField{{Field: "id", Order: domains.SortOrderAsc}}

	page1, err := c.Pagination(ctx, domains.Pagination{Filter: domains.StructuredFilter{SortFields: sortFields}, PageSize: 1})
	if err != nil {
		t.Fatalf("page1 error: %v", err)
	}
	cursor := page1.NextCursor

	page2, err := c.Pagination(ctx, domains.Pagination{Filter: domains.StructuredFilter{SortFields: sortFields}, PageSize: 1, Cursor: cursor})
	if err != nil {
		t.Fatalf("page2 error: %v", err)
	}
	if page2.CurrentCursor != cursor {
		t.Fatalf("expected CurrentCursor to be the exact same pointer as the input Cursor (documenting current aliasing behavior), got a different pointer")
	}

	// Mutate what the shared pointer points to; because CurrentCursor
	// aliases it (no defensive copy was made), the already-returned
	// page2.CurrentCursor observes the change too.
	*cursor = "mutated-after-the-fact"
	if *page2.CurrentCursor != "mutated-after-the-fact" {
		t.Fatalf("expected page2.CurrentCursor to observe the mutation through the shared pointer (documenting current behavior), got %q", *page2.CurrentCursor)
	}
}
