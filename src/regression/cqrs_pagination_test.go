package regression

import (
	"context"
	"testing"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/domains"
)

// This file covers CQRSImpl's own pagination wrappers
// (cqrs.pagination.go) — thin delegations to the embedded
// paginationService plus, for the *Format variants, a ToModels conversion
// to TResponse. The deeper pagination semantics (AND-merge, cursor
// walking, mixed-direction sort, ModeSearch, ...) are already covered at
// the pagination.PaginationService level elsewhere in this suite; these
// tests only need to prove this wrapper layer delegates correctly and
// converts TData<->TResponse correctly, not re-litigate that depth.

func TestCQRSPaginate_HappyPath_ReturnsRawTData(t *testing.T) {
	t.Parallel()
	c, write := newTestCQRS(t)
	ctx := context.Background()
	seedWidget(t, c, widget{ID: "w1", Name: "Alpha"})
	seedWidget(t, c, widget{ID: "w2", Name: "Beta"})

	result, err := c.Paginate(ctx, domains.Pagination{})
	if err != nil {
		t.Fatalf("Paginate returned error: %v", err)
	}
	var _ []*widget = result.Data
	if len(result.Data) != 2 {
		t.Fatalf("expected 2 rows, got %+v", result.Data)
	}
	if _, ok := readWidgetFrom(t, write, "w1"); !ok {
		t.Fatal("expected w1 to exist in the underlying db")
	}
}

func TestCQRSPaginateFormat_HappyPath_ReturnsFormattedTResponse(t *testing.T) {
	t.Parallel()
	c, _ := newTestCQRS(t)
	ctx := context.Background()
	seedWidget(t, c, widget{ID: "w1", Name: "Alpha"})

	result, err := c.PaginateFormat(ctx, domains.Pagination{})
	if err != nil {
		t.Fatalf("PaginateFormat returned error: %v", err)
	}
	var _ []*widgetResource = result.Data
	if len(result.Data) != 1 || result.Data[0].Name != "Alpha" {
		t.Fatalf("expected [Alpha] as *widgetResource, got %+v", result.Data)
	}
}

// TestCQRSPaginateFilter_HappyPath_CombinesHardcodedAndFrontendFilters
// proves this wrapper passes both filter (hardcoded) and pagination.Filter
// (frontend) through to PaginationService.PaginateFilter's AND-merge
// intact, not just that it compiles.
func TestCQRSPaginateFilter_HappyPath_CombinesHardcodedAndFrontendFilters(t *testing.T) {
	t.Parallel()
	c, _ := newTestCQRS(t)
	ctx := context.Background()
	seedWidget(t, c, widget{ID: "w1", Name: "Alpha", Priority: new(1)})
	seedWidget(t, c, widget{ID: "w2", Name: "Alpha", Priority: new(2)})
	seedWidget(t, c, widget{ID: "w3", Name: "Beta", Priority: new(1)})

	result, err := c.PaginateFilter(ctx,
		domains.StructuredFilter{Filters: []domains.Filter{{Field: "name", Mode: domains.ModeEqual, Value: "Alpha"}}},
		domains.Pagination{Filter: domains.StructuredFilter{Filters: []domains.Filter{
			{Field: "priority", Mode: domains.ModeEqual, Value: 1},
		}}},
	)
	if err != nil {
		t.Fatalf("PaginateFilter returned error: %v", err)
	}
	if len(result.Data) != 1 || result.Data[0].ID != "w1" {
		t.Fatalf("expected only [w1] (name=Alpha AND priority=1), got %+v", result.Data)
	}
}

func TestCQRSPaginateFilterFormat_HappyPath_ReturnsFormattedTResponse(t *testing.T) {
	t.Parallel()
	c, _ := newTestCQRS(t)
	ctx := context.Background()
	seedWidget(t, c, widget{ID: "w1", Name: "Alpha"})
	seedWidget(t, c, widget{ID: "w2", Name: "Beta"})

	result, err := c.PaginateFilterFormat(ctx,
		domains.StructuredFilter{Filters: []domains.Filter{{Field: "name", Mode: domains.ModeEqual, Value: "Alpha"}}},
		domains.Pagination{},
	)
	if err != nil {
		t.Fatalf("PaginateFilterFormat returned error: %v", err)
	}
	var _ []*widgetResource = result.Data
	if len(result.Data) != 1 || result.Data[0].Name != "Alpha" {
		t.Fatalf("expected [Alpha] as *widgetResource, got %+v", result.Data)
	}
}

func TestCQRSFilter_HappyPath_ReturnsRawTData(t *testing.T) {
	t.Parallel()
	c, _ := newTestCQRS(t)
	ctx := context.Background()
	seedWidget(t, c, widget{ID: "w1", Name: "Alpha"})
	seedWidget(t, c, widget{ID: "w2", Name: "Beta"})

	result, err := c.Filter(ctx, domains.StructuredFilter{
		Filters: []domains.Filter{{Field: "name", Mode: domains.ModeEqual, Value: "Beta"}},
	})
	if err != nil {
		t.Fatalf("Filter returned error: %v", err)
	}
	var _ []*widget = result
	if len(result) != 1 || result[0].ID != "w2" {
		t.Fatalf("expected only [w2], got %+v", result)
	}
}

func TestCQRSFilterFormat_HappyPath_ReturnsFormattedTResponse(t *testing.T) {
	t.Parallel()
	c, _ := newTestCQRS(t)
	ctx := context.Background()
	seedWidget(t, c, widget{ID: "w1", Name: "Alpha"})

	result, err := c.FilterFormat(ctx, domains.StructuredFilter{})
	if err != nil {
		t.Fatalf("FilterFormat returned error: %v", err)
	}
	var _ []*widgetResource = result
	if len(result) != 1 || result[0].Name != "Alpha" {
		t.Fatalf("expected [Alpha] as *widgetResource, got %+v", result)
	}
}

// TestCQRSFilterWithTx_HappyPath_SeesUncommittedWritesInTheSameTx mirrors
// pagination_service_interface_test.go's own FilterWithTx tx-visibility
// test, through the CQRSImpl wrapper instead of PaginationService
// directly. fakeSQLService's single-connection pool (see that test's own
// comment) is why this only checks visibility inside the tx and after
// rollback, never concurrently with the tx still open.
func TestCQRSFilterWithTx_HappyPath_SeesUncommittedWritesInTheSameTx(t *testing.T) {
	t.Parallel()
	c, write := newTestCQRS(t)
	ctx := context.Background()

	tx, err := write.Client().BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx returned error: %v", err)
	}
	if _, err := tx.NewInsert().Model(&widget{ID: "w1", Name: "InTx"}).Exec(ctx); err != nil {
		t.Fatalf("inserting inside the transaction: %v", err)
	}

	inTx, err := c.FilterWithTx(ctx, &tx, domains.StructuredFilter{})
	if err != nil {
		t.Fatalf("FilterWithTx returned error: %v", err)
	}
	var _ []*widget = inTx
	if len(inTx) != 1 || inTx[0].ID != "w1" {
		t.Fatalf("expected FilterWithTx to see the uncommitted row, got %+v", inTx)
	}

	if err := tx.Rollback(); err != nil {
		t.Fatalf("rolling back: %v", err)
	}
	after, err := c.Filter(ctx, domains.StructuredFilter{})
	if err != nil {
		t.Fatalf("Filter (after rollback) returned error: %v", err)
	}
	if len(after) != 0 {
		t.Fatalf("expected the rolled-back row to be gone, got %+v", after)
	}
}

func TestCQRSFilterWithTxFormat_HappyPath_ReturnsFormattedTResponse(t *testing.T) {
	t.Parallel()
	c, write := newTestCQRS(t)
	ctx := context.Background()

	tx, err := write.Client().BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx returned error: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.NewInsert().Model(&widget{ID: "w1", Name: "Alpha"}).Exec(ctx); err != nil {
		t.Fatalf("inserting inside the transaction: %v", err)
	}

	result, err := c.FilterWithTxFormat(ctx, &tx, domains.StructuredFilter{})
	if err != nil {
		t.Fatalf("FilterWithTxFormat returned error: %v", err)
	}
	var _ []*widgetResource = result
	if len(result) != 1 || result[0].Name != "Alpha" {
		t.Fatalf("expected [Alpha] as *widgetResource, got %+v", result)
	}
}

// TestCQRSPaginateWithHertz_HappyPath_CombinesHardcodedFilterWithParsedRequest
// mirrors pagination_service_interface_test.go's own PaginateWithHertz
// AND-merge proof, through the CQRSImpl wrapper instead of
// PaginationService directly: filter (hardcoded) and the request's own
// "?filter=..." query param (parsed via domains.Pagination.Parse) must
// both apply, neither overriding the other.
func TestCQRSPaginateWithHertz_HappyPath_CombinesHardcodedFilterWithParsedRequest(t *testing.T) {
	t.Parallel()
	c, write := newTestCQRS(t)
	ctx := context.Background()
	seedWidget(t, c, widget{ID: "w1", Name: "Alpha", Priority: new(1)})
	seedWidget(t, c, widget{ID: "w2", Name: "Alpha", Priority: new(2)})
	seedWidget(t, c, widget{ID: "w3", Name: "Beta", Priority: new(1)})

	// Request's own filter: priority = 1. Hardcoded filter below: name =
	// "Alpha". Only w1 satisfies both.
	reqFilter := domains.StructuredFilter{Filters: []domains.Filter{
		{Field: "priority", Mode: domains.ModeEqual, Value: float64(1)},
	}}
	reqCtx := newPaginationTestContext(t, map[string]string{"filter": encodeQueryParam(t, reqFilter)})

	tx, err := write.Client().BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx returned error: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	result, err := c.PaginateWithHertz(ctx, &tx,
		domains.StructuredFilter{Filters: []domains.Filter{{Field: "name", Mode: domains.ModeEqual, Value: "Alpha"}}},
		reqCtx,
	)
	if err != nil {
		t.Fatalf("PaginateWithHertz returned error: %v", err)
	}
	var _ []*widget = result.Data
	if len(result.Data) != 1 || result.Data[0].ID != "w1" {
		t.Fatalf("expected only [w1] (name=Alpha AND priority=1), got %+v", result.Data)
	}
}

func TestCQRSPaginateWithHertzFormat_HappyPath_ReturnsFormattedTResponse(t *testing.T) {
	t.Parallel()
	c, write := newTestCQRS(t)
	ctx := context.Background()
	seedWidget(t, c, widget{ID: "w1", Name: "Alpha"})
	seedWidget(t, c, widget{ID: "w2", Name: "Beta"})

	reqCtx := newPaginationTestContext(t, map[string]string{"pageSize": "1"})

	tx, err := write.Client().BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx returned error: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	result, err := c.PaginateWithHertzFormat(ctx, &tx, domains.StructuredFilter{}, reqCtx)
	if err != nil {
		t.Fatalf("PaginateWithHertzFormat returned error: %v", err)
	}
	var _ []*widgetResource = result.Data
	if len(result.Data) != 1 {
		t.Fatalf("expected the request's pageSize=1 to be honored, got %+v", result.Data)
	}
	if result.NextCursor == nil {
		t.Fatal("expected a NextCursor given 2 rows and pageSize=1")
	}
}

func TestCQRSPaginateWithHertz_SadPath_ReturnsErrorForMalformedFilterParam(t *testing.T) {
	t.Parallel()
	c, write := newTestCQRS(t)
	ctx := context.Background()

	tx, err := write.Client().BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx returned error: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	reqCtx := newPaginationTestContext(t, map[string]string{"filter": "not-valid-base64!!!"})
	if _, err := c.PaginateWithHertz(ctx, &tx, domains.StructuredFilter{}, reqCtx); err == nil {
		t.Fatal("expected an error for a malformed filter query param, got nil")
	}
}
