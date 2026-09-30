package regression

import (
	"encoding/base64"
	"encoding/json"
	"net/url"
	"testing"

	"github.com/cloudwego/hertz/pkg/app"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/domains"
)

// newPaginationTestContext builds a bare *app.RequestContext (no real
// network connection needed) carrying the given query params, so
// Pagination.Parse can be exercised without spinning up a hertz server.
func newPaginationTestContext(t *testing.T, query map[string]string) *app.RequestContext {
	t.Helper()
	ctx := app.NewContext(16)
	ctx.Request.Header.SetMethod("GET")
	ctx.Request.SetRequestURI("/pagination")
	args := ctx.QueryArgs()
	for k, v := range query {
		args.Set(k, v)
	}
	return ctx
}

// encodeQueryParam mirrors the encoding utils.DecodeQueryParam expects to
// reverse: JSON, then base64, then URL-escaped — matching what a
// well-behaved caller sends as a single query string value.
func encodeQueryParam(t *testing.T, v any) string {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshaling %#v: %v", v, err)
	}
	return url.QueryEscape(base64.StdEncoding.EncodeToString(raw))
}

func TestPaginationParse_HappyPath_DefaultsWhenNoQueryParamsGiven(t *testing.T) {
	t.Parallel()
	ctx := newPaginationTestContext(t, nil)

	var p domains.Pagination
	if err := p.Parse(ctx); err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}
	if p.Cursor != nil {
		t.Errorf("expected a nil Cursor on the first request (no cursor param), got %q", *p.Cursor)
	}
	if p.PageSize != 10 {
		t.Errorf("expected default PageSize 10, got %d", p.PageSize)
	}
	if p.Filter.Logic != domains.LogicAnd {
		t.Errorf("expected default Logic %q, got %q", domains.LogicAnd, p.Filter.Logic)
	}
	if len(p.Filter.Filters) != 0 || len(p.Filter.SortFields) != 0 {
		t.Errorf("expected an empty filter with no query params, got %+v", p.Filter)
	}
}

func TestPaginationParse_HappyPath_CursorAndPageSizeAreBound(t *testing.T) {
	t.Parallel()
	ctx := newPaginationTestContext(t, map[string]string{
		"cursor":   "eyJpZCI6IjQyIn0",
		"pageSize": "25",
	})

	var p domains.Pagination
	if err := p.Parse(ctx); err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}
	if p.Cursor == nil || *p.Cursor != "eyJpZCI6IjQyIn0" {
		t.Errorf("expected Cursor \"eyJpZCI6IjQyIn0\", got %v", p.Cursor)
	}
	if p.PageSize != 25 {
		t.Errorf("expected PageSize 25, got %d", p.PageSize)
	}
}

func TestPaginationParse_HappyPath_FilterParamIsDecoded(t *testing.T) {
	t.Parallel()
	filter := domains.StructuredFilter{
		Logic:   domains.LogicOr,
		Preload: []string{"Author"},
		SortFields: []domains.SortField{
			{Field: "createdAt", Order: domains.SortOrderDesc},
		},
	}
	ctx := newPaginationTestContext(t, map[string]string{
		"filter": encodeQueryParam(t, filter),
	})

	var p domains.Pagination
	if err := p.Parse(ctx); err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}
	if p.Filter.Logic != domains.LogicOr {
		t.Errorf("expected Logic %q, got %q", domains.LogicOr, p.Filter.Logic)
	}
	if len(p.Filter.Preload) != 1 || p.Filter.Preload[0] != "Author" {
		t.Errorf("expected Preload [Author], got %v", p.Filter.Preload)
	}
	if len(p.Filter.SortFields) != 1 || p.Filter.SortFields[0].Field != "createdAt" {
		t.Errorf("expected SortFields from the filter blob to survive, got %+v", p.Filter.SortFields)
	}
}

func TestPaginationParse_HappyPath_FilterWithEmptyLogicDefaultsToAnd(t *testing.T) {
	t.Parallel()
	ctx := newPaginationTestContext(t, map[string]string{
		"filter": encodeQueryParam(t, domains.StructuredFilter{}),
	})

	var p domains.Pagination
	if err := p.Parse(ctx); err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}
	if p.Filter.Logic != domains.LogicAnd {
		t.Errorf("expected an empty Logic in the filter blob to default to %q, got %q", domains.LogicAnd, p.Filter.Logic)
	}
}

func TestPaginationParse_HappyPath_SortParamOverridesFilterSortFields(t *testing.T) {
	t.Parallel()
	filter := domains.StructuredFilter{
		SortFields: []domains.SortField{{Field: "fromFilter", Order: domains.SortOrderAsc}},
	}
	sortFields := []domains.SortField{{Field: "fromSortParam", Order: domains.SortOrderDesc}}
	ctx := newPaginationTestContext(t, map[string]string{
		"filter": encodeQueryParam(t, filter),
		"sort":   encodeQueryParam(t, sortFields),
	})

	var p domains.Pagination
	if err := p.Parse(ctx); err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}
	if len(p.Filter.SortFields) != 1 || p.Filter.SortFields[0].Field != "fromSortParam" {
		t.Errorf("expected the standalone sort param to override the filter's SortFields, got %+v", p.Filter.SortFields)
	}
}

func TestPaginationParse_PoisonPill_InvalidSortOrderNormalizesToAscending(t *testing.T) {
	t.Parallel()
	sortFields := []domains.SortField{
		{Field: "a", Order: "banana"},
		{Field: "b", Order: "DESC"},
		{Field: "c", Order: "  Asc  "},
		{Field: "d", Order: ""},
	}
	ctx := newPaginationTestContext(t, map[string]string{
		"sort": encodeQueryParam(t, sortFields),
	})

	var p domains.Pagination
	if err := p.Parse(ctx); err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}
	want := map[string]domains.SortOrder{
		"a": domains.SortOrderAsc,  // garbage -> asc
		"b": domains.SortOrderDesc, // uppercase -> normalized desc
		"c": domains.SortOrderAsc,  // padded/mixed-case -> normalized asc
		"d": domains.SortOrderAsc,  // empty -> asc
	}
	if len(p.Filter.SortFields) != len(want) {
		t.Fatalf("expected %d sort fields, got %+v", len(want), p.Filter.SortFields)
	}
	for _, sf := range p.Filter.SortFields {
		if got, expected := sf.Order, want[sf.Field]; got != expected {
			t.Errorf("field %q: expected Order %q, got %q", sf.Field, expected, got)
		}
	}
}

func TestPaginationParse_SadPath_InvalidPageSizeIsRejected(t *testing.T) {
	t.Parallel()
	ctx := newPaginationTestContext(t, map[string]string{
		"pageSize": "not-a-number",
	})

	var p domains.Pagination
	if err := p.Parse(ctx); err == nil {
		t.Fatal("expected an error for a non-numeric pageSize, got nil")
	}
}

func TestPaginationParse_SadPath_MalformedFilterParamIsRejected(t *testing.T) {
	t.Parallel()
	ctx := newPaginationTestContext(t, map[string]string{
		"filter": url.QueryEscape("not-valid-base64!!!"),
	})

	var p domains.Pagination
	if err := p.Parse(ctx); err == nil {
		t.Fatal("expected an error for a malformed filter param, got nil")
	}
}

func TestPaginationParse_SadPath_ValidBase64ButInvalidJSONFilterIsRejected(t *testing.T) {
	t.Parallel()
	ctx := newPaginationTestContext(t, map[string]string{
		"filter": url.QueryEscape(base64.StdEncoding.EncodeToString([]byte("not json"))),
	})

	var p domains.Pagination
	if err := p.Parse(ctx); err == nil {
		t.Fatal("expected an error for a filter param that decodes to invalid JSON, got nil")
	}
}

func TestPaginationParse_SadPath_MalformedSortParamIsRejected(t *testing.T) {
	t.Parallel()
	ctx := newPaginationTestContext(t, map[string]string{
		"sort": url.QueryEscape("not-valid-base64!!!"),
	})

	var p domains.Pagination
	if err := p.Parse(ctx); err == nil {
		t.Fatal("expected an error for a malformed sort param, got nil")
	}
}

// TestPaginationParse_HappyPath_ComplexRealWorldQuery exercises everything
// Parse does in one request: an explicit cursor and page size, a "filter" blob with multiple
// modes/data types (including RangeNumber- and RangeDate-shaped values),
// Preload, sort fields embedded in that same filter blob, and a standalone
// "sort" param that overrides them with orders needing normalization.
//
// Filters.Value is deliberately `any` (see its doc comment in
// domain.pagination.go), so this also pins down what a caller actually gets
// back for a range value: json.Unmarshal decodes an object into `any` as a
// map[string]any, not into a domains.RangeNumber/RangeDate — there's no
// custom UnmarshalJSON dispatching on Mode/DataType, so the caller has to
// re-marshal/assert it themselves. Encoding this as raw JSON (rather than
// constructing domains.StructuredFilter Go values) also matches how a real
// frontend actually sends this: as wire JSON, not as our internal Go types.
func TestPaginationParse_HappyPath_ComplexRealWorldQuery(t *testing.T) {
	t.Parallel()

	filterPayload := map[string]any{
		"logic":   "or",
		"preload": []string{"Author", "Comments"},
		"filters": []map[string]any{
			{"field": "age", "mode": "gt", "dataType": "number", "value": 18},
			{"field": "name", "mode": "contains", "dataType": "text", "value": "john"},
			{"field": "price", "mode": "range", "dataType": "number", "value": map[string]any{"from": 10, "to": 100}},
			{"field": "createdAt", "mode": "range", "dataType": "date", "value": map[string]any{
				"from": "2024-01-01T00:00:00Z",
				"to":   "2024-12-31T00:00:00Z",
			}},
		},
		// Deliberately overridden below by the standalone "sort" param.
		"sortFields": []map[string]any{
			{"field": "name", "order": "asc"},
		},
	}
	sortPayload := []map[string]any{
		{"field": "price", "order": "DESC"},   // normalizes to desc
		{"field": "age", "order": "sideways"}, // garbage normalizes to asc
	}

	ctx := newPaginationTestContext(t, map[string]string{
		"cursor":   "eyJpZCI6IjQyIn0",
		"pageSize": "50",
		"filter":   encodeQueryParam(t, filterPayload),
		"sort":     encodeQueryParam(t, sortPayload),
	})

	var p domains.Pagination
	if err := p.Parse(ctx); err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}

	if p.Cursor == nil || *p.Cursor != "eyJpZCI6IjQyIn0" {
		t.Errorf("expected Cursor \"eyJpZCI6IjQyIn0\", got %v", p.Cursor)
	}
	if p.PageSize != 50 {
		t.Errorf("expected PageSize 50, got %d", p.PageSize)
	}
	if p.Filter.Logic != domains.LogicOr {
		t.Errorf("expected Logic %q, got %q", domains.LogicOr, p.Filter.Logic)
	}
	if len(p.Filter.Preload) != 2 || p.Filter.Preload[0] != "Author" || p.Filter.Preload[1] != "Comments" {
		t.Errorf("expected Preload [Author Comments], got %v", p.Filter.Preload)
	}

	if len(p.Filter.Filters) != 4 {
		t.Fatalf("expected 4 filters, got %d: %+v", len(p.Filter.Filters), p.Filter.Filters)
	}

	age := p.Filter.Filters[0]
	if age.Field != "age" || age.Mode != domains.ModeGT || age.DataType != domains.DataTypeNumber {
		t.Errorf("unexpected age filter: %+v", age)
	}
	if got, ok := age.Value.(float64); !ok || got != 18 {
		t.Errorf("expected age.Value to decode as float64(18), got %#v", age.Value)
	}

	name := p.Filter.Filters[1]
	if got, ok := name.Value.(string); !ok || got != "john" {
		t.Errorf("expected name.Value to decode as string \"john\", got %#v", name.Value)
	}

	price := p.Filter.Filters[2]
	priceRange, ok := price.Value.(map[string]any)
	if !ok {
		t.Fatalf("expected price.Value to decode as map[string]any (not domains.RangeNumber), got %T", price.Value)
	}
	if priceRange["from"] != float64(10) || priceRange["to"] != float64(100) {
		t.Errorf("expected price range {10, 100}, got %+v", priceRange)
	}

	createdAt := p.Filter.Filters[3]
	dateRange, ok := createdAt.Value.(map[string]any)
	if !ok {
		t.Fatalf("expected createdAt.Value to decode as map[string]any (not domains.RangeDate), got %T", createdAt.Value)
	}
	if dateRange["from"] != "2024-01-01T00:00:00Z" || dateRange["to"] != "2024-12-31T00:00:00Z" {
		t.Errorf("expected raw date strings (no automatic time.Time parsing through `any`), got %+v", dateRange)
	}

	// The standalone "sort" param must have fully replaced the filter
	// blob's embedded SortFields, with garbage/mixed-case orders normalized.
	if len(p.Filter.SortFields) != 2 {
		t.Fatalf("expected 2 sort fields from the override, got %+v", p.Filter.SortFields)
	}
	if p.Filter.SortFields[0].Field != "price" || p.Filter.SortFields[0].Order != domains.SortOrderDesc {
		t.Errorf("expected price/desc, got %+v", p.Filter.SortFields[0])
	}
	if p.Filter.SortFields[1].Field != "age" || p.Filter.SortFields[1].Order != domains.SortOrderAsc {
		t.Errorf("expected age/asc (garbage order normalized), got %+v", p.Filter.SortFields[1])
	}
}
