package regression

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/uptrace/bun"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/domains"
	"github.com/Lands-Horizon-Corp/cqrs-go/src/pagination"
)

// quickSearch mirrors the multi-word, multi-column shape of a typical quick search:
// every word must match at least one column.
func quickSearch(q *bun.SelectQuery, value any) (*bun.SelectQuery, error) {
	term, ok := value.(string)
	if !ok {
		return nil, errors.New("quickSearch requires a string value")
	}
	for word := range strings.FieldsSeq(term) {
		pattern := "%" + word + "%"
		q = q.WhereGroup("AND", func(g *bun.SelectQuery) *bun.SelectQuery {
			return g.Where("name LIKE ?", pattern).WhereOr("notes LIKE ?", pattern)
		})
	}
	return q, nil
}

func quickSearchFilter(value any) domains.Filter {
	return domains.Filter{Field: "quickSearch", Mode: domains.ModeCustom, Value: value, Custom: quickSearch}
}

func newCustomFilterService(t *testing.T) (*pagination.PaginationService[widget, string], *fakeSQLService) {
	t.Helper()
	read := newFakeSQLService(t)
	p := pagination.NewPaginationService(pagination.PaginationService[widget, string]{
		ReadSQLService: read,
	})
	seedWidgets(t, read,
		widget{ID: "w1", Name: "Ann Lee", Notes: new("passbook 100")},
		widget{ID: "w2", Name: "Ann Cole", Notes: new("passbook 200")},
		widget{ID: "w3", Name: "Bob Lee", Notes: new("passbook 300")},
	)
	return p, read
}

func customPagination(t *testing.T, filters ...domains.Filter) domains.Pagination {
	t.Helper()
	reqCtx := newPaginationTestContext(t, map[string]string{
		"filter": encodeQueryParam(t, domains.StructuredFilter{Filters: filters}),
	})
	var p domains.Pagination
	if err := p.Parse(reqCtx); err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}
	return p
}

func idsOf(data []*widget) []string {
	ids := make([]string, 0, len(data))
	for _, w := range data {
		ids = append(ids, w.ID)
	}
	return ids
}

func TestCustomFilter_HappyPath_WordsMustEachMatchSomeColumn(t *testing.T) {
	t.Parallel()
	svc, _ := newCustomFilterService(t)

	scope := domains.StructuredFilter{Filters: []domains.Filter{quickSearchFilter("ann 100")}}
	result, err := svc.PaginateFilter(context.Background(), scope, domains.Pagination{})
	if err != nil {
		t.Fatalf("PaginateFilter returned error: %v", err)
	}
	if got := idsOf(result.Data); len(got) != 1 || got[0] != "w1" {
		t.Fatalf("expected only [w1] (name matches ann, notes matches 100), got %v", got)
	}
}

func TestCustomFilter_HappyPath_CombinesWithFrontendFilterUsingAnd(t *testing.T) {
	t.Parallel()
	svc, _ := newCustomFilterService(t)

	p := customPagination(t, domains.Filter{Field: "name", Mode: domains.ModeStartsWith, Value: "Bob"})
	backend := domains.StructuredFilter{Filters: []domains.Filter{quickSearchFilter("lee")}}
	result, err := svc.PaginateFilter(context.Background(), backend, p)
	if err != nil {
		t.Fatalf("PaginateFilter returned error: %v", err)
	}
	if got := idsOf(result.Data); len(got) != 1 || got[0] != "w3" {
		t.Fatalf("expected only [w3] (frontend name=Bob* AND backend custom lee), got %v", got)
	}
}

func TestCustomFilter_SadPath_ClientSentCustomFilterIsDroppedNotExecuted(t *testing.T) {
	t.Parallel()
	svc, _ := newCustomFilterService(t)

	p := customPagination(t,
		domains.Filter{Field: "quickSearch", Mode: domains.ModeCustom, Value: "ann"},
		domains.Filter{Field: "LOWER(name) <-> 'x'", Mode: domains.ModeCustom, Value: "ann"},
	)
	result, err := svc.Paginate(context.Background(), p)
	if err != nil {
		t.Fatalf("Paginate returned error: %v", err)
	}
	if len(result.Data) != 3 {
		t.Fatalf("expected the client's custom filters to be dropped (all 3 rows), got %v", idsOf(result.Data))
	}
}

func TestCustomFilter_SadPath_WithoutAFunctionFromBackendIsAnError(t *testing.T) {
	t.Parallel()
	svc, _ := newCustomFilterService(t)

	backend := domains.StructuredFilter{Filters: []domains.Filter{
		{Field: "typo", Mode: domains.ModeCustom, Value: "ann"},
	}}
	if _, err := svc.PaginateFilter(context.Background(), backend, domains.Pagination{}); err == nil {
		t.Fatal("expected an error for a custom filter without a function, got nil")
	}
}

func TestCustomFilter_HappyPath_CursorPagesThroughCustomMatches(t *testing.T) {
	t.Parallel()
	svc, _ := newCustomFilterService(t)

	scope := domains.StructuredFilter{Filters: []domains.Filter{quickSearchFilter("passbook")}}
	p := domains.Pagination{
		Filter:   domains.StructuredFilter{SortFields: []domains.SortField{{Field: "id", Order: domains.SortOrderAsc}}},
		PageSize: 2,
	}

	first, err := svc.PaginateFilter(context.Background(), scope, p)
	if err != nil {
		t.Fatalf("first page returned error: %v", err)
	}
	if got := idsOf(first.Data); len(got) != 2 || first.NextCursor == nil {
		t.Fatalf("expected 2 rows and a next cursor, got %v / %v", got, first.NextCursor)
	}

	p.Cursor = first.NextCursor
	second, err := svc.PaginateFilter(context.Background(), scope, p)
	if err != nil {
		t.Fatalf("second page returned error: %v", err)
	}
	if got := idsOf(second.Data); len(got) != 1 || got[0] != "w3" {
		t.Fatalf("expected the remaining [w3], got %v", got)
	}
}

func TestCustomFilter_HappyPath_InlineFuncCombinesWithOtherBackendFilters(t *testing.T) {
	t.Parallel()
	svc, _ := newCustomFilterService(t)

	scope := domains.StructuredFilter{
		Logic: domains.LogicAnd,
		Filters: []domains.Filter{
			{Field: "name", Mode: domains.ModeStartsWith, Value: "Ann"},
			{Field: "name", Mode: domains.ModeCustom, Value: "Cole", Custom: func(q *bun.SelectQuery, value any) (*bun.SelectQuery, error) {
				return q.Where("name LIKE ?", "%"+value.(string)+"%"), nil
			}},
		},
	}
	result, err := svc.PaginateFilter(context.Background(), scope, domains.Pagination{})
	if err != nil {
		t.Fatalf("PaginateFilter returned error: %v", err)
	}
	if got := idsOf(result.Data); len(got) != 1 || got[0] != "w2" {
		t.Fatalf("expected only [w2] (Ann* AND inline Cole), got %v", got)
	}
}

func TestCustomFilter_HappyPath_InlineFuncErrorSurfaces(t *testing.T) {
	t.Parallel()
	svc, _ := newCustomFilterService(t)

	scope := domains.StructuredFilter{Filters: []domains.Filter{
		{Mode: domains.ModeCustom, Custom: func(*bun.SelectQuery, any) (*bun.SelectQuery, error) {
			return nil, errors.New("boom")
		}},
	}}
	if _, err := svc.PaginateFilter(context.Background(), scope, domains.Pagination{}); err == nil {
		t.Fatal("expected the inline function's error to surface, got nil")
	}
}

func TestCustomFilter_SadPath_ClientCannotSupplyInlineFuncViaJSON(t *testing.T) {
	t.Parallel()
	var f domains.Filter
	raw := `{"field":"x","mode":"custom","value":"v","Custom":"anything","custom":"anything"}`
	if err := json.Unmarshal([]byte(raw), &f); err != nil {
		t.Fatalf("Unmarshal returned error: %v", err)
	}
	if f.Custom != nil {
		t.Fatal("expected Filter.Custom to never be populated from JSON")
	}
}

func TestCustomFilter_HappyPath_InlineClosureCapturesSearchWithoutValue(t *testing.T) {
	t.Parallel()
	svc, _ := newCustomFilterService(t)

	search := "ann cole"
	scope := domains.StructuredFilter{
		Logic: domains.LogicAnd,
		Filters: []domains.Filter{
			{
				Field: "quickSearch",
				Mode:  domains.ModeCustom,
				Custom: func(q *bun.SelectQuery, _ any) (*bun.SelectQuery, error) {
					for word := range strings.FieldsSeq(search) {
						pattern := "%" + word + "%"
						q = q.WhereGroup("AND", func(g *bun.SelectQuery) *bun.SelectQuery {
							return g.Where("name LIKE ?", pattern).WhereOr("notes LIKE ?", pattern)
						})
					}
					return q, nil
				},
			},
		},
	}
	result, err := svc.PaginateFilter(context.Background(), scope, domains.Pagination{})
	if err != nil {
		t.Fatalf("PaginateFilter returned error: %v", err)
	}
	if got := idsOf(result.Data); len(got) != 1 || got[0] != "w2" {
		t.Fatalf("expected only [w2] (Ann Cole), got %v", got)
	}
}
