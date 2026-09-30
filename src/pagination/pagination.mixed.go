package pagination

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/uptrace/bun"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/domains"
)

// paginateMixedDirection handles a keyset cursor whose sort columns don't
// all share one comparison direction (cursorIsUniform is false) — no single
// SQL comparison expresses "rows after this point" for a genuinely mixed
// ascending/descending multi-column sort. Instead this builds one
// independently-indexable sub-query per disjunct of the classic keyset
// OR/AND expansion (the same terms appendCursorTerm already builds for the
// single-query case — here each one gets its own query instead of being
// OR'd together into one), each as its own named CTE (fully resolved, with
// its own ORDER BY + LIMIT), then merges them as a plain, unparenthesized
// "SELECT * FROM b0 UNION ALL SELECT * FROM b1 ..." — deliberately not
// bun's own .UnionAll(), which always wraps each side in parens
// ("(SELECT ...) UNION ALL (SELECT ...)"): valid Postgres, but a syntax
// error on SQLite's compound-select grammar, and this engine's whole
// regression suite runs on SQLite. Every branch already has its ordering
// and limit baked into its own CTE, so the outer union doesn't need
// per-arm ORDER BY/LIMIT (which is what forces the parenthesized form in
// the first place) — it only needs one final ORDER BY + LIMIT over the
// combined rows, which is completely standard, portable SQL.
//
// Verified directly against a real 500k-row table: evaluating the OR/AND
// form as a single WHERE clause never produces an index condition in
// Postgres no matter how it's phrased — it always becomes a Filter over a
// full index scan. This CTE-per-branch form keeps every branch on a genuine
// Index Cond range scan instead — ~250x faster on that same table and
// query shape.
func (c *PaginationService[TData, TResponse, TRequest, TID]) paginateMixedDirection(
	ctx context.Context,
	db bun.IDB,
	data *[]TData,
	filterRoot domains.StructuredFilter,
	sortFields []domains.SortField,
	payload cursorPayload,
	backward bool,
	limit int,
) error {
	orderFields := sortFields
	if backward {
		orderFields = reverseSortFields(sortFields)
	}
	applyOrder := func(q *bun.SelectQuery) *bun.SelectQuery {
		for _, sf := range orderFields {
			dir := "DESC"
			if sf.Order == domains.SortOrderAsc {
				dir = "ASC"
			}
			q = q.OrderExpr("? "+dir, bun.Ident(sf.Field))
		}
		return q
	}

	outer := db.NewSelect().Model(data)
	branchArgs := make([]any, 0, len(sortFields))
	for idx := range sortFields {
		branch := db.NewSelect().Model((*TData)(nil))
		branch, err := c.applyFilters(branch, filterRoot)
		if err != nil {
			return fmt.Errorf("applying filters: %w", err)
		}
		branch = appendCursorTerm(branch, sortFields, payload.Values, idx, backward)
		branch = applyOrder(branch).Limit(limit)

		name := fmt.Sprintf("cqrs_branch_%d", idx)
		outer = outer.With(name, branch)
		branchArgs = append(branchArgs, bun.Ident(name))
	}

	// bun's generated column list is qualified with the model's own alias
	// (e.g. "widget"."id") regardless of the FROM clause, so the combined
	// branches need to be presented under that same alias — verified
	// directly: without this, Postgres/SQLite both report a missing
	// FROM-clause entry for the model's alias. Table() isn't part of
	// bun.IDB (only *bun.DB exposes it), but the alias it returns is purely
	// structural — derived from TData's own struct tags, not from which
	// connection or transaction is running the query — so resolving it
	// through ReadSQLService's *bun.DB is correct even when db here is a
	// caller-supplied *bun.Tx (see FilterWithTx).
	table := c.ReadSQLService.Client().Table(reflect.TypeFor[TData]())
	var fromExpr strings.Builder
	fromExpr.WriteString("(")
	for i := range branchArgs {
		if i > 0 {
			fromExpr.WriteString(" UNION ALL ")
		}
		fromExpr.WriteString("SELECT * FROM ?")
	}
	fromExpr.WriteString(") AS ?")
	branchArgs = append(branchArgs, bun.Ident(table.Alias))
	outer = outer.ModelTableExpr(fromExpr.String(), branchArgs...)

	outer = applyOrder(outer).Limit(limit)
	if err := outer.Scan(ctx); err != nil {
		return fmt.Errorf("scanning page: %w", err)
	}
	return nil
}
