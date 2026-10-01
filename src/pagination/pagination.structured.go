package pagination

import (
	"context"
	"fmt"
	"math"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/domains"
	"github.com/Lands-Horizon-Corp/cqrs-go/src/utils"
)

// Pagination runs a cursor (keyset) pagination query against ReadSQLService
// only — this whole service is query-side only, it never writes. It fetches
// one row more than PageSize to detect whether another page exists without
// a separate COUNT(*), which is what lets every page stay cheap regardless
// of how large the table is (see domains.PaginationResult's doc comment for
// why there's no total count/page number).
//
// Direction isn't a separate field on domains.Pagination: it's stamped
// into the cursor token itself (see cursorPayload), so a caller never has
// to track it independently — it just always sends back whichever cursor
// (NextCursor or PreviousCursor) the previous response gave it. A cursor
// built with backward=true re-runs the query with every sort column's
// direction and every cursor comparison flipped (walking the index from the
// opposite end of where the cursor points), then reverses the fetched rows
// back into normal forward display order before returning — Data always
// reads the same way no matter which direction fetched it.
//
// The keyset WHERE clause takes one of two shapes depending on whether the
// resolved sort columns share one comparison direction (cursorIsUniform) —
// verified against a real 500k-row table that this choice is not cosmetic,
// it's a ~250-500x difference in query time:
//   - Uniform (the common case — a single sort column, or several that all
//     move the same way): a single SQL row-value comparison
//     "(col0, col1, ...) op (v0, v1, ...)", which Postgres pushes straight
//     into a composite-index range scan.
//   - Mixed (a genuinely mixed ascending/descending multi-column sort): no
//     single comparison expresses that, so paginateMixedDirection decomposes
//     it into one independently-indexable UNION ALL branch per sort column
//     instead of an OR/AND WHERE clause, which Postgres never turns into an
//     index condition no matter how it's phrased — confirmed directly
//     against a real Postgres instance before choosing this approach.
func (c *PaginationService[TData, TID]) Pagination(
	ctx context.Context,
	pagination domains.Pagination,
	preloads ...string,
) (*domains.PaginationResult[TData], error) {
	if err := c.checkReady(); err != nil {
		return nil, err
	}
	return c.paginate(ctx, c.ReadSQLService.Client(), domains.StructuredFilter{}, pagination, false, preloads...)
}

// checkReady validates the operator-configured fields Pagination and every
// domains.PaginationService entry point (Paginate/PaginateFilter/Filter/
// FilterWithTx) depend on before touching the database at all — a caller
// bypassing NewPaginationService (whose own nil-check only guards
// ReadSQLService) could otherwise reach paginate with an empty
// ColumnDefaultID and get a broken query instead of a clear error.
func (c *PaginationService[TData, TID]) checkReady() error {
	if c.ReadSQLService == nil {
		return fmt.Errorf("pagination requires ReadSQLService to be set")
	}
	if c.ColumnDefaultID == "" {
		// resolveSortFields/defaultSortField always fall back to
		// ColumnDefaultID as the keyset tiebreaker column; left empty (a
		// caller bypassing NewPaginationService, which normally defaults it
		// to "id"), that becomes bun.Ident("") in the generated ORDER
		// BY/keyset WHERE clause — confirmed this produces a broken query
		// ("no such column: DESC") instead of a clear setup error.
		return fmt.Errorf("pagination requires ColumnDefaultID to be set")
	}
	return nil
}

// paginate is Pagination's core, running against an explicit db rather than
// always going through c.ReadSQLService.Client() — this is what lets
// FilterWithTx run the exact same logic against a caller-supplied *bun.Tx
// (e.g. reading back rows written earlier in the same transaction, before
// it commits) instead of a separate connection that wouldn't see them yet.
//
// It also combines two independent StructuredFilters with AND: extraFilter
// (backend-hardcoded, e.g. tenant scoping — trusted, so an unknown field in
// it is a real error, not something to silently drop) and
// pagination.Filter (frontend-supplied, run through normalizeFilters'
// lenient drop-and-warn first). PaginateFilter/FilterWithTx are what give
// callers a way to supply extraFilter; Pagination itself always passes a
// zero-value one, which applyFilters treats as a no-op.
//
// forUpdate adds a pessimistic row lock ("SELECT ... FOR UPDATE") to the
// query: every *WithTx single/multi-row fetch (FilterWithTx, FindWithTx,
// FindOneWithTx, GetByIDWithTx, MaxWithTx, MinWithTx) passes true, since
// the whole reason a caller reaches for one of those instead of the plain
// (ReadSQLService-backed) version is "I'm about to act on what I just read,
// inside this same transaction" — the classic check-then-act gap (read a
// balance, decide what to write, write it) is exactly what a second,
// concurrent transaction could otherwise run straight through, between the
// first one's read and its write. Pagination/PaginateFilter/
// PaginateWithHertz (a browsing/listing path, not a read-then-write one)
// always pass false. Only PostgreSQL actually supports this — SQLite has
// no such clause at all (confirmed directly: it's a syntax error) — so
// forUpdate is a no-op against any other dialect, which is what lets this
// package's SQLite-backed unit tests keep exercising every *WithTx method
// without a live Postgres instance; real lock-contention/deadlock
// behavior is only meaningful, and only tested, against real Postgres.
func (c *PaginationService[TData, TID]) paginate(
	ctx context.Context,
	db bun.IDB,
	extraFilter domains.StructuredFilter,
	pagination domains.Pagination,
	forUpdate bool,
	preloads ...string,
) (*domains.PaginationResult[TData], error) {
	if pagination.PageSize <= 0 {
		pagination.PageSize = 30
	} else if pagination.PageSize > math.MaxInt-1 {
		// limit := PageSize + 1 (below) fetches one extra row to detect a
		// next page — an unclamped PageSize near math.MaxInt would overflow
		// that +1 into a negative number. Go doesn't panic on int overflow,
		// it silently wraps, so this was reachable and confirmed to
		// actually break against real Postgres (which rejects a negative
		// LIMIT outright with "LIMIT must not be negative" — SQLite
		// happens to treat a negative LIMIT as unlimited instead, which is
		// why this only surfaced by testing the real target database).
		pagination.PageSize = math.MaxInt - 1
	}

	pagination.Filter.Filters = c.normalizeFilters(ctx, pagination.Filter.Filters)

	sortFields, err := c.resolveSortFields(pagination.Filter.SortFields)
	if err != nil {
		return nil, fmt.Errorf("resolving sort fields: %w", err)
	}
	payload, hasCursor, err := c.decodeCursor(pagination.Cursor, sortFields)
	if err != nil {
		return nil, fmt.Errorf("applying cursor: %w", err)
	}
	backward := hasCursor && payload.Backward
	op, uniform := cursorIsUniform(sortFields, backward)
	if uniform && anyNullableSortField[TData](sortFields) {
		// A nullable sort column has no well-defined row-value comparison
		// (applyCursorUniform) — see appendCursorTerm's doc comment for the
		// NULL-aware WHERE shape paginateMixedDirection's per-branch terms
		// use instead, which is the only path that knows how to handle it.
		uniform = false
	}
	limit := pagination.PageSize + 1

	var data []TData
	if hasCursor && !uniform {
		if forUpdate {
			// Unreachable by every current forUpdate=true caller (none of
			// them ever supply a cursor — see paginate's own doc comment),
			// kept as a safety net: Postgres rejects FOR UPDATE combined
			// with the UNION ALL paginateMixedDirection builds outright, so
			// this would otherwise surface as a confusing driver error
			// instead of a clear one if that ever changed.
			return nil, fmt.Errorf("pagination: row locking (FOR UPDATE) is not supported with mixed-direction cursor pagination")
		}
		if err := c.paginateMixedDirection(ctx, db, &data, extraFilter, pagination.Filter, sortFields, payload, backward, limit); err != nil {
			return nil, err
		}
	} else {
		q := db.NewSelect().Model(&data)
		// Two separate WhereGroup calls, not one merged Filters slice: each
		// side keeps its own Logic (AND/OR among its own Filters), and bun
		// ANDs the two resulting groups together at the top level — exactly
		// "(extraFilter) AND (pagination.Filter)", never one clobbering the
		// other's internal logic.
		if q, err = c.applyFilters(q, extraFilter); err != nil {
			return nil, fmt.Errorf("applying hardcoded filter: %w", err)
		}
		if q, err = c.applyFilters(q, pagination.Filter); err != nil {
			return nil, fmt.Errorf("applying filters: %w", err)
		}
		if hasCursor {
			q = applyCursorUniform(q, sortFields, payload.Values, op)
		}
		orderFields := sortFields
		if backward {
			orderFields = reverseSortFields(sortFields)
		}
		for _, sf := range orderFields {
			dir := "DESC"
			if sf.Order == domains.SortOrderAsc {
				dir = "ASC"
			}
			// NULLS LAST regardless of direction: a no-op for a non-nullable
			// column, but this path is still what a cursor-less first page
			// uses even when a later sort column is nullable (see
			// anyNullableSortField), so its row order has to already match
			// what appendCursorTerm's NULLS-LAST-always convention expects
			// once a page 2 cursor comes back through the other path.
			q = q.OrderExpr("? "+dir+" NULLS LAST", bun.Ident(sf.Field))
		}
		q = q.Limit(limit)
		if forUpdate && db.Dialect().Name() == dialect.PG {
			q = q.For("UPDATE")
		}
		if err := q.Scan(ctx); err != nil {
			return nil, fmt.Errorf("scanning page: %w", err)
		}
	}

	hasMore := len(data) > pagination.PageSize
	if hasMore {
		data = data[:pagination.PageSize]
	}
	if backward {
		// data came back in reversed (walked-from-the-other-end) order —
		// flip it back so Data always reads in the same forward order
		// regardless of which direction fetched it.
		for i, j := 0, len(data)-1; i < j; i, j = i+1, j-1 {
			data[i], data[j] = data[j], data[i]
		}
	}

	droppedPreloads, err := utils.ApplyPreloadsMany(ctx, db, &data, c.Preloads, preloads...)
	for _, d := range droppedPreloads {
		c.warn(ctx, fmt.Sprintf("pagination: dropping unknown preload relation %q", d))
	}
	if err != nil {
		return nil, fmt.Errorf("loading preloads: %w", err)
	}

	result := &domains.PaginationResult[TData]{
		PageSize:      pagination.PageSize,
		CurrentCursor: pagination.Cursor,
	}
	if len(data) > 0 {
		if !backward {
			// Forward: hasMore tells us there's a next page. A previous
			// page exists whenever we were given a cursor at all — that
			// cursor came from somewhere, i.e. at least one row precedes
			// this page.
			if hasMore {
				next, err := c.encodeCursor(&data[len(data)-1], sortFields, false)
				if err != nil {
					return nil, fmt.Errorf("encoding next cursor: %w", err)
				}
				result.NextCursor = &next
			}
			if hasCursor {
				prev, err := c.encodeCursor(&data[0], sortFields, true)
				if err != nil {
					return nil, fmt.Errorf("encoding previous cursor: %w", err)
				}
				result.PreviousCursor = &prev
			}
		} else {
			// Backward: we only got here by walking back from some later
			// page, so a next page always exists. hasMore tells us there's
			// an even-earlier previous page.
			next, err := c.encodeCursor(&data[len(data)-1], sortFields, false)
			if err != nil {
				return nil, fmt.Errorf("encoding next cursor: %w", err)
			}
			result.NextCursor = &next
			if hasMore {
				prev, err := c.encodeCursor(&data[0], sortFields, true)
				if err != nil {
					return nil, fmt.Errorf("encoding previous cursor: %w", err)
				}
				result.PreviousCursor = &prev
			}
		}
	}

	result.Data = make([]*TData, len(data))
	for i := range data {
		result.Data[i] = &data[i]
	}
	return result, nil
}
