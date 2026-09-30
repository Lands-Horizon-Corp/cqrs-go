package cqrs

import (
	"context"
	"fmt"

	"github.com/uptrace/bun"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/domains"
)

// Pagination runs a cursor (keyset) pagination query against ReadSQLService
// only — never WriteSQLService, this is a query-side operation. It fetches
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
func (c *CQRSImpl[TData, TResponse, TRequest, TID]) Pagination(
	ctx context.Context,
	pagination domains.Pagination,
	preloads ...string,
) (*domains.PaginationResult[TResponse], error) {
	if c.ReadSQLService == nil {
		return nil, fmt.Errorf("pagination requires ReadSQLService to be set")
	}
	if pagination.PageSize <= 0 {
		pagination.PageSize = 30
	}

	sortFields, err := c.resolveSortFields(pagination.Filter.SortFields)
	if err != nil {
		return nil, fmt.Errorf("resolving sort fields: %w", err)
	}
	var data []TData
	q := c.ReadSQLService.Client().NewSelect().Model(&data)
	if q, err = c.applyFilters(q, pagination.Filter); err != nil {
		return nil, fmt.Errorf("applying filters: %w", err)
	}
	var backward bool
	if q, backward, err = c.applyCursor(q, sortFields, pagination.Cursor); err != nil {
		return nil, fmt.Errorf("applying cursor: %w", err)
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
		q = q.OrderExpr("? "+dir, bun.Ident(sf.Field))
	}
	q = q.Limit(pagination.PageSize + 1)

	if err := q.Scan(ctx); err != nil {
		return nil, fmt.Errorf("scanning page: %w", err)
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

	resolvedPreload := c.resolvePreload(preloads)
	if err := c.applyPreloadsMany(ctx, c.ReadSQLService.Client(), &data, resolvedPreload...); err != nil {
		return nil, fmt.Errorf("loading preloads: %w", err)
	}

	result := &domains.PaginationResult[TResponse]{
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
			if pagination.Cursor != nil {
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

	if c.ToResource != nil {
		result.Data = make([]*TResponse, 0, len(data))
		for i := range data {
			if res := c.ToResource(&data[i]); res != nil {
				result.Data = append(result.Data, res)
			}
		}
	}
	return result, nil
}
