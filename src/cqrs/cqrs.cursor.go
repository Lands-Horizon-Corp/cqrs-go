package cqrs

import (
	"fmt"
	"strings"

	"github.com/uptrace/bun"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/domains"
	"github.com/Lands-Horizon-Corp/cqrs-go/src/utils"
)

// cursorPayload is the decoded shape of a domains.Pagination.Cursor token:
// one string value per active sort column (in the same order as the
// resolved sort fields, always ending in ColumnDefaultID), plus which
// direction this specific token means to walk. Baking Backward into the
// token itself — rather than taking it as a separate request field — means
// the caller never has to track direction on its own: it just always sends
// back whichever cursor (NextCursor or PreviousCursor) the previous
// response gave it, and the token itself says how to use it.
type cursorPayload struct {
	Values   []string `json:"v"`
	Backward bool     `json:"b,omitempty"`
}

// resolveSortFields validates caller-supplied sort fields against TData's
// real bun-tagged columns (sortFields comes from client-controlled input —
// the "filter" query param — so an unknown column name is rejected rather
// than silently producing a broken query or a confusing Postgres error),
// defaults to ColumnDefaultSort when none are given, and guarantees
// ColumnDefaultID is present as the final column so the keyset comparison
// tuple is always strictly unique and ordered.
func (c *CQRSImpl[TData, TResponse, TRequest, TID]) resolveSortFields(
	sortFields []domains.SortField,
) ([]domains.SortField, error) {
	resolved := make([]domains.SortField, 0, len(sortFields)+1)
	if len(sortFields) == 0 {
		resolved = append(resolved, c.defaultSortField())
	} else {
		for _, sf := range sortFields {
			if utils.BunColumnFieldIndex[TData](sf.Field) == -1 {
				return nil, fmt.Errorf("unknown sort field %q", sf.Field)
			}
			if sf.Order != domains.SortOrderAsc && sf.Order != domains.SortOrderDesc {
				sf.Order = domains.SortOrderAsc
			}
			resolved = append(resolved, sf)
		}
	}
	for _, sf := range resolved {
		if sf.Field == c.ColumnDefaultID {
			return resolved, nil
		}
	}
	return append(resolved, domains.SortField{Field: c.ColumnDefaultID, Order: domains.SortOrderDesc}), nil
}

// defaultSortField is not validated against TData's columns the way
// caller-supplied sort fields are: ColumnDefaultID/ColumnDefaultSort are
// operator configuration, not client input, and a misconfigured value here
// is a setup bug that surfaces immediately as a Postgres error, consistent
// with how ColumnDefaultID is already used unchecked elsewhere (UpdateByID,
// DeleteByID, ...).
func (c *CQRSImpl[TData, TResponse, TRequest, TID]) defaultSortField() domains.SortField {
	sf := domains.SortField{Field: c.ColumnDefaultID, Order: domains.SortOrderDesc}
	parts := strings.Fields(c.ColumnDefaultSort)
	if len(parts) == 0 {
		return sf
	}
	sf.Field = parts[0]
	if len(parts) > 1 && strings.EqualFold(parts[1], "asc") {
		sf.Order = domains.SortOrderAsc
	}
	return sf
}

// encodeCursor builds the opaque cursor token for resuming a keyset
// pagination scan starting from data — the sort-key tuple of that row, plus
// backward stamped into the token so a later call knows how to interpret it
// without the caller having to also track/send a direction.
func (c *CQRSImpl[TData, TResponse, TRequest, TID]) encodeCursor(
	data *TData, sortFields []domains.SortField, backward bool,
) (string, error) {
	values := make([]string, len(sortFields))
	for i, sf := range sortFields {
		values[i] = utils.FieldValueAt(data, utils.BunColumnFieldIndex[TData](sf.Field))
	}
	return utils.EncodeQueryParam(cursorPayload{Values: values, Backward: backward})
}

// applyCursor adds the keyset WHERE condition that resumes a scan right
// after (or, for a cursor built with backward=true, right before) the row a
// cursor token was built from. A nil/empty cursor is a first-page request
// and is a no-op — reported via the second return value so Pagination()
// knows whether to reverse its ORDER BY too, without decoding the cursor a
// second time itself. sortFields is always the forward-oriented sort order
// regardless of direction — backward only flips the comparison operators
// (see cursorOperator), not the column/direction pairing itself; the actual
// ORDER BY reversal for a backward query happens separately in Pagination,
// via reverseSortFields.
//
// Because sortFields can mix ascending and descending columns, a single
// row-value comparison like "(a, b) < (x, y)" only works when every column
// shares one direction, so this expands into the standard multi-column
// keyset form instead:
//
//	(col0 op0 v0)
//	OR (col0 = v0 AND col1 op1 v1)
//	OR (col0 = v0 AND col1 = v1 AND col2 op2 v2)
func (c *CQRSImpl[TData, TResponse, TRequest, TID]) applyCursor(
	q *bun.SelectQuery, sortFields []domains.SortField, cursor *string,
) (*bun.SelectQuery, bool, error) {
	if cursor == nil || *cursor == "" {
		return q, false, nil
	}
	payload, err := utils.DecodeQueryParam[cursorPayload](*cursor)
	if err != nil {
		return nil, false, fmt.Errorf("decoding cursor: %w", err)
	}
	if len(payload.Values) != len(sortFields) {
		return nil, false, fmt.Errorf(
			"cursor does not match the current sort fields: expected %d values, got %d",
			len(sortFields), len(payload.Values),
		)
	}
	q = q.WhereGroup("AND", func(q *bun.SelectQuery) *bun.SelectQuery {
		for i := range sortFields {
			term := i
			q = q.WhereGroup("OR", func(q *bun.SelectQuery) *bun.SelectQuery {
				return appendCursorTerm(q, sortFields, payload.Values, term, payload.Backward)
			})
		}
		return q
	})
	return q, payload.Backward, nil
}

// cursorOperator picks the comparison for a keyset term: forward wants
// "rows after this value" (opposite of the column's own sort direction —
// "<" on a descending column walks toward later/smaller values, which is
// "after" in that column's own order), backward wants the exact opposite.
func cursorOperator(order domains.SortOrder, backward bool) string {
	lessThan := order == domains.SortOrderDesc
	if backward {
		lessThan = !lessThan
	}
	if lessThan {
		return "<"
	}
	return ">"
}

func appendCursorTerm(
	q *bun.SelectQuery, sortFields []domains.SortField, values []string, idx int, backward bool,
) *bun.SelectQuery {
	for i := range idx {
		q = q.Where("? = ?", bun.Ident(sortFields[i].Field), values[i])
	}
	op := cursorOperator(sortFields[idx].Order, backward)
	return q.Where(fmt.Sprintf("? %s ?", op), bun.Ident(sortFields[idx].Field), values[idx])
}

// reverseSortFields flips every column's direction — used to build the
// ORDER BY for a backward-direction query, which walks the index from the
// opposite end (see Pagination) and then reverses the fetched rows back
// into normal forward display order before returning them.
func reverseSortFields(sortFields []domains.SortField) []domains.SortField {
	reversed := make([]domains.SortField, len(sortFields))
	for i, sf := range sortFields {
		order := domains.SortOrderDesc
		if sf.Order == domains.SortOrderDesc {
			order = domains.SortOrderAsc
		}
		reversed[i] = domains.SortField{Field: sf.Field, Order: order}
	}
	return reversed
}
