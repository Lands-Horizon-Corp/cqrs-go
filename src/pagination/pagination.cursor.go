package pagination

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
func (c *PaginationService[TData, TResponse, TRequest, TID]) resolveSortFields(
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
// is a setup bug that surfaces immediately as a Postgres error.
func (c *PaginationService[TData, TResponse, TRequest, TID]) defaultSortField() domains.SortField {
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
func (c *PaginationService[TData, TResponse, TRequest, TID]) encodeCursor(
	data *TData, sortFields []domains.SortField, backward bool,
) (string, error) {
	values := make([]string, len(sortFields))
	for i, sf := range sortFields {
		values[i] = utils.FieldValueAt(data, utils.BunColumnFieldIndex[TData](sf.Field))
	}
	return utils.EncodeQueryParam(cursorPayload{Values: values, Backward: backward})
}

// decodeCursor decodes a cursor token once, shared by both of Pagination's
// query paths (see cursorIsUniform). A nil/empty cursor decodes to a
// zero-value payload with ok=false, meaning "first page, no keyset
// condition at all" — that's never a backward request, since Backward only
// ever comes from an actual token a previous page produced.
func (c *PaginationService[TData, TResponse, TRequest, TID]) decodeCursor(
	cursor *string, sortFields []domains.SortField,
) (payload cursorPayload, ok bool, err error) {
	if cursor == nil || *cursor == "" {
		return cursorPayload{}, false, nil
	}
	payload, err = utils.DecodeQueryParam[cursorPayload](*cursor)
	if err != nil {
		return cursorPayload{}, false, fmt.Errorf("decoding cursor: %w", err)
	}
	if len(payload.Values) != len(sortFields) {
		return cursorPayload{}, false, fmt.Errorf(
			"cursor does not match the current sort fields: expected %d values, got %d",
			len(sortFields), len(payload.Values),
		)
	}
	return payload, true, nil
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

// cursorIsUniform reports whether every sort column resolves to the same
// comparison operator for this request's direction. When true, the keyset
// condition can be expressed as a single SQL row-value comparison —
// "(col0, col1, ...) op (v0, v1, ...)" — which Postgres pushes straight into
// a composite-index range scan (verified directly against a real 500k-row
// table: ~500x faster than the OR/AND expansion below, which Postgres never
// turns into an index condition no matter how it's written — it evaluates
// it as a Filter over a full index scan instead).
//
// A single sort column (by far the common case — it's what every default,
// unconfigured Pagination call uses) is always uniform. Only a genuinely
// mixed ascending/descending multi-column sort is not, and that case is
// handled by paginateMixedDirection instead (see pagination.mixed.go).
func cursorIsUniform(sortFields []domains.SortField, backward bool) (op string, uniform bool) {
	if len(sortFields) == 0 {
		return "", true
	}
	op = cursorOperator(sortFields[0].Order, backward)
	for _, sf := range sortFields[1:] {
		if cursorOperator(sf.Order, backward) != op {
			return "", false
		}
	}
	return op, true
}

// applyCursorUniform adds the keyset condition as a single row-value
// comparison. Only valid when cursorIsUniform reports true — Postgres's row
// constructor comparison is itself only correct for one uniform direction
// across every column in the tuple.
func applyCursorUniform(
	q *bun.SelectQuery, sortFields []domains.SortField, values []string, op string,
) *bun.SelectQuery {
	n := len(sortFields)
	args := make([]any, 0, n*2)
	for _, sf := range sortFields {
		args = append(args, bun.Ident(sf.Field))
	}
	for _, v := range values {
		args = append(args, v)
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?, ", n), ", ")
	return q.Where(fmt.Sprintf("(%s) %s (%s)", placeholders, op, placeholders), args...)
}

// appendCursorTerm builds one disjunct of the classic multi-column keyset
// expansion:
//
//	idx=0: col0 op0 v0
//	idx=1: col0 = v0 AND col1 op1 v1
//	idx=2: col0 = v0 AND col1 = v1 AND col2 op2 v2
//
// Used two ways: cursorIsUniform's fallback for a genuinely mixed-direction
// sort builds one of these per branch of its UNION ALL (see
// paginateMixedDirection) — each branch stays independently indexable,
// which the equivalent single OR'd-together WHERE clause never was.
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
