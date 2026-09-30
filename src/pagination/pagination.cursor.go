package pagination

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/uptrace/bun"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/domains"
	"github.com/Lands-Horizon-Corp/cqrs-go/src/utils"
)

// cursorPayload is the decoded shape of a domains.Pagination.Cursor token:
// one string value per active sort column (in the same order as the
// resolved sort fields, always ending in ColumnDefaultID), which of those
// were actually NULL on the boundary row (Values alone can't tell —
// utils.FieldValueAt already collapses a nil pointer field to "", the same
// string a real empty value would produce), plus which direction this
// specific token means to walk. Baking Backward into the token itself —
// rather than taking it as a separate request field — means the caller
// never has to track direction on its own: it just always sends back
// whichever cursor (NextCursor or PreviousCursor) the previous response
// gave it, and the token itself says how to use it.
type cursorPayload struct {
	Values   []string `json:"v"`
	Null     []bool   `json:"n,omitempty"`
	Backward bool     `json:"b,omitempty"`
}

// resolveSortFields validates caller-supplied sort fields against TData's
// real bun-tagged columns (sortFields comes from client-controlled input —
// the "filter" query param — so an unknown column name is rejected rather
// than silently producing a broken query or a confusing Postgres error),
// defaults to ColumnDefaultSort when none are given, and guarantees
// ColumnDefaultID is present as the final column so the keyset comparison
// tuple is always strictly unique and ordered.
func (c *PaginationService[TData, TID]) resolveSortFields(
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
func (c *PaginationService[TData, TID]) defaultSortField() domains.SortField {
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
// pagination scan starting from data — the sort-key tuple of that row (plus
// which entries were actually NULL — see cursorPayload), and backward
// stamped into the token so a later call knows how to interpret it without
// the caller having to also track/send a direction.
func (c *PaginationService[TData, TID]) encodeCursor(
	data *TData, sortFields []domains.SortField, backward bool,
) (string, error) {
	values := make([]string, len(sortFields))
	nulls := make([]bool, len(sortFields))
	for i, sf := range sortFields {
		idx := utils.BunColumnFieldIndex[TData](sf.Field)
		values[i] = utils.FieldValueAt(data, idx)
		nulls[i] = utils.FieldIsNilAt(data, idx)
	}
	return utils.EncodeQueryParam(cursorPayload{Values: values, Null: nulls, Backward: backward})
}

// decodeCursor decodes a cursor token once, shared by both of Pagination's
// query paths (see cursorIsUniform). A nil/empty cursor decodes to a
// zero-value payload with ok=false, meaning "first page, no keyset
// condition at all" — that's never a backward request, since Backward only
// ever comes from an actual token a previous page produced.
func (c *PaginationService[TData, TID]) decodeCursor(
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
	if len(payload.Null) < len(payload.Values) {
		// Defensive, not required by any known caller today: pads a
		// shorter/absent Null slice with false (not-null) rather than
		// erroring, so a token encoded before Null existed still decodes.
		payload.Null = append(payload.Null, make([]bool, len(payload.Values)-len(payload.Null))...)
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
// which the equivalent single OR'd-together WHERE clause never was. It's
// also the only path ever used once any sort column is nullable (see
// anyNullableSortField) — applyCursorUniform's row-value comparison has no
// well-defined meaning once a component can be NULL, so a nullable column
// forces this per-term path even for an otherwise-uniform direction.
//
// Every column here sorts NULLS LAST regardless of ASC/DESC (see the
// NULLS LAST appended to every ORDER BY in paginate/paginateMixedDirection)
// — a fixed, dialect-independent convention chosen specifically so this
// function's NULL handling doesn't also have to branch on which way each
// database defaults NULL ordering (confirmed directly: Postgres defaults
// ASC to NULLS LAST but SQLite defaults ASC to NULLS FIRST — the opposite —
// which would otherwise make the correct WHERE shape dialect-dependent
// too). Two cases per column, on top of the ordinary op comparison:
//   - values[idx] is NULL: nothing sorts after a NULL in this column since
//     NULLs are already last, so forward has no match here at all;
//     backward matches every non-NULL value in the column.
//   - values[idx] isn't NULL, walking forward: the ordinary "col op v"
//     match needs "OR col IS NULL" added, since every trailing NULL row
//     also sorts after any non-NULL boundary value under NULLS LAST.
//     Walking backward never needs this — NULLs are already the furthest
//     forward possible, so nothing "before" a non-NULL value is NULL.
func appendCursorTerm(
	q *bun.SelectQuery, sortFields []domains.SortField, values []string, nulls []bool, idx int, backward bool,
) *bun.SelectQuery {
	for i := range idx {
		field := bun.Ident(sortFields[i].Field)
		if nulls[i] {
			q = q.Where("? IS NULL", field)
		} else {
			q = q.Where("? = ?", field, values[i])
		}
	}
	field := bun.Ident(sortFields[idx].Field)
	if nulls[idx] {
		if backward {
			return q.Where("? IS NOT NULL", field)
		}
		return q.Where("1 = 0")
	}
	op := cursorOperator(sortFields[idx].Order, backward)
	if !backward {
		return q.Where(fmt.Sprintf("(? %s ? OR ? IS NULL)", op), field, values[idx], field)
	}
	return q.Where(fmt.Sprintf("? %s ?", op), field, values[idx])
}

// anyNullableSortField reports whether any resolved sort column's Go field
// type is a pointer (nullable) — see appendCursorTerm's doc comment for how
// NULLs are handled once this forces the mixed-direction path, and
// resolveSortFields' Null-handling for why this can't be answered from the
// cursor payload alone (a fresh, cursor-less first page has no payload yet,
// but the query it builds already needs to know whether to expect NULLs).
func anyNullableSortField[TData any](sortFields []domains.SortField) bool {
	t := reflect.TypeFor[TData]()
	for _, sf := range sortFields {
		idx := utils.BunColumnFieldIndex[TData](sf.Field)
		if idx < 0 || idx >= t.NumField() {
			continue
		}
		if t.Field(idx).Type.Kind() == reflect.Pointer {
			return true
		}
	}
	return false
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
