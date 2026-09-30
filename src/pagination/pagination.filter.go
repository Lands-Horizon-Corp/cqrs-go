package pagination

import (
	"fmt"
	"strings"
	"time"

	"github.com/uptrace/bun"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/domains"
	"github.com/Lands-Horizon-Corp/cqrs-go/src/utils"
)

// applyFilters translates a StructuredFilter's Filters into a bun WHERE
// clause, combined across filters using Logic (LogicAnd/LogicOr, defaulting
// to AND). Field names are client-controlled (the "filter" query param), so
// each one is validated against TData's real bun-tagged columns before
// being used. Column names always go through bun.Ident (never
// string-concatenated) and values are always bound via "?" placeholders.
func (c *PaginationService[TData, TResponse, TRequest, TID]) applyFilters(
	q *bun.SelectQuery, filterRoot domains.StructuredFilter,
) (*bun.SelectQuery, error) {
	if len(filterRoot.Filters) == 0 {
		return q, nil
	}
	sep := "AND"
	if filterRoot.Logic == domains.LogicOr {
		sep = "OR"
	}
	var groupErr error
	q = q.WhereGroup("AND", func(q *bun.SelectQuery) *bun.SelectQuery {
		for _, f := range filterRoot.Filters {
			// Defensive only: every path that sets groupErr below also
			// breaks immediately, so this guard is only load-bearing if bun
			// ever invokes this WhereGroup callback more than once per call
			// (it doesn't, today) — kept as a cheap safeguard against that
			// assumption changing.
			if groupErr != nil {
				break
			}
			if utils.BunColumnFieldIndex[TData](f.Field) == -1 {
				groupErr = fmt.Errorf("unknown filter field %q", f.Field)
				break
			}
			q = q.WhereGroup(sep, func(inner *bun.SelectQuery) *bun.SelectQuery {
				// A term-building error must never bubble up as a nil
				// *SelectQuery: bun's WhereGroup callback contract requires
				// a valid query back, even when we're about to abort via
				// groupErr — returning the original inner unchanged keeps
				// bun's internal chain intact.
				newQ, err := applyFilterTerm(inner, f)
				if err != nil {
					groupErr = fmt.Errorf("filter %q: %w", f.Field, err)
					return inner
				}
				return newQ
			})
		}
		return q
	})
	if groupErr != nil {
		return nil, groupErr
	}
	return q, nil
}

func applyFilterTerm(q *bun.SelectQuery, f domains.Filter) (*bun.SelectQuery, error) {
	col := bun.Ident(f.Field)
	switch f.Mode {
	case domains.ModeEqual, domains.ModeNotEqual, domains.ModeGT, domains.ModeGTE,
		domains.ModeLT, domains.ModeLTE, domains.ModeBefore, domains.ModeAfter,
		domains.ModeInside, domains.ModeOutside:
		// JSON has no native date/time type, so a DataTypeDate/DataTypeTime
		// filter's Value always arrives as a string (or a list of strings,
		// for Inside/Outside) in whatever format the caller happened to
		// send — coerce it to a real time.Time here, against the full set
		// of layouts ParseDateTime/ParseTimeOfDay know, so every comparison
		// mode gets one consistent, correctly-typed value bound into the
		// query instead of a raw string literal.
		coerced, err := coerceDateTimeFilterValue(f.DataType, f.Value)
		if err != nil {
			return nil, err
		}
		f.Value = coerced
	}
	switch f.Mode {
	case domains.ModeEqual:
		return q.Where("? = ?", col, f.Value), nil
	case domains.ModeNotEqual:
		return q.Where("? != ?", col, f.Value), nil
	case domains.ModeGT:
		return q.Where("? > ?", col, f.Value), nil
	case domains.ModeGTE:
		return q.Where("? >= ?", col, f.Value), nil
	case domains.ModeLT:
		return q.Where("? < ?", col, f.Value), nil
	case domains.ModeLTE:
		return q.Where("? <= ?", col, f.Value), nil
	case domains.ModeBefore:
		return q.Where("? < ?", col, f.Value), nil
	case domains.ModeAfter:
		return q.Where("? > ?", col, f.Value), nil
	case domains.ModeContains:
		return q.Where("? LIKE ?", col, "%"+escapeLike(fmt.Sprint(f.Value))+"%"), nil
	case domains.ModeNotContains:
		return q.Where("? NOT LIKE ?", col, "%"+escapeLike(fmt.Sprint(f.Value))+"%"), nil
	case domains.ModeStartsWith:
		return q.Where("? LIKE ?", col, escapeLike(fmt.Sprint(f.Value))+"%"), nil
	case domains.ModeEndsWith:
		return q.Where("? LIKE ?", col, "%"+escapeLike(fmt.Sprint(f.Value))), nil
	case domains.ModeInside:
		return q.Where("? IN (?)", col, bun.In(f.Value)), nil
	case domains.ModeOutside:
		return q.Where("? NOT IN (?)", col, bun.In(f.Value)), nil
	case domains.ModeRange:
		from, to, err := extractRangeBounds(f.Value)
		if err != nil {
			return nil, err
		}
		if from, err = coerceDateTimeFilterValue(f.DataType, from); err != nil {
			return nil, err
		}
		if to, err = coerceDateTimeFilterValue(f.DataType, to); err != nil {
			return nil, err
		}
		return q.Where("? BETWEEN ? AND ?", col, from, to), nil
	case domains.ModeIsEmpty:
		// Plain "= ''" rather than a ::text cast: this needs to work
		// against SQLite too, which doesn't support Postgres's :: cast
		// syntax. A bare string comparison is meaningful for the
		// text-typed columns this mode is intended for either way.
		return q.Where("(? IS NULL OR ? = '')", col, col), nil
	case domains.ModeIsNotEmpty:
		return q.Where("(? IS NOT NULL AND ? != '')", col, col), nil
	default:
		return nil, fmt.Errorf("unsupported mode %q", f.Mode)
	}
}

// coerceDateTimeFilterValue parses value into a time.Time when dataType says
// it should be one, trying every layout ParseDateTime/ParseTimeOfDay know
// (see src/utils/datetime.go) so a client isn't locked into one specific
// wire format. A value that isn't a string (already a time.Time from a
// caller building a StructuredFilter directly in Go, or a []any for
// Inside/Outside) is handled accordingly; anything else, or any DataType
// other than Date/Time, passes through unchanged.
func coerceDateTimeFilterValue(dataType domains.DataType, value any) (any, error) {
	var parse func(string) (time.Time, bool)
	switch dataType {
	case domains.DataTypeDate:
		parse = utils.ParseDateTime
	case domains.DataTypeTime:
		parse = utils.ParseTimeOfDay
	default:
		return value, nil
	}
	switch v := value.(type) {
	case string:
		t, ok := parse(v)
		if !ok {
			return nil, fmt.Errorf("unrecognized %s value %q", dataType, v)
		}
		return t, nil
	case []any:
		out := make([]any, len(v))
		for i, elem := range v {
			coerced, err := coerceDateTimeFilterValue(dataType, elem)
			if err != nil {
				return nil, err
			}
			out[i] = coerced
		}
		return out, nil
	default:
		return value, nil
	}
}

// escapeLike escapes LIKE's own wildcard characters in a value that should
// be matched literally (Contains/StartsWith/EndsWith build the % wildcards
// themselves; a literal % or _ inside the search value must not also act as
// a wildcard). Postgres's default LIKE escape character is backslash.
func escapeLike(s string) string {
	return strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(s)
}

// extractRangeBounds reads the two bounds out of a ModeRange filter's
// Value. Value is `any` (see domains.Filter's doc comment), so a range
// arriving through JSON decodes as map[string]any{"from":...,"to":...}
// rather than domains.RangeNumber/RangeDate — both shapes are accepted here
// since a caller building a StructuredFilter directly in Go may use the
// typed struct instead.
func extractRangeBounds(value any) (from, to any, err error) {
	switch v := value.(type) {
	case domains.RangeNumber:
		return v.From, v.To, nil
	case domains.RangeDate:
		return v.From, v.To, nil
	case map[string]any:
		from, okFrom := v["from"]
		to, okTo := v["to"]
		if !okFrom || !okTo {
			return nil, nil, fmt.Errorf("range value missing \"from\"/\"to\": %#v", value)
		}
		return from, to, nil
	default:
		return nil, nil, fmt.Errorf("unsupported range value type %T", value)
	}
}
