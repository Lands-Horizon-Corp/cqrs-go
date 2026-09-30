package cqrs

import (
	"fmt"
	"strings"

	"github.com/uptrace/bun"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/domains"
	"github.com/Lands-Horizon-Corp/cqrs-go/src/utils"
)

func (c *CQRSImpl[TData, TResponse, TRequest, TID]) applyFilters(
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
			if groupErr != nil {
				break
			}
			if utils.BunColumnFieldIndex[TData](f.Field) == -1 {
				groupErr = fmt.Errorf("unknown filter field %q", f.Field)
				break
			}
			q = q.WhereGroup(sep, func(inner *bun.SelectQuery) *bun.SelectQuery {
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
		return q.Where("? BETWEEN ? AND ?", col, from, to), nil
	case domains.ModeIsEmpty:
		return q.Where("(? IS NULL OR ? = '')", col, col), nil
	case domains.ModeIsNotEmpty:
		return q.Where("(? IS NOT NULL AND ? != '')", col, col), nil
	default:
		return nil, fmt.Errorf("unsupported mode %q", f.Mode)
	}
}
func escapeLike(s string) string {
	return strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(s)
}

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
