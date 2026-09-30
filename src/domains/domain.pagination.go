package domains

import (
	"fmt"
	"strings"
	"time"

	"github.com/cloudwego/hertz/pkg/app"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/utils"
)

const (
	ModeEqual       Mode = "equal"
	ModeNotEqual    Mode = "notEqual"
	ModeContains    Mode = "contains"
	ModeNotContains Mode = "notContains"
	ModeStartsWith  Mode = "startsWith"
	ModeEndsWith    Mode = "endsWith"
	ModeSearch      Mode = "search"
	ModeInside      Mode = "inside"
	ModeOutside     Mode = "outside"
	ModeGT          Mode = "gt"
	ModeGTE         Mode = "gte"
	ModeLT          Mode = "lt"
	ModeLTE         Mode = "lte"
	ModeRange       Mode = "range"
	ModeBefore      Mode = "before"
	ModeAfter       Mode = "after"
	ModeIsEmpty     Mode = "isEmpty"
	ModeIsNotEmpty  Mode = "isNotEmpty"

	DataTypeNumber DataType = "number"
	DataTypeText   DataType = "text"
	DataTypeBool   DataType = "bool"
	DataTypeDate   DataType = "date"
	DataTypeTime   DataType = "time"

	LogicAnd Logic = "and"
	LogicOr  Logic = "or"

	SortOrderAsc  SortOrder = "asc"
	SortOrderDesc SortOrder = "desc"
)

type (
	Mode      string
	DataType  string
	Logic     string
	SortOrder string
	// RangeNumber is ModeRange's typed bound pair for a caller building a
	// StructuredFilter directly in Go (JSON-decoded ranges arrive as
	// map[string]any{"from":...,"to":...} instead — see
	// pagination.extractRangeBounds). Both fields are plain float64, not
	// *float64: there's no way to tell an unset RangeNumber{} (Go's zero
	// value, From=To=0) apart from a caller who genuinely wants the range
	// [0, 0] — confirmed directly that an unset RangeNumber{} silently
	// matches only exactly-zero rows instead of erroring. Always set both
	// fields explicitly.
	RangeNumber struct {
		From float64 `json:"from"`
		To   float64 `json:"to"`
	}
	// RangeDate is RangeNumber's date-typed counterpart, with the same
	// zero-value caveat: an unset RangeDate{} is From=To=time.Time{} (year
	// 1, not "unbounded"), not distinguishable from a caller who actually
	// wants that exact instant. Always set both fields explicitly.
	RangeDate struct {
		From time.Time `json:"from"`
		To   time.Time `json:"to"`
	}
	SortField struct {
		Field string    `json:"field"`
		Order SortOrder `json:"order"`
	}
	Filter struct {
		Field    string   `json:"field"`
		Value    any      `json:"value"`
		Mode     Mode     `json:"mode"`
		DataType DataType `json:"dataType"`
	}
	StructuredFilter struct {
		Filters    []Filter    `json:"filters"`
		SortFields []SortField `json:"sortFields"`
		Logic      Logic       `json:"logic"`
		Preload    []string    `json:"preload"`
	}
	Pagination struct {
		Filter   StructuredFilter `json:"filter"`
		PageSize int              `query:"pageSize" default:"10"`
		Cursor   *string          `query:"cursor"`
	}

	PaginationResult[T any] struct {
		Data           []*T    `json:"data"`
		CurrentCursor  *string `json:"currentCursor"`
		NextCursor     *string `json:"nextCursor"`
		PreviousCursor *string `json:"previousCursor"`
		PageSize       int     `json:"pageSize"`
	}
)

func (p *Pagination) Parse(ctx *app.RequestContext) error {
	if err := ctx.BindAndValidate(p); err != nil {
		return fmt.Errorf("invalid pagination parameters: %w", err)
	}
	filter, err := parseFilter(ctx)
	if err != nil {
		return err
	}
	p.Filter = filter
	sortFields, err := parseSort(ctx)
	if err != nil {
		return err
	}
	if sortFields != nil {
		p.Filter.SortFields = sortFields
	}
	return nil
}

func parseFilter(ctx *app.RequestContext) (StructuredFilter, error) {
	filterParam := ctx.Query("filter")
	if filterParam == "" {
		return StructuredFilter{Logic: LogicAnd}, nil
	}
	filter, err := utils.DecodeQueryParam[StructuredFilter](filterParam)
	if err != nil {
		return StructuredFilter{}, fmt.Errorf("decoding filter: %w", err)
	}
	if filter.Logic == "" {
		filter.Logic = LogicAnd
	}
	return filter, nil
}

func parseSort(ctx *app.RequestContext) ([]SortField, error) {
	sortParam := ctx.Query("sort")
	if sortParam == "" {
		return nil, nil
	}
	sortFields, err := utils.DecodeQueryParam[[]SortField](sortParam)
	if err != nil {
		return nil, fmt.Errorf("decoding sort: %w", err)
	}
	for i, field := range sortFields {
		order := strings.ToLower(strings.TrimSpace(string(field.Order)))
		if order != string(SortOrderAsc) && order != string(SortOrderDesc) {
			sortFields[i].Order = SortOrderAsc
		} else {
			sortFields[i].Order = SortOrder(order)
		}
	}
	return sortFields, nil
}
