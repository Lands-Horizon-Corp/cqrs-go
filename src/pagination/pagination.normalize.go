package pagination

import (
	"context"
	"fmt"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/domains"
	"github.com/Lands-Horizon-Corp/cqrs-go/src/utils"
)

// normalizeFilters runs every filter's Field through
// utils.NormalizeColumnName (client input arrives in whatever casing/
// spacing the caller happened to send — "userName", " User Name ",
// "UserName" should all still resolve the same real column) and drops any
// filter whose normalized Field still doesn't match a real TData column.
//
// A dropped filter only warns (via LogService, if set) rather than failing
// the whole page: an unknown/stale/typo'd filter field is untrusted client
// input, not a caller bug worth a hard error — the safe behavior is to
// ignore that one term and keep serving the rest of the request, the same
// way an unknown query-string parameter is typically ignored rather than
// rejected outright.
func (c *PaginationService[TData, TID]) normalizeFilters(
	ctx context.Context, filters []domains.Filter,
) []domains.Filter {
	if len(filters) == 0 {
		return filters
	}
	normalized := make([]domains.Filter, 0, len(filters))
	for _, f := range filters {
		f.Field = utils.NormalizeColumnName(f.Field)
		if utils.BunColumnFieldIndex[TData](f.Field) == -1 {
			c.warn(ctx, fmt.Sprintf("pagination: dropping filter for unknown field %q", f.Field))
			continue
		}
		normalized = append(normalized, f)
	}
	return normalized
}
