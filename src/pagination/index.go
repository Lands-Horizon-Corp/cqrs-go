package pagination

import (
	"github.com/Lands-Horizon-Corp/cqrs-go/src/domains"
)

// PaginationService is a standalone, read-only counterpart to
// cqrs.CQRSImpl: it only ever queries ReadSQLService (see Pagination's own
// doc comment for why pagination is a query-side-only operation), so it
// deliberately doesn't carry a WriteSQLService, Validator, or any of the
// CDC/broadcast machinery CQRSImpl needs for the write path.
type PaginationService[TData any, TRequest any, TID comparable] struct {
	ReadSQLService domains.SQLService

	// LogService is optional — when set, a filter whose Field doesn't
	// resolve to a real TData column after normalization (see
	// utils.NormalizeColumnName) is dropped with a Warn instead of
	// silently vanishing or failing the whole request.
	LogService domains.LogService

	// Default column names for ID and sorting.
	ColumnDefaultID   string
	ColumnDefaultSort string
	Preloads          []string
}

// NewPaginationService applies the same defaults cqrs.NewCQRS uses
// (ColumnDefaultID -> "id", ColumnDefaultSort -> "updated_at DESC") and
// panics if ReadSQLService is nil — fail-fast at construction time, same
// as cqrs.NewCQRS does for WriteSQLService, rather than surfacing a nil
// dependency only when the first page is requested.
func NewPaginationService[TData any, TRequest any, TID comparable](
	p PaginationService[TData, TRequest, TID],
) *PaginationService[TData, TRequest, TID] {
	if p.ColumnDefaultID == "" {
		p.ColumnDefaultID = "id"
	}
	if p.ColumnDefaultSort == "" {
		p.ColumnDefaultSort = "updated_at DESC"
	}
	if p.ReadSQLService == nil {
		panic("ReadSQLService must be initialized")
	}
	return &PaginationService[TData, TRequest, TID]{
		ReadSQLService:    p.ReadSQLService,
		LogService:        p.LogService,
		ColumnDefaultID:   p.ColumnDefaultID,
		ColumnDefaultSort: p.ColumnDefaultSort,
		Preloads:          p.Preloads,
	}
}
