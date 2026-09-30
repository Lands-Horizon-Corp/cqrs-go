package pagination

import (
	"github.com/Lands-Horizon-Corp/cqrs-go/src/domains"
)

// PaginationService is a standalone, read-only counterpart to
// cqrs.CQRSImpl: it only ever queries ReadSQLService (see Pagination's own
// doc comment for why pagination is a query-side-only operation), so it
// deliberately doesn't carry a WriteSQLService, Validator, or any of the
// CDC/broadcast machinery CQRSImpl needs for the write path.
type PaginationService[TData any, TResponse any, TRequest any, TID comparable] struct {
	ReadSQLService domains.SQLService

	// Default column names for ID and sorting.
	ColumnDefaultID   string
	ColumnDefaultSort string
	Preloads          []string

	// ToResource converts a write-model row into the API resource returned
	// from Pagination.
	ToResource func(*TData) *TResponse
}

// NewPaginationService applies the same defaults cqrs.NewCQRS uses
// (ColumnDefaultID -> "id", ColumnDefaultSort -> "updated_at DESC") and
// panics if ReadSQLService is nil — fail-fast at construction time, same
// as cqrs.NewCQRS does for WriteSQLService, rather than surfacing a nil
// dependency only when the first page is requested.
func NewPaginationService[TData any, TResponse any, TRequest any, TID comparable](
	p PaginationService[TData, TResponse, TRequest, TID],
) *PaginationService[TData, TResponse, TRequest, TID] {
	if p.ColumnDefaultID == "" {
		p.ColumnDefaultID = "id"
	}
	if p.ColumnDefaultSort == "" {
		p.ColumnDefaultSort = "updated_at DESC"
	}
	if p.ReadSQLService == nil {
		panic("ReadSQLService must be initialized")
	}
	return &PaginationService[TData, TResponse, TRequest, TID]{
		ReadSQLService:    p.ReadSQLService,
		ColumnDefaultID:   p.ColumnDefaultID,
		ColumnDefaultSort: p.ColumnDefaultSort,
		Preloads:          p.Preloads,
		ToResource:        p.ToResource,
	}
}
