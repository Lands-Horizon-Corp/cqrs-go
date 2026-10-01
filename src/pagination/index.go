package pagination

import (
	"github.com/Lands-Horizon-Corp/cqrs-go/src/domains"
)

// PaginationService is a standalone, read-only counterpart to
// cqrs.CQRSImpl: it only ever queries a database (see Pagination's own doc
// comment for why pagination is a query-side-only operation) — it never
// writes through either service below, so it deliberately doesn't carry a
// Validator or any of the CDC/broadcast machinery CQRSImpl needs for the
// write path.
type PaginationService[TData any, TID comparable] struct {
	ReadSQLService    domains.SQLService
	WriteSQLService   domains.SQLService
	LogService        domains.LogService
	ColumnDefaultID   string
	ColumnDefaultSort string
	Preloads          []string
}

func NewPaginationService[TData any, TID comparable](
	p PaginationService[TData, TID],
) *PaginationService[TData, TID] {
	if p.ColumnDefaultID == "" {
		p.ColumnDefaultID = "id"
	}
	if p.ColumnDefaultSort == "" {
		p.ColumnDefaultSort = "updated_at DESC"
	}
	if p.ReadSQLService == nil {
		p.ReadSQLService = p.WriteSQLService
	}
	if p.ReadSQLService == nil {
		panic("ReadSQLService or WriteSQLService must be initialized")
	}
	return &PaginationService[TData, TID]{
		ReadSQLService:    p.ReadSQLService,
		WriteSQLService:   p.WriteSQLService,
		LogService:        p.LogService,
		ColumnDefaultID:   p.ColumnDefaultID,
		ColumnDefaultSort: p.ColumnDefaultSort,
		Preloads:          p.Preloads,
	}
}
