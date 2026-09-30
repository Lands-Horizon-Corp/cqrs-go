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
	// ReadSQLService is queried preferentially. See WriteSQLService for
	// what happens when it isn't set.
	ReadSQLService domains.SQLService

	// WriteSQLService is the fallback NewPaginationService reads through
	// when ReadSQLService isn't set — e.g. a smaller deployment with a
	// single Postgres instance and no dedicated read replica. Despite the
	// name, nothing in this package ever writes through it; it's only ever
	// read from here, exactly like ReadSQLService would be.
	WriteSQLService domains.SQLService

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
// (ColumnDefaultID -> "id", ColumnDefaultSort -> "updated_at DESC"),
// falls back to WriteSQLService when ReadSQLService isn't set, and panics
// if neither is set — fail-fast at construction time, same as
// cqrs.NewCQRS does for its own WriteSQLService, rather than surfacing a
// nil dependency only when the first page is requested. After
// construction, ReadSQLService is always the effective database to query:
// every other file in this package reads through it alone and never needs
// to know whether that came from the real read config or the fallback.
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
