package cqrs

import (
	"github.com/Lands-Horizon-Corp/cqrs-go/src/domains"
)

func (c *CQRSImpl[TData, TResponse, TRequest, TID]) Pagination(
	filterRoot domains.StructuredFilter,
	pagination domains.Pagination,
	preloads ...string,
) (*domains.PaginationResult[TData], error) {
	if pagination.PageSize <= 0 {
		pagination.PageSize = 30
	}
	result := &domains.PaginationResult[TData]{
		PageIndex: 0,
		PageSize:  pagination.PageSize,
		Data:      []*TData{},
	}
	result.TotalSize = 0
	result.TotalPage = 0

	return result, nil
}
