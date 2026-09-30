package cqrs

import (
	"context"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/domains"
)

func (c *CQRSImpl[TData, TResponse, TRequest, TID]) Paginate(
	ctx context.Context, pagination domains.Pagination) (domains.PaginationResult[TData], error) {
	return c.paginationService.Paginate(ctx, pagination)
}

func (c *CQRSImpl[TData, TResponse, TRequest, TID]) PaginateFormat(
	ctx context.Context, pagination domains.Pagination) (domains.PaginationResult[TResponse], error) {
	paginationResult, err := c.paginationService.Paginate(ctx, pagination)
	if err != nil {
		return domains.PaginationResult[TResponse]{}, err
	}
	return domains.PaginationResult[TResponse]{
		Data:           c.ToModels(paginationResult.Data),
		CurrentCursor:  paginationResult.CurrentCursor,
		NextCursor:     paginationResult.NextCursor,
		PreviousCursor: paginationResult.PreviousCursor,
		PageSize:       paginationResult.PageSize,
	}, nil
}

func (c *CQRSImpl[TData, TResponse, TRequest, TID]) PaginateFilter(
	ctx context.Context, filter domains.StructuredFilter, pagination domains.Pagination) (domains.PaginationResult[TData], error) {
	return c.paginationService.PaginateFilter(ctx, filter, pagination)
}

func (c *CQRSImpl[TData, TResponse, TRequest, TID]) PaginateFilterFormat(
	ctx context.Context, filter domains.StructuredFilter, pagination domains.Pagination) (domains.PaginationResult[TResponse], error) {
	paginationResult, err := c.paginationService.PaginateFilter(ctx, filter, pagination)
	if err != nil {
		return domains.PaginationResult[TResponse]{}, err
	}
	return domains.PaginationResult[TResponse]{
		Data:           c.ToModels(paginationResult.Data),
		CurrentCursor:  paginationResult.CurrentCursor,
		NextCursor:     paginationResult.NextCursor,
		PreviousCursor: paginationResult.PreviousCursor,
		PageSize:       paginationResult.PageSize,
	}, nil
}
