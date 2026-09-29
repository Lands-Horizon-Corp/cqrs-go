package cqrs

import "context"

func (c *CQRSImpl[TData, TResponse, TRequest, TID]) info(ctx context.Context, msg string) {
	if c.LogService != nil {
		c.LogService.Log(ctx, msg)
	}
}

func (c *CQRSImpl[TData, TResponse, TRequest, TID]) error(ctx context.Context, msg string) {
	if c.LogService != nil {
		c.LogService.Error(ctx, msg)
	}
}
func (c *CQRSImpl[TData, TResponse, TRequest, TID]) warn(ctx context.Context, msg string) {
	if c.LogService != nil {
		c.LogService.Warn(ctx, msg)
	}
}

func (c *CQRSImpl[TData, TResponse, TRequest, TID]) success(ctx context.Context, msg string) {
	if c.LogService != nil {
		c.LogService.Success(ctx, msg)
	}
}
