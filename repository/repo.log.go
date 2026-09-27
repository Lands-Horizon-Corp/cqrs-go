package repository

import "context"

func (r *RepositoryImpl[TData, TResponse, TRequest, TID]) info(ctx context.Context, msg string) {
	if r.LogService != nil {
		r.LogService.Log(ctx, msg)
	}
}

func (r *RepositoryImpl[TData, TResponse, TRequest, TID]) error(ctx context.Context, msg string) {
	if r.LogService != nil {
		r.LogService.Error(ctx, msg)
	}
}
func (r *RepositoryImpl[TData, TResponse, TRequest, TID]) warn(ctx context.Context, msg string) {
	if r.LogService != nil {
		r.LogService.Warn(ctx, msg)
	}
}

func (r *RepositoryImpl[TData, TResponse, TRequest, TID]) success(ctx context.Context, msg string) {
	if r.LogService != nil {
		r.LogService.Success(ctx, msg)
	}
}
