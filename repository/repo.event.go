package repository

import (
	"context"
	"fmt"
)

func (r *RepositoryImpl[TData, TResponse, TRequest, TID]) OnCreated(ctx context.Context, data *TData) {
	r.handleEvent(ctx, EventTypeCreated, data, r.Created)
}

func (r *RepositoryImpl[TData, TResponse, TRequest, TID]) OnUpdated(ctx context.Context, data *TData) {
	r.handleEvent(ctx, EventTypeUpdated, data, r.Updated)
}

func (r *RepositoryImpl[TData, TResponse, TRequest, TID]) OnDeleted(ctx context.Context, data *TData) {
	r.handleEvent(ctx, EventTypeDeleted, data, r.Deleted)
}

func (r *RepositoryImpl[TData, TResponse, TRequest, TID]) handleEvent(
	ctx context.Context,
	eventType EventType,
	data *TData,
	getEvents func(*TData) Events,
) {
	if data == nil || r.ToResource == nil {
		return
	}
	asyncCtx := context.WithoutCancel(ctx)
	go func(ctx context.Context, data *TData) {
		defer func() {
			if re := recover(); re != nil {
				r.error(ctx, fmt.Sprintf("%d panic on channel %s: %v", eventType, r.Channel, re))
			}
		}()
		payload := r.ToResource(data)
		if payload == nil {
			return
		}
		var events Events
		if getEvents != nil {
			events = getEvents(data)
		}
		if len(events) == 0 {
			return
		}
		if r.Dispatch != nil {
			if err := r.Dispatch(r.Channel, events, payload); err != nil {
				r.error(ctx, fmt.Sprintf("%d dispatch failed [channel: %s]: %v (type: %T)", eventType, r.Channel, err, data))
			}
		}
		if r.BroadcastService != nil {
			if err := r.BroadcastService.Broadcast([]Channel{r.Channel}, events, payload); err != nil {
				r.error(ctx, fmt.Sprintf("%d broadcast failed [channel: %s]: %v (type: %T)", eventType, r.Channel, err, data))
			}
		}
	}(asyncCtx, data)
}
