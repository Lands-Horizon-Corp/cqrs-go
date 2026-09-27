package repository

import (
	"context"
	"fmt"
	"reflect"
)

func (r *RepositoryImpl[TData, TResponse, TRequest, TID]) OnCreated(ctx context.Context, data *TData) {
	go func() {
		defer func() {
			if re := recover(); re != nil {
				r.error(ctx, fmt.Sprintf("fetching kafka message for channel %s: %v", r.Channel, re))
			}
		}()
		<-ctx.Done()
		if r.Dispatch != nil {
			topics := r.Created(data)
			payload := r.ToResource(data)
			if payload == nil {
				return
			}
			if err := r.Dispatch(r.Channel, topics, payload); err != nil {
				r.error(ctx, fmt.Sprintf("OnCreate dispatch failed: %v - %s", err, reflect.TypeOf(*data)))
			}
			if err := r.BroadcastService.Broadcast([]Channel{r.Channel}, topics, payload); err != nil {
				r.error(ctx, fmt.Sprintf("OnCreate broadcast failed: %v - %s", err, reflect.TypeOf(*data)))
			}
		}
	}()
}

func (r *RepositoryImpl[TData, TResponse, TRequest, TID]) OnUpdated(ctx context.Context, data *TData) {
	go func() {
		defer func() {
			if re := recover(); re != nil {
				r.error(ctx, fmt.Sprintf("fetching kafka message for channel %s: %v", r.Channel, re))
			}
		}()
		<-ctx.Done()
		if r.Dispatch != nil {
			topics := r.Updated(data)
			payload := r.ToResource(data)
			if payload == nil {
				return
			}
			if err := r.Dispatch(r.Channel, topics, payload); err != nil {
				r.error(ctx, fmt.Sprintf("OnUpdate dispatch failed: %v - %s", err, reflect.TypeOf(*data)))
			}
			if err := r.BroadcastService.Broadcast([]Channel{r.Channel}, topics, payload); err != nil {
				r.error(ctx, fmt.Sprintf("OnUpdate broadcast failed: %v - %s", err, reflect.TypeOf(*data)))
			}
		}
	}()
}

func (r *RepositoryImpl[TData, TResponse, TRequest, TID]) OnDeleted(ctx context.Context, data *TData) {
	go func() {
		defer func() {
			if re := recover(); re != nil {
				r.error(ctx, fmt.Sprintf("fetching kafka message for channel %s: %v", r.Channel, re))
			}
		}()
		<-ctx.Done()
		if r.Dispatch != nil {
			topics := r.Deleted(data)
			payload := r.ToResource(data)
			if payload == nil {
				return
			}
			if err := r.Dispatch(r.Channel, topics, payload); err != nil {
				r.error(ctx, fmt.Sprintf("OnDelete dispatch failed: %v - %s", err, reflect.TypeOf(*data)))
			}
			if err := r.BroadcastService.Broadcast([]Channel{r.Channel}, topics, payload); err != nil {
				r.error(ctx, fmt.Sprintf("OnDelete broadcast failed: %v - %s", err, reflect.TypeOf(*data)))
			}
		}
	}()
}
