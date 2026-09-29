package cqrs

import (
	"context"
	"fmt"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/domains"
)

func (c *CQRSImpl[TData, TResponse, TRequest, TID]) OnCreated(ctx context.Context, data *TData) {
	c.handleEvent(ctx, domains.ChangeTypeCreated, data, c.Created)
}

func (c *CQRSImpl[TData, TResponse, TRequest, TID]) OnUpdated(ctx context.Context, data *TData) {
	c.handleEvent(ctx, domains.ChangeTypeUpdated, data, c.Updated)
}

func (c *CQRSImpl[TData, TResponse, TRequest, TID]) OnDeleted(ctx context.Context, data *TData) {
	c.handleEvent(ctx, domains.ChangeTypeDeleted, data, c.Deleted)
}

func (c *CQRSImpl[TData, TResponse, TRequest, TID]) handleEvent(
	ctx context.Context,
	eventType domains.ChangeType,
	data *TData,
	getEvents func(*TData) domains.Events,
) {
	if data == nil || c.ToResource == nil {
		return
	}
	asyncCtx := context.WithoutCancel(ctx)
	go func(ctx context.Context, data *TData) {
		defer func() {
			if re := recover(); re != nil {
				c.error(ctx, fmt.Sprintf("%d panic on channel %s: %v", eventType, c.Channel, re))
			}
		}()
		payload := c.ToResource(data)
		if payload == nil {
			return
		}
		var events domains.Events
		if getEvents != nil {
			events = getEvents(data)
		}
		if len(events) == 0 {
			return
		}
		if c.Dispatch != nil {
			if err := c.Dispatch(c.Channel, events, payload); err != nil {
				c.error(ctx, fmt.Sprintf("%d dispatch failed [channel: %s]: %v (type: %T)", eventType, c.Channel, err, data))
			}
		}
		if c.BroadcastService != nil {
			if err := c.BroadcastService.Broadcast([]domains.Channel{c.Channel}, events, payload); err != nil {
				c.error(ctx, fmt.Sprintf("%d broadcast failed [channel: %s]: %v (type: %T)", eventType, c.Channel, err, data))
			}
		}
	}(asyncCtx, data)
}
