package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/uptrace/bun"
)

func (r *RepositoryImpl[TData, TResponse, TRequest, TID]) Run(ctx context.Context) error {
	if r.MessageBrokerService == nil {
		return errors.New("message broker service is not initialized")
	}
	r.info(ctx, fmt.Sprintf("starting outbox runner for channel: %s", r.Channel))
	return r.MessageBrokerService.Subscribe(ctx, string(r.Channel), func(key, value []byte) error {
		if err := r.processOutboxMessage(ctx, key, value); err != nil {
			r.error(ctx, fmt.Sprintf("processing outbox message for channel %s: %v", r.Channel, err))
			return err
		}
		return nil
	})
}

func (r *RepositoryImpl[TData, TResponse, TRequest, TID]) processOutboxMessage(
	ctx context.Context,
	key []byte,
	value []byte,
) error {
	var env struct {
		EventID   string    `json:"event_id"`
		EventType EventType `json:"event_type"`
		Payload   TData     `json:"payload"`
	}
	if err := json.Unmarshal(value, &env); err != nil {
		return fmt.Errorf("unmarshaling message payload: %w", err)
	}
	eventID := env.EventID
	if eventID == "" {
		if len(key) > 0 {
			eventID = string(key)
		} else {
			eventID = fmt.Sprintf("%s-%d", r.Channel, time.Now().UnixNano())
		}
	}
	if err := r.syncToReadDB(ctx, eventID, env.EventType, &env.Payload); err != nil {
		return fmt.Errorf("synchronizing to read db: %w", err)
	}
	var events Events
	switch env.EventType {
	case EventTypeCreated:
		if r.Created != nil {
			events = r.Created(&env.Payload)
		}
	case EventTypeUpdated:
		if r.Updated != nil {
			events = r.Updated(&env.Payload)
		}
	case EventTypeDeleted:
		if r.Deleted != nil {
			events = r.Deleted(&env.Payload)
		}
	default:
		events = Events{fmt.Sprintf("%d", env.EventType)}
	}
	if r.ToResource == nil {
		return nil
	}
	response := r.ToResource(&env.Payload)
	if response == nil {
		return nil
	}
	if r.Dispatch != nil {
		if err := r.Dispatch(r.Channel, events, response); err != nil {
			r.error(ctx, fmt.Sprintf("dispatch failed for channel %s: %v", r.Channel, err))
		}
	}
	if r.BroadcastService != nil {
		if err := r.BroadcastService.Broadcast([]Channel{r.Channel}, events, response); err != nil {
			r.error(ctx, fmt.Sprintf("broadcast failed for channel %s: %v", r.Channel, err))
		}
	}
	return nil
}
func (r *RepositoryImpl[TData, TResponse, TRequest, TID]) syncToReadDB(
	ctx context.Context,
	eventID string,
	eventType EventType,
	data *TData,
) error {
	if r.ReadDB == nil {
		return errors.New("read db is not initialized")
	}
	return r.ReadDB.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
		res, err := tx.NewInsert().
			Model(&ProcessedEvent{
				EventID:   eventID,
				Channel:   string(r.Channel),
				CreatedAt: time.Now(),
			}).
			Ignore().
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("checking event idempotency: %w", err)
		}
		rows, err := res.RowsAffected()
		if err != nil {
			return fmt.Errorf("reading idempotency rows affected: %w", err)
		}
		if rows == 0 {
			return nil
		}
		switch eventType {
		case EventTypeCreated, EventTypeUpdated:
			_, err = tx.NewInsert().
				Model(data).
				On("CONFLICT (" + r.ColumnDefaultID + ") DO UPDATE").
				Exec(ctx)
			if err != nil {
				return fmt.Errorf("upserting entity to read db: %w", err)
			}
		case EventTypeDeleted:
			_, err = tx.NewDelete().
				Model(data).
				WherePK().
				Exec(ctx)
			if err != nil {
				return fmt.Errorf("deleting entity from read db: %w", err)
			}
		}
		return nil
	})
}
