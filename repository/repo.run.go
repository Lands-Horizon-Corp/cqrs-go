package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/uptrace/bun"
)

func (r *RepositoryImpl[TData, TResponse, TRequest, TID]) Run(ctx context.Context) error {
	if r.Reader == nil {
		return errors.New("kafka reader is not initialized")
	}
	r.info(ctx, fmt.Sprintf("starting outbox runner for channel: %s", r.Channel))
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			msg, err := r.Reader.FetchMessage(ctx)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return nil
				}
				r.error(ctx, fmt.Sprintf("fetching kafka message for channel %s: %v", r.Channel, err))
				continue
			}
			if err := r.processOutboxMessage(ctx, msg); err != nil {
				r.error(ctx, fmt.Sprintf("processing outbox message at offset %d for channel %s: %v", msg.Offset, r.Channel, err))
				continue
			}
			if err := r.Reader.CommitMessages(ctx, msg); err != nil {
				r.error(ctx, fmt.Sprintf("committing offset %d for channel %s: %v", msg.Offset, r.Channel, err))
				continue
			}
		}
	}
}

func (r *RepositoryImpl[TData, TResponse, TRequest, TID]) processOutboxMessage(
	ctx context.Context,
	msg kafka.Message,
) error {
	var envelope CQRSQueuePayload
	if err := json.Unmarshal(msg.Value, &envelope); err != nil {
		return fmt.Errorf("unmarshaling debezium envelope: %w", err)
	}
	eventID := envelope.EventID
	if eventID == "" {
		eventID = fmt.Sprintf("%s-%d-%d", msg.Topic, msg.Partition, msg.Offset)
	}

	var data TData
	if err := json.Unmarshal(envelope.Payload, &data); err != nil {
		return fmt.Errorf("unmarshaling payload entity: %w", err)
	}
	if err := r.syncToReadDB(ctx, eventID, envelope.EventType, &data); err != nil {
		return fmt.Errorf("synchronizing to read db: %w", err)
	}
	var events Events
	switch envelope.EventType {
	case EventTypeCreated:
		if r.Created != nil {
			events = r.Created(&data)
		}
	case EventTypeUpdated:
		if r.Updated != nil {
			events = r.Updated(&data)
		}
	case EventTypeDeleted:
		if r.Deleted != nil {
			events = r.Deleted(&data)
		}
	default:
		events = Events{fmt.Sprintf("%d", envelope.EventType)}
	}
	var response *TResponse
	if r.ToResource != nil {
		response = r.ToResource(&data)
	}
	if response != nil {
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
		processedEvent := &ProcessedEvent{
			EventID:   eventID,
			Channel:   string(r.Channel),
			CreatedAt: time.Now(),
		}
		res, err := tx.NewInsert().
			Model(processedEvent).
			Ignore().
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("checking event idempotency: %w", err)
		}
		rowsAffected, err := res.RowsAffected()
		if err != nil {
			return fmt.Errorf("reading idempotency rows affected: %w", err)
		}
		if rowsAffected == 0 {
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
				Where(r.ColumnDefaultID+" = ?", r.getPrimaryKeyValue(data)).
				Exec(ctx)
			if err != nil {
				return fmt.Errorf("deleting entity from read db: %w", err)
			}
		}
		return nil
	})
}

// Helper to extract primary key value via reflection or fallback
func (r *RepositoryImpl[TData, TResponse, TRequest, TID]) getPrimaryKeyValue(data *TData) any {
	if r.Tabular != nil {
		m := r.Tabular(data)
		if id, ok := m[r.ColumnDefaultID]; ok {
			return id
		}
	}
	return data
}
