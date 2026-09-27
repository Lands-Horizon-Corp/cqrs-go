package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/segmentio/kafka-go"
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

	var data TData
	if err := json.Unmarshal(envelope.Payload, &data); err != nil {
		return fmt.Errorf("unmarshaling payload entity: %w", err)
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

	if r.Dispatch != nil && response != nil {
		if err := r.Dispatch(r.Channel, events, response); err != nil {
			return fmt.Errorf("dispatching event: %w", err)
		}
	}

	return nil
}
