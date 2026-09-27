package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/Lands-Horizon-Corp/cqrs-go/utils"
	"github.com/bytedance/sonic"
	"github.com/uptrace/bun"
)

func (r *RepositoryImpl[TData, TResponse, TRequest, TID]) Run(ctx context.Context) error {
	return r.RunBatch(ctx, 500, 20*time.Millisecond)
}

func (r *RepositoryImpl[TData, TResponse, TRequest, TID]) RunBatch(
	ctx context.Context,
	batchSize int,
	flushInterval time.Duration,
) error {
	if r.MessageBrokerService == nil {
		return errors.New("message broker service is not initialized")
	}

	r.info(ctx, fmt.Sprintf("starting outbox batch runner for channel: %s", r.Channel))

	batcher := utils.NewBatcher(utils.BatcherConfig[CQRSQueuePayload[TData]]{
		BatchSize:     batchSize,
		FlushInterval: flushInterval,
		Handler: func(batchCtx context.Context, batch []CQRSQueuePayload[TData]) error {
			return r.processBatch(batchCtx, batch)
		},
		OnError: func(err error, batch []CQRSQueuePayload[TData]) {
			r.error(ctx, fmt.Sprintf("processing outbox batch for channel %s failed: %v", r.Channel, err))
		},
	})

	batcher.Start(ctx)
	defer batcher.Stop()

	return r.MessageBrokerService.Subscribe(ctx, string(r.Channel), func(key, value []byte) error {
		var env CQRSQueuePayload[TData]
		if err := sonic.Unmarshal(value, &env); err != nil {
			r.error(ctx, fmt.Sprintf("unmarshaling message payload for channel %s: %v", r.Channel, err))
			return nil
		}

		if env.EventID == "" {
			if len(key) > 0 {
				env.EventID = string(key)
			} else {
				env.EventID = fmt.Sprintf("%s-%d", r.Channel, time.Now().UnixNano())
			}
		}

		return batcher.Push(ctx, env)
	})
}

func (r *RepositoryImpl[TData, TResponse, TRequest, TID]) processBatch(
	ctx context.Context,
	batch []CQRSQueuePayload[TData],
) error {
	if len(batch) == 0 {
		return nil
	}
	appliedMessages, err := r.syncBatchToReadDB(ctx, batch)
	if err != nil {
		return fmt.Errorf("synchronizing batch to read db: %w", err)
	}
	for i := range appliedMessages {
		msg := &appliedMessages[i]
		switch msg.EventType {
		case EventTypeCreated:
			r.OnCreated(ctx, &msg.Payload)
		case EventTypeUpdated:
			r.OnUpdated(ctx, &msg.Payload)
		case EventTypeDeleted:
			r.OnDeleted(ctx, &msg.Payload)
		default:
			r.handleEvent(ctx, msg.EventType, &msg.Payload, nil)
		}
	}
	return nil
}

func (r *RepositoryImpl[TData, TResponse, TRequest, TID]) syncBatchToReadDB(
	ctx context.Context,
	batch []CQRSQueuePayload[TData],
) ([]CQRSQueuePayload[TData], error) {
	if r.ReadDB == nil {
		return nil, errors.New("read db is not initialized")
	}
	eventIDsPtr := r.stringSlicePool.Get()
	defer r.stringSlicePool.Put(eventIDsPtr)

	seenInBatch := r.stringSetPool.Get()
	defer r.stringSetPool.Put(seenInBatch)

	existingIDsPtr := r.stringSlicePool.Get()
	defer r.stringSlicePool.Put(existingIDsPtr)

	existingMap := r.stringSetPool.Get()
	defer r.stringSetPool.Put(existingMap)

	eventsToInsertPtr := r.processedEventsPool.Get()
	defer r.processedEventsPool.Put(eventsToInsertPtr)

	eventIDs := *eventIDsPtr
	uniqueBatch := make([]CQRSQueuePayload[TData], 0, len(batch))

	for _, msg := range batch {
		if !seenInBatch[msg.EventID] {
			seenInBatch[msg.EventID] = true
			eventIDs = append(eventIDs, msg.EventID)
			uniqueBatch = append(uniqueBatch, msg)
		}
	}
	*eventIDsPtr = eventIDs

	if len(uniqueBatch) == 0 {
		return nil, nil
	}

	err := r.ReadDB.NewSelect().
		Model((*ProcessedEvent)(nil)).
		Column("event_id").
		Where("event_id IN (?)", bun.In(*eventIDsPtr)).
		Scan(ctx, existingIDsPtr)
	if err != nil {
		return nil, fmt.Errorf("querying existing event ids: %w", err)
	}

	for _, id := range *existingIDsPtr {
		existingMap[id] = true
	}
	newMessages := make([]CQRSQueuePayload[TData], 0, len(uniqueBatch))
	eventsToInsert := *eventsToInsertPtr
	now := time.Now()

	for _, msg := range uniqueBatch {
		if !existingMap[msg.EventID] {
			newMessages = append(newMessages, msg)
			eventsToInsert = append(eventsToInsert, ProcessedEvent{
				EventID:   msg.EventID,
				Channel:   string(r.Channel),
				CreatedAt: now,
			})
		}
	}
	*eventsToInsertPtr = eventsToInsert
	if len(newMessages) == 0 {
		return nil, nil
	}
	latestEntityState := make(map[string]CQRSQueuePayload[TData])
	entityOrder := make([]string, 0, len(newMessages))
	for _, msg := range newMessages {
		var idStr string
		if r.Tabular != nil {
			tabularData := r.Tabular(&msg.Payload)
			if val, ok := tabularData[r.ColumnDefaultID]; ok {
				idStr = fmt.Sprintf("%v", val)
			}
		}
		if idStr != "" {
			if _, exists := latestEntityState[idStr]; !exists {
				entityOrder = append(entityOrder, idStr)
			}
			latestEntityState[idStr] = msg
		}
	}
	upsertEntities := make([]TData, 0, len(latestEntityState))
	deleteEntities := make([]TData, 0, len(latestEntityState))

	if len(latestEntityState) > 0 {
		for _, id := range entityOrder {
			msg := latestEntityState[id]
			switch msg.EventType {
			case EventTypeCreated, EventTypeUpdated:
				upsertEntities = append(upsertEntities, msg.Payload)
			case EventTypeDeleted:
				deleteEntities = append(deleteEntities, msg.Payload)
			}
		}
	} else {
		for _, msg := range newMessages {
			switch msg.EventType {
			case EventTypeCreated, EventTypeUpdated:
				upsertEntities = append(upsertEntities, msg.Payload)
			case EventTypeDeleted:
				deleteEntities = append(deleteEntities, msg.Payload)
			}
		}
	}
	err = r.ReadDB.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
		_, err := tx.NewInsert().
			Model(eventsToInsertPtr).
			Ignore().
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("bulk inserting processed events: %w", err)
		}
		if len(upsertEntities) > 0 {
			_, err = tx.NewInsert().
				Model(&upsertEntities).
				On(fmt.Sprintf("CONFLICT (%s) DO UPDATE", r.ColumnDefaultID)).
				Exec(ctx)
			if err != nil {
				return fmt.Errorf("bulk upserting entities to read db: %w", err)
			}
		}
		if len(deleteEntities) > 0 {
			_, err = tx.NewDelete().
				Model(&deleteEntities).
				WherePK().
				Exec(ctx)
			if err != nil {
				return fmt.Errorf("bulk deleting entities from read db: %w", err)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return newMessages, nil
}
