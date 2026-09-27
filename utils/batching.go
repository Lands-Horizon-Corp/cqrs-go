package utils

import (
	"context"
	"sync"
	"time"
)

type BatchHandler[T any] func(ctx context.Context, batch []T) error

type BatcherConfig[T any] struct {
	BatchSize     int
	FlushInterval time.Duration
	Handler       BatchHandler[T]
	BufferCap     int
	OnError       func(err error, batch []T)
}

type Batcher[T any] struct {
	cfg       BatcherConfig[T]
	ch        chan T
	wg        sync.WaitGroup
	closeOnce sync.Once
}

func NewBatcher[T any](cfg BatcherConfig[T]) *Batcher[T] {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 50 * time.Millisecond
	}
	if cfg.BufferCap <= 0 {
		cfg.BufferCap = cfg.BatchSize * 2
	}
	return &Batcher[T]{
		cfg: cfg,
		ch:  make(chan T, cfg.BufferCap),
	}
}

func (b *Batcher[T]) Start(ctx context.Context) {
	b.wg.Add(1)
	go b.worker(ctx)
}

func (b *Batcher[T]) Push(ctx context.Context, item T) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case b.ch <- item:
		return nil
	}
}

func (b *Batcher[T]) Stop() {
	b.closeOnce.Do(func() {
		close(b.ch)
		b.wg.Wait()
	})
}

func (b *Batcher[T]) worker(ctx context.Context) {
	defer b.wg.Done()
	buffer := make([]T, 0, b.cfg.BatchSize)
	ticker := time.NewTicker(b.cfg.FlushInterval)
	defer ticker.Stop()
	flush := func() {
		if len(buffer) == 0 {
			return
		}
		batchToProcess := make([]T, len(buffer))
		copy(batchToProcess, buffer)
		buffer = buffer[:0]
		if err := b.cfg.Handler(ctx, batchToProcess); err != nil {
			if b.cfg.OnError != nil {
				b.cfg.OnError(err, batchToProcess)
			}
		}
	}
	for {
		select {
		case item, ok := <-b.ch:
			if !ok {
				flush()
				return
			}
			buffer = append(buffer, item)
			if len(buffer) >= b.cfg.BatchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-ctx.Done():
			for {
				select {
				case item, ok := <-b.ch:
					if !ok {
						flush()
						return
					}
					buffer = append(buffer, item)
					if len(buffer) >= b.cfg.BatchSize {
						flush()
					}
				default:
					flush()
					return
				}
			}
		}
	}
}
