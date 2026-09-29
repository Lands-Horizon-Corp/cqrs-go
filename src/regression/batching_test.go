package regression

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/utils"
)

// --- Happy Path Tests ---

// TestBatcher_Defaults_BatchSizeIsOneHundred verifies the default BatchSize
// behaviorally (pushing exactly 100 items flushes immediately, without
// waiting on the ticker) rather than by reading the unexported cfg field —
// this package only touches Batcher's public API.
func TestBatcher_Defaults_BatchSizeIsOneHundred(t *testing.T) {
	var (
		mu       sync.Mutex
		received [][]int
	)
	handler := func(_ context.Context, batch []int) error {
		mu.Lock()
		defer mu.Unlock()
		received = append(received, batch)
		return nil
	}

	batcher := utils.NewBatcher(utils.BatcherConfig[int]{
		FlushInterval: time.Hour, // long enough that only size-based flushing can fire
		Handler:       handler,
	})
	ctx := context.Background()
	batcher.Start(ctx)
	defer batcher.Stop()

	for i := range 100 {
		if err := batcher.Push(ctx, i); err != nil {
			t.Fatalf("unexpected push error: %v", err)
		}
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(received)
		mu.Unlock()
		if n == 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(received) != 1 || len(received[0]) != 100 {
		t.Fatalf("expected exactly one flushed batch of 100 items, got %d batches: %v", len(received), received)
	}
}

// TestBatcher_Defaults_FlushIntervalIsFiftyMilliseconds verifies the
// default FlushInterval behaviorally: a single pushed item (far below the
// default BatchSize) must still get flushed once the default interval
// elapses.
func TestBatcher_Defaults_FlushIntervalIsFiftyMilliseconds(t *testing.T) {
	var (
		mu       sync.Mutex
		received [][]int
	)
	handler := func(_ context.Context, batch []int) error {
		mu.Lock()
		defer mu.Unlock()
		received = append(received, batch)
		return nil
	}

	batcher := utils.NewBatcher(utils.BatcherConfig[int]{Handler: handler}) // defaults: BatchSize=100, FlushInterval=50ms
	ctx := context.Background()
	batcher.Start(ctx)
	defer batcher.Stop()

	_ = batcher.Push(ctx, 1)

	time.Sleep(80 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if !reflect.DeepEqual(received, [][]int{{1}}) {
		t.Errorf("expected a single-item batch flushed by the default ticker, got %v", received)
	}
}

func TestBatcher_HappyPath_BatchSizeAndStop(t *testing.T) {
	var (
		mu       sync.Mutex
		received [][]int
	)

	handler := func(ctx context.Context, batch []int) error {
		mu.Lock()
		defer mu.Unlock()
		received = append(received, batch)
		return nil
	}

	cfg := utils.BatcherConfig[int]{
		BatchSize:     3,
		FlushInterval: 1 * time.Second,
		Handler:       handler,
	}

	batcher := utils.NewBatcher(cfg)
	ctx := context.Background()
	batcher.Start(ctx)

	for _, item := range []int{10, 20, 30, 40, 50} {
		if err := batcher.Push(ctx, item); err != nil {
			t.Fatalf("unexpected push error: %v", err)
		}
	}

	batcher.Stop()

	mu.Lock()
	defer mu.Unlock()

	expected := [][]int{
		{10, 20, 30},
		{40, 50},
	}

	if !reflect.DeepEqual(received, expected) {
		t.Errorf("got batches %v, expected %v", received, expected)
	}
}

func TestBatcher_HappyPath_TickerFlush(t *testing.T) {
	var (
		mu       sync.Mutex
		received [][]int
	)

	handler := func(ctx context.Context, batch []int) error {
		mu.Lock()
		defer mu.Unlock()
		received = append(received, batch)
		return nil
	}

	cfg := utils.BatcherConfig[int]{
		BatchSize:     10,
		FlushInterval: 30 * time.Millisecond,
		Handler:       handler,
	}

	batcher := utils.NewBatcher(cfg)
	ctx := context.Background()
	batcher.Start(ctx)
	defer batcher.Stop()

	_ = batcher.Push(ctx, 1)
	_ = batcher.Push(ctx, 2)

	time.Sleep(80 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	expected := [][]int{{1, 2}}
	if !reflect.DeepEqual(received, expected) {
		t.Errorf("got batches %v, expected %v", received, expected)
	}
}

func TestBatcher_HappyPath_ConcurrentPushes(t *testing.T) {
	var totalProcessed int64

	handler := func(ctx context.Context, batch []int) error {
		atomic.AddInt64(&totalProcessed, int64(len(batch)))
		return nil
	}

	cfg := utils.BatcherConfig[int]{
		BatchSize:     10,
		FlushInterval: 20 * time.Millisecond,
		Handler:       handler,
	}

	batcher := utils.NewBatcher(cfg)
	ctx := context.Background()
	batcher.Start(ctx)

	numGoroutines := 10
	itemsPerGoroutine := 20
	var wg sync.WaitGroup

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for j := 0; j < itemsPerGoroutine; j++ {
				_ = batcher.Push(ctx, base+j)
			}
		}(i * itemsPerGoroutine)
	}

	wg.Wait()
	batcher.Stop()

	expected := int64(numGoroutines * itemsPerGoroutine)
	if atomic.LoadInt64(&totalProcessed) != expected {
		t.Errorf("expected total processed %d, got %d", expected, totalProcessed)
	}
}

func TestBatcher_SadPath_HandlerErrorTriggersOnError(t *testing.T) {
	var (
		mu          sync.Mutex
		capturedErr error
		failedBatch []string
	)

	errHandler := errors.New("handler database connection lost")
	handler := func(ctx context.Context, batch []string) error {
		return errHandler
	}

	onError := func(err error, batch []string) {
		mu.Lock()
		defer mu.Unlock()
		capturedErr = err
		failedBatch = batch
	}

	cfg := utils.BatcherConfig[string]{
		BatchSize:     2,
		FlushInterval: 1 * time.Second,
		Handler:       handler,
		OnError:       onError,
	}

	batcher := utils.NewBatcher(cfg)
	ctx := context.Background()
	batcher.Start(ctx)

	_ = batcher.Push(ctx, "record-1")
	_ = batcher.Push(ctx, "record-2")

	batcher.Stop()

	mu.Lock()
	defer mu.Unlock()

	if !errors.Is(capturedErr, errHandler) {
		t.Errorf("expected error %v, got %v", errHandler, capturedErr)
	}
	expectedBatch := []string{"record-1", "record-2"}
	if !reflect.DeepEqual(failedBatch, expectedBatch) {
		t.Errorf("expected failed batch %v, got %v", expectedBatch, failedBatch)
	}
}

func TestBatcher_SadPath_PushCancelledContext(t *testing.T) {
	cfg := utils.BatcherConfig[int]{
		BatchSize:     1,
		BufferCap:     1,
		FlushInterval: 1 * time.Second,
		Handler:       func(ctx context.Context, batch []int) error { return nil },
	}

	batcher := utils.NewBatcher(cfg)

	ctx := context.Background()
	_ = batcher.Push(ctx, 100)

	cancelCtx, cancel := context.WithCancel(context.Background())
	cancel()

	err := batcher.Push(cancelCtx, 200)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled error, got %v", err)
	}
}

// TestBatcher_SadPath_WorkerContextCancelled asserts the drain-on-cancel
// behavior by polling the observed output with a timeout, rather than
// reaching into the unexported wg field to know when the worker exited.
func TestBatcher_SadPath_WorkerContextCancelled(t *testing.T) {
	var (
		mu       sync.Mutex
		received []int
	)

	handler := func(ctx context.Context, batch []int) error {
		mu.Lock()
		defer mu.Unlock()
		received = append(received, batch...)
		return nil
	}

	cfg := utils.BatcherConfig[int]{
		BatchSize:     100, // high enough to prevent a full-batch flush
		FlushInterval: 5 * time.Second,
		Handler:       handler,
	}

	batcher := utils.NewBatcher(cfg)
	ctx, cancel := context.WithCancel(context.Background())

	batcher.Start(ctx)

	_ = batcher.Push(ctx, 1)
	_ = batcher.Push(ctx, 2)

	cancel()

	expected := []int{1, 2}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		match := reflect.DeepEqual(received, expected)
		mu.Unlock()
		if match {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	t.Errorf("expected drained batch %v on context cancellation, got %v", expected, received)
}

type Message struct {
	ID       int
	IsPoison bool
}

func TestBatcher_PoisonPill_IsolationAndRecovery(t *testing.T) {
	var (
		mu             sync.Mutex
		successBatches [][]Message
		failedBatches  [][]Message
	)

	handler := func(ctx context.Context, batch []Message) error {
		for _, msg := range batch {
			if msg.IsPoison {
				return errors.New("poison pill detected")
			}
		}
		mu.Lock()
		defer mu.Unlock()
		successBatches = append(successBatches, batch)
		return nil
	}

	onError := func(err error, batch []Message) {
		mu.Lock()
		defer mu.Unlock()
		failedBatches = append(failedBatches, batch)
	}

	cfg := utils.BatcherConfig[Message]{
		BatchSize:     2,
		FlushInterval: 1 * time.Second,
		Handler:       handler,
		OnError:       onError,
	}

	batcher := utils.NewBatcher(cfg)
	ctx := context.Background()
	batcher.Start(ctx)

	_ = batcher.Push(ctx, Message{ID: 1})
	_ = batcher.Push(ctx, Message{ID: 2})

	_ = batcher.Push(ctx, Message{ID: 3})
	_ = batcher.Push(ctx, Message{ID: 4, IsPoison: true})

	_ = batcher.Push(ctx, Message{ID: 5})
	_ = batcher.Push(ctx, Message{ID: 6})

	batcher.Stop()

	mu.Lock()
	defer mu.Unlock()

	if len(successBatches) != 2 {
		t.Fatalf("expected 2 successful batches, got %d", len(successBatches))
	}
	if successBatches[0][0].ID != 1 || successBatches[1][0].ID != 5 {
		t.Errorf("unexpected successful batch items: %v", successBatches)
	}

	// 1 batch failed (Batch 2)
	if len(failedBatches) != 1 {
		t.Fatalf("expected 1 failed batch, got %d", len(failedBatches))
	}
	if failedBatches[0][1].ID != 4 {
		t.Errorf("expected failed batch to contain poison message 4, got %v", failedBatches[0])
	}
}
