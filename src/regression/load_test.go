//go:build integration && load

// Opt-in on top of the "integration" tag, specifically: `-tags="integration load"`.
// A million-row-scale run takes real minutes and hammers the local Docker
// stack, so it must never run just because someone asked for the regular
// integration suite.
package regression

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/sonic"
	"github.com/segmentio/kafka-go"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/domains"
)

// loadScale reads CQRS_IT_LOAD_N (default 50,000). Set it to 1000000 for a
// literal million-row run: `CQRS_IT_LOAD_N=1000000 make test-load`.
func loadScale() int {
	n, err := strconv.Atoi(envOr("CQRS_IT_LOAD_N", "50000"))
	if err != nil || n <= 0 {
		return 50000
	}
	return n
}

// loadUpdateScale reads CQRS_IT_LOAD_UPDATE_N (default 5,000, capped to
// loadScale()). Kept separate and smaller by default because, unlike
// create/delete, there is no bulk update method on CQRSImpl — every update
// here is its own UpdateByID round trip, so this phase's cost scales very
// differently from the other two. That asymmetry is itself part of what
// this test is measuring.
func loadUpdateScale(n int) int {
	u, err := strconv.Atoi(envOr("CQRS_IT_LOAD_UPDATE_N", "5000"))
	if err != nil || u <= 0 {
		u = 5000
	}
	if u > n {
		return n
	}
	return u
}

func TestLoad_BulkCreateUpdateDeleteThroughput(t *testing.T) {
	n := loadScale()
	updateN := loadUpdateScale(n)
	batchSize := 2000
	if n < batchSize {
		batchSize = n
	}
	t.Logf("load scale: N=%d creates/deletes, %d updates, BatchSize=%d (override via CQRS_IT_LOAD_N / CQRS_IT_LOAD_UPDATE_N)", n, updateN, batchSize)

	h := newCDCHarnessITWithPool(t, "load", batchSize, 30) // this test runs alone, never under t.Parallel() — see updateWorkers below
	// Default FlushInterval is 5s. At this test's scale, the trailing
	// partial batch after each phase's full-size batches would otherwise
	// sit waiting on that ticker rather than the read db's actual apply
	// speed, making every "sync" measurement below cluster suspiciously
	// close to 5s regardless of N. Tightening it here so the numbers
	// reflect real throughput, not the flush cadence.
	h.c.FlushInterval = 250 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- h.c.Run(ctx) }()

	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("load%08d", i)
	}

	// --- Phase 1: bulk create ---------------------------------------
	writeStart := time.Now()
	bulkCreate(t, h, ids)
	writeDur := time.Since(writeStart)
	t.Logf("CREATE write:   %d rows in %v (%.0f rows/sec)", n, writeDur, float64(n)/writeDur.Seconds())

	publishStart := time.Now()
	bulkPublish(t, h, ids, domains.ChangeTypeCreated, func(id string) widget { return widget{ID: id, Name: "created"} })
	publishDur := time.Since(publishStart)
	t.Logf("CREATE publish: %d events in %v (%.0f msgs/sec)", n, publishDur, float64(n)/publishDur.Seconds())

	syncStart := time.Now()
	waitForCondition(t, loadTimeout(n), func() bool {
		count, err := h.read.db.NewSelect().Model((*widget)(nil)).Count(ctx)
		return err == nil && count == n
	}, fmt.Sprintf("expected all %d rows to sync to the read db", n))
	syncDur := time.Since(syncStart)
	t.Logf("CREATE sync:    %d rows applied to read db in %v (%.0f rows/sec)", n, syncDur, float64(n)/syncDur.Seconds())

	// --- Phase 2: update (no bulk API — one UpdateByID per row) -----
	updateIDs := ids[:updateN]
	const updateWorkers = 100

	updateWriteStart := time.Now()
	runWorkerPool(updateWorkers, updateIDs, func(id string) {
		if _, err := h.c.UpdateByID(ctx, id, widget{ID: id, Name: "updated"}); err != nil {
			t.Errorf("UpdateByID(%s) returned error: %v", id, err)
		}
	})
	updateWriteDur := time.Since(updateWriteStart)
	t.Logf("UPDATE write:   %d rows in %v (%.0f rows/sec, %d workers, single-row API only)", updateN, updateWriteDur, float64(updateN)/updateWriteDur.Seconds(), updateWorkers)

	updatePublishStart := time.Now()
	bulkPublish(t, h, updateIDs, domains.ChangeTypeUpdated, func(id string) widget { return widget{ID: id, Name: "updated"} })
	updatePublishDur := time.Since(updatePublishStart)
	t.Logf("UPDATE publish: %d events in %v (%.0f msgs/sec)", updateN, updatePublishDur, float64(updateN)/updatePublishDur.Seconds())

	updateSyncStart := time.Now()
	waitForCondition(t, loadTimeout(updateN), func() bool {
		count, err := h.read.db.NewSelect().Model((*widget)(nil)).Where("name = ?", "updated").Count(ctx)
		return err == nil && count == updateN
	}, fmt.Sprintf("expected all %d updates to sync to the read db", updateN))
	updateSyncDur := time.Since(updateSyncStart)
	t.Logf("UPDATE sync:    %d rows applied to read db in %v (%.0f rows/sec)", updateN, updateSyncDur, float64(updateN)/updateSyncDur.Seconds())

	// --- Phase 3: bulk delete ----------------------------------------
	deleteWriteStart := time.Now()
	bulkDelete(t, h, ids)
	deleteWriteDur := time.Since(deleteWriteStart)
	t.Logf("DELETE write:   %d rows in %v (%.0f rows/sec)", n, deleteWriteDur, float64(n)/deleteWriteDur.Seconds())

	deletePublishStart := time.Now()
	bulkPublish(t, h, ids, domains.ChangeTypeDeleted, func(id string) widget { return widget{ID: id} })
	deletePublishDur := time.Since(deletePublishStart)
	t.Logf("DELETE publish: %d events in %v (%.0f msgs/sec)", n, deletePublishDur, float64(n)/deletePublishDur.Seconds())

	deleteSyncStart := time.Now()
	waitForCondition(t, loadTimeout(n), func() bool {
		count, err := h.read.db.NewSelect().Model((*widget)(nil)).Count(ctx)
		return err == nil && count == 0
	}, "expected the read db to end empty after the delete phase")
	deleteSyncDur := time.Since(deleteSyncStart)
	t.Logf("DELETE sync:    %d rows removed from read db in %v (%.0f rows/sec)", n, deleteSyncDur, float64(n)/deleteSyncDur.Seconds())

	// Correctness, not just performance: both DBs must actually agree at
	// the end, at scale.
	writeCount, err := h.write.db.NewSelect().Model((*widget)(nil)).Count(ctx)
	if err != nil {
		t.Fatalf("counting write db: %v", err)
	}
	if writeCount != 0 {
		t.Errorf("expected write db to also end empty, got %d rows", writeCount)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("Run did not return after context cancellation")
	}
}

// loadTimeout scales the wait budget with N so this doesn't need constant
// tuning as CQRS_IT_LOAD_N changes.
func loadTimeout(n int) time.Duration {
	d := time.Duration(n/500+30) * time.Second
	if d > 20*time.Minute {
		d = 20 * time.Minute
	}
	return d
}

// runWorkerPool runs fn(item) for every item across a bounded number of
// concurrent workers, waiting for all of them to finish.
func runWorkerPool[T any](workers int, items []T, fn func(T)) {
	var wg sync.WaitGroup
	work := make(chan T)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for item := range work {
				fn(item)
			}
		}()
	}
	for _, item := range items {
		work <- item
	}
	close(work)
	wg.Wait()
}

// bulkCreate inserts every id via CreateMany in chunks, matching how a real
// bulk import would use this API (nobody does a million individual Create
// calls in practice).
func bulkCreate(t *testing.T, h *cdcHarnessIT, ids []string) {
	t.Helper()
	const chunk = 2000
	ctx := context.Background()
	for i := 0; i < len(ids); i += chunk {
		end := min(i+chunk, len(ids))
		batch := make([]widget, 0, end-i)
		for _, id := range ids[i:end] {
			batch = append(batch, widget{ID: id, Name: "created"})
		}
		if _, err := h.c.CreateMany(ctx, batch); err != nil {
			t.Fatalf("CreateMany chunk [%d:%d] returned error: %v", i, end, err)
		}
	}
}

// bulkDelete removes every id via DeleteMany in chunks.
func bulkDelete(t *testing.T, h *cdcHarnessIT, ids []string) {
	t.Helper()
	const chunk = 2000
	ctx := context.Background()
	for i := 0; i < len(ids); i += chunk {
		end := min(i+chunk, len(ids))
		if err := h.c.DeleteMany(ctx, ids[i:end]); err != nil {
			t.Fatalf("DeleteMany chunk [%d:%d] returned error: %v", i, end, err)
		}
	}
}

// bulkPublish marshals and publishes one CDC envelope per id, batched
// through a single kafka.Writer.WriteMessages call per chunk instead of
// one round trip per message.
func bulkPublish(t *testing.T, h *cdcHarnessIT, ids []string, changeType domains.ChangeType, build func(id string) widget) {
	t.Helper()
	ctx := context.Background()
	w := &kafka.Writer{
		Addr:                   kafka.TCP(h.broker.brokers...),
		Topic:                  h.topic,
		Balancer:               &kafka.LeastBytes{},
		AllowAutoTopicCreation: false,
		BatchSize:              500,
	}
	defer w.Close()

	const chunk = 2000
	for i := 0; i < len(ids); i += chunk {
		end := min(i+chunk, len(ids))
		msgs := make([]kafka.Message, 0, end-i)
		for _, id := range ids[i:end] {
			payload := build(id)
			envelope, err := marshalEnvelope(id, changeType, payload)
			if err != nil {
				t.Fatalf("marshalling envelope for %s: %v", id, err)
			}
			msgs = append(msgs, kafka.Message{Key: []byte(id), Value: envelope})
		}
		if err := writeWithRetry(ctx, w, msgs); err != nil {
			t.Fatalf("publishing chunk [%d:%d]: %v", i, end, err)
		}
	}
}

func marshalEnvelope(id string, changeType domains.ChangeType, payload widget) ([]byte, error) {
	return sonic.Marshal(domains.CQRSQueuePayload[widget]{
		EventID:    fmt.Sprintf("%s-%d", id, changeType),
		ChangeType: changeType,
		Payload:    payload,
	})
}

// writeWithRetry mirrors realKafkaBroker.Publish's retry: a Writer created
// right after a topic's own creation can briefly see stale "doesn't exist"
// metadata from kafka-go's shared Transport cache.
func writeWithRetry(ctx context.Context, w *kafka.Writer, msgs []kafka.Message) error {
	deadline := time.Now().Add(10 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		lastErr = w.WriteMessages(ctx, msgs...)
		if lastErr == nil || !strings.Contains(lastErr.Error(), "Unknown Topic Or Partition") {
			return lastErr
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("writing batch after retrying past metadata propagation: %w", lastErr)
}
