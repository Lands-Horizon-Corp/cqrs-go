//go:build integration

package regression

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/sonic"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/cqrs"
	"github.com/Lands-Horizon-Corp/cqrs-go/src/domains"
)

// waitForCondition polls cond until it returns true or the timeout elapses,
// failing the test on timeout. Used throughout instead of a single
// waitForCall, since concurrent load means events can land in any order
// and possibly across several flushed batches.
func waitForCondition(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !cond() {
		t.Fatal(msg)
	}
}

// publishEnvelope is a small helper shared by every test in this file:
// marshal + publish a CDC envelope for one widget change.
func publishEnvelope(t *testing.T, broker *realKafkaBroker, ctx context.Context, topic, eventID string, changeType domains.ChangeType, w widget) {
	t.Helper()
	envelope, err := sonic.Marshal(domains.CQRSQueuePayload[widget]{EventID: eventID, ChangeType: changeType, Payload: w})
	if err != nil {
		t.Fatalf("marshalling envelope: %v", err)
	}
	if err := broker.Publish(ctx, topic, []byte(w.ID), envelope); err != nil {
		t.Fatalf("publishing envelope: %v", err)
	}
}

// TestIntegration_HappyPath_ConcurrentCreatesAcrossManyEntities simulates N
// app instances behind a load balancer all writing distinct new rows to the
// same write DB at once, each independently publishing its own CDC event.
// Every one of them must land correctly and distinctly in the read DB, with
// no data races (run this file with -race) and no lost/duplicated
// broadcasts.
func TestIntegration_HappyPath_ConcurrentCreatesAcrossManyEntities(t *testing.T) {
	t.Parallel()
	const n = 25
	h := newCDCHarnessIT(t, "concurrent-creates", n)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- h.c.Run(ctx) }()

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			w := widget{ID: fmt.Sprintf("w%02d", i), Name: fmt.Sprintf("gadget-%d", i), Active: i%2 == 0}
			if _, err := h.c.CreateFormat(ctx, w); err != nil {
				t.Errorf("Create(%s) returned error: %v", w.ID, err)
				return
			}
			publishEnvelope(t, h.broker, ctx, h.topic, "evt-"+w.ID, domains.ChangeTypeCreated, w)
		}(i)
	}
	wg.Wait()

	waitForCondition(t, 30*time.Second, func() bool {
		count, err := h.read.db.NewSelect().Model((*widget)(nil)).Count(ctx)
		return err == nil && count == n
	}, fmt.Sprintf("expected all %d widgets to land in the read db", n))

	for i := 0; i < n; i++ {
		id := fmt.Sprintf("w%02d", i)
		writeRow, wok := readWidgetFrom(t, h.write, id)
		readRow, rok := readWidgetFrom(t, h.read, id)
		if !wok || !rok {
			t.Errorf("widget %s missing (write found=%v, read found=%v)", id, wok, rok)
			continue
		}
		if writeRow.Name != readRow.Name || writeRow.Active != readRow.Active {
			t.Errorf("widget %s write/read mismatch: write=%+v read=%+v", id, writeRow, readRow)
		}
	}

	waitForCondition(t, 30*time.Second, func() bool {
		return len(h.broadcast.snapshot()) == n
	}, fmt.Sprintf("expected exactly %d broadcasts, got %d", n, len(h.broadcast.snapshot())))

	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after context cancellation")
	}
}

// TestIntegration_HappyPath_ConcurrentCreateRaceOnSameID is the sharpest
// "load balancer" case: several goroutines race to Create the exact same
// entity ID at once, as if two instances both handled a duplicate retry of
// the same request. Postgres's primary key constraint guarantees exactly
// one wins; this asserts that invariant holds under real concurrent load
// and that only the winner's change ever reaches the read DB.
func TestIntegration_HappyPath_ConcurrentCreateRaceOnSameID(t *testing.T) {
	t.Parallel()
	const racers = 8
	h := newCDCHarnessIT(t, "create-race", 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- h.c.Run(ctx) }()

	var succeeded int32
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start // line everyone up to maximize actual collision
			w := widget{ID: "race1", Name: fmt.Sprintf("attempt-%d", i)}
			if _, err := h.c.CreateFormat(ctx, w); err == nil {
				atomic.AddInt32(&succeeded, 1)
			}
		}(i)
	}
	close(start)
	wg.Wait()

	if got := atomic.LoadInt32(&succeeded); got != 1 {
		t.Fatalf("expected exactly 1 Create to win the race, got %d", got)
	}

	writeRow, ok := readWidgetFrom(t, h.write, "race1")
	if !ok {
		t.Fatal("expected the winning row to exist in write db")
	}
	publishEnvelope(t, h.broker, ctx, h.topic, "evt-race1", domains.ChangeTypeCreated, writeRow)

	waitForCondition(t, 15*time.Second, func() bool {
		readRow, ok := readWidgetFrom(t, h.read, "race1")
		return ok && readRow.Name == writeRow.Name
	}, "expected the read db to converge on the winning write's state")

	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after context cancellation")
	}
}

// TestIntegration_HappyPath_ConcurrentUpdatesConvergeToActualFinalState
// races several concurrent UpdateByID calls against the same existing row
// (no constraint to pick a single winner here — every update succeeds at
// the DB level, and whichever one Postgres actually committed last wins).
// Rather than guess which one that is, the test reads the row back after
// the race and publishes an envelope matching that real state — exactly
// what real Debezium would have captured — then asserts the read DB
// converges to it too.
func TestIntegration_HappyPath_ConcurrentUpdatesConvergeToActualFinalState(t *testing.T) {
	t.Parallel()
	const racers = 8
	h := newCDCHarnessIT(t, "update-race", 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- h.c.Run(ctx) }()

	seed := widget{ID: "w1", Name: "seed", Priority: new(0)}
	if _, err := h.c.CreateFormat(ctx, seed); err != nil {
		t.Fatalf("seed Create returned error: %v", err)
	}

	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, _ = h.c.UpdateByIDFormat(ctx, "w1", widget{ID: "w1", Name: fmt.Sprintf("racer-%d", i), Priority: new(i)})
		}(i)
	}
	close(start)
	wg.Wait()

	writeRow, ok := readWidgetFrom(t, h.write, "w1")
	if !ok {
		t.Fatal("expected the row to still exist in write db")
	}
	publishEnvelope(t, h.broker, ctx, h.topic, "evt-final", domains.ChangeTypeUpdated, writeRow)

	waitForCondition(t, 15*time.Second, func() bool {
		readRow, ok := readWidgetFrom(t, h.read, "w1")
		return ok && readRow.Name == writeRow.Name &&
			(readRow.Priority == nil) == (writeRow.Priority == nil) &&
			(readRow.Priority == nil || *readRow.Priority == *writeRow.Priority)
	}, "expected the read db to converge on the write db's actual final state")

	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after context cancellation")
	}
}

// TestIntegration_PoisonPill_FailedWritesNeverReachReadDB mixes deliberately
// failing writes (validation errors, duplicate PKs) into a burst of
// concurrent successful ones across many entities. Only writes that
// actually succeeded get a CDC envelope published — mirroring reality,
// since a rolled-back write never appears in Postgres's WAL, so real
// Debezium could never produce an event for it either.
func TestIntegration_PoisonPill_FailedWritesNeverReachReadDB(t *testing.T) {
	t.Parallel()
	const good = 15
	h := newCDCHarnessIT(t, "poison-concurrent", good)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- h.c.Run(ctx) }()

	var wg sync.WaitGroup

	// Good writes: distinct valid IDs.
	for i := 0; i < good; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			w := widget{ID: fmt.Sprintf("ok%02d", i), Name: fmt.Sprintf("gadget-%d", i)}
			if _, err := h.c.CreateFormat(ctx, w); err != nil {
				t.Errorf("Create(%s) unexpectedly failed: %v", w.ID, err)
				return
			}
			publishEnvelope(t, h.broker, ctx, h.topic, "evt-"+w.ID, domains.ChangeTypeCreated, w)
		}(i)
	}

	// Bad writes: empty Name fails validation before ever reaching the DB
	// or Kafka. No envelope is published for these, by design.
	for i := 0; i < good; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			w := widget{ID: fmt.Sprintf("bad%02d", i), Name: ""}
			if _, err := h.c.CreateFormat(ctx, w); err == nil {
				t.Errorf("Create(%s) with empty Name unexpectedly succeeded", w.ID)
			}
		}(i)
	}
	wg.Wait()

	waitForCondition(t, 30*time.Second, func() bool {
		count, err := h.read.db.NewSelect().Model((*widget)(nil)).Count(ctx)
		return err == nil && count == good
	}, fmt.Sprintf("expected exactly %d rows (the valid ones) in the read db", good))

	for i := 0; i < good; i++ {
		badID := fmt.Sprintf("bad%02d", i)
		if _, ok := readWidgetFrom(t, h.write, badID); ok {
			t.Errorf("expected %s to be absent from write db (validation should have blocked the insert)", badID)
		}
		if _, ok := readWidgetFrom(t, h.read, badID); ok {
			t.Errorf("expected %s to be absent from read db", badID)
		}
		okID := fmt.Sprintf("ok%02d", i)
		if _, ok := readWidgetFrom(t, h.read, okID); !ok {
			t.Errorf("expected %s to be present in read db", okID)
		}
	}

	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after context cancellation")
	}
}

// TestIntegration_HappyPath_HighConcurrencyBatchCoalescing runs full
// Create->Update->Delete lifecycles for K entities concurrently with each
// other (each single entity's own lifecycle stays correctly ordered, since
// one goroutine drives it sequentially; the concurrency pressure is
// cross-entity, stressing the shared Batcher/coalescing pools with many
// interleaved entities' events at once — the actual scenario "many
// requests hitting the same running consumer at once" describes). Every
// entity ends deleted, so the final-state assertion is unambiguous
// regardless of interleaving.
func TestIntegration_HappyPath_HighConcurrencyBatchCoalescing(t *testing.T) {
	t.Parallel()
	const k = 12
	h := newCDCHarnessIT(t, "coalesce", 20)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- h.c.Run(ctx) }()

	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < k; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			id := fmt.Sprintf("life%02d", i)

			created := widget{ID: id, Name: "created"}
			if _, err := h.c.CreateFormat(ctx, created); err != nil {
				t.Errorf("Create(%s) returned error: %v", id, err)
				return
			}
			publishEnvelope(t, h.broker, ctx, h.topic, "evt-"+id+"-c", domains.ChangeTypeCreated, created)

			updated := widget{ID: id, Name: "updated"}
			if _, err := h.c.UpdateByIDFormat(ctx, id, updated); err != nil {
				t.Errorf("UpdateByID(%s) returned error: %v", id, err)
				return
			}
			publishEnvelope(t, h.broker, ctx, h.topic, "evt-"+id+"-u", domains.ChangeTypeUpdated, updated)

			if err := h.c.DeleteByID(ctx, id); err != nil {
				t.Errorf("DeleteByID(%s) returned error: %v", id, err)
				return
			}
			publishEnvelope(t, h.broker, ctx, h.topic, "evt-"+id+"-d", domains.ChangeTypeDeleted, updated)
		}(i)
	}
	close(start)
	wg.Wait()

	waitForCondition(t, 30*time.Second, func() bool {
		return len(h.broadcast.snapshot()) == k*3
	}, fmt.Sprintf("expected exactly %d broadcasts (created+updated+deleted per entity), got %d", k*3, len(h.broadcast.snapshot())))

	for i := 0; i < k; i++ {
		id := fmt.Sprintf("life%02d", i)
		if _, ok := readWidgetFrom(t, h.read, id); ok {
			t.Errorf("expected %s to end deleted in the read db", id)
		}
		if _, ok := readWidgetFrom(t, h.write, id); ok {
			t.Errorf("expected %s to end deleted in the write db", id)
		}
	}

	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after context cancellation")
	}
}

// --- shared concurrency-test harness ---

type cdcHarnessIT struct {
	c         *cqrs.CQRSImpl[widget, widgetResource, any, string]
	write     *fakeSQLService
	read      *fakeSQLService
	broker    *realKafkaBroker
	broadcast *fakeBroadcastService
	topic     string
}

// newCDCHarnessIT is the concurrency-test counterpart of
// newIntegrationHarness in integration_test.go: same real Postgres + real
// Kafka wiring, but with a caller-chosen BatchSize so each test can size
// its batcher to the burst of events it's about to push, and Updated/
// Deleted callbacks included since every test in this file exercises the
// full lifecycle, not just Create.
func newCDCHarnessIT(t *testing.T, topicSuffix string, batchSize int) *cdcHarnessIT {
	return newCDCHarnessITWithPool(t, topicSuffix, batchSize, defaultPoolSize)
}

// newCDCHarnessITWithPool is newCDCHarnessIT with a caller-chosen
// connection pool size — used by the load test (see load_test.go), which
// needs far more than defaultPoolSize to get a meaningful reading out of
// its 100-worker UpdateByID benchmark, and can afford it because it never
// runs under t.Parallel().
func newCDCHarnessITWithPool(t *testing.T, topicSuffix string, batchSize int, maxConns int) *cdcHarnessIT {
	t.Helper()
	skipUnlessInfraReachable(t)

	write := newPostgresSQLServiceWithPool(t, itWriteDSN, maxConns)
	read := newPostgresSQLServiceWithPool(t, itReadDSN, maxConns)
	topic := fmt.Sprintf("cqrs-it-%s-%d", topicSuffix, time.Now().UnixNano())
	broker := newRealKafkaBroker(t, itKafkaAddr, topic)
	broadcast := newFakeBroadcastService()

	c := cqrs.NewCQRS(cqrs.CQRSImpl[widget, widgetResource, any, string]{
		Channel:              domains.Channel(topic),
		WriteSQLService:      write,
		ReadSQLService:       read,
		MessageBrokerService: broker,
		BroadcastService:     broadcast,
		ToResource:           widgetToResource,
		Created:              func(*widget) domains.Events { return domains.Events{"widget.created"} },
		Updated:              func(*widget) domains.Events { return domains.Events{"widget.updated"} },
		Deleted:              func(*widget) domains.Events { return domains.Events{"widget.deleted"} },
		BatchSize:            batchSize,
	})
	return &cdcHarnessIT{c: c, write: write, read: read, broker: broker, broadcast: broadcast, topic: topic}
}
