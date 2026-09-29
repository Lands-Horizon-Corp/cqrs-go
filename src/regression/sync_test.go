package regression

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/bytedance/sonic"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/sqlitedialect"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/cqrs"
	"github.com/Lands-Horizon-Corp/cqrs-go/src/domains"
)

// These tests exercise the actual claim of the CQRS split: Create/Update/
// Delete only ever touch WriteSQLService. ReadSQLService only changes once
// a CDC event for that write has gone through Run(). There is no real
// Debezium here, so each test hand-builds the CDC envelope Debezium would
// have produced for the write it just made — that's the one thing a real
// deployment provides that this suite can't, and it's called out at each
// use.

func emitFor(t *testing.T, h *cdcHarness, eventID string, changeType domains.ChangeType, payload widget) {
	t.Helper()
	envelope, err := sonic.Marshal(domains.CQRSQueuePayload[widget]{
		EventID:    eventID,
		ChangeType: changeType,
		Payload:    payload,
	})
	if err != nil {
		t.Fatalf("marshalling envelope: %v", err)
	}
	h.broker.Emit(t, []byte(payload.ID), envelope)
}

func TestSync_HappyPath_CreateThenCDCAlignsReadWithWrite(t *testing.T) {
	h := newCDCHarness(t, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := h.runInBackground(ctx)

	w := widget{ID: "w1", Name: "gadget", Active: true, Featured: new(true), Notes: new("hi"), Priority: new(3)}
	if _, err := h.c.Create(ctx, w); err != nil {
		t.Fatalf("Create returned error: %v", err)
	}

	// Before the CDC event arrives, the read db must NOT have the row —
	// Create never touches ReadSQLService directly.
	if _, ok := readWidgetFrom(t, h.read, "w1"); ok {
		t.Fatal("expected read db to have no row before the CDC event is applied")
	}

	emitFor(t, h, "evt-1", domains.ChangeTypeCreated, w)
	h.broadcast.waitForCall(t, 2*time.Second)

	writeRow, ok := readWidgetFrom(t, h.write, "w1")
	if !ok {
		t.Fatal("expected row in write db")
	}
	readRow, ok := readWidgetFrom(t, h.read, "w1")
	if !ok {
		t.Fatal("expected read db to now have the row after the CDC event landed")
	}
	if writeRow.Name != readRow.Name || writeRow.Active != readRow.Active {
		t.Errorf("write/read mismatch: write=%+v read=%+v", writeRow, readRow)
	}
	if (writeRow.Featured == nil) != (readRow.Featured == nil) || (writeRow.Featured != nil && *writeRow.Featured != *readRow.Featured) {
		t.Errorf("write/read Featured mismatch: write=%v read=%v", writeRow.Featured, readRow.Featured)
	}
	if (writeRow.Notes == nil) != (readRow.Notes == nil) || (writeRow.Notes != nil && *writeRow.Notes != *readRow.Notes) {
		t.Errorf("write/read Notes mismatch: write=%v read=%v", writeRow.Notes, readRow.Notes)
	}
	if (writeRow.Priority == nil) != (readRow.Priority == nil) || (writeRow.Priority != nil && *writeRow.Priority != *readRow.Priority) {
		t.Errorf("write/read Priority mismatch: write=%v read=%v", writeRow.Priority, readRow.Priority)
	}

	cancel()
	h.waitForRunToStop(t, done, 2*time.Second)
}

func TestSync_HappyPath_CreateWithZeroAndAbsentValuesAligns(t *testing.T) {
	h := newCDCHarness(t, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := h.runInBackground(ctx)

	// Every optional field left at its zero/absent value.
	w := widget{ID: "w1", Name: "n"}
	if _, err := h.c.Create(ctx, w); err != nil {
		t.Fatalf("Create returned error: %v", err)
	}
	emitFor(t, h, "evt-1", domains.ChangeTypeCreated, w)
	h.broadcast.waitForCall(t, 2*time.Second)

	readRow, ok := readWidgetFrom(t, h.read, "w1")
	if !ok {
		t.Fatal("expected read db to have the row")
	}
	if readRow.Active {
		t.Errorf("expected active=false to survive the sync, got true")
	}
	if readRow.Featured != nil || readRow.Notes != nil || readRow.Priority != nil || readRow.ExpiresAt != nil {
		t.Errorf("expected every pointer field to stay nil through the sync, got %+v", readRow)
	}

	cancel()
	h.waitForRunToStop(t, done, 2*time.Second)
}

func TestSync_HappyPath_UpdateThenCDCAlignsReadWithWrite(t *testing.T) {
	h := newCDCHarness(t, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := h.runInBackground(ctx)

	seed := widget{ID: "w1", Name: "old", Active: false, Featured: new(false)}
	if _, err := h.c.Create(ctx, seed); err != nil {
		t.Fatalf("seed Create returned error: %v", err)
	}
	emitFor(t, h, "evt-created", domains.ChangeTypeCreated, seed)
	h.broadcast.waitForCall(t, 2*time.Second)

	updated := widget{ID: "w1", Name: "new", Active: true, Featured: new(true), Priority: new(9)}
	if _, err := h.c.UpdateByID(ctx, "w1", updated); err != nil {
		t.Fatalf("UpdateByID returned error: %v", err)
	}

	// Write db has the new value; read db must still show the OLD value
	// until the CDC event for this update is applied.
	if readRow, ok := readWidgetFrom(t, h.read, "w1"); !ok || readRow.Name != "old" {
		t.Fatalf("expected read db to still show the pre-update state, got %+v (found=%v)", readRow, ok)
	}

	emitFor(t, h, "evt-updated", domains.ChangeTypeUpdated, updated)
	h.broadcast.waitForCall(t, 2*time.Second)

	writeRow, _ := readWidgetFrom(t, h.write, "w1")
	readRow, ok := readWidgetFrom(t, h.read, "w1")
	if !ok {
		t.Fatal("expected read db to have the updated row")
	}
	if writeRow.Name != readRow.Name || writeRow.Active != readRow.Active {
		t.Errorf("write/read mismatch after update: write=%+v read=%+v", writeRow, readRow)
	}
	if readRow.Priority == nil || *readRow.Priority != 9 {
		t.Errorf("expected priority=9 to have synced, got %v", readRow.Priority)
	}

	cancel()
	h.waitForRunToStop(t, done, 2*time.Second)
}

func TestSync_HappyPath_DeleteThenCDCAlignsReadWithWrite(t *testing.T) {
	h := newCDCHarness(t, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := h.runInBackground(ctx)

	seed := widget{ID: "w1", Name: "n"}
	if _, err := h.c.Create(ctx, seed); err != nil {
		t.Fatalf("seed Create returned error: %v", err)
	}
	emitFor(t, h, "evt-created", domains.ChangeTypeCreated, seed)
	h.broadcast.waitForCall(t, 2*time.Second)

	if err := h.c.DeleteByID(ctx, "w1"); err != nil {
		t.Fatalf("DeleteByID returned error: %v", err)
	}

	// Write db no longer has the row; read db must still have it until the
	// CDC delete event is applied.
	if _, ok := readWidgetFrom(t, h.write, "w1"); ok {
		t.Fatal("expected write db to no longer have the row")
	}
	if _, ok := readWidgetFrom(t, h.read, "w1"); !ok {
		t.Fatal("expected read db to still have the row before the delete event is applied")
	}

	emitFor(t, h, "evt-deleted", domains.ChangeTypeDeleted, seed)
	h.broadcast.waitForCall(t, 2*time.Second)

	if _, ok := readWidgetFrom(t, h.read, "w1"); ok {
		t.Error("expected read db row to be gone after the delete event is applied")
	}

	cancel()
	h.waitForRunToStop(t, done, 2*time.Second)
}

func TestSync_SadPath_DeleteEventForRowNotInReadDBIsSafeNoOp(t *testing.T) {
	h := newCDCHarness(t, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := h.runInBackground(ctx)

	// No prior Created event was ever applied to the read db, so this
	// Deleted event targets a row that was never there.
	emitFor(t, h, "evt-1", domains.ChangeTypeDeleted, widget{ID: "ghost", Name: "n"})
	h.broadcast.waitForCall(t, 2*time.Second)

	if _, ok := readWidgetFrom(t, h.read, "ghost"); ok {
		t.Error("expected no row to exist")
	}
	for _, c := range h.logs.snapshot() {
		if c.level == "error" {
			t.Errorf("expected no logged error for a delete-of-nonexistent-row, got: %s", c.msg)
		}
	}

	cancel()
	h.waitForRunToStop(t, done, 2*time.Second)
}

// TestSync_HappyPath_DuplicateEventIDIsAppliedOnce covers Kafka/Debezium
// at-least-once redelivery: the same EventID arriving twice must be
// applied to the read db, and broadcast, exactly once.
func TestSync_HappyPath_DuplicateEventIDIsAppliedOnce(t *testing.T) {
	h := newCDCHarness(t, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := h.runInBackground(ctx)

	w := widget{ID: "w1", Name: "n"}
	emitFor(t, h, "evt-1", domains.ChangeTypeCreated, w)
	h.broadcast.waitForCall(t, 2*time.Second)

	// Redelivery of the exact same event.
	emitFor(t, h, "evt-1", domains.ChangeTypeCreated, w)

	select {
	case <-h.broadcast.notify:
		t.Fatal("expected no second broadcast for a duplicate EventID")
	case <-time.After(300 * time.Millisecond):
	}

	if calls := h.broadcast.snapshot(); len(calls) != 1 {
		t.Fatalf("expected exactly 1 broadcast total, got %d", len(calls))
	}

	cancel()
	h.waitForRunToStop(t, done, 2*time.Second)
}

// conventionIDWidget has no explicit `bun` column name on its PK field —
// bun resolves the "id" column via its naming convention from the Go field
// name ID, but utils.BunColumnFieldIndex only ever looks at explicit `bun`
// tags, so it can't resolve this field. This is the realistic way the
// entity-coalescing key falls back to EventID in production: the SQL
// conflict target ("id") still exists and the upsert succeeds fine, it's
// only the Go-side tag lookup that comes up empty.
type conventionIDWidget struct {
	bun.BaseModel `bun:"table:convention_widgets"`
	ID            string `bun:",pk"`
	Name          string
}

func TestSync_HappyPath_EntityKeyFallsBackToEventIDWhenColumnUnresolved(t *testing.T) {
	sqldb, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("opening sqlite: %v", err)
	}
	sqldb.SetMaxOpenConns(1)
	db := bun.NewDB(sqldb, sqlitedialect.New())
	t.Cleanup(func() { _ = db.Close() })

	if _, err := db.NewCreateTable().Model((*conventionIDWidget)(nil)).Exec(context.Background()); err != nil {
		t.Fatalf("creating table: %v", err)
	}
	if _, err := db.NewCreateTable().Model((*domains.ProcessedEvent)(nil)).Exec(context.Background()); err != nil {
		t.Fatalf("creating processed_events table: %v", err)
	}
	svc := &fakeSQLService{db: db}

	broadcast := newFakeBroadcastService()
	broker := newFakeMessageBroker()

	c := cqrs.NewCQRS(cqrs.CQRSImpl[conventionIDWidget, conventionIDWidget, any, string]{
		Channel:              "conv",
		ColumnDefaultID:      "id",
		WriteSQLService:      svc,
		ReadSQLService:       svc,
		MessageBrokerService: broker,
		BroadcastService:     broadcast,
		ToResource:           func(w *conventionIDWidget) *conventionIDWidget { return w },
		Created:              func(*conventionIDWidget) domains.Events { return domains.Events{"created"} },
		BatchSize:            1,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	envelope, err := sonic.Marshal(domains.CQRSQueuePayload[conventionIDWidget]{
		EventID:    "evt-1",
		ChangeType: domains.ChangeTypeCreated,
		Payload:    conventionIDWidget{ID: "w1", Name: "n"},
	})
	if err != nil {
		t.Fatalf("marshalling: %v", err)
	}
	broker.Emit(t, []byte("w1"), envelope)
	broadcast.waitForCall(t, 2*time.Second)

	var got conventionIDWidget
	if err := db.NewSelect().Model(&got).Where("id = ?", "w1").Scan(context.Background()); err != nil {
		t.Errorf("expected the row to still be applied via the EventID fallback key: %v", err)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after context cancellation")
	}
}

// The next three tests force a real SQL failure at each of the three
// statements inside syncBatchToReadDB's transaction, by dropping the
// table each statement depends on before the event is applied.

func TestSync_SadPath_ProcessedEventsInsertFailureIsReportedViaOnError(t *testing.T) {
	h := newCDCHarness(t, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := h.runInBackground(ctx)

	dropProcessedEventsTable(t, h.read)
	emitFor(t, h, "evt-1", domains.ChangeTypeCreated, widget{ID: "w1", Name: "n"})

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		for _, c := range h.logs.snapshot() {
			if c.level == "error" {
				cancel()
				h.waitForRunToStop(t, done, 2*time.Second)
				return
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("expected the processed_events insert failure to be logged via Batcher's OnError")
}

func TestSync_SadPath_UpsertFailureIsReportedViaOnError(t *testing.T) {
	h := newCDCHarness(t, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := h.runInBackground(ctx)

	dropWidgetsTable(t, h.read)
	// Created -> goes into upsertEntities, not deleteEntities.
	emitFor(t, h, "evt-1", domains.ChangeTypeCreated, widget{ID: "w1", Name: "n"})

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		for _, c := range h.logs.snapshot() {
			if c.level == "error" {
				cancel()
				h.waitForRunToStop(t, done, 2*time.Second)
				return
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("expected the upsert failure to be logged via Batcher's OnError")
}

func TestSync_SadPath_DeleteFailureIsReportedViaOnError(t *testing.T) {
	h := newCDCHarness(t, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := h.runInBackground(ctx)

	dropWidgetsTable(t, h.read)
	// Deleted -> goes into deleteEntities, not upsertEntities, so this
	// specifically exercises the delete statement's own error branch.
	emitFor(t, h, "evt-1", domains.ChangeTypeDeleted, widget{ID: "w1", Name: "n"})

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		for _, c := range h.logs.snapshot() {
			if c.level == "error" {
				cancel()
				h.waitForRunToStop(t, done, 2*time.Second)
				return
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("expected the delete failure to be logged via Batcher's OnError")
}

// TestSync_PoisonPill_ComplexDataWithMixedNilsAligns exercises the "even in
// complex data" case: several pointer fields, some nil, some not, all
// synced together in a single write+CDC round trip.
func TestSync_PoisonPill_ComplexDataWithMixedNilsAligns(t *testing.T) {
	h := newCDCHarness(t, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := h.runInBackground(ctx)

	exp := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	w := widget{
		ID:        "w1",
		Name:      "complex",
		Active:    true,
		Featured:  nil,     // absent
		Notes:     new(""), // explicit zero value
		Priority:  new(-7), // explicit negative
		ExpiresAt: new(exp),
	}
	if _, err := h.c.Create(ctx, w); err != nil {
		t.Fatalf("Create returned error: %v", err)
	}
	emitFor(t, h, "evt-1", domains.ChangeTypeCreated, w)
	h.broadcast.waitForCall(t, 2*time.Second)

	readRow, ok := readWidgetFrom(t, h.read, "w1")
	if !ok {
		t.Fatal("expected read db to have the row")
	}
	if readRow.Featured != nil {
		t.Errorf("expected Featured to stay nil, got %v", *readRow.Featured)
	}
	if readRow.Notes == nil || *readRow.Notes != "" {
		t.Errorf("expected Notes=&\"\", got %v", readRow.Notes)
	}
	if readRow.Priority == nil || *readRow.Priority != -7 {
		t.Errorf("expected Priority=&-7, got %v", readRow.Priority)
	}
	if readRow.ExpiresAt == nil || !readRow.ExpiresAt.Equal(exp) {
		t.Errorf("expected ExpiresAt=%v, got %v", exp, readRow.ExpiresAt)
	}

	cancel()
	h.waitForRunToStop(t, done, 2*time.Second)
}
