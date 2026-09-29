package regression

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/sonic"

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
