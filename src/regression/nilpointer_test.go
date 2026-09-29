package regression

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/bytedance/sonic"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/domains"
	"github.com/Lands-Horizon-Corp/cqrs-go/src/utils"
)

// ptrIDEntity exists purely to exercise BunColumnFieldIndex/FieldValueAt
// against a primary-key column that is itself a pointer type — the exact
// shape that exposed a bug in FieldValueAt: a nil pointer ID used to
// stringify to the literal text "<nil>" (silently colliding every nil-ID
// row into one coalescing bucket instead of falling back to EventID), and
// a non-nil pointer to a scalar used to stringify to its hex address
// instead of its dereferenced value.
type ptrIDEntity struct {
	ID   *string `bun:"id,pk"`
	Name string  `bun:"name"`
}

func TestNilPointer_FieldValueAt_NilPointerIDIsAbsentNotTheStringNil(t *testing.T) {
	idx := utils.BunColumnFieldIndex[ptrIDEntity]("id")
	if idx < 0 {
		t.Fatalf("expected to resolve the id field, got %d", idx)
	}

	nilID := ptrIDEntity{ID: nil, Name: "n"}
	if got := utils.FieldValueAt(&nilID, idx); got != "" {
		t.Errorf(`expected "" for a nil pointer ID, got %q (would silently collide with every other nil-ID row)`, got)
	}

	setID := ptrIDEntity{ID: new("abc"), Name: "n"}
	if got := utils.FieldValueAt(&setID, idx); got != "abc" {
		t.Errorf("expected the dereferenced value %q, got %q (a pointer that isn't dereferenced prints a hex address instead)", "abc", got)
	}
}

// TestNilPointer_CreateUpdateDelete_AllFieldsNilNeverPanics sweeps every
// pointer field left nil through the full write-path lifecycle.
func TestNilPointer_CreateUpdateDelete_AllFieldsNilNeverPanics(t *testing.T) {
	c, _ := newTestCQRS(t)
	ctx := context.Background()

	if _, err := c.Create(ctx, widget{ID: "w1", Name: "n"}); err != nil {
		t.Fatalf("Create returned error: %v", err)
	}
	if _, err := c.UpdateByID(ctx, "w1", widget{ID: "w1", Name: "n2"}); err != nil {
		t.Fatalf("UpdateByID returned error: %v", err)
	}
	if err := c.DeleteByID(ctx, "w1"); err != nil {
		t.Fatalf("DeleteByID returned error: %v", err)
	}
}

// TestNilPointer_CreateUpdateDelete_AllFieldsSetNeverPanics sweeps every
// pointer field populated through the same lifecycle, as the flip side.
func TestNilPointer_CreateUpdateDelete_AllFieldsSetNeverPanics(t *testing.T) {
	c, _ := newTestCQRS(t)
	ctx := context.Background()
	exp := time.Now().UTC()

	full := widget{
		ID: "w1", Name: "n", Active: true,
		Featured: new(true), Notes: new("n"), Priority: new(1), ExpiresAt: &exp,
	}
	if _, err := c.Create(ctx, full); err != nil {
		t.Fatalf("Create returned error: %v", err)
	}
	if _, err := c.UpdateByID(ctx, "w1", full); err != nil {
		t.Fatalf("UpdateByID returned error: %v", err)
	}
	if err := c.DeleteByID(ctx, "w1"); err != nil {
		t.Fatalf("DeleteByID returned error: %v", err)
	}
}

// TestNilPointer_CDCPath_MixedNilAndSetFieldsNeverPanics sweeps a mix of
// nil, explicit-zero, and populated pointer fields through the CDC apply
// path (unmarshal -> entity-coalescing key resolution -> upsert ->
// broadcast), which is the part of the codebase that reflects over
// arbitrary struct fields and is most exposed to a nil-handling bug.
func TestNilPointer_CDCPath_MixedNilAndSetFieldsNeverPanics(t *testing.T) {
	h := newCDCHarness(t, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := h.runInBackground(ctx)

	cases := []widget{
		{ID: "w1", Name: "n"},                                  // every pointer nil
		{ID: "w1", Name: "n2", Featured: new(true)},            // one set
		{ID: "w1", Name: "n3", Featured: nil, Notes: new("x")}, // mixed
		{ID: "w1", Name: "n4", Priority: new(0)},               // explicit zero, not nil
	}

	for i, w := range cases {
		envelope, err := sonic.Marshal(domains.CQRSQueuePayload[widget]{
			EventID:    fmt.Sprintf("evt-%d", i),
			ChangeType: domains.ChangeTypeUpdated,
			Payload:    w,
		})
		if err != nil {
			t.Fatalf("marshalling envelope %d: %v", i, err)
		}
		h.broker.Emit(t, []byte(w.ID), envelope)
	}

	for range cases {
		h.broadcast.waitForCall(t, 2*time.Second)
	}

	got, ok := readWidgetFrom(t, h.read, "w1")
	if !ok {
		t.Fatal("expected the row to exist after the sweep")
	}
	if got.Name != "n4" {
		t.Errorf("expected the read model to reflect the last applied change, got %+v", got)
	}

	cancel()
	h.waitForRunToStop(t, done, 2*time.Second)
}

// TestNilPointer_NilSlicesAndDispatchNeverPanic checks the broadcast path
// with an entity whose Events callback returns nil, and with no Dispatch
// set at all — both are nil function values / nil slices flowing through
// handleEvent.
func TestNilPointer_NilSlicesAndDispatchNeverPanic(t *testing.T) {
	write := newFakeSQLService(t)
	c := cqrsWithNilEventsCallback(t, write)

	ctx := context.Background()
	if _, err := c.Create(ctx, widget{ID: "w1", Name: "n"}); err != nil {
		t.Fatalf("Create returned error: %v", err)
	}
	// OnCreated is not wired through Run() here, so call it directly to
	// exercise handleEvent's nil-Events / nil-Dispatch paths synchronously
	// via the public method surface.
	c.OnCreated(ctx, &widget{ID: "w1", Name: "n"})
	time.Sleep(50 * time.Millisecond) // let the internal goroutine run
}
