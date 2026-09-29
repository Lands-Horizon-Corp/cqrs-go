package regression

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/bytedance/sonic"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/cqrs"
	"github.com/Lands-Horizon-Corp/cqrs-go/src/domains"
)

// TestErrorPaths_NewCQRS_PreservesChannel guards against a regression
// where NewCQRS defaulted an empty Channel to "default" but then never
// copied Channel into the returned struct at all — every constructed
// CQRSImpl silently had Channel == "", which made Run subscribe to an
// empty Kafka topic (a real Kafka client panics on that; the in-process
// fake broker used everywhere else in this suite ignores its topic
// argument, so nothing else here would ever have caught it). Checks both
// the explicit-Channel and the defaulted-Channel cases, and that Channel
// actually reaches Subscribe and a broadcast call, not just the struct
// field.
func TestErrorPaths_NewCQRS_PreservesChannel(t *testing.T) {
	t.Run("Explicit Channel", func(t *testing.T) {
		h := newCDCHarness(t, 1)
		if h.c.Channel != "widgets" {
			t.Fatalf("expected Channel 'widgets' on the constructed CQRSImpl, got %q", h.c.Channel)
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := h.runInBackground(ctx)

		envelope, err := sonic.Marshal(domains.CQRSQueuePayload[widget]{
			EventID: "evt-1", ChangeType: domains.ChangeTypeCreated, Payload: widget{ID: "w1", Name: "n"},
		})
		if err != nil {
			t.Fatalf("marshalling: %v", err)
		}
		h.broker.Emit(t, []byte("w1"), envelope)
		h.broadcast.waitForCall(t, 2*time.Second)

		if got := h.broker.subscribedTopic(); got != "widgets" {
			t.Errorf("expected Run to Subscribe to topic 'widgets', got %q", got)
		}
		calls := h.broadcast.snapshot()
		if len(calls) != 1 || len(calls[0].channels) != 1 || calls[0].channels[0] != "widgets" {
			t.Errorf("expected a broadcast on channel 'widgets', got %+v", calls)
		}

		cancel()
		h.waitForRunToStop(t, done, 2*time.Second)
	})

	t.Run("Defaulted Channel", func(t *testing.T) {
		write := newFakeSQLService(t)
		c := cqrs.NewCQRS(cqrs.CQRSImpl[widget, widgetResource, any, string]{
			WriteSQLService: write,
		})
		if c.Channel != "default" {
			t.Fatalf(`expected Channel to default to "default", got %q`, c.Channel)
		}
	})
}

func TestErrorPaths_NewCQRS_PanicsWithoutWriteSQLService(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected NewCQRS to panic when WriteSQLService is nil")
		}
	}()
	cqrs.NewCQRS(cqrs.CQRSImpl[widget, widgetResource, any, string]{})
}

func TestErrorPaths_Run_PanicsWhenWriteSQLServiceUnreachable(t *testing.T) {
	write := newFakeSQLService(t)
	write.db.Close() // subsequent Ping fails
	c := cqrs.NewCQRS(cqrs.CQRSImpl[widget, widgetResource, any, string]{
		WriteSQLService:      write,
		MessageBrokerService: newFakeMessageBroker(),
	})

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected Run to panic when WriteSQLService is unreachable")
		}
	}()
	_ = c.Run(context.Background())
}

func TestErrorPaths_Run_PanicsWhenReadSQLServiceUnreachable(t *testing.T) {
	write := newFakeSQLService(t)
	read := newFakeSQLService(t)
	read.db.Close() // subsequent Ping fails
	c := cqrs.NewCQRS(cqrs.CQRSImpl[widget, widgetResource, any, string]{
		WriteSQLService:      write,
		ReadSQLService:       read,
		MessageBrokerService: newFakeMessageBroker(),
	})

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected Run to panic when ReadSQLService is unreachable")
		}
	}()
	_ = c.Run(context.Background())
}

func TestErrorPaths_Run_ReturnsErrorWithoutMessageBrokerService(t *testing.T) {
	write := newFakeSQLService(t)
	c := cqrs.NewCQRS(cqrs.CQRSImpl[widget, widgetResource, any, string]{
		WriteSQLService: write,
	})

	err := c.Run(context.Background())
	if err == nil {
		t.Fatal("expected an error when MessageBrokerService is nil, got nil")
	}
}

func TestErrorPaths_Run_MalformedJSONIsLoggedAndSkipped(t *testing.T) {
	h := newCDCHarness(t, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := h.runInBackground(ctx)

	h.broker.Emit(t, []byte("bad"), []byte("{not valid json"))

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
	t.Fatal("expected an error to be logged for the malformed message")
}

// TestErrorPaths_Run_EventIDDefaultsFromKafkaKeyThenFallback covers both
// branches of Run's EventID-fallback logic: when the envelope has no
// event_id, it's taken from the Kafka message key if present, otherwise a
// channel+timestamp value is synthesized.
func TestErrorPaths_Run_EventIDDefaultsFromKafkaKeyThenFallback(t *testing.T) {
	h := newCDCHarness(t, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := h.runInBackground(ctx)

	t.Run("EventID From Kafka Key", func(t *testing.T) {
		envelope, err := sonic.Marshal(struct {
			ChangeType domains.ChangeType `json:"change_type"`
			Payload    widget             `json:"payload"`
		}{ChangeType: domains.ChangeTypeCreated, Payload: widget{ID: "w1", Name: "n"}})
		if err != nil {
			t.Fatalf("marshalling: %v", err)
		}
		h.broker.Emit(t, []byte("kafka-key-1"), envelope)
		h.broadcast.waitForCall(t, 2*time.Second)

		var got domains.ProcessedEvent
		if err := h.read.db.NewSelect().Model(&got).Where("event_id = ?", "kafka-key-1").Scan(ctx); err != nil {
			t.Errorf("expected processed_events row keyed by the kafka message key, got error: %v", err)
		}
	})

	t.Run("EventID Synthesized When Key Also Empty", func(t *testing.T) {
		envelope, err := sonic.Marshal(struct {
			ChangeType domains.ChangeType `json:"change_type"`
			Payload    widget             `json:"payload"`
		}{ChangeType: domains.ChangeTypeCreated, Payload: widget{ID: "w2", Name: "n"}})
		if err != nil {
			t.Fatalf("marshalling: %v", err)
		}
		h.broker.Emit(t, nil, envelope)
		h.broadcast.waitForCall(t, 2*time.Second)

		if _, ok := readWidgetFrom(t, h.read, "w2"); !ok {
			t.Error("expected the widget to be applied even with a synthesized EventID")
		}
	})

	cancel()
	h.waitForRunToStop(t, done, 2*time.Second)
}

// TestErrorPaths_ProcessBatch_UnknownChangeTypeGoesThroughDefaultBranch
// covers processBatch's default case, for a ChangeType outside
// Created/Updated/Deleted. syncBatchToReadDB's own switch also doesn't
// recognize it, so it isn't written to the read db — but it must still be
// reported through handleEvent, not silently dropped.
func TestErrorPaths_ProcessBatch_UnknownChangeTypeGoesThroughDefaultBranch(t *testing.T) {
	h := newCDCHarness(t, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := h.runInBackground(ctx)

	const unknownChangeType = domains.ChangeType(99)
	envelope, err := sonic.Marshal(domains.CQRSQueuePayload[widget]{
		EventID:    "evt-unknown",
		ChangeType: unknownChangeType,
		Payload:    widget{ID: "w1", Name: "n"},
	})
	if err != nil {
		t.Fatalf("marshalling: %v", err)
	}
	h.broker.Emit(t, []byte("w1"), envelope)

	// processBatch's default branch calls handleEvent with a nil
	// getEvents, so handleEvent returns before ever broadcasting — give it
	// a moment to (not) happen rather than waiting on a call that never
	// comes.
	time.Sleep(100 * time.Millisecond)

	// Unrecognized by syncBatchToReadDB's upsert/delete switch, so no row
	// should exist in the read db.
	if _, ok := readWidgetFrom(t, h.read, "w1"); ok {
		t.Error("expected no row written for an unrecognized ChangeType")
	}
	if calls := h.broadcast.snapshot(); len(calls) != 0 {
		t.Errorf("expected no broadcast for an unrecognized ChangeType, got %d", len(calls))
	}

	cancel()
	h.waitForRunToStop(t, done, 2*time.Second)
}

// --- handleEvent branch coverage (via the exported OnCreated/OnUpdated/OnDeleted) ---

func TestErrorPaths_HandleEvent_NilDataIsANoOp(t *testing.T) {
	c, _ := newTestCQRS(t)
	c.OnCreated(context.Background(), nil) // must not panic, must not spawn work
	time.Sleep(20 * time.Millisecond)
}

func TestErrorPaths_HandleEvent_NilToResourceIsANoOp(t *testing.T) {
	write := newFakeSQLService(t)
	c := newCQRSNoResource(t, write)
	c.OnCreated(context.Background(), &widget{ID: "w1", Name: "n"})
	time.Sleep(20 * time.Millisecond)
}

func TestErrorPaths_HandleEvent_ToResourcePanicIsRecoveredAndLogged(t *testing.T) {
	write := newFakeSQLService(t)
	logs := &fakeLogService{}
	c := cqrs.NewCQRS(cqrs.CQRSImpl[widget, widgetResource, any, string]{
		WriteSQLService: write,
		LogService:      logs,
		ToResource: func(w *widget) *widgetResource {
			panic("boom")
		},
	})

	c.OnCreated(context.Background(), &widget{ID: "w1", Name: "n"})

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		for _, call := range logs.snapshot() {
			if call.level == "error" {
				return // panic was recovered and logged
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("expected the ToResource panic to be recovered and logged as an error")
}

func TestErrorPaths_HandleEvent_NilResourceFromToResourceIsANoOp(t *testing.T) {
	write := newFakeSQLService(t)
	broadcast := newFakeBroadcastService()
	c := cqrs.NewCQRS(cqrs.CQRSImpl[widget, widgetResource, any, string]{
		WriteSQLService:  write,
		BroadcastService: broadcast,
		ToResource:       func(w *widget) *widgetResource { return nil },
		Created:          func(*widget) domains.Events { return domains.Events{"widget.created"} },
	})

	c.OnCreated(context.Background(), &widget{ID: "w1", Name: "n"})

	select {
	case <-broadcast.notify:
		t.Fatal("expected no broadcast when ToResource returns nil")
	case <-time.After(200 * time.Millisecond):
	}
}

func TestErrorPaths_HandleEvent_DispatchErrorIsLoggedButDoesNotBlockBroadcast(t *testing.T) {
	write := newFakeSQLService(t)
	logs := &fakeLogService{}
	broadcast := newFakeBroadcastService()
	dispatchErr := errors.New("dispatch backend unavailable")

	c := cqrs.NewCQRS(cqrs.CQRSImpl[widget, widgetResource, any, string]{
		WriteSQLService:  write,
		LogService:       logs,
		BroadcastService: broadcast,
		ToResource:       widgetToResource,
		Created:          func(*widget) domains.Events { return domains.Events{"widget.created"} },
		Dispatch: func(domains.Channel, domains.Events, *widgetResource) error {
			return dispatchErr
		},
	})

	c.OnCreated(context.Background(), &widget{ID: "w1", Name: "n"})
	broadcast.waitForCall(t, 2*time.Second)

	found := false
	for _, call := range logs.snapshot() {
		if call.level == "error" {
			found = true
		}
	}
	if !found {
		t.Error("expected the Dispatch error to be logged")
	}
}

func TestErrorPaths_HandleEvent_BroadcastErrorIsLogged(t *testing.T) {
	write := newFakeSQLService(t)
	logs := &fakeLogService{}
	broadcast := newFakeBroadcastService()
	broadcast.setFailure(errors.New("broadcast backend unavailable"))

	c := cqrs.NewCQRS(cqrs.CQRSImpl[widget, widgetResource, any, string]{
		WriteSQLService:  write,
		LogService:       logs,
		BroadcastService: broadcast,
		ToResource:       widgetToResource,
		Created:          func(*widget) domains.Events { return domains.Events{"widget.created"} },
	})

	c.OnCreated(context.Background(), &widget{ID: "w1", Name: "n"})
	broadcast.waitForCall(t, 2*time.Second)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		for _, call := range logs.snapshot() {
			if call.level == "error" {
				return
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("expected the Broadcast error to be logged")
}
