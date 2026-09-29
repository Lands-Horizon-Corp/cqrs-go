//go:build integration

package regression

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/bytedance/sonic"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/cqrs"
	"github.com/Lands-Horizon-Corp/cqrs-go/src/domains"
)

// newIntegrationHarness wires a CQRSImpl against real Postgres (write and
// read) and a real Kafka topic. BroadcastService and LogService stay
// mocked — there's no real push/webhook target in this repo to test
// against, and the log fake lets us assert on error paths the same way
// the SQLite-backed tests do.
func newIntegrationHarness(t *testing.T, topicSuffix string) (*cqrs.CQRSImpl[widget, widgetResource, any, string], *fakeSQLService, *fakeSQLService, *realKafkaBroker, *fakeBroadcastService, *fakeLogService) {
	t.Helper()
	skipUnlessInfraReachable(t)

	write := newPostgresSQLService(t, itWriteDSN)
	read := newPostgresSQLService(t, itReadDSN)
	topic := fmt.Sprintf("cqrs-it-%s-%d", topicSuffix, time.Now().UnixNano())
	broker := newRealKafkaBroker(t, itKafkaAddr, topic)
	broadcast := newFakeBroadcastService()
	logs := &fakeLogService{}

	c := cqrs.NewCQRS(cqrs.CQRSImpl[widget, widgetResource, any, string]{
		Channel:              domains.Channel(topic),
		WriteSQLService:      write,
		ReadSQLService:       read,
		MessageBrokerService: broker,
		BroadcastService:     broadcast,
		LogService:           logs,
		ToResource:           widgetToResource,
		Created:              func(*widget) domains.Events { return domains.Events{"widget.created"} },
		Updated:              func(*widget) domains.Events { return domains.Events{"widget.updated"} },
		BatchSize:            1,
	})
	return c, write, read, broker, broadcast, logs
}

// TestIntegration_HappyPath_RealPostgresWriteThenRealKafkaAlignsRealPostgresRead
// is the actual end-to-end proof: Create() against real Postgres, an event
// published to a real Kafka topic (standing in for what a Debezium ->
// transform step would produce — see local/docker-compose/README.md for
// why that step still doesn't exist), consumed by Run() over a real Kafka
// connection, applied to a second real Postgres database.
func TestIntegration_HappyPath_RealPostgresWriteThenRealKafkaAlignsRealPostgresRead(t *testing.T) {
	c, write, read, broker, broadcast, _ := newIntegrationHarness(t, "happy")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	w := widget{ID: "w1", Name: "gadget", Active: true, Featured: new(true), Priority: new(3)}
	if _, err := c.Create(ctx, w); err != nil {
		t.Fatalf("Create against real postgres-write returned error: %v", err)
	}

	writeRow, ok := readWidgetFrom(t, write, "w1")
	if !ok {
		t.Fatal("expected the row to exist in real postgres-write")
	}

	envelope, err := sonic.Marshal(domains.CQRSQueuePayload[widget]{
		EventID:    "evt-1",
		ChangeType: domains.ChangeTypeCreated,
		Payload:    w,
	})
	if err != nil {
		t.Fatalf("marshalling envelope: %v", err)
	}
	if err := broker.Publish(ctx, string(c.Channel), []byte(w.ID), envelope); err != nil {
		t.Fatalf("publishing to real kafka: %v", err)
	}

	broadcast.waitForCall(t, 15*time.Second) // real Kafka round trip is slower than the in-process fakes

	readRow, ok := readWidgetFrom(t, read, "w1")
	if !ok {
		t.Fatal("expected the row to exist in real postgres-read after the CDC event was consumed")
	}
	if writeRow.Name != readRow.Name || writeRow.Active != readRow.Active {
		t.Errorf("write/read mismatch: write=%+v read=%+v", writeRow, readRow)
	}
	if readRow.Featured == nil || *readRow.Featured != true {
		t.Errorf("expected Featured=true to survive the real Postgres round trip, got %v", readRow.Featured)
	}
	if readRow.Priority == nil || *readRow.Priority != 3 {
		t.Errorf("expected Priority=3 to survive the real Postgres round trip, got %v", readRow.Priority)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after context cancellation")
	}
}

// TestIntegration_PoisonPill_ZeroAndNilValuesSurviveRealPostgresRoundTrip
// re-runs the zero-value/nil-pointer checks from sync_test.go, but against
// real Postgres instead of SQLite — Postgres has its own NULL/type
// coercion behavior that the SQLite-backed suite can't verify.
func TestIntegration_PoisonPill_ZeroAndNilValuesSurviveRealPostgresRoundTrip(t *testing.T) {
	c, _, read, broker, broadcast, _ := newIntegrationHarness(t, "poison")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	// Active left false (zero value), every pointer field left nil.
	w := widget{ID: "w1", Name: "n"}
	if _, err := c.Create(ctx, w); err != nil {
		t.Fatalf("Create returned error: %v", err)
	}

	envelope, err := sonic.Marshal(domains.CQRSQueuePayload[widget]{
		EventID:    "evt-1",
		ChangeType: domains.ChangeTypeCreated,
		Payload:    w,
	})
	if err != nil {
		t.Fatalf("marshalling envelope: %v", err)
	}
	if err := broker.Publish(ctx, string(c.Channel), []byte(w.ID), envelope); err != nil {
		t.Fatalf("publishing to real kafka: %v", err)
	}
	broadcast.waitForCall(t, 15*time.Second)

	readRow, ok := readWidgetFrom(t, read, "w1")
	if !ok {
		t.Fatal("expected the row to exist in real postgres-read")
	}
	if readRow.Active {
		t.Errorf("expected active=false (zero value) to survive real Postgres, got true")
	}
	if readRow.Featured != nil || readRow.Notes != nil || readRow.Priority != nil || readRow.ExpiresAt != nil {
		t.Errorf("expected every pointer field to stay NULL through real Postgres, got %+v", readRow)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after context cancellation")
	}
}
