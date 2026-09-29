//go:build integration

// This is the test that closes the gap flagged repeatedly earlier in this
// project's history: every other "real Kafka" integration test still
// hand-built the {event_id, change_type, payload} envelope and published
// it directly, bypassing Debezium's actual output entirely. This one
// doesn't — it writes to real Postgres, lets the real, already-running
// Debezium connector (see local/docker-compose) capture that change for
// real, runs it through transformDebezium (see debezium_test.go) via
// runDebeziumBridge below, and only then hands it to a real CQRSImpl.Run.
// Nothing here is hand-built.
//
// The bridge is test-only glue, not shipped library code: this project
// ships domains.MessageBrokerService as a plain interface and doesn't
// bundle a Debezium adapter, so a real consumer brings their own. This
// exists to prove the *shape* of that bridge actually works against this
// connector's real output, not to be an adapter someone imports.
package regression

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/bytedance/sonic"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/cqrs"
	"github.com/Lands-Horizon-Corp/cqrs-go/src/domains"

	segmentio "github.com/segmentio/kafka-go"
)

// debeziumSourceTopic is what the connector registered in
// local/docker-compose/debezium/connector-postgres-write.json actually
// publishes "widgets" table changes to (topic.prefix "cqrs" + schema +
// table, Debezium's own naming convention).
const debeziumSourceTopic = "cqrs.public.widgets"

// runDebeziumBridge consumes sourceTopic from source, runs each message
// through transformDebezium, and republishes the result to destTopic via
// dest — mirroring what a real deployment's Debezium-to-Run adapter would
// do. A message that fails to transform is logged via onError and
// skipped, not fatal to the bridge; a failure to publish the transformed
// result is fatal, since silently dropping a change Debezium successfully
// captured would be real data loss.
func runDebeziumBridge[T any](ctx context.Context, source domains.MessageBrokerService, sourceTopic string, dest domains.MessageBrokerService, destTopic string, onError func(err error, raw []byte)) error {
	return source.Subscribe(ctx, sourceTopic, func(key, value []byte) error {
		payload, err := transformDebezium[T](value)
		if err != nil {
			if errors.Is(err, errDebeziumTombstone) {
				return nil
			}
			if onError != nil {
				onError(err, value)
			}
			return nil
		}
		out, err := sonic.Marshal(payload)
		if err != nil {
			if onError != nil {
				onError(fmt.Errorf("marshalling transformed envelope: %w", err), value)
			}
			return nil
		}
		return dest.Publish(ctx, destTopic, key, out)
	})
}

func TestIntegration_HappyPath_RealDebeziumThroughBridgeAlignsRealPostgresRead(t *testing.T) {
	skipUnlessInfraReachable(t)

	write := newPostgresSQLService(t, itWriteDSN)
	read := newPostgresSQLService(t, itReadDSN)

	destTopic := fmt.Sprintf("cqrs-it-bridge-%d", time.Now().UnixNano())

	// debeziumSourceTopic already carries real history from every prior
	// integration/load test run against postgres-write (anything that
	// touches its "widgets" table, which is all of them) — replaying that
	// from the beginning would mean processing millions of old records
	// before reaching this test's own change. A brand-new consumer group
	// starting from LastOffset avoids that; the tradeoff is that the
	// group must have finished joining before the write below happens, or
	// that write is missed. The sleep after starting the bridge is this
	// test's (imperfect, but practical) way of waiting for that join —
	// kafka-go's Reader doesn't expose a "group join complete" signal to
	// synchronize on directly.
	sourceBroker := newRealKafkaGroupBroker(itKafkaAddr, fmt.Sprintf("cqrs-it-bridge-source-%d", time.Now().UnixNano()), segmentio.LastOffset)
	destBroker := newRealKafkaGroupBroker(itKafkaAddr, fmt.Sprintf("cqrs-it-bridge-dest-%d", time.Now().UnixNano()), segmentio.FirstOffset)

	bridgeCtx, bridgeCancel := context.WithCancel(context.Background())
	defer bridgeCancel()
	bridgeDone := make(chan error, 1)
	go func() {
		bridgeDone <- runDebeziumBridge[widget](bridgeCtx, sourceBroker, debeziumSourceTopic, destBroker, destTopic, func(err error, raw []byte) {
			t.Logf("bridge: skipping a record that failed to transform: %v (raw=%s)", err, raw)
		})
	}()
	time.Sleep(3 * time.Second) // let the source consumer group finish joining

	broadcast := newFakeBroadcastService()
	c := cqrs.NewCQRS(cqrs.CQRSImpl[widget, widgetResource, any, string]{
		Channel:              domains.Channel(destTopic),
		WriteSQLService:      write,
		ReadSQLService:       read,
		MessageBrokerService: destBroker,
		BroadcastService:     broadcast,
		ToResource:           widgetToResource,
		Created:              func(*widget) domains.Events { return domains.Events{"widget.created"} },
		BatchSize:            1,
	})

	runCtx, runCancel := context.WithCancel(context.Background())
	defer runCancel()
	runDone := make(chan error, 1)
	go func() { runDone <- c.Run(runCtx) }()

	// A real write against real Postgres. Nothing here publishes to Kafka
	// directly — the real Debezium connector is what notices this and
	// captures it, exactly as it would in production.
	id := fmt.Sprintf("bridge-%d", time.Now().UnixNano())
	w := widget{ID: id, Name: "real-debezium-gadget", Active: true, Priority: new(7)}
	if _, err := c.Create(context.Background(), w); err != nil {
		t.Fatalf("Create returned error: %v", err)
	}

	broadcast.waitForCall(t, 30*time.Second) // real Debezium capture + bridge + Run round trip

	readRow, ok := readWidgetFrom(t, read, id)
	if !ok {
		t.Fatal("expected the row to exist in read db, synced entirely through the real Debezium connector")
	}
	if readRow.Name != w.Name || !readRow.Active {
		t.Errorf("unexpected read-db row: %+v", readRow)
	}
	if readRow.Priority == nil || *readRow.Priority != 7 {
		t.Errorf("expected Priority=7 to survive the real Debezium round trip, got %v", readRow.Priority)
	}

	runCancel()
	select {
	case <-runDone:
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after context cancellation")
	}
	bridgeCancel()
	select {
	case <-bridgeDone:
	case <-time.After(10 * time.Second):
		t.Fatal("bridge did not return after context cancellation")
	}
}
