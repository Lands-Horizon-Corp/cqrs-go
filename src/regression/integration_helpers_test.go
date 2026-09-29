//go:build integration

// This file and integration_test.go only build with `-tags=integration`,
// so `go test ./...` never needs Docker. Run these against the stack in
// local/docker-compose (`docker compose up -d` there first):
//
//	go test -tags=integration ./src/regression/... -run TestIntegration -v
//
// Everything here is a REAL connection — real Postgres (both write and
// read), real Kafka. The only things still mocked are BroadcastService and
// LogService, because there's no real Pusher/webhook target in this repo
// to test against.
package regression

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/driver/pgdriver"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/domains"
)

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// Ports here match local/docker-compose/docker-compose.yml's published
// ports, which are deliberately namespaced away from common defaults
// (5432-5434, 2181, 8083, 9092) to avoid clashing with other local infra.
var (
	itWriteDSN  = envOr("CQRS_IT_WRITE_DSN", "postgres://postgres:postgres@localhost:15433/cqrs_write?sslmode=disable")
	itReadDSN   = envOr("CQRS_IT_READ_DSN", "postgres://postgres:postgres@localhost:15434/cqrs_read?sslmode=disable")
	itWriteAddr = envOr("CQRS_IT_WRITE_ADDR", "localhost:15433")
	itReadAddr  = envOr("CQRS_IT_READ_ADDR", "localhost:15434")
	itKafkaAddr = envOr("CQRS_IT_KAFKA_ADDR", "localhost:29092")
)

// skipUnlessInfraReachable does a fast TCP dial against each dependency and
// skips the test (rather than failing/hanging) if the local docker-compose
// stack isn't up.
func skipUnlessInfraReachable(t *testing.T) {
	t.Helper()
	check := func(label, addr string) {
		conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
		if err != nil {
			t.Skipf("skipping: %s not reachable at %s (%v) — run `docker compose up -d` in local/docker-compose first", label, addr, err)
		}
		_ = conn.Close()
	}
	check("postgres-write", itWriteAddr)
	check("postgres-read", itReadAddr)
	check("kafka", itKafkaAddr)
}

// newPostgresSQLService opens a real Postgres connection via bun/pgdriver.
// It shares fakeSQLService's shape (Ping/Client over a *bun.DB) since that
// wrapper never actually depended on SQLite — only the DSN and dialect
// differ here.
func newPostgresSQLService(t *testing.T, dsn string) *fakeSQLService {
	t.Helper()
	sqldb := sql.OpenDB(pgdriver.NewConnector(pgdriver.WithDSN(dsn)))
	db := bun.NewDB(sqldb, pgdialect.New())
	t.Cleanup(func() { _ = db.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("pinging postgres at %s: %v", dsn, err)
	}

	// Idempotent: real Postgres data persists across test runs, unlike the
	// in-memory SQLite fakes, so drop and recreate rather than assuming a
	// clean table.
	if _, err := db.NewDropTable().Model((*widget)(nil)).IfExists().Exec(ctx); err != nil {
		t.Fatalf("dropping widgets table: %v", err)
	}
	if _, err := db.NewDropTable().Model((*domains.ProcessedEvent)(nil)).IfExists().Exec(ctx); err != nil {
		t.Fatalf("dropping processed_events table: %v", err)
	}
	if _, err := db.NewCreateTable().Model((*widget)(nil)).Exec(ctx); err != nil {
		t.Fatalf("creating widgets table: %v", err)
	}
	if _, err := db.NewCreateTable().Model((*domains.ProcessedEvent)(nil)).Exec(ctx); err != nil {
		t.Fatalf("creating processed_events table: %v", err)
	}

	return &fakeSQLService{db: db}
}

// realKafkaBroker is a real, minimal domains.MessageBrokerService backed by
// segmentio/kafka-go. No such implementation exists anywhere else in this
// repo (only the interface does) — this is written for this test, and
// deliberately kept out of src/ since it hasn't been through the design
// questions a production Kafka adapter needs (consumer groups, offset
// commit strategy, retry/backoff, partition count).
type realKafkaBroker struct {
	brokers []string
	topic   string
}

func newRealKafkaBroker(t *testing.T, addr, topic string) *realKafkaBroker {
	t.Helper()
	b := &realKafkaBroker{brokers: []string{addr}, topic: topic}
	b.ensureTopic(t)
	return b
}

func (b *realKafkaBroker) ensureTopic(t *testing.T) {
	t.Helper()
	conn, err := kafka.Dial("tcp", b.brokers[0])
	if err != nil {
		t.Fatalf("dialing kafka at %s: %v", b.brokers[0], err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		t.Fatalf("finding kafka controller: %v", err)
	}
	controllerConn, err := kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		t.Fatalf("dialing kafka controller: %v", err)
	}
	defer controllerConn.Close()

	err = controllerConn.CreateTopics(kafka.TopicConfig{
		Topic:             b.topic,
		NumPartitions:     1,
		ReplicationFactor: 1,
	})
	if err != nil {
		t.Fatalf("creating topic %q: %v", b.topic, err)
	}

	// CreateTopics returning doesn't guarantee the topic is visible to a
	// subsequent Produce/Fetch yet — metadata propagation to the broker(s)
	// a client's Writer/Reader talk to is asynchronous. Without this, the
	// very next Publish can fail with "Unknown Topic Or Partition".
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if partitions, err := conn.ReadPartitions(b.topic); err == nil && len(partitions) > 0 {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("topic %q did not become visible within the timeout", b.topic)
}

func (b *realKafkaBroker) Client() *kafka.Client {
	return &kafka.Client{Addr: kafka.TCP(b.brokers...)}
}

// Publish retries on "Unknown Topic Or Partition" for a few seconds.
// kafka.Writer defaults to kafka-go's shared, process-wide Transport,
// which caches topic/broker metadata with its own TTL — a Writer created
// moments after a topic was created (as every test here does) can briefly
// see stale "doesn't exist" metadata for it even though the broker itself
// already has it. Retrying past that window is the standard way to ride
// out this specific, well-known race rather than trying to invalidate or
// bypass the shared cache.
func (b *realKafkaBroker) Publish(ctx context.Context, topic string, key, value []byte) error {
	w := &kafka.Writer{
		Addr:                   kafka.TCP(b.brokers...),
		Topic:                  topic,
		Balancer:               &kafka.LeastBytes{},
		AllowAutoTopicCreation: false,
	}
	defer w.Close()

	deadline := time.Now().Add(10 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		lastErr = w.WriteMessages(ctx, kafka.Message{Key: key, Value: value})
		if lastErr == nil || !strings.Contains(lastErr.Error(), "Unknown Topic Or Partition") {
			return lastErr
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("publishing to %q after retrying past metadata propagation: %w", topic, lastErr)
}

// Subscribe reads from partition 0 from the beginning — no consumer group,
// since this test always talks to a topic it just created for itself.
func (b *realKafkaBroker) Subscribe(ctx context.Context, topic string, handler func(key, value []byte) error) error {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   b.brokers,
		Topic:     topic,
		Partition: 0,
		MinBytes:  1,
		MaxBytes:  10e6,
	})
	defer r.Close()
	if err := r.SetOffset(kafka.FirstOffset); err != nil {
		return fmt.Errorf("setting reader offset: %w", err)
	}

	for {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
		if err := handler(m.Key, m.Value); err != nil {
			return err
		}
	}
}
