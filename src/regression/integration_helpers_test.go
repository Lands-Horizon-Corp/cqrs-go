//go:build integration

// This file and integration_test.go only build with `-tags=integration`,
// so `go test ./...` never needs Docker. Run these against the stack in
// local/docker-compose (`docker compose up -d` there first):
//
//	go test -tags=integration ./src/regression/... -run TestIntegration -v
//
// Everything here is a REAL connection — real Postgres (both write and
// read), real Kafka, and a real Pusher-protocol-compatible broadcaster
// (sockudo). Only LogService is still mocked, since there's no real log
// sink in this repo to test against.
package regression

import (
	"context"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
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

	itPusherAddr      = envOr("CQRS_IT_PUSHER_ADDR", "localhost:16001")
	itPusherHTTPBase  = envOr("CQRS_IT_PUSHER_HTTP_BASE", "http://localhost:16001")
	itPusherWSBase    = envOr("CQRS_IT_PUSHER_WS_BASE", "ws://localhost:16001")
	itPusherAppID     = envOr("CQRS_IT_PUSHER_APP_ID", "cqrs-app")
	itPusherAppKey    = envOr("CQRS_IT_PUSHER_APP_KEY", "cqrs-app-key")
	itPusherAppSecret = envOr("CQRS_IT_PUSHER_APP_SECRET", "cqrs-app-secret")
)

// skipUnlessInfraReachable does a fast TCP dial against each dependency and
// skips the test (rather than failing/hanging) if the local docker-compose
// stack isn't up. Takes testing.TB rather than *testing.T so benchmarks
// (ledger_bench_test.go) can reuse it too — t.Skipf is part of the shared
// interface either way.
func skipUnlessInfraReachable(t testing.TB) {
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
	check("sockudo", itPusherAddr)
}

// schemaNameRE strips anything that isn't a valid unquoted Postgres
// identifier character out of a generated schema name (t.Name() can
// contain "/" for subtests, spaces from t.Run names with spaces, etc).
var schemaNameRE = regexp.MustCompile(`[^a-zA-Z0-9_]+`)

// newPostgresSQLService opens a real Postgres connection via bun/pgdriver,
// isolated into its own, freshly created schema (named after the test plus
// a random suffix) rather than the shared "public" schema. That isolation
// is what makes it safe for tests using this to run under t.Parallel():
// every earlier version of this helper drop-and-recreated a literal
// "widgets" table shared by every test, which made real concurrent runs
// corrupt each other's data. It shares fakeSQLService's shape (Ping/Client
// over a *bun.DB) since that wrapper never actually depended on SQLite —
// only the DSN and dialect differ here.
// defaultPoolSize is deliberately small — see the comment inside
// newPostgresSQLServiceInSchema on the connection budget this leaves under
// t.Parallel(). Only the load test (which never runs under t.Parallel())
// asks for a larger one, via newPostgresSQLServiceWithPool.
const defaultPoolSize = 4

func newPostgresSQLService(t *testing.T, dsn string) *fakeSQLService {
	t.Helper()
	return newPostgresSQLServiceWithPool(t, dsn, defaultPoolSize)
}

func newPostgresSQLServiceWithPool(t *testing.T, dsn string, maxConns int) *fakeSQLService {
	t.Helper()
	schema := schemaNameRE.ReplaceAllString(t.Name(), "_")
	if len(schema) > 40 { // Postgres identifiers cap at 63 bytes; leave room for the suffix
		schema = schema[:40]
	}
	schema = fmt.Sprintf("it_%s_%d", schema, time.Now().UnixNano())
	return newPostgresSQLServiceInSchema(t, dsn, schema, maxConns)
}

// newPostgresSQLServicePublicSchema is the one deliberate exception to
// newPostgresSQLService's isolation: the real Debezium connector (see
// local/docker-compose) is configured to watch schema.include.list=public
// specifically, so the one test that depends on the connector actually
// capturing its changes (integration_debezium_test.go) has to use the
// real "public" schema, not an isolated one Debezium was never told to
// watch. Every other integration test doesn't care which schema it's in,
// since it publishes its own synthetic CDC envelope rather than relying
// on the real connector.
func newPostgresSQLServicePublicSchema(t *testing.T, dsn string) *fakeSQLService {
	t.Helper()
	return newPostgresSQLServiceInSchema(t, dsn, "public", defaultPoolSize)
}

func newPostgresSQLServiceInSchema(t *testing.T, dsn string, schema string, maxConns int) *fakeSQLService {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if schema != "public" {
		admin := sql.OpenDB(pgdriver.NewConnector(pgdriver.WithDSN(dsn)))
		defer admin.Close()
		if _, err := admin.ExecContext(ctx, "CREATE SCHEMA IF NOT EXISTS "+pgQuoteIdent(schema)); err != nil {
			t.Fatalf("creating schema %s: %v", schema, err)
		}
		t.Cleanup(func() {
			admin := sql.OpenDB(pgdriver.NewConnector(pgdriver.WithDSN(dsn)))
			defer admin.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if _, err := admin.ExecContext(ctx, "DROP SCHEMA IF EXISTS "+pgQuoteIdent(schema)+" CASCADE"); err != nil {
				t.Logf("dropping schema %s during cleanup: %v", schema, err)
			}
		})
	}

	scopedDSN, err := withSearchPath(dsn, schema)
	if err != nil {
		t.Fatalf("building scoped DSN for schema %s: %v", schema, err)
	}
	sqldb := sql.OpenDB(pgdriver.NewConnector(pgdriver.WithDSN(scopedDSN)))
	// Bounded on purpose: database/sql defaults to an unbounded pool, and
	// this stack's Postgres instances are both configured with the default
	// max_connections=100 (verified, not assumed — see the commit that
	// introduced this comment). With every test now running under
	// t.Parallel(), up to $(go test -parallel) tests can be mid-test at
	// once, each holding up to two of these pools (write + read) — at this
	// machine's default parallelism (GOMAXPROCS=10) that's up to 20 pools
	// live simultaneously, so defaultPoolSize has to stay small (4), and
	// the Makefile pins -parallel explicitly rather than trusting whatever
	// GOMAXPROCS happens to be on a given machine, so this budget holds
	// everywhere the Makefile target is used. maxConns lets the load test
	// opt into a much larger pool for its own 100-worker UpdateByID
	// benchmark, since that test runs alone and never under t.Parallel().
	sqldb.SetMaxOpenConns(maxConns)
	sqldb.SetMaxIdleConns(maxConns)
	db := bun.NewDB(sqldb, pgdialect.New())
	t.Cleanup(func() { _ = db.Close() })

	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("pinging postgres at %s (schema %s): %v", dsn, schema, err)
	}

	// public is shared and reused across runs, so stay idempotent there;
	// an isolated schema is always brand new, so a plain create is enough.
	if schema == "public" {
		if _, err := db.NewDropTable().Model((*widget)(nil)).IfExists().Exec(ctx); err != nil {
			t.Fatalf("dropping widgets table: %v", err)
		}
		if _, err := db.NewDropTable().Model((*domains.ProcessedEvent)(nil)).IfExists().Exec(ctx); err != nil {
			t.Fatalf("dropping processed_events table: %v", err)
		}
	}
	if _, err := db.NewCreateTable().Model((*widget)(nil)).Exec(ctx); err != nil {
		t.Fatalf("creating widgets table: %v", err)
	}
	if _, err := db.NewCreateTable().Model((*domains.ProcessedEvent)(nil)).Exec(ctx); err != nil {
		t.Fatalf("creating processed_events table: %v", err)
	}

	return &fakeSQLService{db: db}
}

// pgQuoteIdent double-quotes a Postgres identifier we generated ourselves
// (from a sanitized test name, [a-zA-Z0-9_] only) — not untrusted input,
// but quoting it is free and avoids any surprise with a reserved word.
func pgQuoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// withSearchPath returns dsn with its search_path query parameter set to
// schema, verified against a live pgdriver connection (see the commit
// this helper was introduced in) to actually scope every query issued
// over that connection to that schema.
func withSearchPath(dsn, schema string) (string, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return "", err
	}
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	return u.String(), nil
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

// --- realKafkaGroupBroker: consumer-group variant of realKafkaBroker ---
//
// realKafkaBroker above always reads partition 0 from the beginning, which
// is fine for a test that creates its own fresh topic and consumes it
// once. This variant uses a real consumer group with an offset commit
// only after a message is successfully handled (so a crash mid-processing
// redelivers rather than loses it) — needed by the chaos test (does a
// group survive a broker restart?) and the Debezium bridge test (which
// consumes debeziumSourceTopic, a topic shared with every other test that
// has ever touched postgres-write's "widgets" table, so it needs
// StartOffset control to avoid replaying that entire history).
type realKafkaGroupBroker struct {
	brokers     []string
	groupID     string
	startOffset int64 // kafka.FirstOffset or kafka.LastOffset
}

func newRealKafkaGroupBroker(addr, groupID string, startOffset int64) *realKafkaGroupBroker {
	return &realKafkaGroupBroker{brokers: []string{addr}, groupID: groupID, startOffset: startOffset}
}

func (b *realKafkaGroupBroker) Publish(ctx context.Context, topic string, key, value []byte) error {
	w := &kafka.Writer{
		Addr:                   kafka.TCP(b.brokers...),
		Topic:                  topic,
		Balancer:               &kafka.LeastBytes{},
		AllowAutoTopicCreation: false,
	}
	defer w.Close()

	// Same retry as realKafkaBroker.Publish: a Writer used moments after a
	// topic was created can briefly see stale "doesn't exist" metadata
	// from kafka-go's shared Transport cache.
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

func (b *realKafkaGroupBroker) Subscribe(ctx context.Context, topic string, handler func(key, value []byte) error) error {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     b.brokers,
		Topic:       topic,
		GroupID:     b.groupID,
		StartOffset: b.startOffset,
	})
	defer r.Close()

	for {
		m, err := r.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("fetching from %q: %w", topic, err)
		}
		if err := handler(m.Key, m.Value); err != nil {
			return err
		}
		if err := r.CommitMessages(ctx, m); err != nil {
			return fmt.Errorf("committing offset for %q partition %d offset %d: %w", topic, m.Partition, m.Offset, err)
		}
	}
}

// --- realPusherBroadcaster: a real domains.BroadcastService, backed by ---
// --- sockudo (Pusher Protocol V1 compatible)                          ---
//
// Signing scheme and WS message shapes were verified against the live
// server (crypto/hmac query signing per sockudo's own http-endpoints.mdx,
// and the pusher:connection_established / pusher:subscribe /
// pusher_internal:subscription_succeeded frames per its protocol.mdx)
// before this was written, not assumed from generic Pusher docs.
type realPusherBroadcaster struct {
	httpBase, appID, appKey, appSecret string
}

func newRealPusherBroadcaster() *realPusherBroadcaster {
	return &realPusherBroadcaster{
		httpBase:  itPusherHTTPBase,
		appID:     itPusherAppID,
		appKey:    itPusherAppKey,
		appSecret: itPusherAppSecret,
	}
}

func (b *realPusherBroadcaster) sign(method, path string, query url.Values) string {
	keys := make([]string, 0, len(query))
	for k := range query {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+"="+query.Get(k))
	}
	toSign := method + "\n" + path + "\n" + strings.Join(parts, "&")
	mac := hmac.New(sha256.New, []byte(b.appSecret))
	mac.Write([]byte(toSign))
	return hex.EncodeToString(mac.Sum(nil))
}

// Broadcast implements domains.BroadcastService: one Pusher trigger call
// per (channel, event) pair, all carrying the same payload — matching how
// handleEvent calls it (one payload, possibly several channels/events).
func (b *realPusherBroadcaster) Broadcast(channels []domains.Channel, events domains.Events, payload any) error {
	dataJSON, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshalling payload: %w", err)
	}

	for _, ch := range channels {
		for _, ev := range events {
			if err := b.trigger(string(ch), ev, dataJSON); err != nil {
				return err
			}
		}
	}
	return nil
}

func (b *realPusherBroadcaster) trigger(channel, event string, dataJSON []byte) error {
	body, err := json.Marshal(map[string]any{
		"name":     event,
		"channels": []string{channel},
		"data":     string(dataJSON),
	})
	if err != nil {
		return fmt.Errorf("marshalling trigger body: %w", err)
	}
	bodyMD5 := md5.Sum(body)

	path := fmt.Sprintf("/apps/%s/events", b.appID)
	q := url.Values{}
	q.Set("auth_key", b.appKey)
	q.Set("auth_timestamp", strconv.FormatInt(time.Now().Unix(), 10))
	q.Set("auth_version", "1.0")
	q.Set("body_md5", hex.EncodeToString(bodyMD5[:]))
	q.Set("auth_signature", b.sign("POST", path, q))

	req, err := http.NewRequest(http.MethodPost, b.httpBase+path+"?"+q.Encode(), strings.NewReader(string(body)))
	if err != nil {
		return fmt.Errorf("building trigger request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("triggering event: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("trigger returned HTTP %d: %s", resp.StatusCode, string(b))
	}
	return nil
}

// pusherMessage is one frame received over the WS connection.
type pusherMessage struct {
	Event   string `json:"event"`
	Channel string `json:"channel"`
	Data    string `json:"data"`
}

// pusherSubscriber is a real Pusher Protocol V1 WebSocket client: it
// connects, subscribes to one channel, and forwards every subsequent
// frame on a channel tests can wait on — the only way to actually confirm
// "was this broadcasted" rather than just "was Broadcast() called".
type pusherSubscriber struct {
	conn  *websocket.Conn
	msgs  chan pusherMessage
	errs  chan error
	close func()
}

func newPusherSubscriber(t *testing.T, channel string) *pusherSubscriber {
	t.Helper()
	url := fmt.Sprintf("%s/app/%s?protocol=1", itPusherWSBase, itPusherAppKey)
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("dialing sockudo websocket: %v", err)
	}

	s := &pusherSubscriber{
		conn: conn,
		msgs: make(chan pusherMessage, 64),
		errs: make(chan error, 1),
	}
	s.close = func() { _ = conn.Close() }
	t.Cleanup(s.close)

	// pusher:connection_established
	var established pusherMessage
	if err := conn.ReadJSON(&established); err != nil {
		t.Fatalf("reading connection_established: %v", err)
	}
	if established.Event != "pusher:connection_established" {
		t.Fatalf("expected pusher:connection_established, got %q", established.Event)
	}

	sub, err := json.Marshal(map[string]any{
		"event": "pusher:subscribe",
		"data":  map[string]any{"channel": channel},
	})
	if err != nil {
		t.Fatalf("marshalling subscribe frame: %v", err)
	}
	if err := conn.WriteMessage(websocket.TextMessage, sub); err != nil {
		t.Fatalf("writing subscribe frame: %v", err)
	}

	var subAck pusherMessage
	if err := conn.ReadJSON(&subAck); err != nil {
		t.Fatalf("reading subscription_succeeded: %v", err)
	}
	if subAck.Event != "pusher_internal:subscription_succeeded" {
		t.Fatalf("expected pusher_internal:subscription_succeeded, got %q", subAck.Event)
	}

	go func() {
		for {
			var m pusherMessage
			if err := conn.ReadJSON(&m); err != nil {
				select {
				case s.errs <- err:
				default:
				}
				return
			}
			select {
			case s.msgs <- m:
			default:
			}
		}
	}()

	return s
}

// waitForEvent blocks until a frame with the given event name arrives (any
// other frames, e.g. pings, are ignored), or fails the test on timeout.
func (s *pusherSubscriber) waitForEvent(t *testing.T, event string, timeout time.Duration) pusherMessage {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case m := <-s.msgs:
			if m.Event == event {
				return m
			}
		case err := <-s.errs:
			t.Fatalf("websocket read error while waiting for %q: %v", event, err)
		case <-deadline:
			t.Fatalf("timed out waiting for event %q", event)
		}
	}
}
