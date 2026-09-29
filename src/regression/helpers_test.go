// Package regression holds this project's full test suite as black-box
// tests against the public API of src/cqrs, src/utils and src/domains —
// nothing in this package reaches into an unexported symbol of those
// packages. That's a deliberate constraint, not an accident: it's what
// lets every test in the project live under this one directory instead of
// being split across each package's own folder.
package regression

import (
	"context"
	"database/sql"
	"sync"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/cqrs"
	"github.com/Lands-Horizon-Corp/cqrs-go/src/domains"
	"github.com/segmentio/kafka-go"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/sqlitedialect"

	_ "modernc.org/sqlite"
)

// widget is the entity shared by every test in this package. It carries
// several different pointer-typed columns (bool/string/int/time), not just
// one, so "nil pointer" coverage isn't limited to a single field shape —
// this is the "complex data" entity referenced throughout the suite.
type widget struct {
	bun.BaseModel `bun:"table:widgets"`

	ID        string     `bun:"id,pk" json:"id"`
	Name      string     `bun:"name,notnull" json:"name" validate:"required"`
	Active    bool       `bun:"active,notnull" json:"active"`
	Featured  *bool      `bun:"featured" json:"featured"`
	Notes     *string    `bun:"notes" json:"notes"`
	Priority  *int       `bun:"priority" json:"priority"`
	ExpiresAt *time.Time `bun:"expires_at" json:"expires_at"`
	UpdatedAt time.Time  `bun:"updated_at,nullzero" json:"updated_at"`
}

type widgetResource struct {
	ID        string     `json:"id"`
	Name      string     `json:"name"`
	Active    bool       `json:"active"`
	Featured  *bool      `json:"featured"`
	Notes     *string    `json:"notes"`
	Priority  *int       `json:"priority"`
	ExpiresAt *time.Time `json:"expires_at"`
}

func widgetToResource(w *widget) *widgetResource {
	return &widgetResource{
		ID:        w.ID,
		Name:      w.Name,
		Active:    w.Active,
		Featured:  w.Featured,
		Notes:     w.Notes,
		Priority:  w.Priority,
		ExpiresAt: w.ExpiresAt,
	}
}

//go:fix inline
func boolPtr(b bool) *bool { return new(b) }

//go:fix inline
func stringPtr(s string) *string { return new(s) }

//go:fix inline
func intPtr(i int) *int { return new(i) }

//go:fix inline
func timePtr(t time.Time) *time.Time { return new(t) }

// --- fake SQLService backed by a private in-memory SQLite database ---
//
// Plain ":memory:" (no cache=shared) plus a single-connection pool gives
// each fakeSQLService its own isolated database for the lifetime of the
// test, with no DSN-collision risk between concurrently running tests or
// between a test's separate write/read fakes.

type fakeSQLService struct {
	db *bun.DB
}

func newFakeSQLService(t *testing.T) *fakeSQLService {
	t.Helper()
	sqldb, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("opening sqlite: %v", err)
	}
	sqldb.SetMaxOpenConns(1) // keep every query on the same connection/db

	db := bun.NewDB(sqldb, sqlitedialect.New())
	t.Cleanup(func() { _ = db.Close() })

	ctx := context.Background()
	if _, err := db.NewCreateTable().Model((*widget)(nil)).Exec(ctx); err != nil {
		t.Fatalf("creating widgets table: %v", err)
	}
	if _, err := db.NewCreateTable().Model((*domains.ProcessedEvent)(nil)).Exec(ctx); err != nil {
		t.Fatalf("creating processed_events table: %v", err)
	}

	return &fakeSQLService{db: db}
}

func (f *fakeSQLService) Ping(ctx context.Context) error { return f.db.PingContext(ctx) }
func (f *fakeSQLService) Client() *bun.DB                { return f.db }

// --- fake LogService: records every call, for asserting on error paths ---

type logCall struct {
	level string
	msg   string
}

type fakeLogService struct {
	mu    sync.Mutex
	calls []logCall
}

func (f *fakeLogService) record(level, msg string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, logCall{level: level, msg: msg})
}

func (f *fakeLogService) Log(_ context.Context, msg string)     { f.record("info", msg) }
func (f *fakeLogService) Error(_ context.Context, msg string)   { f.record("error", msg) }
func (f *fakeLogService) Warn(_ context.Context, msg string)    { f.record("warn", msg) }
func (f *fakeLogService) Panic(_ context.Context, msg string)   { f.record("panic", msg) }
func (f *fakeLogService) Success(_ context.Context, msg string) { f.record("success", msg) }

func (f *fakeLogService) snapshot() []logCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]logCall, len(f.calls))
	copy(out, f.calls)
	return out
}

// --- fake BroadcastService: records every call and lets tests wait on it ---
//
// OnCreated/OnUpdated/OnDeleted run their work in a separate goroutine, so
// tests need a way to wait for a Broadcast call to land instead of
// asserting immediately after the triggering call returns.

type broadcastCall struct {
	channels []domains.Channel
	events   domains.Events
	payload  any
}

type fakeBroadcastService struct {
	mu      sync.Mutex
	calls   []broadcastCall
	notify  chan struct{}
	failErr error // when set, Broadcast returns this instead of recording success
}

func newFakeBroadcastService() *fakeBroadcastService {
	return &fakeBroadcastService{notify: make(chan struct{}, 64)}
}

func (f *fakeBroadcastService) Broadcast(channels []domains.Channel, events domains.Events, payload any) error {
	f.mu.Lock()
	failErr := f.failErr
	f.calls = append(f.calls, broadcastCall{channels: channels, events: events, payload: payload})
	f.mu.Unlock()
	select {
	case f.notify <- struct{}{}:
	default:
	}
	return failErr
}

func (f *fakeBroadcastService) setFailure(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failErr = err
}

func (f *fakeBroadcastService) snapshot() []broadcastCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]broadcastCall, len(f.calls))
	copy(out, f.calls)
	return out
}

// waitForCall blocks until at least one Broadcast call has landed since the
// last call to waitForCall/drain, or fails the test after timeout.
func (f *fakeBroadcastService) waitForCall(t *testing.T, timeout time.Duration) {
	t.Helper()
	select {
	case <-f.notify:
	case <-time.After(timeout):
		t.Fatal("timed out waiting for BroadcastService.Broadcast call")
	}
}

// --- fake MessageBrokerService: simulates Debezium/Kafka without a broker ---
//
// Subscribe registers the handler and blocks, exactly like a real Kafka
// consumer loop would. Emit then delivers a single (key, value) pair to
// that handler synchronously, as if Debezium had published one CDC change
// event for this topic.

type fakeMessageBroker struct {
	mu      sync.Mutex
	handler func(key, value []byte) error
	subDone chan struct{}
	once    sync.Once
}

func newFakeMessageBroker() *fakeMessageBroker {
	return &fakeMessageBroker{subDone: make(chan struct{})}
}

func (f *fakeMessageBroker) Client() *kafka.Client { return nil }

func (f *fakeMessageBroker) Publish(_ context.Context, _ string, _, _ []byte) error {
	return nil
}

func (f *fakeMessageBroker) Subscribe(ctx context.Context, _ string, handler func(key, value []byte) error) error {
	f.mu.Lock()
	f.handler = handler
	f.mu.Unlock()
	f.once.Do(func() { close(f.subDone) })
	<-ctx.Done()
	return ctx.Err()
}

// Emit waits for Subscribe to have registered a handler, then delivers a
// single message to it.
func (f *fakeMessageBroker) Emit(t *testing.T, key, value []byte) {
	t.Helper()
	select {
	case <-f.subDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Emit: Subscribe was never called")
	}
	f.mu.Lock()
	h := f.handler
	f.mu.Unlock()
	if h == nil {
		t.Fatal("Emit: handler is nil")
	}
	if err := h(key, value); err != nil {
		t.Fatalf("handler returned error: %v", err)
	}
}

// --- shared CQRSImpl construction helpers ---

// newTestCQRS builds a write-only engine (no ReadSQLService/broker/broadcast
// wired up) for the plain Create/Update/Delete tests that don't need the
// CDC pipeline.
func newTestCQRS(t *testing.T) (*cqrs.CQRSImpl[widget, widgetResource, any, string], *fakeSQLService) {
	t.Helper()
	write := newFakeSQLService(t)
	c := cqrs.NewCQRS(cqrs.CQRSImpl[widget, widgetResource, any, string]{
		WriteSQLService: write,
		ToResource:      widgetToResource,
	})
	return c, write
}

// cqrsWithNilEventsCallback builds an engine with ToResource set but no
// Created/Updated/Deleted/Dispatch/BroadcastService — used to prove
// handleEvent's nil-Events / nil-Dispatch / nil-BroadcastService branches
// don't panic.
func cqrsWithNilEventsCallback(t *testing.T, write *fakeSQLService) *cqrs.CQRSImpl[widget, widgetResource, any, string] {
	t.Helper()
	return cqrs.NewCQRS(cqrs.CQRSImpl[widget, widgetResource, any, string]{
		WriteSQLService: write,
		ToResource:      widgetToResource,
	})
}

// newCQRSNoResource builds an engine with no ToResource set, for tests that
// exercise the "ToResource is nil" branches.
func newCQRSNoResource(t *testing.T, write *fakeSQLService) *cqrs.CQRSImpl[widget, widgetResource, any, string] {
	t.Helper()
	return cqrs.NewCQRS(cqrs.CQRSImpl[widget, widgetResource, any, string]{
		WriteSQLService: write,
	})
}

// cdcHarness wires a CQRSImpl with separate write/read fake SQLite
// databases plus recording fakes for logging, broadcasting and the message
// broker (our stand-in for Debezium/Kafka).
type cdcHarness struct {
	c         *cqrs.CQRSImpl[widget, widgetResource, any, string]
	read      *fakeSQLService
	write     *fakeSQLService
	broadcast *fakeBroadcastService
	logs      *fakeLogService
	broker    *fakeMessageBroker
}

func newCDCHarness(t *testing.T, batchSize int) *cdcHarness {
	t.Helper()
	read := newFakeSQLService(t)
	write := newFakeSQLService(t)
	broadcast := newFakeBroadcastService()
	logs := &fakeLogService{}
	broker := newFakeMessageBroker()

	c := cqrs.NewCQRS(cqrs.CQRSImpl[widget, widgetResource, any, string]{
		Channel:              "widgets",
		WriteSQLService:      write,
		ReadSQLService:       read,
		MessageBrokerService: broker,
		BroadcastService:     broadcast,
		LogService:           logs,
		ToResource:           widgetToResource,
		Created:              func(*widget) domains.Events { return domains.Events{"widget.created"} },
		Updated:              func(*widget) domains.Events { return domains.Events{"widget.updated"} },
		Deleted:              func(*widget) domains.Events { return domains.Events{"widget.deleted"} },
		BatchSize:            batchSize,
	})
	return &cdcHarness{c: c, read: read, write: write, broadcast: broadcast, logs: logs, broker: broker}
}

// runInBackground starts h.c.Run(ctx) in a goroutine and returns a channel
// that receives its eventual return value, so tests can drive the fake
// broker and assert while Run blocks on Subscribe.
func (h *cdcHarness) runInBackground(ctx context.Context) <-chan error {
	done := make(chan error, 1)
	go func() { done <- h.c.Run(ctx) }()
	return done
}

func (h *cdcHarness) waitForRunToStop(t *testing.T, done <-chan error, timeout time.Duration) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(timeout):
		t.Fatal("Run did not return after context cancellation")
	}
}

func seedWidget(t *testing.T, c *cqrs.CQRSImpl[widget, widgetResource, any, string], w widget) {
	t.Helper()
	if _, err := c.Create(context.Background(), w); err != nil {
		t.Fatalf("seeding widget: %v", err)
	}
}

func readWidgetFrom(t *testing.T, svc *fakeSQLService, id string) (widget, bool) {
	t.Helper()
	var got widget
	err := svc.db.NewSelect().Model(&got).Where("id = ?", id).Scan(context.Background())
	if err != nil {
		return widget{}, false
	}
	return got, true
}

// dropWidgetsTable and dropProcessedEventsTable let a test force a generic
// SQL-level failure (distinct from ErrNoRows/validation) on a specific
// table, to exercise the DB-error branches that a live constraint
// violation would otherwise be needed for.
func dropWidgetsTable(t *testing.T, svc *fakeSQLService) {
	t.Helper()
	if _, err := svc.db.NewDropTable().Model((*widget)(nil)).Exec(context.Background()); err != nil {
		t.Fatalf("dropping widgets table: %v", err)
	}
}

func dropProcessedEventsTable(t *testing.T, svc *fakeSQLService) {
	t.Helper()
	if _, err := svc.db.NewDropTable().Model((*domains.ProcessedEvent)(nil)).Exec(context.Background()); err != nil {
		t.Fatalf("dropping processed_events table: %v", err)
	}
}
