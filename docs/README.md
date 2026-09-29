# cqrs-go

A generic, type-safe Go implementation of the CQRS (Command Query Responsibility
Segregation) pattern, with an event-driven read-model sync fed by CDC (Debezium
over Kafka) and realtime broadcasting of applied changes.

## The pattern, in one picture

```
                 ┌──────────────┐
   Create/       │              │
   Update/  ───► │ WriteSQLService  (command side / write DB)
   Delete        │              │
                 └──────┬───────┘
                        │  Debezium CDC
                        ▼
                 ┌──────────────┐
                 │    Kafka     │
                 └──────┬───────┘
                        │  MessageBrokerService.Subscribe
                        ▼
                 ┌──────────────┐        ┌──────────────────┐
                 │  Run() /     │──────► │  ReadSQLService   │ (query side / read DB)
                 │  Batcher     │        └──────────────────┘
                 └──────┬───────┘
                        │  per applied change
                        ▼
             OnCreated / OnUpdated / OnDeleted
                        │
              ┌─────────┴─────────┐
              ▼                   ▼
          Dispatch()        BroadcastService
        (app-level hook)   (Pusher/webhook/websocket push)
```

Writes go straight to the write database. A CDC pipeline (Debezium watching the
write DB, publishing to Kafka) is what actually propagates each change; this
library's `Run()` consumes that Kafka topic, batches the incoming change
events, and replicates them into the read database. Once a batch of changes
has been applied to the read DB, each change is handed to a callback
(`Created`/`Updated`/`Deleted`) that decides what to broadcast, and the result
is pushed out via `Dispatch` and/or `BroadcastService`.

## Package layout

| Package | Responsibility |
|---|---|
| `src/cqrs` | The `CQRSImpl` engine: `Create`/`Update`/`Delete` (write path), `Run`/`processBatch`/`syncBatchToReadDB` (CDC ingestion + read-model sync), `OnCreated`/`OnUpdated`/`OnDeleted` (broadcast path), log helpers. |
| `src/domains` | Shared types: `Channel`, `Events`, `ChangeType`, `CQRSQueuePayload[T]` (the CDC envelope), `ProcessedEvent` (idempotency ledger), and the service interfaces (`SQLService`, `LogService`, `BroadcastService`, `MessageBrokerService`). |
| `src/utils` | Generic infrastructure used on the hot batching path: `Batcher` (channel + ticker batching), `BufferPool`/`MapPool` (`sync.Pool` wrappers to cut allocations), `BunColumnFieldIndex`/`FieldValueAt` (reflection over a `bun` struct tag, resolved once and cached rather than re-scanned per message). |
| `src/debezium` | Bridges a real Debezium Postgres connector's actual output into what `Run` consumes: `Transform[T]` (Debezium's `{before,after,source,op}` envelope → `domains.CQRSQueuePayload[T]`) and `RunBridge` (wires a raw Debezium topic through `Transform` to the topic `Run` subscribes to). |
| `src/kafka` | Production `domains.MessageBrokerService` backed by `segmentio/kafka-go` — real consumer group, offset committed only after a message is successfully handled (not on read), retry on Kafka's metadata-propagation race on publish. |
| `src/pusher` | Production `domains.BroadcastService` for a Pusher Protocol V1 compatible server (e.g. sockudo) — signed HTTP trigger calls, verified against a live server. |

## `CQRSImpl[TData, TResponse, TRequest, TID]`

Type parameters:

- **`TData`** — the write-model / DB row shape (a `bun`-tagged struct).
- **`TResponse`** — the API resource shape returned to callers. `ToResource`
  converts `TData` → `*TResponse`.
- **`TRequest`** — reserved for request/DTO validation; not yet wired to any
  method.
- **`TID`** — the primary key type used by the by-ID `Update`/`Delete` calls.

Construction is via `NewCQRS(...)`, which applies defaults
(`ColumnDefaultID` → `"id"`, `ColumnDefaultSort` → `"updated_at DESC"`,
`Channel` → `"default"`, `BatchSize` → `100`, `FlushInterval` → `5s`), panics
if `WriteSQLService` is nil, and resolves `idFieldIndex` once up front (see
[Entity coalescing](#idempotency--entity-level-coalescing) below) instead of
on every message. `BatchSize`/`FlushInterval` are exported config fields, so
callers outside the package can actually set them — they weren't for a while,
which meant every `CQRSImpl` silently used the 100/5s defaults regardless of
what was passed in.

### Config fields

| Field | Purpose |
|---|---|
| `Channel` | Kafka topic to subscribe on **and** the broadcast channel name used when pushing events out. |
| `ColumnDefaultID` | DB column name for the primary key (default `"id"`). Used both for `WHERE`/`WherePK()` clauses and to resolve which `TData` field is the entity ID. |
| `ColumnDefaultSort` | Default ordering for read queries (default `"updated_at DESC"`). |
| `Preloads` | Reserved for relation preloading; not yet consumed. |
| `ToResource func(*TData) *TResponse` | Converts a write-model row into the API resource. Called after every write, and after every CDC-applied change, before broadcasting. |
| `TocCSV func(*TData) *map[string]any` | Row → column-map converter for CSV export. Not used internally by the engine; available for callers. |
| `Created / Updated / Deleted func(*TData) domains.Events` | Per-change-type hook. Return the event names this change should be published under; return an empty/nil slice to suppress broadcasting for that specific change. |
| `Dispatch func(channel, events, payload) error` | Optional application-level pub/sub hook, called alongside `BroadcastService`. |
| `ReadSQLService` / `WriteSQLService` | The two halves of the CQRS split — query side and command side. |
| `LogService` | Structured logging (`info`/`error`/`warn`/`success`). |
| `BroadcastService` | Realtime fan-out — e.g. Pusher, websockets, or webhooks. |
| `MessageBrokerService` | Kafka client; `Run()` uses it to `Subscribe` to the CDC topic. |
| `Validator` | `go-playground/validator`; run against the payload in `Create`/`Update`. |

## Write path (commands)

Each of these validates (if `Validator` is set), executes against
`WriteSQLService` (or a supplied `bun.Tx` for the `*WithTx` variants), and
converts the result via `ToResource`:

- `Create`, `CreateMany`, `CreateWithTx`, `CreateManyWithTx`
- `UpdateByID`, `UpdateByIDWithTx`, `UpdateMany`, `UpdateManyWithTx` (bulk
  update via bun's `Bulk()` — one `UPDATE ... FROM VALUES(...)` statement
  matched by primary key, not one round trip per row)
- `DeleteByID`, `DeleteByIDWithTx`, `DeleteMany`, `DeleteManyWithTx`

These do **not** go through the CDC/broadcast pipeline directly — that only
fires once Debezium has captured the change and it has round-tripped through
Kafka into `Run()`. There is currently no generic `Get`/`List`/`Read` on
`CQRSImpl`; reads are expected to be served directly off `ReadSQLService`
outside this engine.

## The CDC ingestion loop (`Run`)

1. Pings `WriteSQLService` (and `ReadSQLService`, if set); panics if
   unreachable.
2. Starts a `utils.Batcher[domains.CQRSQueuePayload[TData]]` — items pushed
   in are flushed either when `BatchSize` is reached or every
   `FlushInterval`, whichever comes first (see [batching.go](../src/utils/batching.go)).
3. `MessageBrokerService.Subscribe`s to `Channel`; every Kafka message is
   unmarshalled into a `CQRSQueuePayload[TData]` envelope and pushed into the
   batcher.
4. Each flushed batch goes through `processBatch` → `syncBatchToReadDB`.

### `syncBatchToReadDB`

For each flushed batch:

1. Dedupe by `EventID` within the batch (Kafka/CDC redelivery guard).
2. Query `processed_events` for which of those `EventID`s were already
   applied; skip anything already processed (idempotency).
3. For everything new, collapse to **one entry per entity** — see below —
   and split into upsert vs. delete sets.
4. In a single transaction: insert the `processed_events` audit rows, bulk
   upsert (`ON CONFLICT DO UPDATE`), bulk delete (`WherePK`).
5. Return the applied messages so `processBatch` can fire the per-change
   callbacks.

### Idempotency & entity-level coalescing

If the same row changes multiple times within one flushed batch (e.g. an
update immediately followed by a delete), only the **last** `ChangeType` for
that entity is applied — this avoids redundant writes and avoids applying a
stale intermediate state. The entity key is the `TData` field whose `bun` tag
matches `ColumnDefaultID`, resolved **once** at construction time
(`utils.BunColumnFieldIndex`, cached as `idFieldIndex`) rather than
re-scanned via reflection per message; if no matching field is found, the
`EventID` is used as a fallback key.

## Broadcast path (`OnCreated` / `OnUpdated` / `OnDeleted`)

For each applied message, `processBatch` calls the matching `On*` method,
which:

1. Bails out if `ToResource` is nil or the data is nil.
2. Runs asynchronously (own goroutine, with panic recovery logged via
   `LogService`, and a context detached from cancellation via
   `context.WithoutCancel` so in-flight broadcasts survive shutdown).
3. Converts the row via `ToResource`.
4. Calls the matching `Created`/`Updated`/`Deleted` callback to get the
   `domains.Events` to publish; if empty, nothing is broadcast.
5. Calls `Dispatch` (if set) and `BroadcastService.Broadcast` (if set) with
   the channel, events, and resource payload.

## Supporting infrastructure (`src/utils`)

- **`Batcher[T]`** — generic channel-based batcher backing `Run()`. Flushes
  on size or on a ticker; on `ctx` cancellation it drains whatever's left
  using an uncancelled context so in-flight DB writes still complete.
- **`BufferPool[T]` / `MapPool[K, V]`** — `sync.Pool` wrappers used to avoid
  allocating fresh slices/maps for every batch (`stringSlicePool`,
  `stringSetPool`, `processedEventsPool` on `CQRSImpl`). Oversized items
  (`cap`/`len` > 10000) are dropped rather than pooled, and pooled items are
  cleared before reuse so they don't pin stale data.
- **`BunColumnFieldIndex[T]` / `FieldValueAt[T]`** — resolves a struct
  field's index from its `bun` tag once, then reads it by index thereafter;
  avoids re-scanning struct tags on every message in the hot batching path.

## Production adapters (`src/debezium`, `src/kafka`, `src/pusher`)

Real, production-quality implementations of the interfaces `domains`
declares — not test doubles:

- **`src/debezium`** closes the gap that every earlier version of this
  project's test suite had: nothing had ever verified what a real Debezium
  connector actually publishes, or transformed it into what `Run`
  consumes. `Transform[T]` and `RunBridge` do that, verified against a live
  connector (see `local/docker-compose`) and proven end-to-end by
  `src/regression/integration_debezium_test.go` — a real write, captured
  by a real Debezium connector, bridged, and consumed by a real `Run`,
  with no hand-built envelope anywhere in that path.
- **`src/kafka.Broker`** is a real `MessageBrokerService`: consumer group
  with explicit offset commits (only after a message is successfully
  handled, so a crash mid-processing redelivers rather than loses it), and
  retries the specific, well-understood kafka-go metadata-propagation race
  on publish. It does not include TLS/SASL auth or general retry/backoff
  beyond that one case — add those via kafka-go's `Dialer`/`Transport` if
  a deployment needs them.
- **`src/pusher.Broadcaster`** is a real `BroadcastService` for any Pusher
  Protocol V1 compatible server (sockudo, soketi, or Pusher itself):
  signed HTTP trigger calls, request-level timeout, verified against a
  live server.

## Operational behavior under real failure (chaos tests)

`src/regression/integration_chaos_test.go` restarts real containers
mid-test and reports what actually happens, rather than assuming a client
library's documented behavior holds for this stack:

- **Postgres restart**: `Create` recovers on its own once `postgres-write`
  is back — `database/sql`'s pool transparently dials a fresh connection.
- **Kafka broker restart**: a real consumer group (`src/kafka.Broker`)
  resumes on its own once the broker is back up, with no supervision code
  needed around it.

This covers two concrete scenarios, not exhaustive chaos coverage —
connector crash-recovery, sockudo/broadcast-side failures, and network
partitions (as opposed to full container restarts) aren't exercised here.

## Testing

- `make test` / `make test-unit` — the normal suite (`src/regression`,
  ~99% statement coverage of `src/cqrs`+`src/utils`+`src/domains`), no
  Docker required.
- `make test-integration` — brings up `local/docker-compose` (blocking on
  `docker compose up --wait` until every service is actually healthy, not
  just started) and runs the real-infra suite against it: real Postgres
  x2, real Kafka, real Debezium, real sockudo.
- `make test-load` — bulk create/update/delete throughput at configurable
  scale (`make test-load N=1000000 UPDATE_N=50000` for a literal
  million-row run; defaults to 50k/5k). Logs write/publish/sync throughput
  per phase and asserts both databases end correctly empty.

## Known gaps

- `TRequest` and `Preloads` are declared but not yet consumed anywhere.
- No generic read/list method on `CQRSImpl` — reads go directly against
  `ReadSQLService`.
- No TLS/SASL support in `src/kafka.Broker`; add via kafka-go's
  `Dialer`/`Transport` if needed.
- Chaos coverage is scoped to Postgres and Kafka broker restarts (see
  above) — connector crash-recovery and broadcast-side failures aren't
  covered yet.
