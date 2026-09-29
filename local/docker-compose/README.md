# Local CQRS infra (Debezium + 2x Postgres)

A local dev stack for exercising the real CDC pipeline this repo is built
around: a write-side Postgres, a read-side Postgres, and Kafka + Debezium
watching the write DB and publishing changes.

## What's in it

| Service | Purpose | Host port |
|---|---|---|
| `postgres-write` | Command/write DB. `wal_level=logical` set for CDC. | `15433` → 5432 |
| `postgres-read` | Query/read DB, updated only via the CDC pipeline. | `15434` → 5432 |
| `zookeeper` | Kafka coordination. | `12181` |
| `kafka` | Broker Debezium publishes to. | `29092` (external listener — see below) |
| `connect` | Kafka Connect running the Debezium Postgres connector. | `18083` (REST API) |
| `connector-register` | One-shot job that registers the connector against `connect`, then exits. | — |

Ports are deliberately namespaced away from the common defaults
(5432-5434, 2181, 8083, 9092) so this doesn't collide with other local
Postgres/Kafka instances you may already have running.

Kafka runs **two listeners**: `INTERNAL` (`kafka:9092`, used by `connect`
over the Docker network) and `EXTERNAL` (`localhost:29092`, used by
anything connecting from the host — e.g. the Go integration tests). This
matters because Kafka clients don't just connect to the bootstrap address
you give them; after the initial handshake the broker tells them the
*advertised* address to use for actual reads/writes, and a host process
can't resolve the internal-only hostname `kafka`. If you ever see a client
connect successfully but then hang or fail on the first fetch/produce,
this is almost always why — check both `KAFKA_LISTENERS` and
`KAFKA_ADVERTISED_LISTENERS` in `docker-compose.yml`.

Both Postgres databases run `pgvector/pgvector:pg16` (official Postgres +
pgvector extension pre-built) and get `pgcrypto` and `vector` created
automatically on first start, via `postgres-write-init/01-extensions.sql`
and `postgres-read-init/01-extensions.sql` — Postgres only runs files in
`/docker-entrypoint-initdb.d` the very first time a fresh data volume is
initialized, so editing those `.sql` files after the first `up` won't do
anything until you also drop the corresponding volume.

Credentials are `postgres` / `postgres` for local dev only — do not reuse
these anywhere real.

## Usage

```sh
cd local/docker-compose
docker compose up -d
```

Everything (both Postgres instances, Zookeeper, Kafka, Connect) starts,
then `connector-register` waits for Connect's REST API to come up and
registers `connector-postgres-write.json` against it. Check it landed:

```sh
curl -s localhost:18083/connectors/cqrs-write-db-connector/status | jq .
```

Connect from the host:

```sh
psql "postgresql://postgres:postgres@localhost:15433/cqrs_write"
psql "postgresql://postgres:postgres@localhost:15434/cqrs_read"
```

Tear down (and wipe the data volumes, so init scripts re-run next time):

```sh
docker compose down -v
```

## Debezium's real output vs. what `CQRSImpl.Run` consumes

`connector-postgres-write.json` runs Debezium's **default** envelope shape
— `{before, after, source, op, ts_ms}` — because that's what a real
Debezium deployment actually produces. `CQRSImpl.Run` (`src/cqrs/cqrs.run.go`)
only understands its own `{event_id, change_type, payload}` shape
(`domains.CQRSQueuePayload`), so it can't consume the connector's raw topic
(`cqrs.public.<table>`) directly.

The bridge between them is `src/debezium`:
- `debezium.Transform[T]` turns one raw Debezium record into
  `domains.CQRSQueuePayload[T]` — captured and verified against this
  connector's real live output, including the caveat that a delete
  record's `before` only reliably carries the primary key under the
  default `REPLICA IDENTITY` (see the package doc for why that's fine for
  this library's own needs).
- `debezium.RunBridge` wires a source topic (the connector's raw output)
  through `Transform` to a destination topic (whatever `Run`'s `Channel`
  is actually subscribed to).

`src/regression/integration_debezium_test.go`
(`TestIntegration_HappyPath_RealDebeziumThroughBridgeAlignsRealPostgresRead`)
proves the full chain end to end against this exact stack: a real write to
`postgres-write`, captured for real by this connector, transformed by
`RunBridge`, consumed by a real `CQRSImpl.Run`, landing in `postgres-read`
— no hand-built envelope anywhere in that path. Run it with
`go test -tags=integration ./src/regression/... -run TestIntegration_HappyPath_RealDebeziumThroughBridgeAlignsRealPostgresRead -v`.

## If an image pull fails

The Debezium images are pinned to `2.7.3.Final` (verified against Docker
Hub's tag list — Debezium doesn't publish a bare `2.7` floating tag, only
full versions like this). If that tag is gone by the time you run this,
check https://hub.docker.com/r/debezium/connect/tags
for a current tag (or the `quay.io/debezium/*` mirror) and update the
`image:` lines in `docker-compose.yml` for `zookeeper`, `kafka`, and
`connect` together — they're released in lockstep and must match.
