.PHONY: test test-unit test-integration test-all test-load test-ledger-load test-ledger-bench docker-up docker-down docker-logs

COMPOSE := cd local/docker-compose && docker compose

# Default: fast, no Docker required.
test: test-unit

## test-unit: the normal suite — no Docker, no network, safe in CI.
## Every test runs under t.Parallel() (each on its own isolated in-memory
## SQLite DB, so there's nothing for them to contend over).
test-unit:
	go test -parallel 8 ./...

## docker-up: starts local/docker-compose and blocks until every long-running
## service reports healthy (docker compose --wait) — not just "started", so
## whatever runs after this is talking to real, ready services, not racing
## container startup. connector-register is a one-shot job (it registers the
## Debezium connector then exits 0), so it's excluded from --wait — that flag
## treats "exited" as a failure regardless of exit code — and run separately
## once its dependency (connect) is already confirmed healthy.
docker-up:
	$(COMPOSE) up -d --wait postgres-write postgres-read zookeeper kafka connect sockudo
	$(COMPOSE) run --rm connector-register

## docker-down: tears the stack down and wipes its volumes, so the next
## docker-up starts from a clean slate (init scripts re-run, etc).
docker-down:
	$(COMPOSE) down -v

docker-logs:
	$(COMPOSE) logs -f

## test-integration: brings up the real stack (Postgres x2, Kafka,
## Debezium, sockudo) via docker-up, then runs the integration suite
## against those real services. Because docker-up blocks on --wait,
## a pass here means the tests genuinely exercised the real local
## infrastructure, not a silent skip.
##
## Covers both the CDC/widget suite (TestIntegration...) and the
## transaction-layer ledger suite (TestLedger...  — see
## integration_ledger_helpers_test.go): Count/Exists/Find/FindOne/GetByID/
## Max/Min/Start/End/IncrementByID/row-locking only get their real
## concurrency/ACID/deadlock coverage here, against real Postgres — the
## plain SQLite-backed `go test ./...` suite can't exercise row-level
## locking or deadlocks at all (SQLite has no FOR UPDATE support).
##
## Every test except the chaos tests (which restart real containers — see
## integration_chaos_test.go and TestLedgerACID_FaultInjection... in
## integration_ledger_acid_test.go) runs under t.Parallel(), each in its
## own Postgres schema so they can't corrupt each other's tables.
## -parallel is pinned to 8 explicitly (not left to default GOMAXPROCS)
## because every test's connection pool size is budgeted against this
## exact number and against max_connections=100 on both Postgres
## instances (verified directly, not assumed) — see
## newPostgresSQLServiceInSchema's comment in integration_helpers_test.go
## and newLedgerPostgres's pool-size choices in
## integration_ledger_helpers_test.go before raising it. Go's test runner
## guarantees the non-parallel chaos tests run to full completion before
## any parallel test's body starts (verified empirically, not assumed),
## so this is safe as-is.
test-integration: docker-up
	go test -tags=integration ./src/regression/... -run 'TestIntegration|TestLedger' -parallel 8 -v

## test-all: everything, in order: unit suite, real integration suite, then
## the opt-in load, ledger-load and ledger-bench runs. docker-up is a shared
## prerequisite, so the stack comes up once. Takes real minutes; use
## test-unit or test-integration for a faster run. N, UPDATE_N and SECONDS
## still override the load sizes, e.g. make test-all N=100000 SECONDS=60
test-all: test-unit test-integration test-load test-ledger-load test-ledger-bench

## test-load: opt-in, large-scale create/update/delete throughput test
## (CDC pipeline — bulk Create/Update/Delete plus Debezium/Kafka
## replication to the read db). Defaults to 50k rows; override with N
## (and optionally UPDATE_N) for a bigger run, e.g.:
## make test-load N=1000000 UPDATE_N=50000
## A million-row run takes real minutes. It runs as part of test-all with
## the default sizes, but never as part of test-integration.
test-load: docker-up
	CQRS_IT_LOAD_N=$(or $(N),50000) CQRS_IT_LOAD_UPDATE_N=$(or $(UPDATE_N),5000) \
		go test -tags="integration load" ./src/regression/... -run TestLoad -v -timeout 30m

## test-ledger-load: opt-in, sustained-throughput/soak test for the
## transaction layer itself (concurrent transfers against a real ledger
## schema, not the CDC pipeline — see ledger_load_test.go). Defaults to a
## 10s window; override with SECONDS for a longer soak run, e.g.:
## make test-ledger-load SECONDS=120
test-ledger-load: docker-up
	CQRS_IT_LEDGER_LOAD_SECONDS=$(or $(SECONDS),10) \
		go test -tags="integration load" ./src/regression/... -run TestLedgerLoad -v -timeout 10m

## test-ledger-bench: opt-in benchmark of the transaction layer (transfer
## throughput/latency against real Postgres — see ledger_bench_test.go).
## Inspired by TPC-B's debit/credit transaction shape, not a literal
## TPC-B/TPC-C implementation — see that file's own doc comment.
test-ledger-bench: docker-up
	go test -tags=integration ./src/regression/... -bench Ledger -run '^$$' -benchtime=3s
