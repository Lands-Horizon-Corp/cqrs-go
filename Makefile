.PHONY: test test-unit test-integration test-all test-load docker-up docker-down docker-logs

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
## Every test except the two chaos tests (which restart real containers —
## see integration_chaos_test.go) runs under t.Parallel(), each in its own
## Postgres schema so they can't corrupt each other's tables. -parallel is
## pinned to 8 explicitly (not left to default GOMAXPROCS) because the
## per-test connection pool size is budgeted against this exact number —
## see newPostgresSQLServiceInSchema's comment in integration_helpers_test.go
## before raising it. Go's test runner guarantees the non-parallel chaos
## tests run to full completion before any parallel test's body starts
## (verified empirically, not assumed), so this is safe as-is.
test-integration: docker-up
	go test -tags=integration ./src/regression/... -run TestIntegration -parallel 8 -v

## test-all: unit suite, then the real integration suite.
test-all: test-unit test-integration

## test-load: opt-in, large-scale create/update/delete throughput test.
## Defaults to 50k rows; override with N (and optionally UPDATE_N) for a
## bigger run, e.g.: make test-load N=1000000 UPDATE_N=50000
## A million-row run takes real minutes — this never runs as part of
## test-all or test-integration.
test-load: docker-up
	CQRS_IT_LOAD_N=$(or $(N),50000) CQRS_IT_LOAD_UPDATE_N=$(or $(UPDATE_N),5000) \
		go test -tags="integration load" ./src/regression/... -run TestLoad -v -timeout 30m
