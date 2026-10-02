# Querying, filtering, and transactions (`src/pagination` + `CQRSImpl`)

[README.md](README.md) covers the write path and the CDC pipeline. This
doc covers the other half: how to actually **read** data back out —
filtering, pagination, single-row lookups, aggregates — and how to run
**real transactions** against the write database, safely, under real
concurrency. Everything here is backed by `src/pagination`, exposed on
`CQRSImpl` as a thin, pre-wired convenience layer.

## Quickstart with a real model

```go
package main

import (
	"context"
	"database/sql"
	"log"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/driver/pgdriver"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/cqrs"
	"github.com/Lands-Horizon-Corp/cqrs-go/src/domains"
)

// Product is an ordinary bun model — nothing CQRS-specific about the
// struct itself. `bun:"id,pk"` is what ColumnDefaultID ("id" by default)
// resolves against.
type Product struct {
	bun.BaseModel `bun:"table:products"`
	ID            string    `bun:"id,pk"`
	Name          string    `bun:"name,notnull"`
	PriceCents    int64     `bun:"price_cents,notnull"`
	StockQty      int       `bun:"stock_qty,notnull"`
	CategoryID    string    `bun:"category_id"`
	Category      *Category `bun:"rel:belongs-to,join:category_id=id"`
}

type Category struct {
	bun.BaseModel `bun:"table:categories"`
	ID            string `bun:"id,pk"`
	Name          string `bun:"name"`
}

// ProductResource is the API-shaped view ToResource converts into.
type ProductResource struct {
	ID       string
	Name     string
	Price    float64
	InStock  bool
}

func productToResource(p *Product) *ProductResource {
	return &ProductResource{
		ID:      p.ID,
		Name:    p.Name,
		Price:   float64(p.PriceCents) / 100,
		InStock: p.StockQty > 0,
	}
}

type myDB struct{ db *bun.DB }

func (m *myDB) Ping(ctx context.Context) error { return m.db.PingContext(ctx) }
func (m *myDB) Client() *bun.DB                { return m.db }

func main() {
	sqldb := sql.OpenDB(pgdriver.NewConnector(pgdriver.WithDSN("postgres://...")))
	db := bun.NewDB(sqldb, pgdialect.New())
	svc := &myDB{db: db}

	// TID is string here (Product.ID's type) — any comparable type works.
	c := cqrs.NewCQRS(cqrs.CQRSImpl[Product, ProductResource, any, string]{
		WriteSQLService: svc, // ReadSQLService falls back to this if left nil
		ToResource:      productToResource,
	})

	ctx := context.Background()

	// Single row by primary key.
	p, err := c.GetByID(ctx, "prod_123")

	// Every row matching a filter.
	cheap, err := c.Find(ctx, domains.StructuredFilter{
		Filters: []domains.Filter{
			{Field: "price_cents", Mode: domains.ModeLT, Value: 5000},
		},
	})

	// First match only.
	anyInStock, err := c.FindOne(ctx, domains.StructuredFilter{
		Filters: []domains.Filter{{Field: "stock_qty", Mode: domains.ModeGT, Value: 0}},
	})

	// Keyset-paginated, cursor-driven listing (what an API list endpoint uses).
	page, err := c.PaginateFormat(ctx, domains.Pagination{
		Filter:   domains.StructuredFilter{SortFields: []domains.SortField{{Field: "name", Order: domains.SortOrderAsc}}},
		PageSize: 20,
	})

	_, _, _, _, _ = p, cheap, anyInStock, page, err
	log.Println("see the sections below for what each of these actually guarantees")
}
```

`NewCQRS` builds the pagination layer for you automatically from
`ReadSQLService`/`WriteSQLService`/`LogService`/`ColumnDefaultID`/
`ColumnDefaultSort` — you never construct `pagination.PaginationService`
yourself unless you want to inject a different implementation (see
[`CQRSImpl.PaginationService`](#standalone-use-without-cqrsimpl) below).

## The filter DSL (`domains.StructuredFilter`)

```go
type StructuredFilter struct {
	Filters    []Filter
	SortFields []SortField
	Logic      Logic // LogicAnd (default) or LogicOr, combining Filters
	Preload    []string
}

type Filter struct {
	Field    string
	Value    any
	Mode     Mode
	DataType DataType     // only matters for ModeRange/date-typed comparisons
	Custom   CustomFilter // Go-only (json:"-"), see ModeCustom below
}
```

This is also what a frontend sends as JSON over a `?filter=...` query
param — `domains.Pagination.Parse` decodes it straight off a Hertz
request (base64 → JSON → `StructuredFilter`), so the same struct works
whether you build it in Go or a client builds it and sends it over HTTP.

### TypeScript client and Hertz handler

The browser sends ordinary query parameters: `filter` and optional `sort`
are JSON encoded as standard Base64 and URL-escaped; `pageSize` is a
decimal number, and `cursor` is the opaque cursor returned by the previous
page. `URLSearchParams` builds the final URL. The encoder below handles
UTF-8 filter values and matches the query encoding expected by
`domains.Pagination.Parse`:

```typescript
type Filter = {
	field: string;
	mode: string;
	value: unknown;
	dataType?: string;
};

type SortField = { field: string; order: "asc" | "desc" };

type StructuredFilter = {
	filters?: Filter[];
	sortFields?: SortField[];
	logic?: "and" | "or";
	preload?: string[];
};

type Page<T> = {
	data: T[];
	nextCursor: string | null;
	previousCursor: string | null;
	pageSize: number;
};

function encodeQueryParam(value: unknown): string {
	const bytes = new TextEncoder().encode(JSON.stringify(value));
	let binary = "";
	for (const byte of bytes) binary += String.fromCharCode(byte);

	// Match Go's EncodeQueryParam: standard Base64, then URL escaping.
	// URLSearchParams escapes the percent signs when it serializes the URL.
	return encodeURIComponent(btoa(binary));
}

const baseParams = new URLSearchParams();
baseParams.set(
	"filter",
	encodeQueryParam({
		filters: [{ field: "stock_qty", mode: "gt", value: 0 }],
		logic: "and",
	} satisfies StructuredFilter),
);
baseParams.set("sort", encodeQueryParam([{ field: "name", order: "asc" }]));
baseParams.set("pageSize", "20");

async function fetchProductPage(cursor?: string): Promise<Page<unknown>> {
	const params = new URLSearchParams(baseParams);
	if (cursor) params.set("cursor", cursor);

	const response = await fetch(`/api/products?${params.toString()}`);
	if (!response.ok) throw new Error(`Request failed: ${response.status}`);
	return response.json() as Promise<Page<unknown>>;
}

const firstPage = await fetchProductPage();
if (firstPage.nextCursor) {
	const nextPage = await fetchProductPage(firstPage.nextCursor);
	console.log(nextPage.data);
}
```

In a Hertz handler, parse the request and use the normal read-backed CQRS
pagination method:

```go
var pagination domains.Pagination
if err := pagination.Parse(reqCtx); err != nil {
	// Return a 400 response for invalid query parameters.
	return
}

page, err := c.PaginateFormat(ctx, pagination)
if err != nil {
	// Handle the database/query error.
	return
}
// Return page as the JSON response.
```

Use `c.PaginateFilter(ctx, backendFilter, pagination)` instead when the
server must add a scope such as the current tenant; that trusted filter is
ANDed with the frontend filter. `PaginateWithHertz` also parses directly
from `reqCtx`, but it requires a caller-supplied `*bun.Tx`; ordinary
read-only list endpoints should parse and call `Paginate`/
`PaginateFormat` (or `PaginateFilter`) so the configured read service is
used without opening a transaction.

| `Mode` | SQL shape | Notes |
|---|---|---|
| `ModeEqual` / `ModeNotEqual` | `col = ?` / `col != ?` | |
| `ModeGT` / `ModeGTE` / `ModeLT` / `ModeLTE` | `col > ?` etc. | |
| `ModeContains` / `ModeNotContains` | `col LIKE '%...%'` | Values are LIKE-escaped. **Known gap**: the escape has no effect under SQLite (no `ESCAPE` clause is emitted) — a literal `%`/`_` in the search value won't match there. Works correctly under Postgres. See `TestFilterRootCause_Problem_WildcardCharacterNeverMatchesUnderSQLite`. |
| `ModeStartsWith` / `ModeEndsWith` | `col LIKE '...%'` / `col LIKE '%...'` | |
| `ModeInside` / `ModeOutside` | `col IN (...)` / `col NOT IN (...)` | `Value` must be a slice. |
| `ModeBefore` / `ModeAfter` | `col < ?` / `col > ?` | Date/time-typed; strictly exclusive at the boundary. |
| `ModeRange` | `col BETWEEN ? AND ?` | `Value` is `RangeNumber{From,To}`/`RangeDate{From,To}`, or the `{"from":...,"to":...}` shape JSON decodes into. |
| `ModeIsEmpty` / `ModeIsNotEmpty` | `(col IS NULL OR col = '')` / negation | On a nullable `bool` column, only `NULL` counts as "empty" — `false` does not. |
| `ModeSearch` | `col @@@ ?` (ParadeDB BM25) | `Field: ""` searches every `EnableSearchIndex`-indexed column at once. Requires the `pg_search` extension — see the read Postgres instance in `local/docker-compose`. |
| `ModeCustom` | whatever your Go function adds | Set `Filter.Custom` to a Go function; `Value` is passed to it and `Field` is only a label. Backend-only. See [Custom filters](#custom-filters-modecustom). |

**Nested relation preloads.** `preloads ...string` (the trailing variadic
arg most read methods take) supports dotted, arbitrarily deep paths —
`"Author.Company.Country"` — and every segment is independently
normalized (`author_posts`/`authorPosts`/`AuthorPosts` all resolve the
same way) and validated against the real `bun` relation graph before
being used. A segment (or a whole dotted entry) that doesn't name a real
relation — a typo, or a relation renamed/removed elsewhere in the
codebase — is **dropped with a `Warn` log**, not a hard error: a stale
preload reference can't take a production read down just because a model
changed shape somewhere else. See `nested_preload_test.go` for the
5-level-deep worked example this claim is tested against.

### Custom filters (`ModeCustom`)

When no built-in mode can express a search — for example "every word must
match at least one of several columns" — put the function directly on the
filter as `Filter.Custom`, next to the other filters. The SQL stays in your
backend code, typically in the trusted `filter` argument of
`PaginateFilter`:

```go
scope := domains.StructuredFilter{
	Logic: domains.LogicAnd,
	Filters: []domains.Filter{
		{Field: "organization_id", Mode: domains.ModeEqual, Value: orgID}, // from auth
		{
			Field: "quickSearch", // only a label
			Mode:  domains.ModeCustom,
			Custom: func(q *bun.SelectQuery, _ any) (*bun.SelectQuery, error) {
				for word := range strings.FieldsSeq(search) {
					pattern := "%" + escapeLike(word) + "%" // escape \, % and _
					q = q.WhereGroup("AND", func(g *bun.SelectQuery) *bun.SelectQuery {
						return g.Where(`full_name ILIKE ? ESCAPE '\'`, pattern).
							WhereOr(`first_name ILIKE ? ESCAPE '\'`, pattern).
							WhereOr(`last_name ILIKE ? ESCAPE '\'`, pattern)
					})
				}
				return q, nil
			},
		},
	},
}
page, err := c.PaginateFilter(ctx, scope, pagination)
```

The function receives the query to add conditions to and returns it. It
captures `search` from the surrounding handler, so `Value` is optional; if
you do set `Value`, it arrives as the second argument.

`Custom` is tagged `json:"-"`, so a client can never supply a function
through the `?filter=` param. The frontend instead sends only the text
(for example `?q=ann%20lee`), and your handler reads it into `search`.

Behavior worth knowing:

- **`Field` is only a label.** It isn't validated against columns or
  looked up anywhere.
- **Client-sent custom filters are dropped.** A `mode: "custom"` entry in
  the frontend `?filter=` param has no function, so it is dropped with a
  `Warn` log and never runs. In a backend filter, a `ModeCustom` filter
  without a `Custom` function is a hard error, so mistakes are caught.
- **Errors.** An error returned by your function surfaces as the filter
  error, and a function that returns a `nil` query without an error
  produces `custom filter returned a nil query` instead of a panic.
- **Composition.** A custom term is ANDed with the other filters like any
  other term, and each group keeps its own `Logic`, so a frontend `OR`
  group cannot widen a backend tenant scope. It works inside `OR` groups
  with other filters, and keeps its place across cursor pages, including
  mixed-direction sorts.

Caveats:

- **`WHERE` only.** A custom filter adds a condition; it cannot add an
  `ORDER BY` such as a similarity ranking (`LOWER(full_name) <-> ?`).
  Cursor pagination sorts by real columns, so ranking by a computed score
  needs a deliberate extension of the cursor, not a custom filter.
- **Safety is your function's job.** Bind every value with `?`, never
  concatenate it into SQL. Escape `LIKE` wildcards (`\`, `%`, `_`) or a
  search for `%` matches every row. If you read `value`, type-check it
  with the two-value form (`s, ok := value.(string)`): a bare
  `value.(string)` panics on a non-string, and the engine does not
  recover panics. Cap the length and reject NUL bytes for free-text
  searches. Never build column names or expressions from user input.
- **Dialect.** `ILIKE` and `<->` are PostgreSQL-specific. The test suite
  runs on SQLite, so its examples use `LIKE`.

## Which read method do I actually want?

| Method | Returns | Use when |
|---|---|---|
| `Filter` / `FilterWithTx` | `[]*TData` | You want every match and don't need pagination metadata. |
| `Find` / `FindWithTx` | `[]*TData` | Same as `Filter`, but lets you override `Preloads` per call. |
| `FindOne` / `FindOneWithTx` | `*TData`, or `sql.ErrNoRows` | Exactly one row, by filter — "first match" (deterministic: paginate's own default ordering, not arbitrary). |
| `GetByID` / `GetByIDWithTx` | `*TData`, or `sql.ErrNoRows` | Exactly one row, by primary key. `FindOne` scoped to `ColumnDefaultID = id`. |
| `Max` / `Min` (+`WithTx`) | `*TData`, or `sql.ErrNoRows` | The row with the highest/lowest value of a given field, among rows matching a filter. |
| `Count` / `CountWithTx` | `int64` | How many rows match, without scanning any back. |
| `Exists` / `ExistsWithTx` | `bool` | A real SQL `EXISTS`, not a `COUNT(*) > 0` — stops at the first match. |
| `Paginate` / `PaginateFilter` / `PaginateWithHertz` | `PaginationResult[TData]` | Real keyset (cursor) pagination for a listing endpoint — `NextCursor`/`PreviousCursor`, no `OFFSET`, stays fast regardless of table size. |

Every one of these has a `...Format` sibling (`FindOneFormat`,
`GetByIDFormat`, ...) that runs the result through `ToResource` for you,
returning `TResponse` instead of `TData`.

**The `filter` argument everywhere above is treated as trusted,
backend-supplied input, not frontend input** — an unknown field name in
it is a real error (`"unknown filter field"`), not something silently
dropped. That's the opposite of `Pagination`/`PaginateFilter`'s own
`pagination.Filter` (the frontend-supplied half, parsed off a request),
where an unknown field is dropped with a warning instead. If you're
taking a filter directly from a client, parse it through
`domains.Pagination.Parse` / `PaginateWithHertz`, not into `Filter`'s
`filter` argument directly.

## Transactions

### `Start` / `End`

```go
tx, err := c.Start(ctx) // begins on WriteSQLService — see "why the writer" below
if err != nil {
	return err
}
defer func() { err = c.End(ctx, tx, err) }()

// ... call *WithTx methods with tx below, always assigning back to err ...
```

`End` commits if `err` is `nil` when it runs, or rolls back and returns
`err` unchanged otherwise. The transaction is returned to the caller
rather than stored on `CQRSImpl` itself — one `CQRSImpl` instance is
typically shared across concurrent requests, and a transaction is
inherently single-request state; storing it as a field would let two
goroutines stomp on each other's transaction.

### OpenTelemetry spans around a transaction

If the application wraps `context.Context` in a type with a
`StartCallerSpan` method, start the span before opening the transaction
and pass the returned context through every CQRS call. Embedding
`context.Context` keeps the wrapper compatible with the CQRS APIs; the
derived context makes spans created downstream children of this span.
Keep this wrapper in the application layer — CQRS itself only requires a
standard `context.Context`.

```go
ctx, span := ctx.StartCallerSpan()
defer span.End()

tx, err := c.Start(ctx)
if err != nil {
	return fmt.Errorf("starting transfer: %w", err)
}
defer func() { err = c.End(ctx, tx, err) }()

// Use ctx and tx for the rest of the transaction.
```

`StartCallerSpan` can use `runtime.Caller` to name the span after the
function that called it, avoiding a hard-coded name at each call site.
Caller-derived names can change when functions are renamed; use explicit
span names when stable operation names are important. The OpenTelemetry
API alone does not export spans — configure an SDK tracer provider and
exporter in the application to record or send them.

### Every `*WithTx` read locks the row it reads

`GetByIDWithTx`, `FindWithTx`, `FindOneWithTx`, `MaxWithTx`, `MinWithTx`,
and `FilterWithTx` all issue `SELECT ... FOR UPDATE` under Postgres — a
pessimistic row lock held until `tx` ends. **This is deliberate**: the
whole reason to reach for a `*WithTx` read instead of the plain
(`ReadSQLService`-backed) version is "I'm about to act on this inside the
same transaction" — the classic check-then-act gap (read a balance,
decide what to write, write it) is exactly what a second, concurrent
transaction could otherwise run straight through, between the first
one's read and its write.

The one deliberate exception is `PaginateWithHertz` — a browsing/listing
path, not a read-then-write one, so it does **not** lock. Locking every
row of a paginated page by default would be a surprising and likely
harmful default for what's usually a read-only request.

`FOR UPDATE` is a no-op under SQLite (it's not supported there at all —
confirmed directly, it's a syntax error), which is exactly what lets the
fast, Docker-free unit suite still exercise every `*WithTx` method. Real
locking is only meaningful, and only tested, against real Postgres — see
[Testing this layer](#testing-this-layer).

### `IncrementByID` — atomic, race-free counters/balances

```go
updated, err := c.IncrementByIDWithTx(ctx, tx, "acc_1", "balance_cents", -500)
```

This exists specifically to avoid the "lost update" a naive
`GetByID` → add in Go → `UpdateByID` sequence has: two concurrent callers
reading the same starting value, and whichever `UpdateByID` commits last
silently discarding the other's change. `IncrementByID` instead issues a
single `SET field = field + ?`, evaluated by the database itself — there
is no window where two concurrent increments can observe the same
pre-update value.

`delta` is `float64`, which sounds riskier than it is: the precision
limit it implies bounds `delta` itself, not the column's stored value —
confirmed directly, a column already at `1<<60` incremented by an
ordinary small delta lands exactly, every time, because the database
does the actual addition against its own exact integer type once `delta`
is formatted into the query. The only real limit is a single call's
`delta` exceeding 2^53 (about $90 trillion at cent granularity) — not a
concern for realistic monetary deltas. See
`TestIncrementByID_HappyPath_LargeExistingValuePlusSmallDeltaStaysExact`
and its sibling tests for the exact boundary, pinned down rather than
just asserted.

### Why the writer, never the reader

Every `*WithTx` method's own doc comment says this, but it's worth
stating once, plainly: a transaction only shows its own uncommitted work
to callers sharing that same connection. In a real deployment,
`ReadSQLService` may point at a replica that doesn't even share the
writer's connection — so a transaction has to begin on
`WriteSQLService.Client().BeginTx(...)` (which is exactly what `Start`
does), never on the reader, or "read your own writes inside this
transaction" simply doesn't hold.

### A full worked example: a ledger transfer

This is deliberately the same example the test suite itself uses (see
`integration_ledger_helpers_test.go`) — a funds transfer is the canonical
case that needs every piece above at once: locking (to avoid a
check-then-act race on the balance check), atomic arithmetic (to avoid a
lost update on the balance write), and real atomicity (so a failure
midway through never leaves one side debited without the other credited).

```go
func transferFunds(ctx context.Context, c *cqrs.CQRSImpl[Account, Account, any, string], fromID, toID string, amountCents int64) (err error) {
	tx, err := c.Start(ctx)
	if err != nil {
		return fmt.Errorf("starting transfer: %w", err)
	}
	defer func() { err = c.End(ctx, tx, err) }()

	// Lock both accounts in a fixed, consistent order (lexicographically
	// by ID) — not fromID-then-toID — so two transfers running in
	// opposite directions can never deadlock against each other.
	first, second := fromID, toID
	if second < first {
		first, second = second, first
	}
	firstAcc, err := c.GetByIDWithTx(ctx, &tx, first)
	if err != nil {
		return fmt.Errorf("locking %s: %w", first, err)
	}
	secondAcc, err := c.GetByIDWithTx(ctx, &tx, second)
	if err != nil {
		return fmt.Errorf("locking %s: %w", second, err)
	}

	from := firstAcc
	if fromID == second {
		from = secondAcc
	}
	if from.BalanceCents < amountCents {
		return fmt.Errorf("insufficient funds in %s", fromID) // rolled back by End
	}

	if _, err = c.IncrementByIDWithTx(ctx, tx, fromID, "balance_cents", float64(-amountCents)); err != nil {
		return fmt.Errorf("debiting %s: %w", fromID, err)
	}
	if _, err = c.IncrementByIDWithTx(ctx, tx, toID, "balance_cents", float64(amountCents)); err != nil {
		return fmt.Errorf("crediting %s: %w", toID, err)
	}
	return nil
}
```

If you need a check-then-act sequence that **doesn't** have a natural
"lock both in a fixed order" shape, the same lock-ordering discipline
still applies: always acquire row locks in the same global order across
every code path that might take more than one, or accept that you'll
occasionally hit a real deadlock and need to retry — which Postgres
itself detects and resolves by aborting one side with a clear
`"deadlock detected"` error (see
`TestLedgerConcurrency_LockContentionAndDeadlocks_...` for a worked
retry loop).

### Standalone use, without `CQRSImpl`

`src/pagination` doesn't require the write/CDC engine at all — if you
only need filtering/pagination/transactions and not `Create`/`Update`/
`Delete`+CDC, use `pagination.NewPaginationService` directly:

```go
svc := pagination.NewPaginationService(pagination.PaginationService[Product, string]{
	ReadSQLService: myReadDB,
})
products, err := svc.Filter(ctx, domains.StructuredFilter{...})
```

`CQRSImpl.PaginationService` (the field `NewCQRS` wires up automatically)
is this exact same type, exposed so you can also inject your own
implementation — a test double, or a decorator around the real one — by
setting it yourself before calling `NewCQRS`; your value is left alone
instead of being overwritten.

## Testing this layer

Pagination/filtering is covered by the same fast, Docker-free unit suite
as the rest of the engine (`make test-unit`) — but transactions,
locking, and deadlocks fundamentally cannot be: SQLite has no row-level
locking, no real MVCC, and no `FOR UPDATE` support at all. Proving any of
that actually works needs a real transactional database, so there's a
dedicated integration suite for it — a "ledger" of accounts and
transfers, run against real Postgres via `make test-integration` (now
also covering `TestLedger...`, not just `TestIntegration...` — see the
Makefile), organized into six categories:

| Category | File | What it proves |
|---|---|---|
| Concurrency & isolation | `integration_ledger_concurrency_test.go` | Conservation of total balance under many concurrent transfers; `IncrementByID` never loses an update under real concurrent load; `GetByIDWithTx`'s row lock really blocks a second transaction (not a no-op); `READ COMMITTED` hides an uncommitted debit from a separate connection; a primary-key race only ever lets one creation win; `PaginateWithHertz` deliberately does *not* block a locker (the one method in the family that shouldn't). |
| Lock contention & deadlocks | (same file) | Two transfers running in opposite lock order reliably produce a *real* Postgres deadlock (`SQLSTATE 40P01`, not simulated) — asserts it surfaces as a clean Go error, `End` still rolls back correctly, and a simple retry lets both sides eventually succeed with correct final balances. |
| ACID & fault injection | `integration_ledger_acid_test.go` | One test per property — atomicity (a failed credit rolls back an already-applied debit), consistency (insufficient funds aborts before any write), isolation (two transfers sharing one account stay mathematically correct), durability (a committed transfer survives a brand-new connection) — plus a real **hard-crash test**: an uncommitted transfer is left open while `docker restart`s the actual Postgres container, and only the already-committed work survives. |
| Jepsen-style testing | `integration_ledger_jepsen_test.go` | Named honestly: this is Jepsen's *methodology* (record a full concurrent history, including real commit/abort outcomes, then check invariants no valid serialization could violate) scaled to one Postgres instance — not literal multi-node Jepsen with a nemesis and nothing here to run it against. |
| Load, stress & soak | `ledger_load_test.go` (`-tags="integration load"`) | Sustained concurrent transfer throughput over a configurable wall-clock window (not a fixed-N burst), with conservation/non-negativity re-checked afterward — the thing that actually exercises connection-pool pressure held over time. |
| Transactional benchmarking | `ledger_bench_test.go` (`-bench Ledger`) | Inspired by TPC-B's debit/credit transaction shape — explicitly *not* a literal TPC-B/TPC-C implementation, which specifies exact schemas/scale factors/terminal simulation this doesn't attempt. Measures real transfer throughput/latency (serial, parallel, and a pure-`IncrementByID` baseline) against real Postgres. |

Run them with `make test-integration` (brings the real stack up first),
`make test-ledger-load` (`SECONDS=120 make test-ledger-load` for a longer
soak), and `make test-ledger-bench`.

Custom filters have their own fast suite (`filter_custom_test.go` and
`filter_custom_quicksearch_test.go`). The second models a member and an
account quick search — multi-word, multi-column, nullable columns,
organization/branch scoping, a `Media` preload, 15-row pages with a
cursor — and adds poison-pill cases: SQL-injection strings, `LIKE`
wildcards, empty and whitespace searches, wrong value types, oversized
and NUL-byte input, Unicode, a frontend `OR` group trying to escape the
backend tenant scope, and client-sent custom filters (which must never
run). It runs on
SQLite, so `LIKE` stands in for `ILIKE`.

## Why use this as a pattern

The underlying claim this whole layer makes — "filters, pagination, and
transactions on top of `bun` are safe to build an application on" — isn't
backed by reasoning about the code or by unit tests against an in-memory
stand-in alone. It's backed by actually reproducing, on purpose, against a
real Postgres instance: a lost update, a real deadlock, a real crash
mid-transaction, sustained concurrent load, and a from-scratch
serialization-invariant check — and showing the code behaves correctly
through every one of them. That's a meaningfully higher bar than "the
tests pass," and it's the reason to adopt this as the query/transaction
layer under a service rather than wiring `bun` calls ad hoc: the sharp
edges (lost updates, dirty reads, deadlocks, crash atomicity) are the
exact places hand-rolled transaction code usually gets wrong, silently,
until a real incident finds it first.
