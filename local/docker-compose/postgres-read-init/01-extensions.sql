CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pg_duckdb;

-- timescaledb ships its own time_bucket() overloads, which collide with
-- pg_duckdb's own time_bucket() overloads if both land in `public`:
-- `CREATE EXTENSION timescaledb` fails outright ("function time_bucket
-- already exists with same argument types") when pg_duckdb is already
-- installed in the same schema -- reproduced directly before writing this.
-- Installing it into its own schema avoids the conflict entirely. Call its
-- functions as ts.time_bucket(...)/ts.create_hypertable(...), or add `ts`
-- to search_path.
CREATE SCHEMA IF NOT EXISTS ts;
CREATE EXTENSION IF NOT EXISTS timescaledb SCHEMA ts;

-- Standard pg_partman convention: its own schema, not public.
CREATE SCHEMA IF NOT EXISTS partman;
CREATE EXTENSION IF NOT EXISTS pg_partman SCHEMA partman;

-- pg_search (ParadeDB, BM25 search via the @@@ operator) requires pgvector
-- (created above) and shared_preload_libraries=pg_search (set in
-- docker-compose.yml's postgres-read command). CASCADE is a no-op safety
-- net here, not what actually satisfies the pgvector prerequisite.
CREATE EXTENSION IF NOT EXISTS pg_search CASCADE;
