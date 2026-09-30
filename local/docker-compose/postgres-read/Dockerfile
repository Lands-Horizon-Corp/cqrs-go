# The read-side Postgres gets three extra extensions beyond pgvector/pgcrypto
# (which the write side still gets from the plain pgvector/pgvector:pg16
# image): pg_duckdb, timescaledb, and pg_partman. All three are aimed at the
# query side specifically -- vectorized analytical scans, continuous
# aggregates, and keeping partition/index sizes bounded for cheap cursor
# pagination -- so they're only added here, not on the write side.
#
# No single published image ships all of these together:
#   - pg_duckdb has no prebuilt .deb/.rpm releases at all (checked its
#     GitHub releases directly -- source only) and building it from scratch
#     means replicating its Rust + vendored-DuckDB build toolchain. The
#     official pgduckdb/pgduckdb image already has it compiled, so that's
#     the base here instead of building it ourselves.
#   - timescaledb and pg_partman are ordinary, lightweight Debian packages
#     (PGDG's own postgresql-16-partman/postgresql-16-pgvector, and
#     TimescaleDB's own apt repo), so they're layered on top via apt rather
#     than trying to move pg_duckdb's compiled .so onto a different base
#     image and OS (pgduckdb's image is Debian bookworm; risking a glibc/ABI
#     mismatch by copying its binary elsewhere wasn't worth it when apt
#     already solves the other two cleanly).
FROM pgduckdb/pgduckdb:16-main

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl gnupg ca-certificates \
    && apt-get install -y --no-install-recommends postgresql-16-partman postgresql-16-pgvector \
    && curl -fsSL https://packagecloud.io/timescale/timescaledb/gpgkey | gpg --dearmor -o /usr/share/keyrings/timescaledb.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/timescaledb.gpg] https://packagecloud.io/timescale/timescaledb/debian/ bookworm main" > /etc/apt/sources.list.d/timescaledb.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends timescaledb-2-postgresql-16 \
    && apt-get purge -y curl gnupg \
    && apt-get autoremove -y \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER postgres
