# Performance, Optimization & Scaling Notes

This project runs on ~200K transactions on a single machine, by design — a
portfolio demo needs to run anywhere without a cluster. This doc records
the optimization choices actually made, and how each layer would scale to
production banking volumes (hundreds of millions of transactions/day).

---

## 1. PySpark ETL (`etl/pyspark_etl.py`)

**What's already in place:**
- **Adaptive Query Execution** (`spark.sql.adaptive.enabled=true` +
  `coalescePartitions.enabled=true`) — lets Spark right-size shuffle
  partitions at runtime instead of a fixed guess, which matters more as
  data volume grows and skews unpredictably.
- **Deliberately small shuffle partition count** (`spark.sql.shuffle.partitions=8`)
  for this data volume — the default of 200 would create excessive small
  tasks and scheduling overhead on a single-node/local run. This is called
  out in-code as a demo-scale setting, not a production one.
- **Window functions instead of self-joins** for duplicate-ID resolution in
  `clean_transactions()` — a single `ROW_NUMBER() OVER (PARTITION BY ...)`
  pass instead of a join-based dedup, which avoids a shuffle-heavy join on
  a potentially large table.
- **Broadcast-friendly join order**: `dim_branch` (10 rows) and `dim_account`
  are joined onto the much larger transaction fact, letting Spark's planner
  broadcast the small side automatically rather than shuffling both sides.
- **Column pruning before joins** (`.select(...)` on dimension tables before
  joining) so only needed columns move across the shuffle boundary.

**How this scales to production volume:**
- Move from local mode to a real cluster (EMR / Databricks / on-prem YARN)
  and let `spark.sql.shuffle.partitions` scale with total executor cores
  (a common rule of thumb: 2-3x total core count) instead of the pinned
  value used here.
- Partition the landing/clean parquet zones by date (already done via
  `ingest_date=YYYY-MM-DD` in the landing zone) so downstream jobs can
  prune partitions and only reprocess new data, rather than a full
  historical rescan on every run.
- Switch `dropDuplicates()` (a full shuffle) to a bucketed/pre-partitioned
  table design once volumes are large enough that duplicate transaction IDs
  are rare cross-partition, keeping dedup work local to each partition.
- Cache/persist `enriched_txns` if it is read multiple times downstream
  (risk metrics + 3 aggregation tables all read it) — at this data volume
  recomputation is cheap enough that caching isn't worth the memory
  pressure, but it is the first lever to pull at 10-100x this volume.

## 2. SQL Data Warehouse (`sql/schema.sql`)

**What's already in place:**
- Indexes on `fact_transaction(transaction_date, customer_id, branch_id, payment_status)`
  and `fact_loan(customer_id, loan_status)` — the columns every analytical
  query in `sql/queries.sql` filters or joins on.
- Star schema (not snowflake) to keep BI-tool joins shallow and fast.

**How this scales to production volume:**
- Partition `fact_transaction` by `transaction_date` (PostgreSQL native
  range partitioning, or a partitioned/clustered table in Snowflake/SQL
  Server) once the table is too large for a single scan to be fast —
  documented in the portability notes at the bottom of `schema.sql`.
- Add a columnstore index (SQL Server) or rely on Snowflake's automatic
  micro-partitioning/clustering keys for analytic (scan-heavy,
  aggregation-heavy) workloads instead of the row-store B-tree indexes used
  here, which are optimized for SQLite's simpler engine.
- Consider pre-aggregated summary tables (this project already has
  `daily_aggregation`/`monthly_aggregation`/`branch_performance` as exactly
  this pattern) so Power BI's Executive Overview page reads a small
  pre-computed table instead of scanning the full fact table on every
  dashboard interaction.

## 3. Data Quality (`quality/data_quality.py`)

Checks currently run with pandas boolean masks (vectorized, not row-by-row
loops) — appropriate at this volume. At production volume, the same check
logic would move into the PySpark job itself (Spark DataFrame filters,
which is exactly the pattern already used in `etl/pyspark_etl.py`'s
`completeness_ok`/`validity_ok` conditions) so quality checks run in the
same distributed pass as cleaning, rather than a second full read of raw
data in pandas.

## 4. Airflow (`airflow_dags/banking_etl_dag.py`)

- `execution_timeout` is set per-task (30 min default, 45 min for the Spark
  transform) so a hung task fails fast rather than blocking the daily
  schedule indefinitely.
- `retries=2` with a 5-minute delay absorbs transient failures (a flaky
  source API, a momentary DB connection blip) without manual intervention.
- `max_active_runs=1` prevents overlapping runs if a previous day's run
  is still finishing when the next scheduled run fires — important once
  data volume means a run might occasionally exceed 24 hours without this
  guard.

## 5. What was *not* over-engineered, on purpose

This is a portfolio project, not a production system — some choices favor
clarity and fast local iteration over premature optimization:
- No caching/persistence tuning in Spark beyond what's described above,
  since the dataset comfortably fits in memory at this scale.
- No connection pooling or async I/O in the ingestion/load scripts, since
  they run once per pipeline invocation, not as a long-lived service.
- SQLite instead of a client-server RDBMS, trading production realism for
  zero-install portability — explicitly called out as a demo choice with a
  migration path in `sql/schema.sql`.

The goal throughout was to make deliberate, explainable trade-offs and
document them — which is itself the point being demonstrated for a risk
technology role: knowing *why* a design choice was made, and what the next
lever to pull would be as volume grows.
