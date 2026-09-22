# 🏦 Banking ETL & Risk Analytics Pipeline

An end-to-end data engineering pipeline project build by Suravi Behera that simulates a real-world banking
data platform: synthetic data generation → ingestion → PySpark ETL →
automated data quality gating → SQL data warehouse (star schema) → Airflow
orchestration → Power BI risk & transaction dashboard.

> Every phase below has actually been run against 200K+ synthetic
> transactions in this repo — see `logs/` and `docs/sample_query_outputs.md`
> for real, captured output, not illustrative samples.

---

## Architecture

```
Synthetic Banking CSVs (customers, accounts, branches, transactions, loans)
        │
        ▼
Python Ingestion  (schema validation, landing zone, structured logs)
        │
        ▼
PySpark ETL  (dedup, standardize, clean, join, calculated columns,
              customer risk scoring, daily/monthly aggregations)
        │
        ▼
Automated Data Quality Gate  (completeness / validity / uniqueness /
                               consistency checks, ≥90% score required)
        │
        ▼
SQL Data Warehouse  (star schema: dim_date, dim_branch, dim_customer,
                      dim_account, fact_transaction, fact_loan,
                      customer_risk_metrics)
        │
        ▼
Power BI Dashboard  (Executive Overview / Transaction Analysis / Risk Analysis)

        ▲
        │
Apache Airflow  (orchestrates the whole chain: extract → validate →
                  transform → quality_check → load_to_sql → success)
```

---

## Tech stack

Python · PySpark · SQL (SQLite, portable to PostgreSQL/SQL Server/Snowflake) · Apache Airflow · Power BI

---

## Project structure

```
banking_etl_pipeline/
├── data_generator/        Phase 0 — synthetic data generation (Faker + pandas)
├── ingestion/              Phase 1 — Python ingestion & validation
├── etl/                    Phase 2 — PySpark cleaning/transform/risk scoring
├── quality/                Phase 3 — automated data quality framework
├── sql/                    Phase 4 — star-schema DDL, load script, analytical queries
├── airflow_dags/           Phase 5 — Airflow DAG orchestrating all phases
├── powerbi/                Phase 6 — Power BI build guide + CSV data extracts
├── raw_data/                Generated source CSVs
├── processed_data/          landing/ (raw parquet) · clean/ (post-ETL) · rejected/ (DQ failures)
├── warehouse/                banking_dw.sqlite (the SQL data warehouse)
├── logs/                     Structured logs + machine-readable JSON reports
├── tests/                    pytest suite validating pipeline outputs
├── docs/                     Sample query outputs, architecture & performance notes
├── .github/workflows/        CI: runs the full pipeline + test suite on every push
├── requirements.txt
└── run_pipeline.sh           One-command end-to-end run
```

---

## Quick start

```bash
pip install -r requirements.txt
bash run_pipeline.sh
```

This runs every phase in order — generate data, ingest, PySpark ETL, data
quality gate, warehouse load, Power BI CSV export, and the test suite — and
stops immediately if any phase fails (`set -e`), so bad data never reaches
the warehouse silently.

Or run phases individually:

```bash
python3 data_generator/generate_synthetic_data.py
python3 ingestion/ingest.py
python3 etl/pyspark_etl.py
python3 quality/data_quality.py
python3 sql/load_to_sql.py
python3 powerbi/export_for_powerbi.py
pytest tests/test_pipeline.py -v
```

---

## Results from the latest run

| Metric | Value |
|---|---|
| Raw transactions generated | 201,401 |
| Valid transactions after cleaning | 193,955 |
| Data Quality Score | 96.3% (gate: ≥90%) |
| Customers | 5,000 |
| Loans | 3,000 |
| Overall loan default rate | 8.2% |
| Risk tiers | Low (4,683) · Medium (304) · High (13) |
| Default rate — Medium risk tier | 50.8% |
| Default rate — Low risk tier | 0.0% |
| Tests passing | 21 / 21 |

The risk-tier-to-default-rate relationship (Medium: 50.8% vs Low: 0%) is
the key validation that the customer risk score is actually predictive —
see `sql/queries.sql` Query 8c and `docs/sample_query_outputs.md`.

---

## Design notes & Technology framing

- **Intentional data quality issues** are injected at generation time (nulls,
  duplicate IDs, negative amounts, malformed dates, orphan foreign keys) so
  the cleaning/validation layers have real problems to solve — not sample
  code validating already-clean data.
- **Every phase writes machine-readable artifacts** (`ingestion_manifest.json`,
  `data_quality_report.json`) alongside human-readable logs, so the pipeline
  is automatable (Airflow reads exit codes) and auditable (an analyst can
  read the JSON/text reports directly).
- **The data quality gate is a hard stop**: `quality/data_quality.py` exits
  non-zero if the score falls below 90%, which fails the Airflow
  `quality_check` task and prevents `load_to_sql` from ever running —
  bad data never silently reaches the warehouse.
- **The risk score is transparent and explainable** (a weighted rules-based
  formula over credit score, failed-transaction rate, defaulted loans, and
  overdue loans) rather than a black-box model — appropriate for a
  risk-technology context where auditability matters as much as accuracy.
- **SQL is written for SQLite portability** with explicit migration notes
  for PostgreSQL, SQL Server, and Snowflake in `sql/schema.sql` — pragmatic
  for a zero-install local demo while signaling awareness of production
  warehouse platforms.
- **The Airflow DAG was actually installed and test-executed** in this
  environment (not just written) — `airflow tasks test` was run against
  `start`, `validate_data`, and `quality_check`, all passing, with
  `airflow dags list-import-errors` confirming zero import errors.

See `docs/performance_optimization.md` for the specific tuning choices made
at this data volume (Spark AQE, join/partition strategy, warehouse
indexing) and how each layer would scale to production banking volumes.

---

## Testing

`tests/test_pipeline.py` validates pipeline *outputs* (the right approach
for a data pipeline): ingestion success, the data quality gate, warehouse
table population, referential integrity between facts and dimensions,
transaction ID uniqueness, risk score bounds, and the risk-tier/default-rate
relationship. All 21 tests pass against the live warehouse.

---

## Power BI dashboard

Power BI Desktop is Windows-only and can't run in this environment, so
`powerbi/README.md` is the complete build spec (data model, relationships,
DAX measures, page-by-page layout) and `powerbi/data_extracts/*.csv` are the
verified, ready-to-import data files — every number in them has already been
validated by the pipeline and the test suite above.
