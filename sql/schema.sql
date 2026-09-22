-- ============================================================================
-- schema.sql
-- Phase 4 — SQL Data Warehouse: Dimensional Model (Star Schema)
-- ============================================================================
-- Written in SQLite-compatible syntax (this is what load_to_sql.py executes
-- against for the demo). Notes are included for adapting to
-- PostgreSQL / SQL Server / Snowflake in a real deployment.
--
-- Grain:
--   fact_transaction : one row per valid, deduplicated transaction
--   fact_loan        : one row per loan
--
-- Design choices:
--   - Star schema (not snowflake) for simpler, faster BI queries in Power BI.
--   - dim_date is pre-populated (a standard warehousing pattern) rather than
--     derived on the fly, so Power BI's date-hierarchy slicers and
--     time-intelligence DAX (SAMEPERIODLASTYEAR, etc.) work correctly.
--   - Surrogate keys are omitted in this demo in favor of natural/business
--     keys (customer_id, account_id, etc.) for readability; a production
--     warehouse would typically add integer surrogate keys + SCD Type 2
--     tracking on dim_customer for point-in-time risk reporting.
-- ============================================================================

DROP TABLE IF EXISTS fact_transaction;
DROP TABLE IF EXISTS fact_loan;
DROP TABLE IF EXISTS customer_risk_metrics;
DROP TABLE IF EXISTS dim_account;
DROP TABLE IF EXISTS dim_customer;
DROP TABLE IF EXISTS dim_branch;
DROP TABLE IF EXISTS dim_date;
DROP TABLE IF EXISTS data_quality_log;

-- ----------------------------------------------------------------------------
-- dim_date : standard calendar dimension
-- ----------------------------------------------------------------------------
CREATE TABLE dim_date (
    date_key        TEXT PRIMARY KEY,     -- 'YYYY-MM-DD'
    day             INTEGER NOT NULL,
    month           INTEGER NOT NULL,
    month_name      TEXT NOT NULL,
    quarter         INTEGER NOT NULL,
    year            INTEGER NOT NULL,
    year_month      TEXT NOT NULL,         -- 'YYYY-MM'
    day_of_week     TEXT NOT NULL,
    is_weekend      INTEGER NOT NULL       -- 0/1
);

-- ----------------------------------------------------------------------------
-- dim_branch
-- ----------------------------------------------------------------------------
CREATE TABLE dim_branch (
    branch_id       TEXT PRIMARY KEY,
    branch_name     TEXT NOT NULL,
    city            TEXT,
    country         TEXT
);

-- ----------------------------------------------------------------------------
-- dim_customer
-- ----------------------------------------------------------------------------
CREATE TABLE dim_customer (
    customer_id       TEXT PRIMARY KEY,
    first_name        TEXT,
    last_name         TEXT,
    dob               TEXT,
    gender            TEXT,
    email             TEXT,
    phone             TEXT,
    city              TEXT,
    country           TEXT,
    customer_income   REAL,
    credit_score      INTEGER,
    customer_since    TEXT
);

-- ----------------------------------------------------------------------------
-- dim_account
-- ----------------------------------------------------------------------------
CREATE TABLE dim_account (
    account_id            TEXT PRIMARY KEY,
    customer_id           TEXT NOT NULL,
    account_type          TEXT,
    branch_id             TEXT,
    account_opened_date   TEXT,
    account_status        TEXT,
    FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id),
    FOREIGN KEY (branch_id)   REFERENCES dim_branch(branch_id)
);

-- ----------------------------------------------------------------------------
-- fact_transaction  (grain: 1 row per valid transaction)
-- ----------------------------------------------------------------------------
CREATE TABLE fact_transaction (
    transaction_id      TEXT PRIMARY KEY,
    account_id           TEXT NOT NULL,
    customer_id           TEXT,
    branch_id             TEXT,
    transaction_date      TEXT NOT NULL,     -- FK -> dim_date.date_key
    transaction_amount    REAL NOT NULL,
    transaction_type      TEXT,
    payment_status        TEXT,
    channel               TEXT,
    account_type          TEXT,
    income_bracket        TEXT,
    is_high_value         INTEGER,           -- 0/1
    is_failed             INTEGER,           -- 0/1
    txn_year              INTEGER,
    txn_month             INTEGER,
    txn_year_month        TEXT,
    FOREIGN KEY (account_id)  REFERENCES dim_account(account_id),
    FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id),
    FOREIGN KEY (branch_id)   REFERENCES dim_branch(branch_id),
    FOREIGN KEY (transaction_date) REFERENCES dim_date(date_key)
);

CREATE INDEX idx_fact_txn_date     ON fact_transaction(transaction_date);
CREATE INDEX idx_fact_txn_customer ON fact_transaction(customer_id);
CREATE INDEX idx_fact_txn_branch   ON fact_transaction(branch_id);
CREATE INDEX idx_fact_txn_status   ON fact_transaction(payment_status);

-- ----------------------------------------------------------------------------
-- fact_loan  (grain: 1 row per loan)
-- ----------------------------------------------------------------------------
CREATE TABLE fact_loan (
    loan_id               TEXT PRIMARY KEY,
    customer_id           TEXT NOT NULL,
    loan_type             TEXT,
    loan_amount           REAL,
    interest_rate         REAL,
    tenure_months         INTEGER,
    loan_start_date       TEXT,               -- FK -> dim_date.date_key
    loan_status           TEXT,
    emi_payment_status    TEXT,
    FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id),
    FOREIGN KEY (loan_start_date) REFERENCES dim_date(date_key)
);

CREATE INDEX idx_fact_loan_customer ON fact_loan(customer_id);
CREATE INDEX idx_fact_loan_status   ON fact_loan(loan_status);

-- ----------------------------------------------------------------------------
-- customer_risk_metrics  (grain: 1 row per customer — a "risk mart" table
-- sitting alongside the pure star schema for fast dashboard reads)
-- ----------------------------------------------------------------------------
CREATE TABLE customer_risk_metrics (
    customer_id               TEXT PRIMARY KEY,
    customer_income           REAL,
    credit_score              INTEGER,
    total_transactions        INTEGER,
    total_transaction_value   REAL,
    avg_transaction_value     REAL,
    failed_transactions       INTEGER,
    high_value_transactions   INTEGER,
    total_loans               INTEGER,
    total_loan_amount         REAL,
    defaulted_loans           INTEGER,
    overdue_loans             INTEGER,
    failed_txn_rate           REAL,
    risk_score                REAL,
    risk_tier                 TEXT,
    FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id)
);

CREATE INDEX idx_risk_tier ON customer_risk_metrics(risk_tier);

-- ----------------------------------------------------------------------------
-- data_quality_log  : one row per pipeline run, feeds the "Data Quality %"
-- KPI on the Power BI Executive Overview page
-- ----------------------------------------------------------------------------
CREATE TABLE data_quality_log (
    run_id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    run_at                 TEXT NOT NULL,
    total_records          INTEGER,
    valid_records          INTEGER,
    rejected_records       INTEGER,
    data_quality_score_pct REAL,
    passed_quality_gate    INTEGER   -- 0/1
);

-- ============================================================================
-- Portability notes (for adapting this schema to a production RDBMS):
--
-- PostgreSQL:
--   - AUTOINCREMENT -> GENERATED ALWAYS AS IDENTITY
--   - Add proper DATE/TIMESTAMP/NUMERIC types instead of TEXT/REAL
--   - Consider partitioning fact_transaction by RANGE (transaction_date)
--     for a real multi-year transaction volume
--
-- SQL Server:
--   - AUTOINCREMENT -> IDENTITY(1,1)
--   - Use DATETIME2 / DECIMAL(18,2) instead of TEXT/REAL
--   - Add clustered columnstore index on fact_transaction for analytic scans
--
-- Snowflake:
--   - Drop explicit FK constraints (Snowflake does not enforce them) but
--     keep them as documentation/lineage hints; rely on clustering keys
--     (transaction_date, branch_id) instead of secondary indexes
-- ============================================================================
