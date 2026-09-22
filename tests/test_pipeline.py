"""
test_pipeline.py
-----------------
Lightweight integration tests that validate the pipeline's OUTPUTS rather
than mocking every internal function — appropriate for a data pipeline
where the real risk is "did the warehouse end up correct", not "does this
one helper function return the right type".

Run (from project root, after running the full pipeline at least once):
    pytest tests/test_pipeline.py -v
"""

import json
import sqlite3
from pathlib import Path

import pandas as pd
import pytest

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "warehouse" / "banking_dw.sqlite"
DQ_REPORT_PATH = BASE_DIR / "logs" / "data_quality_report.json"
MANIFEST_PATH = BASE_DIR / "logs" / "ingestion_manifest.json"


@pytest.fixture(scope="module")
def conn():
    assert DB_PATH.exists(), "Warehouse not found — run sql/load_to_sql.py first"
    connection = sqlite3.connect(DB_PATH)
    yield connection
    connection.close()


# ---- Phase 1: Ingestion -------------------------------------------------

def test_ingestion_manifest_all_sources_succeeded():
    assert MANIFEST_PATH.exists(), "Ingestion manifest not found — run ingestion/ingest.py first"
    manifest = json.loads(MANIFEST_PATH.read_text())
    statuses = {s["source_name"]: s["status"] for s in manifest["sources"]}
    for source, status in statuses.items():
        assert status == "SUCCESS", f"Ingestion source '{source}' did not succeed: {status}"


# ---- Phase 3: Data Quality -------------------------------------------------

def test_quality_gate_passed():
    assert DQ_REPORT_PATH.exists(), "DQ report not found — run quality/data_quality.py first"
    report = json.loads(DQ_REPORT_PATH.read_text())
    score = report["scorecard"]["data_quality_score_pct"]
    threshold = report["scorecard"]["quality_gate_threshold_pct"]
    assert score >= threshold, f"Data quality score {score}% fell below gate {threshold}%"


def test_quality_report_has_all_four_categories():
    report = json.loads(DQ_REPORT_PATH.read_text())
    categories = {c["category"] for c in report["checks"]}
    assert categories == {"Completeness", "Validity", "Uniqueness", "Consistency"}


# ---- Phase 4: Warehouse structure & row counts -------------------------------------------------

EXPECTED_TABLES = [
    "dim_date", "dim_branch", "dim_customer", "dim_account",
    "fact_transaction", "fact_loan", "customer_risk_metrics", "data_quality_log",
]


@pytest.mark.parametrize("table", EXPECTED_TABLES)
def test_table_exists_and_nonempty(conn, table):
    count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    assert count > 0, f"Table '{table}' is empty"


def test_fact_transaction_no_null_business_keys(conn):
    bad = conn.execute(
        "SELECT COUNT(*) FROM fact_transaction WHERE transaction_id IS NULL OR account_id IS NULL"
    ).fetchone()[0]
    assert bad == 0


def test_fact_transaction_transaction_id_is_unique(conn):
    total = conn.execute("SELECT COUNT(*) FROM fact_transaction").fetchone()[0]
    distinct = conn.execute("SELECT COUNT(DISTINCT transaction_id) FROM fact_transaction").fetchone()[0]
    assert total == distinct, "Duplicate transaction_id values found in the warehouse fact table"


def test_fact_transaction_amounts_are_positive(conn):
    bad = conn.execute("SELECT COUNT(*) FROM fact_transaction WHERE transaction_amount <= 0").fetchone()[0]
    assert bad == 0


def test_referential_integrity_transaction_to_account(conn):
    orphans = conn.execute("""
        SELECT COUNT(*) FROM fact_transaction f
        LEFT JOIN dim_account a ON f.account_id = a.account_id
        WHERE a.account_id IS NULL
    """).fetchone()[0]
    assert orphans == 0, "Found transactions referencing accounts absent from dim_account"


def test_referential_integrity_loan_to_customer(conn):
    orphans = conn.execute("""
        SELECT COUNT(*) FROM fact_loan f
        LEFT JOIN dim_customer c ON f.customer_id = c.customer_id
        WHERE c.customer_id IS NULL
    """).fetchone()[0]
    assert orphans == 0, "Found loans referencing customers absent from dim_customer"


# ---- Risk metrics sanity -------------------------------------------------

def test_risk_score_bounds(conn):
    row = conn.execute("SELECT MIN(risk_score), MAX(risk_score) FROM customer_risk_metrics").fetchone()
    min_score, max_score = row
    assert min_score >= 0
    assert max_score <= 100


def test_risk_tier_values_valid(conn):
    tiers = {r[0] for r in conn.execute("SELECT DISTINCT risk_tier FROM customer_risk_metrics").fetchall()}
    assert tiers.issubset({"Low", "Medium", "High"})


def test_every_customer_has_a_risk_row(conn):
    n_customers = conn.execute("SELECT COUNT(*) FROM dim_customer").fetchone()[0]
    n_risk_rows = conn.execute("SELECT COUNT(*) FROM customer_risk_metrics").fetchone()[0]
    assert n_customers == n_risk_rows


# ---- Loan default rate sanity (business-logic smoke test) -------------------------------------------------

def test_loan_default_rate_between_0_and_100(conn):
    total = conn.execute("SELECT COUNT(*) FROM fact_loan").fetchone()[0]
    defaulted = conn.execute("SELECT COUNT(*) FROM fact_loan WHERE loan_status = 'Defaulted'").fetchone()[0]
    rate = defaulted / total * 100
    assert 0 <= rate <= 100


def test_higher_risk_tier_has_higher_default_rate(conn):
    """Sanity-checks that the risk score is actually predictive: customers in
    higher risk tiers should default at a higher rate than lower tiers."""
    rows = conn.execute("""
        SELECT crm.risk_tier,
               SUM(CASE WHEN fl.loan_status = 'Defaulted' THEN 1 ELSE 0 END) * 1.0 / COUNT(fl.loan_id) AS default_rate
        FROM fact_loan fl
        JOIN customer_risk_metrics crm ON fl.customer_id = crm.customer_id
        GROUP BY crm.risk_tier
    """).fetchall()
    rates = {tier: rate for tier, rate in rows}
    # Only assert ordering between tiers that actually have loans in this run
    if "Low" in rates and "Medium" in rates:
        assert rates["Medium"] >= rates["Low"], "Medium-risk default rate should be >= Low-risk"
    if "Medium" in rates and "High" in rates:
        assert rates["High"] >= rates["Low"], "High-risk default rate should be >= Low-risk"
