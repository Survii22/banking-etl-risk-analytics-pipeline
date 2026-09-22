"""
load_to_sql.py
---------------
Phase 4 — SQL Data Warehouse Load

Reads the clean-zone parquet tables written by pyspark_etl.py (Phase 2) and
loads them into a SQL data warehouse built from sql/schema.sql (star schema:
dim_date, dim_branch, dim_customer, dim_account, fact_transaction, fact_loan,
customer_risk_metrics), plus the latest data_quality_log entry.

Uses SQLite for a zero-install, fully portable local warehouse -- ideal for a
portfolio project that needs to run anywhere without a DB server. The schema
and queries carry explicit notes for migrating to Postgres/SQL
Server/Snowflake (see sql/schema.sql).

Run:
    python load_to_sql.py
"""

import json
import logging
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
CLEAN_DIR = BASE_DIR / "processed_data" / "clean"
SQL_DIR = BASE_DIR / "sql"
WAREHOUSE_DIR = BASE_DIR / "warehouse"
LOG_DIR = BASE_DIR / "logs"
DB_PATH = WAREHOUSE_DIR / "banking_dw.sqlite"


def _setup_logging() -> logging.Logger:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("load_to_sql")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    fmt = logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s")
    fh = logging.FileHandler(LOG_DIR / "load_to_sql.log")
    fh.setFormatter(fmt)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(sh)
    return logger


def build_dim_date(start_year=2015, end_year=2027) -> pd.DataFrame:
    dates = pd.date_range(f"{start_year}-01-01", f"{end_year}-12-31", freq="D")
    df = pd.DataFrame({"date_key": dates.strftime("%Y-%m-%d")})
    df["day"] = dates.day
    df["month"] = dates.month
    df["month_name"] = dates.strftime("%B")
    df["quarter"] = dates.quarter
    df["year"] = dates.year
    df["year_month"] = dates.strftime("%Y-%m")
    df["day_of_week"] = dates.strftime("%A")
    df["is_weekend"] = dates.dayofweek.isin([5, 6]).astype(int)
    return df


def read_clean(table_name: str) -> pd.DataFrame:
    path = CLEAN_DIR / table_name
    return pd.read_parquet(path)


def main():
    logger = _setup_logging()
    logger.info("=" * 70)
    logger.info("STARTING SQL WAREHOUSE LOAD (Phase 4)")
    logger.info("=" * 70)

    WAREHOUSE_DIR.mkdir(parents=True, exist_ok=True)
    if DB_PATH.exists():
        DB_PATH.unlink()

    conn = sqlite3.connect(DB_PATH)
    try:
        # 1. Build schema
        schema_sql = (SQL_DIR / "schema.sql").read_text()
        conn.executescript(schema_sql)
        logger.info("Schema created from sql/schema.sql")

        # 2. dim_date (independently generated, not sourced from parquet)
        dim_date = build_dim_date()
        dim_date.to_sql("dim_date", conn, if_exists="append", index=False)
        logger.info(f"dim_date loaded: {len(dim_date):,} rows")

        # 3. Dimensions from clean zone
        dim_branch = read_clean("dim_branch")
        dim_branch.to_sql("dim_branch", conn, if_exists="append", index=False)
        logger.info(f"dim_branch loaded: {len(dim_branch):,} rows")

        dim_customer = read_clean("dim_customer")
        # dim_customer table only expects these columns (income/credit_score etc.)
        dim_customer_cols = [
            "customer_id", "first_name", "last_name", "dob", "gender", "email",
            "phone", "city", "country", "customer_income", "credit_score", "customer_since",
        ]
        dim_customer = dim_customer.reindex(columns=dim_customer_cols)
        dim_customer["dob"] = dim_customer["dob"].astype(str)
        dim_customer["customer_since"] = dim_customer["customer_since"].astype(str)
        dim_customer.to_sql("dim_customer", conn, if_exists="append", index=False)
        logger.info(f"dim_customer loaded: {len(dim_customer):,} rows")

        dim_account = read_clean("dim_account")
        dim_account_cols = ["account_id", "customer_id", "account_type", "branch_id",
                             "account_opened_date", "account_status"]
        dim_account = dim_account.reindex(columns=dim_account_cols)
        dim_account["account_opened_date"] = dim_account["account_opened_date"].astype(str)
        dim_account.to_sql("dim_account", conn, if_exists="append", index=False)
        logger.info(f"dim_account loaded: {len(dim_account):,} rows")

        # 4. Facts
        fact_txn = read_clean("fact_transaction")
        fact_txn_cols = [
            "transaction_id", "account_id", "customer_id", "branch_id", "transaction_date",
            "transaction_amount", "transaction_type", "payment_status", "channel",
            "account_type", "income_bracket", "is_high_value", "is_failed",
            "txn_year", "txn_month", "txn_year_month",
        ]
        fact_txn = fact_txn.reindex(columns=fact_txn_cols)
        fact_txn["transaction_date"] = fact_txn["transaction_date"].astype(str)
        fact_txn["is_high_value"] = fact_txn["is_high_value"].astype(int)
        fact_txn["is_failed"] = fact_txn["is_failed"].astype(int)
        fact_txn.to_sql("fact_transaction", conn, if_exists="append", index=False)
        logger.info(f"fact_transaction loaded: {len(fact_txn):,} rows")

        fact_loan = read_clean("fact_loan")
        fact_loan_cols = ["loan_id", "customer_id", "loan_type", "loan_amount", "interest_rate",
                           "tenure_months", "loan_start_date", "loan_status", "emi_payment_status"]
        fact_loan = fact_loan.reindex(columns=fact_loan_cols)
        fact_loan["loan_start_date"] = fact_loan["loan_start_date"].astype(str)
        fact_loan.to_sql("fact_loan", conn, if_exists="append", index=False)
        logger.info(f"fact_loan loaded: {len(fact_loan):,} rows")

        # 5. Risk mart
        risk = read_clean("customer_risk_metrics")
        risk_cols = [
            "customer_id", "customer_income", "credit_score", "total_transactions",
            "total_transaction_value", "avg_transaction_value", "failed_transactions",
            "high_value_transactions", "total_loans", "total_loan_amount",
            "defaulted_loans", "overdue_loans", "failed_txn_rate", "risk_score", "risk_tier",
        ]
        risk = risk.reindex(columns=risk_cols)
        risk.to_sql("customer_risk_metrics", conn, if_exists="append", index=False)
        logger.info(f"customer_risk_metrics loaded: {len(risk):,} rows")

        # 6. Data quality log (pulls the latest DQ report written by Phase 3)
        dq_report_path = LOG_DIR / "data_quality_report.json"
        if dq_report_path.exists():
            dq_report = json.loads(dq_report_path.read_text())
            sc = dq_report["scorecard"]
            conn.execute(
                """INSERT INTO data_quality_log
                   (run_at, total_records, valid_records, rejected_records,
                    data_quality_score_pct, passed_quality_gate)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    dq_report["run_at"], sc["total_records"], sc["valid_records"],
                    sc["rejected_records"], sc["data_quality_score_pct"],
                    int(sc["passed_quality_gate"]),
                ),
            )
            conn.commit()
            logger.info(
                f"data_quality_log loaded: score={sc['data_quality_score_pct']}%, "
                f"gate_passed={sc['passed_quality_gate']}"
            )
        else:
            logger.warning("No data_quality_report.json found — skipping data_quality_log load. "
                            "Run quality/data_quality.py first.")

        conn.commit()

        # Quick sanity counts
        logger.info("-" * 70)
        for table in ["dim_date", "dim_branch", "dim_customer", "dim_account",
                      "fact_transaction", "fact_loan", "customer_risk_metrics", "data_quality_log"]:
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            logger.info(f"  {table:<24}: {count:,} rows")

        logger.info(f"Warehouse written to {DB_PATH}")
        logger.info("SQL WAREHOUSE LOAD COMPLETE")
        logger.info("=" * 70)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
