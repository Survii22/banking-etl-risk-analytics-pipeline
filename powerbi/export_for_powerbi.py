"""
export_for_powerbi.py
----------------------
Phase 6 — Power BI data prep

Exports every warehouse table to CSV under powerbi/data_extracts/, ready to
be pulled into Power BI Desktop via Get Data > Text/CSV (or Get Data >
SQLite directly, pointing at warehouse/banking_dw.sqlite — see
powerbi/README.md for both options).

Run:
    python export_for_powerbi.py
"""

import sqlite3
from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "warehouse" / "banking_dw.sqlite"
OUT_DIR = Path(__file__).resolve().parent / "data_extracts"

TABLES = [
    "dim_date", "dim_branch", "dim_customer", "dim_account",
    "fact_transaction", "fact_loan", "customer_risk_metrics", "data_quality_log",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    for table in TABLES:
        df = pd.read_sql(f"SELECT * FROM {table}", conn)
        out_path = OUT_DIR / f"{table}.csv"
        df.to_csv(out_path, index=False)
        print(f"{table:<24}: {len(df):,} rows -> {out_path}")
    conn.close()


if __name__ == "__main__":
    main()
