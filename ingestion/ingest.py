"""
ingest.py
---------
Phase 1 — Data Ingestion

Responsibilities:
  * Discover and validate incoming CSV files (schema + presence check)
  * Load raw data into a landing zone (parquet, partitioned by ingestion date)
  * Handle missing files / malformed CSVs without crashing the pipeline
  * Emit structured ingestion logs + a machine-readable ingestion manifest
    (consumed later by Airflow / the data-quality layer)

Design notes:
  - Every source is described in EXPECTED_SCHEMAS below (name -> required
    columns). Adding a new source is a one-line change.
  - Ingestion is intentionally "dumb": it does NOT clean data. It only
    confirms a file exists, is readable, and has the right shape. Cleaning
    is PySpark's job (Phase 2). This separation mirrors how real banking
    pipelines split "can we even read this file" from "is this file's data
    trustworthy".
"""

import json
import logging
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DATA_DIR = BASE_DIR / "raw_data"
LANDING_DIR = BASE_DIR / "processed_data" / "landing"
LOG_DIR = BASE_DIR / "logs"
MANIFEST_PATH = LOG_DIR / "ingestion_manifest.json"

EXPECTED_SCHEMAS = {
    "customers": {
        "file": "customers.csv",
        "required_columns": [
            "customer_id", "first_name", "last_name", "dob", "gender",
            "email", "phone", "city", "country", "customer_income",
            "credit_score", "customer_since",
        ],
    },
    "branches": {
        "file": "branches.csv",
        "required_columns": ["branch_id", "branch_name", "city", "country"],
    },
    "accounts": {
        "file": "accounts.csv",
        "required_columns": [
            "account_id", "customer_id", "account_type", "branch_id",
            "account_opened_date", "account_status",
        ],
    },
    "transactions": {
        "file": "transactions.csv",
        "required_columns": [
            "transaction_id", "account_id", "transaction_date",
            "transaction_amount", "transaction_type", "payment_status", "channel",
        ],
    },
    "loans": {
        "file": "loans.csv",
        "required_columns": [
            "loan_id", "customer_id", "loan_type", "loan_amount",
            "interest_rate", "tenure_months", "loan_start_date",
            "loan_status", "emi_payment_status",
        ],
    },
}


def _setup_logging() -> logging.Logger:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOG_DIR / f"ingestion_{datetime.now():%Y%m%d_%H%M%S}.log"

    logger = logging.getLogger("ingestion")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s")

    fh = logging.FileHandler(log_file)
    fh.setFormatter(fmt)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(sh)
    return logger


@dataclass
class IngestionResult:
    source_name: str
    status: str  # SUCCESS | FAILED | SKIPPED
    file_path: Optional[str] = None
    row_count: int = 0
    column_count: int = 0
    missing_columns: list = field(default_factory=list)
    error: Optional[str] = None
    output_path: Optional[str] = None
    ingested_at: str = field(default_factory=lambda: datetime.now().isoformat())


class BankingDataIngestor:
    """Handles discovery, validation, and landing of raw banking CSV files."""

    def __init__(self, raw_dir: Path = RAW_DATA_DIR, landing_dir: Path = LANDING_DIR):
        self.raw_dir = raw_dir
        self.landing_dir = landing_dir
        self.logger = _setup_logging()
        self.results: list[IngestionResult] = []

    # ---- validation -------------------------------------------------

    def _validate_schema(self, df: pd.DataFrame, required_cols: list[str]) -> list[str]:
        return [c for c in required_cols if c not in df.columns]

    # ---- per-source ingestion -----------------------------------------

    def ingest_source(self, source_name: str, config: dict) -> IngestionResult:
        file_path = self.raw_dir / config["file"]
        self.logger.info(f"Ingesting source '{source_name}' from {file_path}")

        # 1. File existence check
        if not file_path.exists():
            msg = f"File not found: {file_path}"
            self.logger.error(msg)
            return IngestionResult(source_name, "FAILED", str(file_path), error=msg)

        # 2. Attempt read; handle malformed CSV gracefully
        try:
            df = pd.read_csv(file_path, low_memory=False)
        except Exception as exc:  # noqa: BLE001 - want to catch any parse error
            msg = f"Failed to parse CSV: {exc}"
            self.logger.error(msg)
            return IngestionResult(source_name, "FAILED", str(file_path), error=msg)

        # 3. Empty file check
        if df.empty:
            msg = "File parsed but contains zero rows."
            self.logger.warning(msg)
            return IngestionResult(source_name, "FAILED", str(file_path), error=msg)

        # 4. Schema validation
        missing_cols = self._validate_schema(df, config["required_columns"])
        if missing_cols:
            msg = f"Missing required columns: {missing_cols}"
            self.logger.error(msg)
            return IngestionResult(
                source_name, "FAILED", str(file_path),
                row_count=len(df), column_count=len(df.columns),
                missing_columns=missing_cols, error=msg,
            )

        # 5. Land the raw data (parquet, partitioned by ingestion date) — untouched
        ingest_date = datetime.now().strftime("%Y-%m-%d")
        out_dir = self.landing_dir / source_name / f"ingest_date={ingest_date}"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"{source_name}.parquet"
        df.to_parquet(out_path, index=False)

        self.logger.info(
            f"'{source_name}' ingested successfully: {len(df):,} rows, "
            f"{len(df.columns)} columns -> {out_path}"
        )
        return IngestionResult(
            source_name, "SUCCESS", str(file_path),
            row_count=len(df), column_count=len(df.columns),
            output_path=str(out_path),
        )

    # ---- orchestration --------------------------------------------------

    def run(self) -> list[IngestionResult]:
        self.logger.info("=" * 70)
        self.logger.info("STARTING INGESTION RUN")
        self.logger.info("=" * 70)

        for source_name, config in EXPECTED_SCHEMAS.items():
            result = self.ingest_source(source_name, config)
            self.results.append(result)

        self._write_manifest()
        self._log_summary()
        return self.results

    def _write_manifest(self):
        MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
        manifest = {
            "run_at": datetime.now().isoformat(),
            "sources": [r.__dict__ for r in self.results],
        }
        with open(MANIFEST_PATH, "w") as f:
            json.dump(manifest, f, indent=2)
        self.logger.info(f"Ingestion manifest written to {MANIFEST_PATH}")

    def _log_summary(self):
        succeeded = [r for r in self.results if r.status == "SUCCESS"]
        failed = [r for r in self.results if r.status == "FAILED"]

        self.logger.info("-" * 70)
        self.logger.info(f"INGESTION SUMMARY: {len(succeeded)} succeeded, {len(failed)} failed")
        for r in self.results:
            marker = "OK  " if r.status == "SUCCESS" else "FAIL"
            self.logger.info(f"  [{marker}] {r.source_name:<15} rows={r.row_count:<10} status={r.status}")
        if failed:
            self.logger.warning(
                f"{len(failed)} source(s) failed ingestion. Downstream ETL should skip these "
                f"or use last-known-good landing data, depending on pipeline policy."
            )
        self.logger.info("=" * 70)


def main():
    ingestor = BankingDataIngestor()
    results = ingestor.run()
    failed = [r for r in results if r.status == "FAILED"]
    # Non-zero exit code lets Airflow/CI treat partial ingestion failures as task failures
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
