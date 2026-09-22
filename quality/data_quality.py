"""
data_quality.py
----------------
Phase 3 — Automated Data Quality Checks

Runs four categories of checks against the RAW (pre-clean) transaction data
so the quality report reflects what actually came in from source systems,
not the already-cleaned output:

  1. Completeness  - required fields present (customer/txn id, amount)
  2. Validity      - values fall in allowed ranges/enumerations
  3. Uniqueness    - no duplicate business keys (transaction_id)
  4. Consistency   - foreign keys resolve against master/dimension data
                     (e.g. every account_id on a transaction exists in accounts)

Produces:
  - logs/data_quality_report.json   (machine-readable, consumed by Airflow
    to decide whether to proceed to load_to_sql or halt the pipeline)
  - logs/data_quality_report.txt    (human-readable summary, e.g. for a
    Slack/email alert or an audit trail)

Run:
    python data_quality.py
"""

import json
import logging
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DATA_DIR = BASE_DIR / "raw_data"
LOG_DIR = BASE_DIR / "logs"

VALID_ACCOUNT_TYPES = {"Savings", "Current", "Salary", "NRI", "Fixed Deposit"}
VALID_TXN_TYPES = {"Deposit", "Withdrawal", "Transfer", "Bill Payment", "POS Purchase", "ATM Withdrawal", "Loan EMI"}

# Below this overall score, the pipeline should halt rather than load to the
# warehouse. Wired up to the Airflow DAG's quality_check task.
QUALITY_GATE_THRESHOLD = 90.0


def _setup_logging() -> logging.Logger:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("data_quality")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    fmt = logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s")
    fh = logging.FileHandler(LOG_DIR / "data_quality.log")
    fh.setFormatter(fmt)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(sh)
    return logger


class DataQualityChecker:
    def __init__(self, logger):
        self.logger = logger
        self.checks: list[dict] = []
        self.transactions: pd.DataFrame | None = None
        self.customers: pd.DataFrame | None = None
        self.accounts: pd.DataFrame | None = None

    def load(self):
        self.transactions = pd.read_csv(RAW_DATA_DIR / "transactions.csv", low_memory=False)
        self.customers = pd.read_csv(RAW_DATA_DIR / "customers.csv", low_memory=False)
        self.accounts = pd.read_csv(RAW_DATA_DIR / "accounts.csv", low_memory=False)
        self.logger.info(
            f"Loaded {len(self.transactions):,} transactions, {len(self.customers):,} customers, "
            f"{len(self.accounts):,} accounts for quality assessment"
        )

    def _record(self, category: str, check_name: str, failed_count: int, total: int, details: str = ""):
        pass_rate = 100.0 if total == 0 else round((total - failed_count) / total * 100, 2)
        self.checks.append({
            "category": category,
            "check_name": check_name,
            "total_records": total,
            "failed_records": int(failed_count),
            "pass_rate_pct": pass_rate,
            "details": details,
        })
        self.logger.info(f"[{category}] {check_name}: {failed_count:,}/{total:,} failed ({pass_rate}% pass) {details}")

    # ---- Completeness --------------------------------------------------

    def check_completeness(self):
        df = self.transactions
        total = len(df)

        missing_txn_id = df["transaction_id"].isna().sum()
        self._record("Completeness", "Missing Transaction ID", missing_txn_id, total)

        missing_account_id = df["account_id"].isna().sum()
        self._record("Completeness", "Missing Account ID (proxy for Customer linkage)", missing_account_id, total)

        missing_amount = df["transaction_amount"].isna().sum()
        self._record("Completeness", "Missing Transaction Amount", missing_amount, total)

        missing_type = df["transaction_type"].isna().sum()
        self._record("Completeness", "Missing Transaction Type", missing_type, total)

    # ---- Validity -------------------------------------------------------

    def check_validity(self):
        df = self.transactions
        total = len(df)

        amounts = pd.to_numeric(df["transaction_amount"], errors="coerce")
        invalid_amount = ((amounts <= 0) | amounts.isna()).sum()
        self._record("Validity", "Negative/Invalid/Zero Transaction Amount", invalid_amount, total)

        parsed_dates = pd.to_datetime(df["transaction_date"], errors="coerce", format="mixed")
        invalid_dates = parsed_dates.isna().sum()
        self._record("Validity", "Invalid Transaction Date", invalid_dates, total)

        invalid_txn_type = (~df["transaction_type"].isin(VALID_TXN_TYPES)).sum()
        self._record("Validity", "Invalid Transaction Type", invalid_txn_type, total)

        acct_total = len(self.accounts)
        invalid_acct_type = (~self.accounts["account_type"].isin(VALID_ACCOUNT_TYPES)).sum()
        self._record("Validity", "Invalid Account Type", invalid_acct_type, acct_total)

    # ---- Uniqueness -------------------------------------------------------

    def check_uniqueness(self):
        df = self.transactions
        total = len(df)

        dup_txn_ids = df["transaction_id"].duplicated(keep=False).sum()
        self._record("Uniqueness", "Duplicate Transaction IDs", dup_txn_ids, total)

        cust_total = len(self.customers)
        dup_cust_ids = self.customers["customer_id"].duplicated(keep=False).sum()
        self._record("Uniqueness", "Duplicate Customer IDs", dup_cust_ids, cust_total)

    # ---- Consistency -------------------------------------------------------

    def check_consistency(self):
        total = len(self.transactions)

        valid_account_ids = set(self.accounts["account_id"].dropna())
        orphan_accounts = (~self.transactions["account_id"].isin(valid_account_ids)).sum()
        self._record(
            "Consistency",
            "Transaction Account ID not found in Account master",
            orphan_accounts,
            total,
        )

        valid_customer_ids = set(self.customers["customer_id"].dropna())
        orphan_customers = (~self.accounts["customer_id"].isin(valid_customer_ids)).sum()
        self._record(
            "Consistency",
            "Account Customer ID not found in Customer master",
            orphan_customers,
            len(self.accounts),
        )

    # ---- Overall scorecard -------------------------------------------------------

    def compute_scorecard(self) -> dict:
        df = self.transactions
        total_records = len(df)

        amounts = pd.to_numeric(df["transaction_amount"], errors="coerce")
        parsed_dates = pd.to_datetime(df["transaction_date"], errors="coerce", format="mixed")
        valid_account_ids = set(self.accounts["account_id"].dropna())

        is_valid = (
            df["transaction_id"].notna()
            & df["account_id"].notna()
            & amounts.notna()
            & (amounts > 0)
            & parsed_dates.notna()
            & df["transaction_type"].isin(VALID_TXN_TYPES)
            & df["account_id"].isin(valid_account_ids)
            & (~df["transaction_id"].duplicated(keep="first"))
        )

        valid_records = int(is_valid.sum())
        rejected_records = total_records - valid_records
        quality_score = round(valid_records / total_records * 100, 2) if total_records else 0.0

        scorecard = {
            "total_records": total_records,
            "valid_records": valid_records,
            "rejected_records": rejected_records,
            "data_quality_score_pct": quality_score,
            "quality_gate_threshold_pct": QUALITY_GATE_THRESHOLD,
            "passed_quality_gate": quality_score >= QUALITY_GATE_THRESHOLD,
        }
        return scorecard

    def run_all(self) -> dict:
        self.load()
        self.logger.info("=" * 70)
        self.logger.info("RUNNING DATA QUALITY CHECKS (Phase 3)")
        self.logger.info("=" * 70)

        self.check_completeness()
        self.check_validity()
        self.check_uniqueness()
        self.check_consistency()

        scorecard = self.compute_scorecard()

        report = {
            "run_at": datetime.now().isoformat(),
            "scorecard": scorecard,
            "checks": self.checks,
        }
        return report


def write_reports(report: dict, logger):
    json_path = LOG_DIR / "data_quality_report.json"
    with open(json_path, "w") as f:
        json.dump(report, f, indent=2)
    logger.info(f"JSON quality report written to {json_path}")

    sc = report["scorecard"]
    lines = [
        "=" * 60,
        " BANKING DATA QUALITY SCORECARD",
        "=" * 60,
        f" Total Records       : {sc['total_records']:,}",
        f" Valid Records       : {sc['valid_records']:,}",
        f" Rejected Records    : {sc['rejected_records']:,}",
        f" Data Quality Score  : {sc['data_quality_score_pct']}%",
        f" Quality Gate ({sc['quality_gate_threshold_pct']}%)  : {'PASSED' if sc['passed_quality_gate'] else 'FAILED'}",
        "=" * 60,
        "",
        " Detail by category:",
        "-" * 60,
    ]
    for category in ["Completeness", "Validity", "Uniqueness", "Consistency"]:
        lines.append(f" {category}:")
        for c in report["checks"]:
            if c["category"] == category:
                lines.append(
                    f"   - {c['check_name']:<55} failed={c['failed_records']:>7,}  "
                    f"pass_rate={c['pass_rate_pct']}%"
                )
        lines.append("")

    txt_path = LOG_DIR / "data_quality_report.txt"
    with open(txt_path, "w") as f:
        f.write("\n".join(lines))
    logger.info(f"Text quality report written to {txt_path}")

    print("\n".join(lines))


def main():
    logger = _setup_logging()
    checker = DataQualityChecker(logger)
    report = checker.run_all()
    write_reports(report, logger)

    # Non-zero exit signals Airflow to halt the pipeline before load_to_sql
    sys.exit(0 if report["scorecard"]["passed_quality_gate"] else 1)


if __name__ == "__main__":
    main()
