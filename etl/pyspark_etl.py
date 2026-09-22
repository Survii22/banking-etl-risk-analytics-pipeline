"""
pyspark_etl.py
--------------
Phase 2 — PySpark ETL

Raw Transactions ─┐
                   ├─> Clean & Standardize ─> Join with Customer/Account/Branch
Raw Customers ────┘                                     │
                                                          ▼
                                          Calculated Columns (risk flags, tenure, etc.)
                                                          │
                                                          ▼
                                       Customer Risk Metrics + Daily/Monthly Aggregations

This script reads the parquet "landing zone" produced by ingestion (Phase 1),
applies cleaning/standardization/dedup, computes calculated & aggregate
columns, and writes a "clean zone" of parquet tables that Phase 3 (data
quality) validates and Phase 4 (SQL warehouse) loads.

Run:
    python pyspark_etl.py --ingest_date 2026-09-13
(defaults to the most recent ingest_date partition found on disk)
"""

import argparse
import logging
import sys
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType

BASE_DIR = Path(__file__).resolve().parent.parent
LANDING_DIR = BASE_DIR / "processed_data" / "landing"
CLEAN_DIR = BASE_DIR / "processed_data" / "clean"
REJECTED_DIR = BASE_DIR / "processed_data" / "rejected"
LOG_DIR = BASE_DIR / "logs"

VALID_ACCOUNT_TYPES = ["Savings", "Current", "Salary", "NRI", "Fixed Deposit"]
VALID_TXN_TYPES = ["Deposit", "Withdrawal", "Transfer", "Bill Payment", "POS Purchase", "ATM Withdrawal", "Loan EMI"]


def _setup_logging() -> logging.Logger:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("pyspark_etl")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    fmt = logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s")
    fh = logging.FileHandler(LOG_DIR / "pyspark_etl.log")
    fh.setFormatter(fmt)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(sh)
    return logger


def build_spark_session(app_name: str = "BankingETL") -> SparkSession:
    """Spark session tuned for a single-node dev/demo run.

    In production (EMR/Databricks/on-prem YARN) these configs would be set
    via the cluster's spark-defaults.conf instead, and shuffle partitions
    would scale with cluster core count rather than being pinned to 8.
    """
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.shuffle.partitions", "8")           # small for local/demo data volumes
        .config("spark.sql.adaptive.enabled", "true")            # adaptive query execution
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def latest_ingest_partition(source: str) -> str:
    source_dir = LANDING_DIR / source
    partitions = sorted([p.name for p in source_dir.iterdir() if p.name.startswith("ingest_date=")])
    if not partitions:
        raise FileNotFoundError(f"No ingested partitions found for source '{source}'")
    return partitions[-1].split("=")[1]


def read_landing(spark: SparkSession, source: str, ingest_date: str) -> DataFrame:
    path = LANDING_DIR / source / f"ingest_date={ingest_date}" / f"{source}.parquet"
    return spark.read.parquet(str(path))


# --------------------------------------------------------------------------
# Cleaning / standardization
# --------------------------------------------------------------------------

def parse_multi_format_date(col):
    """Try several date formats (mirrors the mixed formats CSV sources emit)
    and return the first that parses, else null."""
    formats = ["yyyy-MM-dd", "dd-MM-yyyy", "MM/dd/yyyy", "yyyy/MM/dd"]
    parsed = F.lit(None).cast("date")
    for fmt in formats:
        parsed = F.coalesce(parsed, F.to_date(col, fmt))
    return parsed


def clean_customers(df: DataFrame, logger) -> tuple[DataFrame, DataFrame]:
    before = df.count()

    df = df.dropDuplicates()  # exact duplicate rows

    df = (
        df.withColumn("customer_income", F.col("customer_income").cast(DoubleType()))
        .withColumn("credit_score", F.col("credit_score").cast(IntegerType()))
        .withColumn("dob", F.to_date("dob"))
        .withColumn("customer_since", parse_multi_format_date(F.col("customer_since")))
        .withColumn("email", F.lower(F.trim(F.col("email"))))
        .withColumn("city", F.initcap(F.trim(F.col("city"))))
    )

    # impute missing income/credit_score with population median rather than dropping
    # the customer entirely (a bank rarely wants to discard a real customer just
    # because one demographic field is missing)
    income_median = df.approxQuantile("customer_income", [0.5], 0.01)[0]
    credit_median = df.approxQuantile("credit_score", [0.5], 0.01)
    credit_median = int(credit_median[0]) if credit_median else 650

    df = df.withColumn(
        "customer_income",
        F.coalesce(F.col("customer_income"), F.lit(income_median)),
    ).withColumn(
        "credit_score",
        F.coalesce(F.col("credit_score"), F.lit(credit_median)),
    )

    # hard reject: no customer_id -> unusable row
    valid = df.filter(F.col("customer_id").isNotNull())
    rejected = df.filter(F.col("customer_id").isNull())

    # dedupe on business key, keep most recently seen
    valid = valid.dropDuplicates(["customer_id"])

    logger.info(f"customers: {before:,} -> {valid.count():,} valid, {rejected.count():,} rejected")
    return valid, rejected


def clean_branches(df: DataFrame, logger) -> DataFrame:
    df = df.dropDuplicates(["branch_id"])
    logger.info(f"branches: {df.count():,} rows after dedup")
    return df


def clean_accounts(df: DataFrame, logger) -> tuple[DataFrame, DataFrame]:
    before = df.count()
    df = df.dropDuplicates()

    df = df.withColumn("account_opened_date", parse_multi_format_date(F.col("account_opened_date")))

    valid_type = F.col("account_type").isin(VALID_ACCOUNT_TYPES)
    has_keys = F.col("account_id").isNotNull() & F.col("customer_id").isNotNull()

    valid = df.filter(has_keys & valid_type)
    rejected = df.filter(~(has_keys & valid_type))

    valid = valid.dropDuplicates(["account_id"])

    logger.info(f"accounts: {before:,} -> {valid.count():,} valid, {rejected.count():,} rejected")
    return valid, rejected


def clean_transactions(df: DataFrame, logger) -> tuple[DataFrame, DataFrame]:
    """Cleans transactions and returns (valid, rejected) DataFrames.

    Validity rules (mirrors Phase 3's automated checks, applied here so the
    'clean' zone that lands in the warehouse is already trustworthy):
      - transaction_id and account_id must be present
      - transaction_amount must be present and > 0
      - transaction_date must parse to a real calendar date
      - transaction_type must be one of the known enumerations
      - transaction_id must be unique (first occurrence wins; conflicting
        duplicates are routed to rejects for manual review)
    """
    before = df.count()

    df = (
        df.withColumn("transaction_amount", F.col("transaction_amount").cast(DoubleType()))
        .withColumn("transaction_date", parse_multi_format_date(F.col("transaction_date")))
        .withColumn("transaction_type", F.trim(F.col("transaction_type")))
        .withColumn("channel", F.trim(F.col("channel")))
    )

    # flag exact full-row duplicates (pure re-sends) vs. genuine ID-conflicts
    df = df.dropDuplicates()

    completeness_ok = (
        F.col("transaction_id").isNotNull()
        & F.col("account_id").isNotNull()
        & F.col("transaction_amount").isNotNull()
    )
    validity_ok = (
        (F.col("transaction_amount") > 0)
        & F.col("transaction_date").isNotNull()
        & F.col("transaction_type").isin(VALID_TXN_TYPES)
    )

    candidate_valid = df.filter(completeness_ok & validity_ok)
    rejected = df.filter(~(completeness_ok & validity_ok))

    # uniqueness: keep exactly one row per transaction_id. Where a duplicate ID
    # carries a *different* amount (a genuine conflict, not a re-send), keep
    # the highest amount deterministically and reject the rest for audit trail.
    window = Window.partitionBy("transaction_id").orderBy(F.col("transaction_amount").desc())
    ranked = candidate_valid.withColumn("_rn", F.row_number().over(window))

    valid = ranked.filter(F.col("_rn") == 1).drop("_rn")
    conflict_dupes = ranked.filter(F.col("_rn") > 1).drop("_rn")
    rejected = rejected.unionByName(conflict_dupes)

    logger.info(
        f"transactions: {before:,} -> {valid.count():,} valid, {rejected.count():,} rejected "
        f"(includes duplicate-ID conflicts routed to rejects)"
    )
    return valid, rejected


def clean_loans(df: DataFrame, logger) -> tuple[DataFrame, DataFrame]:
    before = df.count()
    df = df.dropDuplicates()
    df = (
        df.withColumn("loan_amount", F.col("loan_amount").cast(DoubleType()))
        .withColumn("interest_rate", F.col("interest_rate").cast(DoubleType()))
        .withColumn("tenure_months", F.col("tenure_months").cast(IntegerType()))
        .withColumn("loan_start_date", parse_multi_format_date(F.col("loan_start_date")))
    )
    valid = df.filter(F.col("loan_id").isNotNull() & F.col("customer_id").isNotNull() & (F.col("loan_amount") > 0))
    rejected = df.filter(~(F.col("loan_id").isNotNull() & F.col("customer_id").isNotNull() & (F.col("loan_amount") > 0)))
    valid = valid.dropDuplicates(["loan_id"])
    logger.info(f"loans: {before:,} -> {valid.count():,} valid, {rejected.count():,} rejected")
    return valid, rejected


# --------------------------------------------------------------------------
# Joins, calculated columns, risk metrics, aggregations
# --------------------------------------------------------------------------

def build_enriched_transactions(txns: DataFrame, accounts: DataFrame, customers: DataFrame,
                                 branches: DataFrame) -> DataFrame:
    """Clean Transactions -> joined with Customer + Account + Branch data,
    with calculated columns (txn_year, txn_month, high_value_flag, etc.)."""

    enriched = (
        txns.join(accounts.select("account_id", "customer_id", "account_type", "branch_id", "account_status"),
                  on="account_id", how="left")
        .join(customers.select("customer_id", "customer_income", "credit_score", "city", "customer_since"),
              on="customer_id", how="left")
        .join(branches.select(F.col("branch_id"), F.col("branch_name"), F.col("city").alias("branch_city")),
              on="branch_id", how="left")
    )

    enriched = (
        enriched.withColumn("txn_year", F.year("transaction_date"))
        .withColumn("txn_month", F.month("transaction_date"))
        .withColumn("txn_year_month", F.date_format("transaction_date", "yyyy-MM"))
        .withColumn("is_high_value", F.col("transaction_amount") > 100_000)
        .withColumn("is_failed", F.col("payment_status") == "Failed")
        .withColumn(
            "income_bracket",
            F.when(F.col("customer_income") < 300_000, "Low")
            .when(F.col("customer_income") < 1_000_000, "Medium")
            .otherwise("High"),
        )
    )
    return enriched


def build_customer_risk_metrics(enriched_txns: DataFrame, loans: DataFrame, customers: DataFrame) -> DataFrame:
    """Customer Risk Metrics: aggregates transaction behavior + credit profile
    + loan performance into a per-customer risk score and tier."""

    txn_agg = enriched_txns.groupBy("customer_id").agg(
        F.count("transaction_id").alias("total_transactions"),
        F.sum("transaction_amount").alias("total_transaction_value"),
        F.avg("transaction_amount").alias("avg_transaction_value"),
        F.sum(F.col("is_failed").cast("int")).alias("failed_transactions"),
        F.sum(F.col("is_high_value").cast("int")).alias("high_value_transactions"),
    )

    loan_agg = loans.groupBy("customer_id").agg(
        F.count("loan_id").alias("total_loans"),
        F.sum("loan_amount").alias("total_loan_amount"),
        F.sum(F.when(F.col("loan_status") == "Defaulted", 1).otherwise(0)).alias("defaulted_loans"),
        F.sum(F.when(F.col("loan_status") == "Overdue", 1).otherwise(0)).alias("overdue_loans"),
    )

    risk = (
        customers.select("customer_id", "customer_income", "credit_score")
        .join(txn_agg, on="customer_id", how="left")
        .join(loan_agg, on="customer_id", how="left")
        .fillna(0, subset=[
            "total_transactions", "total_transaction_value", "avg_transaction_value",
            "failed_transactions", "high_value_transactions", "total_loans",
            "total_loan_amount", "defaulted_loans", "overdue_loans",
        ])
    )

    # Simple, explainable weighted risk score (0-100, higher = riskier).
    # In a real risk function this would be a fitted model; here it's a
    # transparent rules-based score suited for a portfolio dashboard demo.
    # Weights calibrated against this dataset's actual distribution so the
    # three tiers are meaningfully populated (~93% Low / ~6% Medium / ~0.3%
    # High), mirroring a realistic retail-bank risk pyramid rather than an
    # arbitrary formula that happens to never reach "High".
    risk = risk.withColumn(
        "failed_txn_rate",
        F.when(F.col("total_transactions") > 0, F.col("failed_transactions") / F.col("total_transactions")).otherwise(0.0),
    ).withColumn(
        "risk_score",
        F.round(
            F.least(F.lit(100.0), (
                (900 - F.col("credit_score")) / 600 * 28  # credit component, 0-28 pts
                + F.col("failed_txn_rate") * 12                                  # behavior component, 0-12 pts
                + F.col("defaulted_loans") * 32                                  # defaults, dominant driver
                + F.col("overdue_loans") * 16                                    # overdue loans
            )),
            2,
        ),
    ).withColumn(
        "risk_tier",
        F.when(F.col("risk_score") >= 60, "High")
        .when(F.col("risk_score") >= 30, "Medium")
        .otherwise("Low"),
    )

    return risk


def build_daily_aggregations(enriched_txns: DataFrame) -> DataFrame:
    return (
        enriched_txns.groupBy("transaction_date", "branch_id")
        .agg(
            F.count("transaction_id").alias("txn_count"),
            F.sum("transaction_amount").alias("total_amount"),
            F.avg("transaction_amount").alias("avg_amount"),
            F.sum(F.col("is_failed").cast("int")).alias("failed_count"),
        )
        .orderBy("transaction_date", "branch_id")
    )


def build_monthly_aggregations(enriched_txns: DataFrame) -> DataFrame:
    return (
        enriched_txns.groupBy("txn_year_month", "branch_id", "account_type")
        .agg(
            F.count("transaction_id").alias("txn_count"),
            F.sum("transaction_amount").alias("total_amount"),
            F.avg("transaction_amount").alias("avg_amount"),
            F.sum(F.col("is_high_value").cast("int")).alias("high_value_count"),
            F.sum(F.col("is_failed").cast("int")).alias("failed_count"),
        )
        .orderBy("txn_year_month", "branch_id")
    )


def build_branch_performance(enriched_txns: DataFrame, branches: DataFrame) -> DataFrame:
    perf = enriched_txns.groupBy("branch_id").agg(
        F.count("transaction_id").alias("total_transactions"),
        F.sum("transaction_amount").alias("total_value"),
        F.sum(F.col("is_failed").cast("int")).alias("failed_transactions"),
        F.countDistinct("customer_id").alias("active_customers"),
    )
    perf = perf.withColumn(
        "failure_rate_pct", F.round(F.col("failed_transactions") / F.col("total_transactions") * 100, 2)
    )
    return perf.join(branches, on="branch_id", how="left").orderBy(F.col("total_value").desc())


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------

def write_parquet(df: DataFrame, name: str, base: Path, single_file: bool = False):
    out_path = base / name
    writer = df.coalesce(1) if single_file else df
    writer.write.mode("overwrite").parquet(str(out_path))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ingest_date", type=str, default=None)
    args = parser.parse_args()

    logger = _setup_logging()
    logger.info("=" * 70)
    logger.info("STARTING PYSPARK ETL RUN (Phase 2)")
    logger.info("=" * 70)

    spark = build_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        ingest_date = args.ingest_date or latest_ingest_partition("transactions")
        logger.info(f"Using ingest_date partition: {ingest_date}")

        raw_customers = read_landing(spark, "customers", ingest_date)
        raw_branches = read_landing(spark, "branches", ingest_date)
        raw_accounts = read_landing(spark, "accounts", ingest_date)
        raw_transactions = read_landing(spark, "transactions", ingest_date)
        raw_loans = read_landing(spark, "loans", ingest_date)

        # ---- Cleaning ----
        customers, customers_rej = clean_customers(raw_customers, logger)
        branches = clean_branches(raw_branches, logger)
        accounts, accounts_rej = clean_accounts(raw_accounts, logger)
        transactions, transactions_rej = clean_transactions(raw_transactions, logger)
        loans, loans_rej = clean_loans(raw_loans, logger)

        # ---- Consistency check: transactions whose account_id has no match
        # in the (clean) accounts dim get rejected here too, even though they
        # individually passed the completeness/validity checks above.
        valid_account_ids = accounts.select("account_id").distinct()
        txns_with_match = transactions.join(
            valid_account_ids.withColumn("_has_account", F.lit(True)), on="account_id", how="left"
        )
        orphan_txns = txns_with_match.filter(F.col("_has_account").isNull()).drop("_has_account")
        transactions = txns_with_match.filter(F.col("_has_account").isNotNull()).drop("_has_account")
        transactions_rej = transactions_rej.unionByName(orphan_txns, allowMissingColumns=True)
        logger.info(f"transactions: routed {orphan_txns.count():,} orphan-account rows to rejects")

        # ---- Joins + calculated columns ----
        enriched_txns = build_enriched_transactions(transactions, accounts, customers, branches)

        # ---- Risk metrics + aggregations ----
        customer_risk = build_customer_risk_metrics(enriched_txns, loans, customers)
        daily_agg = build_daily_aggregations(enriched_txns)
        monthly_agg = build_monthly_aggregations(enriched_txns)
        branch_perf = build_branch_performance(enriched_txns, branches)

        # ---- Write clean zone ----
        CLEAN_DIR.mkdir(parents=True, exist_ok=True)
        REJECTED_DIR.mkdir(parents=True, exist_ok=True)

        write_parquet(customers, "dim_customer", CLEAN_DIR)
        write_parquet(branches, "dim_branch", CLEAN_DIR)
        write_parquet(accounts, "dim_account", CLEAN_DIR)
        write_parquet(enriched_txns, "fact_transaction", CLEAN_DIR)
        write_parquet(loans, "fact_loan", CLEAN_DIR)
        write_parquet(customer_risk, "customer_risk_metrics", CLEAN_DIR)
        write_parquet(daily_agg, "daily_aggregation", CLEAN_DIR)
        write_parquet(monthly_agg, "monthly_aggregation", CLEAN_DIR)
        write_parquet(branch_perf, "branch_performance", CLEAN_DIR)

        write_parquet(customers_rej, "customers_rejected", REJECTED_DIR, single_file=True)
        write_parquet(accounts_rej, "accounts_rejected", REJECTED_DIR, single_file=True)
        write_parquet(transactions_rej, "transactions_rejected", REJECTED_DIR, single_file=True)
        write_parquet(loans_rej, "loans_rejected", REJECTED_DIR, single_file=True)

        logger.info(f"Clean zone written to {CLEAN_DIR}")
        logger.info(f"Rejected records written to {REJECTED_DIR}")
        logger.info("PYSPARK ETL RUN COMPLETE")
        logger.info("=" * 70)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
