#!/usr/bin/env bash
# run_pipeline.sh
# Runs the full Banking ETL & Risk Analytics pipeline end-to-end, in order,
# mirroring the Airflow DAG's task sequence (airflow_dags/banking_etl_dag.py)
# but runnable directly with plain Python — no Airflow install required.
#
# Usage:
#   bash run_pipeline.sh
#
# Exits non-zero (and stops) if any phase fails, so a broken pipeline never
# silently loads bad data into the warehouse.

set -e  # stop immediately on any command failure
cd "$(dirname "$0")"

echo "=============================================================="
echo " BANKING ETL & RISK ANALYTICS PIPELINE — FULL RUN"
echo "=============================================================="

echo -e "\n[1/5] Generating synthetic source data..."
(cd data_generator && python3 generate_synthetic_data.py \
    --num_customers 5000 --num_accounts 8000 \
    --num_transactions 200000 --num_loans 3000)

echo -e "\n[2/5] Ingesting raw data (Phase 1)..."
(cd ingestion && python3 ingest.py)

echo -e "\n[3/5] Running PySpark ETL (Phase 2)..."
(cd etl && python3 pyspark_etl.py)

echo -e "\n[4/5] Running data quality checks (Phase 3)..."
(cd quality && python3 data_quality.py)

echo -e "\n[5/5] Loading SQL data warehouse (Phase 4)..."
(cd sql && python3 load_to_sql.py)

echo -e "\nExporting Power BI-ready CSV extracts..."
(cd powerbi && python3 export_for_powerbi.py)

echo -e "\nRunning test suite..."
python3 -m pytest tests/test_pipeline.py -v

echo -e "\n=============================================================="
echo " PIPELINE RUN COMPLETE"
echo " Warehouse: warehouse/banking_dw.sqlite"
echo " Logs:      logs/"
echo " Power BI:  powerbi/data_extracts/"
echo "=============================================================="
