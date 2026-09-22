# Power BI — Banking Risk & Transaction Dashboard

Phase 6 of the pipeline. Power BI Desktop is a Windows GUI application, so it
can't be scripted/run from this Linux sandbox — but everything it needs is
ready to import:

- `data_extracts/*.csv` — every warehouse table, already exported and
  verified (see `export_for_powerbi.py`)
- `warehouse/banking_dw.sqlite` — same data, importable directly via
  Get Data > ODBC/SQLite if you install a SQLite ODBC driver, or via the CSVs
  (simpler, zero-driver-install option — recommended for the demo)

This document is the exact build spec: connect, model, measure, build pages.

---

## 1. Connect

**Option A — CSV import (recommended, no drivers needed)**
Power BI Desktop → Get Data → Text/CSV → import each file from
`powerbi/data_extracts/`:
`dim_date.csv, dim_branch.csv, dim_customer.csv, dim_account.csv, fact_transaction.csv, fact_loan.csv, customer_risk_metrics.csv, data_quality_log.csv`

**Option B — Direct SQLite connection**
Get Data → More → Database → ODBC (requires a SQLite ODBC driver installed
locally) → point at `warehouse/banking_dw.sqlite`. This mirrors how you'd
connect Power BI to a real production warehouse (SQL Server/Snowflake) with
Get Data → SQL Server / Snowflake instead.

Set data types on import: `dim_date.date_key`, `fact_transaction.transaction_date`,
`fact_loan.loan_start_date` → Date; monetary columns → Fixed decimal number.

---

## 2. Data model (star schema relationships)

Build these relationships in Model view (all Many-to-One, single direction,
from fact → dimension):

| From (fact)                          | To (dimension)          |
|---------------------------------------|--------------------------|
| fact_transaction[transaction_date]     | dim_date[date_key]       |
| fact_transaction[customer_id]          | dim_customer[customer_id]|
| fact_transaction[branch_id]            | dim_branch[branch_id]    |
| fact_transaction[account_id]           | dim_account[account_id]  |
| fact_loan[customer_id]                 | dim_customer[customer_id]|
| fact_loan[loan_start_date]             | dim_date[date_key]       |
| dim_account[branch_id]                 | dim_branch[branch_id]    |
| customer_risk_metrics[customer_id]     | dim_customer[customer_id]|

This gives you a clean star schema: `dim_date` and `dim_branch` sit at the
center, `fact_transaction`/`fact_loan` are the transactional grain, and
`customer_risk_metrics` is a risk mart hanging off `dim_customer` for
fast, pre-aggregated risk visuals.

Mark **dim_date** as a Date Table (Model view → select table → "Mark as
date table" → column `date_key`) so time-intelligence DAX works correctly.

---

## 3. DAX measures

Create a dedicated **Measures** table (Modeling → New Table → `Measures = {}`)
and add these:

```dax
Total Transactions = COUNTROWS(fact_transaction)

Total Transaction Value = SUM(fact_transaction[transaction_amount])

Avg Transaction Value = AVERAGE(fact_transaction[transaction_amount])

Active Customers = DISTINCTCOUNT(fact_transaction[customer_id])

Failed Transactions = CALCULATE([Total Transactions], fact_transaction[payment_status] = "Failed")

Failed Transaction Rate % =
DIVIDE([Failed Transactions], [Total Transactions], 0) * 100

High Value Transactions = CALCULATE([Total Transactions], fact_transaction[is_high_value] = 1)

High Risk Customers = CALCULATE(DISTINCTCOUNT(customer_risk_metrics[customer_id]), customer_risk_metrics[risk_tier] = "High")

Medium Risk Customers = CALCULATE(DISTINCTCOUNT(customer_risk_metrics[customer_id]), customer_risk_metrics[risk_tier] = "Medium")

Low Risk Customers = CALCULATE(DISTINCTCOUNT(customer_risk_metrics[customer_id]), customer_risk_metrics[risk_tier] = "Low")

Avg Risk Score = AVERAGE(customer_risk_metrics[risk_score])

Total Loans = COUNTROWS(fact_loan)

Defaulted Loans = CALCULATE([Total Loans], fact_loan[loan_status] = "Defaulted")

Loan Default Rate % = DIVIDE([Defaulted Loans], [Total Loans], 0) * 100

Total Loan Book = SUM(fact_loan[loan_amount])

Data Quality Score % = MAX(data_quality_log[data_quality_score_pct])

-- Time intelligence (works because dim_date is marked as a Date Table)
Transaction Value MoM % =
VAR CurrentValue = [Total Transaction Value]
VAR PriorValue = CALCULATE([Total Transaction Value], DATEADD(dim_date[date_key], -1, MONTH))
RETURN DIVIDE(CurrentValue - PriorValue, PriorValue, 0) * 100
```

---

## 4. Page layouts

### Page 1 — Executive Overview
- **KPI cards** (top row): `[Total Transactions]`, `[Total Transaction Value]`,
  `[Active Customers]`, `[Failed Transactions]`, `[High Risk Customers]`,
  `[Data Quality Score %]`
- Line chart: `[Total Transaction Value]` by `dim_date[year_month]`
- Donut: transactions by `payment_status`

### Page 2 — Transaction Analysis
- Line/area chart: `[Total Transactions]` by `dim_date[month_name]` (sorted by month number)
- Bar chart: `[Total Transaction Value]` by `fact_transaction[transaction_type]`
- Bar chart: `[Total Transaction Value]` by `dim_branch[branch_name]`
- Stacked column: `[Total Transactions]` split by `payment_status` (Success vs Failed vs Pending vs Reversed)
- Slicers: `dim_date[year]`, `dim_branch[branch_name]`, `fact_transaction[channel]`

### Page 3 — Risk Analysis
- Donut/bar: customer count by `customer_risk_metrics[risk_tier]` (High/Medium/Low)
- Bar chart: `[Avg Risk Score]` by `dim_branch[branch_name]` (via dim_account → dim_branch)
- Table/bar: `[High Value Transactions]` by `dim_branch[branch_name]`
- Bar chart: `[Loan Default Rate %]` by `fact_loan[loan_type]`
- Histogram: `dim_customer[credit_score]` binned (use the `credit_score_band`
  logic from `sql/queries.sql` Query 10, or Power BI's built-in binning on
  `credit_score` with a bin size of 100)
- Scatter: `credit_score` (x) vs `risk_score` (y), sized by `total_loan_amount`,
  colored by `risk_tier` — visually demonstrates the risk score is doing its job

---

## 5. Suggested visual polish (screenshot-readiness)

- Apply a consistent theme: View → Themes → pick a clean corporate theme (navy/gold or navy/teal reads well for a "risk/banking" aesthetic)
- Use card visuals with conditional formatting (red/amber/green) on
  `[Loan Default Rate %]` and `[Data Quality Score %]`
- Add a title text box per page: "Banking Risk & Transaction Dashboard — Executive Overview / Transaction Analysis / Risk Analysis"
- Pin the Data Quality Score as a gauge visual (target = 90%, matching the
  `QUALITY_GATE_THRESHOLD` in `quality/data_quality.py`) — nice visual tie-back
  to the automated DQ gate story

---

## 6. Refresh story (tie back to Airflow)

In a real deployment, Power BI would connect live/via scheduled refresh to
the SQL warehouse (Postgres/SQL Server/Snowflake) that `load_to_sql.py`
populates. Since the Airflow DAG (Phase 5) runs `load_to_sql` as its final
data step before `pipeline_success`, a Power BI scheduled refresh set to run
shortly after the DAG's daily schedule keeps the dashboard current
automatically — worth a sentence in your resume/portfolio write-up as the
"last mile" of the architecture diagram.
