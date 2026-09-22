-- ============================================================================
-- queries.sql
-- Phase 4 — Analytical Queries against the Banking Risk Data Warehouse
-- ============================================================================
-- Target: sqlite (warehouse/banking_dw.sqlite). Every query below has been
-- executed against the loaded warehouse to confirm it runs correctly (see
-- docs/sample_query_outputs.md for captured results).
-- ============================================================================


-- ----------------------------------------------------------------------------
-- 1. Total transaction value (overall, and by year-month)
-- ----------------------------------------------------------------------------
SELECT
    COUNT(*)                    AS total_transactions,
    ROUND(SUM(transaction_amount), 2) AS total_transaction_value
FROM fact_transaction;

SELECT
    txn_year_month,
    COUNT(*)                           AS transaction_count,
    ROUND(SUM(transaction_amount), 2)  AS total_value
FROM fact_transaction
GROUP BY txn_year_month
ORDER BY txn_year_month;


-- ----------------------------------------------------------------------------
-- 2. Average transaction amount (overall, by transaction type)
-- ----------------------------------------------------------------------------
SELECT ROUND(AVG(transaction_amount), 2) AS avg_transaction_amount
FROM fact_transaction;

SELECT
    transaction_type,
    COUNT(*)                          AS txn_count,
    ROUND(AVG(transaction_amount), 2) AS avg_amount
FROM fact_transaction
GROUP BY transaction_type
ORDER BY avg_amount DESC;


-- ----------------------------------------------------------------------------
-- 3. High-value transactions (> 100,000) — detail + summary
-- ----------------------------------------------------------------------------
SELECT
    transaction_id, customer_id, branch_id, transaction_date,
    transaction_amount, transaction_type, payment_status
FROM fact_transaction
WHERE is_high_value = 1
ORDER BY transaction_amount DESC
LIMIT 100;

SELECT
    COUNT(*)                          AS high_value_txn_count,
    ROUND(SUM(transaction_amount), 2) AS high_value_txn_total
FROM fact_transaction
WHERE is_high_value = 1;


-- ----------------------------------------------------------------------------
-- 4. Failed transactions — rate overall and by channel
-- ----------------------------------------------------------------------------
SELECT
    COUNT(*)                                                        AS total_txns,
    SUM(is_failed)                                                  AS failed_txns,
    ROUND(SUM(is_failed) * 100.0 / COUNT(*), 2)                     AS failed_rate_pct
FROM fact_transaction;

SELECT
    channel,
    COUNT(*)                                     AS total_txns,
    SUM(is_failed)                               AS failed_txns,
    ROUND(SUM(is_failed) * 100.0 / COUNT(*), 2)  AS failed_rate_pct
FROM fact_transaction
GROUP BY channel
ORDER BY failed_rate_pct DESC;


-- ----------------------------------------------------------------------------
-- 5. Customer risk — top 20 highest-risk customers, and tier distribution
-- ----------------------------------------------------------------------------
SELECT
    customer_id, credit_score, risk_score, risk_tier,
    total_transactions, failed_transactions, defaulted_loans, overdue_loans
FROM customer_risk_metrics
ORDER BY risk_score DESC
LIMIT 20;

SELECT
    risk_tier,
    COUNT(*)                                            AS customer_count,
    ROUND(AVG(risk_score), 2)                           AS avg_risk_score,
    ROUND(AVG(credit_score), 0)                         AS avg_credit_score,
    ROUND(SUM(total_transaction_value), 2)              AS total_txn_value
FROM customer_risk_metrics
GROUP BY risk_tier
ORDER BY avg_risk_score DESC;


-- ----------------------------------------------------------------------------
-- 6. Monthly transaction trends (count, value, high-value share)
-- ----------------------------------------------------------------------------
SELECT
    d.year,
    d.month,
    d.month_name,
    COUNT(f.transaction_id)                                          AS txn_count,
    ROUND(SUM(f.transaction_amount), 2)                              AS total_value,
    ROUND(SUM(f.is_high_value) * 100.0 / COUNT(f.transaction_id), 2) AS high_value_pct
FROM fact_transaction f
JOIN dim_date d ON f.transaction_date = d.date_key
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;


-- ----------------------------------------------------------------------------
-- 7. Branch performance — value, failure rate, active customers, risk mix
-- ----------------------------------------------------------------------------
SELECT
    b.branch_name,
    b.city,
    COUNT(f.transaction_id)                                        AS total_transactions,
    ROUND(SUM(f.transaction_amount), 2)                            AS total_value,
    ROUND(SUM(f.is_failed) * 100.0 / COUNT(f.transaction_id), 2)   AS failure_rate_pct,
    COUNT(DISTINCT f.customer_id)                                  AS active_customers
FROM fact_transaction f
JOIN dim_branch b ON f.branch_id = b.branch_id
GROUP BY b.branch_id, b.branch_name, b.city
ORDER BY total_value DESC;

-- Branch performance joined with average customer risk score of its customers
SELECT
    b.branch_name,
    COUNT(DISTINCT a.customer_id)          AS customers_served,
    ROUND(AVG(crm.risk_score), 2)          AS avg_customer_risk_score,
    SUM(CASE WHEN crm.risk_tier = 'High' THEN 1 ELSE 0 END) AS high_risk_customers
FROM dim_account a
JOIN dim_branch b ON a.branch_id = b.branch_id
JOIN customer_risk_metrics crm ON a.customer_id = crm.customer_id
GROUP BY b.branch_name
ORDER BY avg_customer_risk_score DESC;


-- ----------------------------------------------------------------------------
-- 8. Loan default rate — overall and by loan type
-- ----------------------------------------------------------------------------
SELECT
    COUNT(*)                                                     AS total_loans,
    SUM(CASE WHEN loan_status = 'Defaulted' THEN 1 ELSE 0 END)   AS defaulted_loans,
    ROUND(SUM(CASE WHEN loan_status = 'Defaulted' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS default_rate_pct
FROM fact_loan;

SELECT
    loan_type,
    COUNT(*)                                                     AS total_loans,
    SUM(CASE WHEN loan_status = 'Defaulted' THEN 1 ELSE 0 END)   AS defaulted_loans,
    ROUND(SUM(CASE WHEN loan_status = 'Defaulted' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS default_rate_pct,
    ROUND(AVG(interest_rate), 2)                                 AS avg_interest_rate,
    ROUND(SUM(loan_amount), 2)                                   AS total_loan_book
FROM fact_loan
GROUP BY loan_type
ORDER BY default_rate_pct DESC;

-- Loan default rate cross-cut by customer risk tier (a key Deloitte-style
-- risk-technology query: "does our risk score actually predict default?")
SELECT
    crm.risk_tier,
    COUNT(fl.loan_id)                                                     AS total_loans,
    SUM(CASE WHEN fl.loan_status = 'Defaulted' THEN 1 ELSE 0 END)         AS defaulted_loans,
    ROUND(SUM(CASE WHEN fl.loan_status = 'Defaulted' THEN 1 ELSE 0 END)
          * 100.0 / COUNT(fl.loan_id), 2)                                 AS default_rate_pct
FROM fact_loan fl
JOIN customer_risk_metrics crm ON fl.customer_id = crm.customer_id
GROUP BY crm.risk_tier
ORDER BY default_rate_pct DESC;


-- ----------------------------------------------------------------------------
-- 9. Data quality trend (for the Executive Overview "Data Quality %" KPI)
-- ----------------------------------------------------------------------------
SELECT
    run_at, total_records, valid_records, rejected_records,
    data_quality_score_pct, passed_quality_gate
FROM data_quality_log
ORDER BY run_at DESC
LIMIT 10;


-- ----------------------------------------------------------------------------
-- 10. Credit score distribution (for Power BI Risk Analysis page histogram)
-- ----------------------------------------------------------------------------
SELECT
    CASE
        WHEN credit_score < 500 THEN '< 500'
        WHEN credit_score < 600 THEN '500-599'
        WHEN credit_score < 700 THEN '600-699'
        WHEN credit_score < 800 THEN '700-799'
        ELSE '800+'
    END AS credit_score_band,
    COUNT(*) AS customer_count
FROM dim_customer
GROUP BY credit_score_band
ORDER BY MIN(credit_score);
