# Sample Query Outputs

Captured from a live run of sql/queries.sql against warehouse/banking_dw.sqlite.

## 1a. Total transaction value (overall)

| total_transactions | total_transaction_value |
|---|---|
| 193955 | 5010801699.67 |

## 1b. Total transaction value by month

| txn_year_month | transaction_count | total_value |
|---|---|---|
| 2024-01 | 5962 | 142454174.47 |
| 2024-02 | 5829 | 145762877.85 |
| 2024-03 | 5986 | 153863132.71 |
| 2024-04 | 5928 | 162110277.14 |
| 2024-05 | 6141 | 153699376.77 |
| 2024-06 | 5737 | 169367974.51 |
| 2024-07 | 6026 | 145642575.01 |
| 2024-08 | 6173 | 173249123.8 |

## 2a. Average transaction amount (overall)

| avg_transaction_amount |
|---|
| 25834.87 |

## 2b. Average transaction amount by type

| transaction_type | txn_count | avg_amount |
|---|---|---|
| Deposit | 27732 | 27727.59 |
| Transfer | 28025 | 26149.11 |
| Loan EMI | 27453 | 25730.18 |
| ATM Withdrawal | 27962 | 25681.78 |
| Withdrawal | 27805 | 25568.58 |
| POS Purchase | 27836 | 25086.9 |
| Bill Payment | 27142 | 24880.03 |

## 3a. High-value transactions (detail, top rows)

| transaction_id | customer_id | branch_id | transaction_date | transaction_amount | transaction_type | payment_status |
|---|---|---|---|---|---|---|
| TXN00034275 | CUST004646 | BR009 | 2024-05-18 | 1999807.04 | Bill Payment | Success |
| TXN00002960 | CUST003117 | BR002 | 2026-01-18 | 1999522.14 | Deposit | Success |
| TXN00045057 | CUST000947 | BR007 | 2025-04-19 | 1999514.71 | Transfer | Success |
| TXN00075993 | CUST000165 | BR010 | 2026-04-22 | 1998945.77 | Transfer | Success |
| TXN00034073 | CUST004251 | BR005 | 2024-01-18 | 1998637.61 | Transfer | Success |
| TXN00182057 | CUST004858 | BR007 | 2024-06-16 | 1998488.47 | Loan EMI | Success |
| TXN00174292 | CUST000750 | BR008 | 2024-03-24 | 1998446.99 | Withdrawal | Success |
| TXN00090030 | CUST004594 | BR004 | 2024-05-13 | 1998185.04 | ATM Withdrawal | Pending |

## 3b. High-value transactions (summary)

| high_value_txn_count | high_value_txn_total |
|---|---|
| 3888 | 4062001078.44 |

## 4a. Failed transactions (overall rate)

| total_txns | failed_txns | failed_rate_pct |
|---|---|---|
| 193955 | 11576 | 5.97 |

## 4b. Failed transactions by channel

| channel | total_txns | failed_txns | failed_rate_pct |
|---|---|---|---|
| POS | 38686 | 2381 | 6.15 |
| Branch | 38966 | 2357 | 6.05 |
| Internet Banking | 38769 | 2335 | 6.02 |
| Mobile App | 38667 | 2306 | 5.96 |
| ATM | 38867 | 2197 | 5.65 |

## 5a. Top 20 highest-risk customers

| customer_id | credit_score | risk_score | risk_tier | total_transactions | failed_transactions | defaulted_loans | overdue_loans |
|---|---|---|---|---|---|---|---|
| CUST000835 | 702 | 89.24 | High | 0 | 0 | 2 | 1 |
| CUST000645 | 499 | 67.02 | High | 79 | 2 | 1 | 1 |
| CUST004990 | 510 | 66.91 | High | 17 | 1 | 1 | 1 |
| CUST002103 | 552 | 64.55 | High | 39 | 1 | 1 | 1 |
| CUST000575 | 610 | 62.3 | High | 94 | 6 | 1 | 1 |
| CUST003777 | 625 | 61.99 | High | 52 | 5 | 1 | 1 |
| CUST004909 | 630 | 61.86 | High | 57 | 6 | 1 | 1 |
| CUST001856 | 616 | 61.56 | High | 39 | 1 | 1 | 1 |

## 5b. Risk tier distribution

| risk_tier | customer_count | avg_risk_score | avg_credit_score | total_txn_value |
|---|---|---|---|---|
| High | 13 | 64.61 | 611.0 | 13062648.74 |
| Medium | 304 | 41.44 | 656.0 | 322263049.8 |
| Low | 4683 | 11.63 | 681.0 | 4675476001.13 |

## 6. Monthly transaction trends

| year | month | month_name | txn_count | total_value | high_value_pct |
|---|---|---|---|---|---|
| 2024 | 1 | January | 5962 | 142454174.47 | 1.88 |
| 2024 | 2 | February | 5829 | 145762877.85 | 2.08 |
| 2024 | 3 | March | 5986 | 153863132.71 | 2.04 |
| 2024 | 4 | April | 5928 | 162110277.14 | 1.96 |
| 2024 | 5 | May | 6141 | 153699376.77 | 1.89 |
| 2024 | 6 | June | 5737 | 169367974.51 | 2.3 |
| 2024 | 7 | July | 6026 | 145642575.01 | 1.86 |
| 2024 | 8 | August | 6173 | 173249123.8 | 2.04 |

## 7a. Branch performance

| branch_name | city | total_transactions | total_value | failure_rate_pct | active_customers |
|---|---|---|---|---|---|
| Bandra West | Mumbai | 20587 | 543380065.76 | 5.92 | 778 |
| Salt Lake | Kolkata | 19877 | 529614363.24 | 6.14 | 770 |
| Connaught Place | Delhi | 20451 | 506357398.1 | 5.94 | 767 |
| Civil Lines | Jaipur | 19623 | 496297214.57 | 5.69 | 745 |
| Mumbai Fort | Mumbai | 18937 | 496224681.28 | 6.14 | 720 |
| Anna Nagar | Chennai | 18853 | 496067378.94 | 5.79 | 714 |
| Whitefield | Bangalore | 18934 | 494189334.22 | 6.05 | 719 |
| Koregaon Park | Pune | 19060 | 492537529.57 | 5.97 | 732 |

## 7b. Branch performance vs avg customer risk

| branch_name | customers_served | avg_customer_risk_score | high_risk_customers |
|---|---|---|---|
| Navrangpura | 709 | 14.38 | 2 |
| Bandra West | 778 | 14.14 | 2 |
| Salt Lake | 770 | 14.0 | 3 |
| Mumbai Fort | 720 | 13.96 | 3 |
| Anna Nagar | 714 | 13.85 | 2 |
| Connaught Place | 767 | 13.82 | 4 |
| Koregaon Park | 732 | 13.63 | 4 |
| Civil Lines | 745 | 13.5 | 3 |

## 8a. Loan default rate (overall)

| total_loans | defaulted_loans | default_rate_pct |
|---|---|---|
| 3000 | 246 | 8.2 |

## 8b. Loan default rate by loan type

| loan_type | total_loans | defaulted_loans | default_rate_pct | avg_interest_rate | total_loan_book |
|---|---|---|---|---|---|
| Auto Loan | 600 | 55 | 9.17 | 11.43 | 1537759053.06 |
| Business Loan | 568 | 47 | 8.27 | 11.48 | 1464144910.48 |
| Home Loan | 644 | 52 | 8.07 | 11.47 | 1625326268.8 |
| Personal Loan | 590 | 46 | 7.8 | 11.51 | 1461796755.12 |
| Education Loan | 598 | 46 | 7.69 | 11.47 | 1528131454.66 |

## 8c. Loan default rate by risk tier

| risk_tier | total_loans | defaulted_loans | default_rate_pct |
|---|---|---|---|
| Medium | 457 | 232 | 50.77 |
| High | 35 | 14 | 40.0 |
| Low | 2508 | 0 | 0.0 |

## 9. Data quality log

| run_at | total_records | valid_records | rejected_records | data_quality_score_pct | passed_quality_gate |
|---|---|---|---|---|---|
| 2026-09-14T09:51:42.911421 | 201401 | 193955 | 7446 | 96.3 | 1 |

## 10. Credit score distribution

| credit_score_band | customer_count |
|---|---|
| < 500 | 87 |
| 500-599 | 829 |
| 600-699 | 2072 |
| 700-799 | 1567 |
| 800+ | 445 |
