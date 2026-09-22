"""
generate_synthetic_data.py
---------------------------
Generates synthetic banking datasets (customers, branches, accounts,
transactions, loans) that intentionally include realistic data-quality
problems: nulls, duplicates, negative amounts, bad dates, orphan keys.

This lets the downstream ingestion / PySpark ETL / data-quality layers
have real work to do, instead of validating already-clean data.

Usage:
    python generate_synthetic_data.py --num_customers 5000 --num_transactions 200000
"""

import argparse
import random
import string
import uuid
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

BRANCHES = [
    ("BR001", "Mumbai Fort", "Mumbai", "India"),
    ("BR002", "Bandra West", "Mumbai", "India"),
    ("BR003", "Connaught Place", "Delhi", "India"),
    ("BR004", "Whitefield", "Bangalore", "India"),
    ("BR005", "Salt Lake", "Kolkata", "India"),
    ("BR006", "Anna Nagar", "Chennai", "India"),
    ("BR007", "Banjara Hills", "Hyderabad", "India"),
    ("BR008", "Koregaon Park", "Pune", "India"),
    ("BR009", "Navrangpura", "Ahmedabad", "India"),
    ("BR010", "Civil Lines", "Jaipur", "India"),
]

ACCOUNT_TYPES = ["Savings", "Current", "Salary", "NRI", "Fixed Deposit"]
TXN_TYPES = ["Deposit", "Withdrawal", "Transfer", "Bill Payment", "POS Purchase", "ATM Withdrawal", "Loan EMI"]
PAYMENT_STATUS = ["Success", "Failed", "Pending", "Reversed"]
LOAN_TYPES = ["Home Loan", "Personal Loan", "Auto Loan", "Education Loan", "Business Loan"]
LOAN_STATUS = ["Active", "Closed", "Defaulted", "Overdue"]


def rand_date_str(start_year=2023, end_year=2026, bad_chance=0.0):
    """Return a date string, occasionally malformed to simulate dirty data."""
    if bad_chance and random.random() < bad_chance:
        return random.choice(["31/13/2025", "2025-02-30", "", "NaT", "not_a_date", "00/00/0000"])
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 9, 13)
    delta = end - start
    d = start + timedelta(days=random.randint(0, delta.days))
    # mix formats to simulate different source systems feeding the lake
    fmt = random.choice(["%Y-%m-%d", "%d-%m-%Y", "%m/%d/%Y", "%Y/%m/%d"])
    return d.strftime(fmt)


def gen_customers(n):
    rows = []
    for i in range(1, n + 1):
        cust_id = f"CUST{i:06d}"
        income = round(random.lognormvariate(10.8, 0.6), 2)  # skewed income distribution
        credit_score = int(random.gauss(680, 90))
        credit_score = max(300, min(900, credit_score))

        # inject some missingness
        if random.random() < 0.01:
            income = None
        if random.random() < 0.01:
            credit_score = None

        rows.append({
            "customer_id": cust_id,
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "dob": fake.date_of_birth(minimum_age=18, maximum_age=80).isoformat(),
            "gender": random.choice(["Male", "Female", "Other"]),
            "email": fake.email(),
            "phone": fake.msisdn()[:10],
            "city": random.choice([b[2] for b in BRANCHES]),
            "country": "India",
            "customer_income": income,
            "credit_score": credit_score,
            "customer_since": rand_date_str(2015, 2025),
        })

    df = pd.DataFrame(rows)
    # inject a handful of exact-duplicate customer rows (uniqueness issue)
    dupes = df.sample(frac=0.005, random_state=1)
    df = pd.concat([df, dupes], ignore_index=True)
    return df


def gen_branches():
    return pd.DataFrame(BRANCHES, columns=["branch_id", "branch_name", "city", "country"])


def gen_accounts(customer_ids, n_accounts):
    rows = []
    for i in range(1, n_accounts + 1):
        acct_id = f"ACC{i:07d}"
        rows.append({
            "account_id": acct_id,
            "customer_id": random.choice(customer_ids),
            "account_type": random.choice(ACCOUNT_TYPES),
            "branch_id": random.choice([b[0] for b in BRANCHES]),
            "account_opened_date": rand_date_str(2015, 2026),
            "account_status": random.choices(["Active", "Dormant", "Closed"], weights=[0.85, 0.1, 0.05])[0],
        })
    return pd.DataFrame(rows)


def gen_transactions(account_ids, n_txn):
    rows = []
    for i in range(1, n_txn + 1):
        txn_id = f"TXN{i:08d}"

        # Most retail transactions are small (exponential around a ~5k mean),
        # but a realistic minority are large-value transfers / disbursements
        # (wire transfers, business payments, loan disbursals) that a bank's
        # risk/AML monitoring would flag as "high value". Modeling this
        # bimodal mix makes the high-value-transaction KPI meaningful instead
        # of always being ~0.
        if random.random() < 0.02:
            amount = round(random.uniform(100_000, 2_000_000), 2)  # high-value transfer
        else:
            amount = round(random.expovariate(1 / 5000), 2)

        # inject dirty data at controlled rates so the DQ report has real findings
        r = random.random()
        if r < 0.008:
            amount = -abs(amount)  # invalid negative amount
        elif r < 0.012:
            amount = None  # missing amount

        acct = random.choice(account_ids) if random.random() > 0.003 else None  # orphan/missing account
        cust_missing = random.random() < 0.004

        rows.append({
            "transaction_id": txn_id,
            "account_id": acct,
            "transaction_date": rand_date_str(2024, 2026, bad_chance=0.01),
            "transaction_amount": amount,
            "transaction_type": random.choice(TXN_TYPES) if random.random() > 0.005 else None,
            "payment_status": random.choices(PAYMENT_STATUS, weights=[0.88, 0.06, 0.03, 0.03])[0],
            "channel": random.choice(["Mobile App", "Internet Banking", "Branch", "ATM", "POS"]),
        })

    df = pd.DataFrame(rows)

    # inject exact duplicate transaction_ids (uniqueness violation)
    dupe_rows = df.sample(frac=0.006, random_state=2).copy()
    df = pd.concat([df, dupe_rows], ignore_index=True)

    # inject a few duplicate transaction_id but DIFFERENT payload (data conflict)
    conflict_rows = df.sample(frac=0.001, random_state=3).copy()
    conflict_rows["transaction_amount"] = conflict_rows["transaction_amount"].fillna(0) + 999
    df = pd.concat([df, conflict_rows], ignore_index=True)

    return df


def gen_loans(customer_ids, n_loans):
    rows = []
    for i in range(1, n_loans + 1):
        loan_id = f"LOAN{i:06d}"
        principal = round(random.uniform(50_000, 5_000_000), 2)
        status = random.choices(LOAN_STATUS, weights=[0.55, 0.25, 0.08, 0.12])[0]
        rows.append({
            "loan_id": loan_id,
            "customer_id": random.choice(customer_ids),
            "loan_type": random.choice(LOAN_TYPES),
            "loan_amount": principal,
            "interest_rate": round(random.uniform(6.5, 16.5), 2),
            "tenure_months": random.choice([12, 24, 36, 60, 84, 120, 180, 240]),
            "loan_start_date": rand_date_str(2018, 2026),
            "loan_status": status,
            "emi_payment_status": random.choice(PAYMENT_STATUS) if status != "Closed" else "Success",
        })
    return pd.DataFrame(rows)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--num_customers", type=int, default=5000)
    parser.add_argument("--num_accounts", type=int, default=8000)
    parser.add_argument("--num_transactions", type=int, default=200_000)
    parser.add_argument("--num_loans", type=int, default=3000)
    parser.add_argument("--out_dir", type=str, default="../raw_data")
    args = parser.parse_args()

    out_dir = Path(__file__).parent / args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    print("Generating branches...")
    branches = gen_branches()

    print(f"Generating {args.num_customers} customers...")
    customers = gen_customers(args.num_customers)

    print(f"Generating {args.num_accounts} accounts...")
    accounts = gen_accounts(customers["customer_id"].tolist(), args.num_accounts)

    print(f"Generating {args.num_transactions} transactions...")
    transactions = gen_transactions(accounts["account_id"].tolist(), args.num_transactions)

    print(f"Generating {args.num_loans} loans...")
    loans = gen_loans(customers["customer_id"].tolist(), args.num_loans)

    branches.to_csv(out_dir / "branches.csv", index=False)
    customers.to_csv(out_dir / "customers.csv", index=False)
    accounts.to_csv(out_dir / "accounts.csv", index=False)
    transactions.to_csv(out_dir / "transactions.csv", index=False)
    loans.to_csv(out_dir / "loans.csv", index=False)

    print("\nDone. Row counts:")
    for name, df in [("branches", branches), ("customers", customers),
                      ("accounts", accounts), ("transactions", transactions),
                      ("loans", loans)]:
        print(f"  {name:<14}: {len(df):,}")
    print(f"\nFiles written to: {out_dir.resolve()}")


if __name__ == "__main__":
    main()
