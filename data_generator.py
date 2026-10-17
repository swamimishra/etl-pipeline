"""Generate dummy raw data (users.csv and transactions.csv) for the ETL pipeline.

The data is intentionally dirty, just to stress test data quality checks:
  * some transactions have a null user_id
  * some transactions have a negative amount
  * some transactions reference a user_id that is missing from users.csv
  * payment_status is a mix of Success, Failed, Pending

Usage:
    python data_generator.py
    python data_generator.py --rows 10000 --seed 7 --out-dir data/raw

If users.csv and transactions.csv already exist the script skips generation;
pass --force to overwrite them.
"""

import argparse
import uuid
from pathlib import Path

import numpy as np
import pandas as pd

# Resolve defaults relative to this file so the script works from any directory.
PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_OUT_DIR = PROJECT_ROOT / "data" / "raw"

NUM_USERS = 500
HISTORY_DAYS = 3 * 365          # users registered within the last 3 years
TRANSACTION_WINDOW_DAYS = 30    # transactions spread over the last 30 days

COUNTRIES = ["USA", "India", "UK", "Germany", "France", "Canada", "Australia", "Brazil", "Japan"]
COUNTRY_WEIGHTS = [0.30, 0.20, 0.12, 0.09, 0.08, 0.07, 0.05, 0.05, 0.04]

CATEGORIES = ["Electronics", "Clothing", "Home", "Books", "Beauty", "Sports"]
STATUSES = ["Success", "Failed", "Pending"]
STATUS_WEIGHTS = [0.80, 0.12, 0.08]

# Share of transaction rows that get deliberately corrupted.
NULL_USER_RATE = 0.02
NEGATIVE_AMOUNT_RATE = 0.02
ORPHAN_USER_RATE = 0.01         # user_id not present in users.csv


def make_uuids(rng: np.random.Generator, n: int) -> list:
    """Return n valid version-4 UUID strings drawn from the seeded generator."""
    return [str(uuid.UUID(bytes=rng.bytes(16), version=4)) for _ in range(n)]


def generate_users(rng: np.random.Generator, num_users: int = NUM_USERS) -> pd.DataFrame:
    """Create the users table: user_id, registration_date, country."""
    today = pd.Timestamp.today().normalize()
    days_ago = rng.integers(0, HISTORY_DAYS, size=num_users)
    return pd.DataFrame({
        "user_id": make_uuids(rng, num_users),
        "registration_date": (today - pd.to_timedelta(days_ago, unit="D")).strftime("%Y-%m-%d"),
        "country": rng.choice(COUNTRIES, size=num_users, p=COUNTRY_WEIGHTS),
    })


def generate_transactions(rng: np.random.Generator, users: pd.DataFrame, rows: int) -> pd.DataFrame:
    """Create clean transactions that all reference an existing user."""
    now = pd.Timestamp.now().floor("s")
    seconds_ago = rng.integers(0, TRANSACTION_WINDOW_DAYS * 24 * 3600, size=rows)
    return pd.DataFrame({
        "transaction_id": make_uuids(rng, rows),
        "user_id": rng.choice(users["user_id"].to_numpy(), size=rows),
        "product_category": rng.choice(CATEGORIES, size=rows),
        # Log-normal gives many small purchases and a few large ones, like real prices.
        "amount": np.round(rng.lognormal(mean=3.8, sigma=0.8, size=rows), 2),
        "timestamp": (now - pd.to_timedelta(seconds_ago, unit="s")).strftime("%Y-%m-%d %H:%M:%S"),
        "payment_status": rng.choice(STATUSES, size=rows, p=STATUS_WEIGHTS),
    })


def inject_bad_data(rng: np.random.Generator, transactions: pd.DataFrame) -> dict:
    """Corrupt random rows in place. Returns how many rows of each kind were injected."""
    n = len(transactions)
    # Pick distinct rows for each defect so the counts are exact and do not overlap.
    n_null = int(n * NULL_USER_RATE)
    n_neg = int(n * NEGATIVE_AMOUNT_RATE)
    n_orphan = int(n * ORPHAN_USER_RATE)
    picked = rng.choice(n, size=n_null + n_neg + n_orphan, replace=False)
    null_idx, neg_idx, orphan_idx = np.split(picked, [n_null, n_null + n_neg])

    transactions.loc[null_idx, "user_id"] = None
    transactions.loc[neg_idx, "amount"] = -transactions.loc[neg_idx, "amount"]
    transactions.loc[orphan_idx, "user_id"] = make_uuids(rng, len(orphan_idx))
    return {"null_user_id": n_null, "negative_amount": n_neg, "orphan_user_id": n_orphan}


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate dummy raw CSVs for the ETL pipeline.")
    parser.add_argument("--rows", type=int, default=5000, help="number of transactions (default 5000)")
    parser.add_argument("--seed", type=int, default=42, help="random seed for reproducible data")
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR, help="output folder")
    parser.add_argument("--force", action="store_true", help="regenerate even if the CSVs already exist")
    args = parser.parse_args()

    users_path = args.out_dir / "users.csv"
    transactions_path = args.out_dir / "transactions.csv"

    # Skip only when the dataset is complete; a lone file means it is broken, so rebuild both.
    if users_path.exists() and transactions_path.exists() and not args.force:
        print(f"Raw data already exists in {args.out_dir}, skipping. Use --force to regenerate.")
        return

    rng = np.random.default_rng(args.seed)
    users = generate_users(rng)
    transactions = generate_transactions(rng, users, args.rows)
    injected = inject_bad_data(rng, transactions)

    args.out_dir.mkdir(parents=True, exist_ok=True)
    users.to_csv(users_path, index=False)
    transactions.to_csv(transactions_path, index=False)

    print(f"Wrote {len(users)} users and {len(transactions)} transactions to {args.out_dir}")
    print(f"Injected bad rows: {injected}")

if __name__ == "__main__":
    main()
