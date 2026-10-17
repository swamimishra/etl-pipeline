"""Daily sales ETL pipeline: extract raw CSVs, validate, transform, load to Parquet.

Usage:
    python pipeline.py --date 2023-10-27
    python pipeline.py                      # defaults to today
    python pipeline.py --date 2023-10-27 --raw-dir data/raw --output-dir data/processed

Running the same date twice replaces that date's partition (idempotent).
"""

import argparse
import logging
import shutil
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Optional, Sequence, Tuple

import pandas as pd

# Resolve defaults relative to this file so the script works from any directory.
PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_RAW_DIR = PROJECT_ROOT / "data" / "raw"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data" / "processed"

logger = logging.getLogger("etl_pipeline")


def extract(raw_dir: Path) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Read transactions.csv and users.csv from raw_dir.

    IDs are read as strings and timestamps are parsed to datetimes so that
    later steps get consistent types.
    """
    transactions_path = raw_dir / "transactions.csv"
    users_path = raw_dir / "users.csv"
    for path in (transactions_path, users_path):
        if not path.exists():
            raise FileNotFoundError(
                f"Missing input file: {path}. Run `python data_generator.py` first."
            )

    transactions = pd.read_csv(
        transactions_path,
        dtype={"transaction_id": "string", "user_id": "string",
               "product_category": "string", "payment_status": "string"},
        parse_dates=["timestamp"],
    )
    users = pd.read_csv(
        users_path,
        dtype={"user_id": "string", "country": "string"},
        parse_dates=["registration_date"],
    )
    logger.info("Extracted %d transactions and %d users from %s",
                len(transactions), len(users), raw_dir)
    return transactions, users


def validate(transactions: pd.DataFrame) -> pd.DataFrame:
    """Drop transactions that fail the data quality checks.

    Rules:
      * user_id must not be null
      * amount must be present and non-negative
    Each dropped row is counted under exactly one reason (null user_id is
    checked first), so the logged counts add up to the total dropped.
    """
    null_user = transactions["user_id"].isna()
    # A missing amount is treated as invalid too: NaN < 0 is False and would slip through.
    bad_amount = transactions["amount"].isna() | (transactions["amount"] < 0)

    logger.info("Dropping %d rows with null user_id", null_user.sum())
    logger.info("Dropping %d rows with negative or missing amount", (bad_amount & ~null_user).sum())

    clean = transactions[~(null_user | bad_amount)].copy()
    logger.info("Validation kept %d of %d transactions", len(clean), len(transactions))
    return clean


def transform(transactions: pd.DataFrame, users: pd.DataFrame, processing_date: str) -> pd.DataFrame:
    """Filter to successful payments, enrich with country, and aggregate.

    Returns one row per (country, product_category) with revenue, transaction
    count and distinct buyers, stamped with processing_date.
    """
    # 1. Filter: only completed payments count towards sales.
    successful = transactions[transactions["payment_status"] == "Success"]
    logger.info("Filter kept %d successful of %d transactions",
                len(successful), len(transactions))

    # 2. Join: inner join so every row has a known country. Transactions from
    # users missing in users.csv cannot be attributed to a country and are dropped.
    # Deduplicating users guards against the join multiplying rows.
    country_lookup = users[["user_id", "country"]].drop_duplicates(subset="user_id")
    enriched = successful.merge(country_lookup, on="user_id", how="inner")
    logger.info("Join dropped %d transactions with unknown users",
                len(successful) - len(enriched))

    # 3. Aggregate: nunique (not a sum) gives distinct buyers per group.
    summary = (
        enriched.groupby(["country", "product_category"], as_index=False)
        .agg(
            total_revenue=("amount", "sum"),
            successful_transaction_count=("transaction_id", "count"),
            unique_buyers=("user_id", "nunique"),
        )
        .sort_values(["country", "product_category"])
        .reset_index(drop=True)
    )
    summary["total_revenue"] = summary["total_revenue"].round(2)
    summary.insert(0, "processing_date", processing_date)
    logger.info("Aggregation produced %d summary rows", len(summary))
    return summary


def load(summary: pd.DataFrame, output_dir: Path, processing_date: str) -> Path:
    """Write the summary as Parquet into output_dir/processing_date=<date>/.

    Idempotenty: the partition for this date is replaced, never appended to, so
    re-running a date cannot duplicate rows. Other dates are left untouched.
    The partition is built in a temporary folder and swapped into place, so a
    failed write never leaves the partition missing or half-written.
    """
    partition_dir = output_dir / f"processing_date={processing_date}"
    # A leading dot hides the folder from Parquet readers while it is being built.
    tmp_dir = output_dir / f".tmp_processing_date={processing_date}"

    output_dir.mkdir(parents=True, exist_ok=True)
    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)  # leftover from an earlier crashed run
    tmp_dir.mkdir()

    try:
        # Hive-style partitioning keeps the value in the folder name, not in the file.
        # Storing it in both places makes readers fail on a type clash.
        summary.drop(columns="processing_date").to_parquet(
            tmp_dir / "part-0000.parquet", index=False
        )
        if partition_dir.exists():
            logger.info("Overwriting existing partition %s", partition_dir.name)
            shutil.rmtree(partition_dir)
        tmp_dir.rename(partition_dir)
    except Exception:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        raise

    logger.info("Wrote %d rows to %s", len(summary), partition_dir)
    return partition_dir


def run_pipeline(processing_date: str, raw_dir: Path, output_dir: Path) -> Path:
    """Run extract -> validate -> transform -> load for one processing date."""
    logger.info("Starting pipeline for processing_date=%s", processing_date)
    transactions, users = extract(raw_dir)
    transactions = validate(transactions)
    summary = transform(transactions, users, processing_date)
    partition_dir = load(summary, output_dir, processing_date)
    logger.info("Pipeline finished for processing_date=%s", processing_date)
    return partition_dir


def valid_date(value: str) -> str:
    """argparse type: accept only real calendar dates written as YYYY-MM-DD."""
    try:
        return datetime.strptime(value, "%Y-%m-%d").date().isoformat()
    except ValueError:
        raise argparse.ArgumentTypeError(f"'{value}' is not a valid date, expected YYYY-MM-DD")


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Daily sales ETL: raw CSVs -> partitioned Parquet.")
    parser.add_argument("--date", type=valid_date, default=date.today().isoformat(),
                        help="execution date, YYYY-MM-DD (default: today)")
    parser.add_argument("--raw-dir", type=Path, default=DEFAULT_RAW_DIR,
                        help="folder containing transactions.csv and users.csv")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR,
                        help="folder for the partitioned Parquet output")
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args(argv)
    try:
        run_pipeline(args.date, args.raw_dir, args.output_dir)
    except FileNotFoundError as exc:
        logger.error("%s", exc)
        return 1  # non-zero exit code so a scheduler sees the failure
    return 0

if __name__ == "__main__":
    sys.exit(main())