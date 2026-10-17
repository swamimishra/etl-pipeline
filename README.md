# E-commerce Daily Sales ETL Pipeline

A batch ETL pipeline that turns raw e-commerce transaction CSVs into a daily sales summary, stored as **Parquet partitioned by `processing_date`**. It is **idempotent**: running it twice for the same date replaces that date's partition instead of duplicating data.

Built with Python and Pandas (Parquet written through PyArrow).

## What the pipeline does

```
data/raw/*.csv ──► Extract ──► Validate ──► Transform ──► Load ──► data/processed/
                   read CSVs   drop bad     filter, join,  write      processing_date=<date>/
                               rows         aggregate      Parquet
```

1. **Extract:** reads `transactions.csv` and `users.csv`.
2. **Validate:** drops transactions with a null `user_id` or a negative (or missing) `amount`.
3. **Transform:**
   - keeps only `payment_status == "Success"`,
   - joins with users to get the buyer's `country`,
   - aggregates by `country` and `product_category`.
4. **Load:** writes the summary to `data/processed/processing_date=<date>/` as Parquet, replacing any earlier run for that date.

### Output table

| Column | Meaning |
|---|---|
| `processing_date` | The execution date passed with `--date` (stored in the partition folder name) |
| `country` | Buyer's country |
| `product_category` | Product category |
| `total_revenue` | Sum of `amount` |
| `successful_transaction_count` | Number of successful transactions |
| `unique_buyers` | Number of distinct `user_id`s |

## Requirements

- Python 3.9 or newer
- Packages in `requirements.txt`: `pandas`, `numpy`, `pyarrow`

## Setup

Clone the repository and move into it:

```bash
git clone https://github.com/swamimishra/etl-pipeline.git
cd etl-pipeline
```

Create and activate a virtual environment.

Windows (PowerShell):
```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
```

Windows (Command Prompt):
```Command Prompt
python -m venv .venv
 .venv\Scripts\activate.bat
```

macOS / Linux:
```bash
python3 -m venv .venv
source .venv/bin/activate
```

Install the dependencies:
```bash
pip install -r requirements.txt
```

> If PowerShell blocks the activation script, run `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned` once and try again.

## Step 1: Generate the raw data

```bash
python data_generator.py
```

This creates two files in `data/raw/`:

| File | Rows | Columns |
|---|---|---|
| `users.csv` | 500 | `user_id`, `registration_date`, `country` |
| `transactions.csv` | 5,000 | `transaction_id`, `user_id`, `product_category`, `amount`, `timestamp`, `payment_status` |

The data is deliberately a little dirty so the data quality checks have something to catch: 100 transactions with a null `user_id`, 100 with a negative `amount`, 50 from users missing in `users.csv`, and a mix of Success, Failed and Pending payments.

Expected output:
```
Wrote 500 users and 5000 transactions to .../data/raw
Injected bad rows: {'null_user_id': 100, 'negative_amount': 100, 'orphan_user_id': 50}
```

Options:

| Flag | Default | Meaning |
|---|---|---|
| `--rows` | `5000` | Number of transactions |
| `--seed` | `42` | Random seed (same seed gives the same data) |
| `--out-dir` | `data/raw` | Output folder |
| `--force` | off | Regenerate even if the CSVs already exist |

If both CSVs already exist the generator skips and tells you so; use `--force` to overwrite them.

## Step 2: Run the pipeline

```bash
python pipeline.py --date 2023-10-27
```

Expected log output (numbers for the default generated data):
```
INFO Starting pipeline for processing_date=2023-10-27
INFO Extracted 5000 transactions and 500 users from .../data/raw
INFO Dropping 100 rows with null user_id
INFO Dropping 100 rows with negative or missing amount
INFO Validation kept 4800 of 5000 transactions
INFO Filter kept 3830 successful of 4800 transactions
INFO Join dropped 42 transactions with unknown users
INFO Aggregation produced 54 summary rows
INFO Wrote 54 rows to .../data/processed/processing_date=2023-10-27
INFO Pipeline finished for processing_date=2023-10-27
```

Options:

| Flag | Default | Meaning |
|---|---|---|
| `--date` | today | Execution date, `YYYY-MM-DD` (invalid dates are rejected) |
| `--raw-dir` | `data/raw` | Folder with `transactions.csv` and `users.csv` |
| `--output-dir` | `data/processed` | Folder for the Parquet output |

The script exits with code `0` on success and non-zero on failure (for example `1` if an input file is missing), so a scheduler can detect failed runs.

## Step 3: Check the output

The output is one folder per date:

```
data/processed/
└── processing_date=2023-10-27/
    └── part-0000.parquet
```

Read it with Pandas:

```python
import pandas as pd

# One date
pd.read_parquet("data/processed/processing_date=2023-10-27")

# All dates (the processing_date column is rebuilt from the folder names)
pd.read_parquet("data/processed")
```

## Idempotency

Run the same date twice:

```bash
python pipeline.py --date 2023-10-27
python pipeline.py --date 2023-10-27
```

The second run logs `Overwriting existing partition processing_date=2023-10-27`. Afterwards there is still exactly one partition for that date with 54 rows, not 108. Other dates are never touched:

```bash
python pipeline.py --date 2023-10-28
```
creates a second folder, `processing_date=2023-10-28`, and leaves the first one as it was.

How it works: each date has its own folder. The new partition is written to a hidden temporary folder first and then swapped into place, so a failed run leaves the previous partition intact instead of missing or half-written.

## Project structure

```
etl-pipeline/
├── data_generator.py   # creates the dummy raw CSVs
├── pipeline.py         # the ETL script (extract, validate, transform, load, CLI)
├── requirements.txt    # dependencies
├── README.md
├── .gitignore          # keeps generated data and the venv out of git
└── data/               # created by the scripts, not committed
    ├── raw/            # users.csv, transactions.csv
    └── processed/      # Parquet output, one folder per date
```

Paths are resolved relative to the project folder, so the scripts work from any directory. The `/data/raw/` and `/data/processed/` locations in the assignment correspond to `data/raw/` and `data/processed/` inside the repository.

## Design decisions and assumptions

- **Pandas, not PySpark.** The dataset is small, and Pandas needs no JVM or cluster, so anyone can run it. The same logic maps directly to Spark for larger data.
- **`processing_date` is the run date.** As the brief defines it, it is "the date the pipeline ran", supplied with `--date`. It is a label and partition key only. Transactions are **not** filtered by their own timestamp; everything valid in the raw files is aggregated and stamped with the run date.
- **Invalid rows are dropped, never repaired.** There is no safe way to guess a missing user or a negative amount. Every dropped row is counted in the log.
- **Inner join with users.** A transaction whose user is missing from `users.csv` has no country, so it cannot appear in a country-level report. The number dropped is logged.
- **Missing amounts are rejected too.** In Pandas `NaN < 0` is `False`, so a null amount would otherwise slip past the negative check.
- **`processing_date` lives in the folder name, not inside the Parquet file.** This is the standard Hive-style layout. Storing it in both places makes readers fail on a type clash when loading the whole dataset.
- **Sorted output.** Rows are sorted by country and category, so identical input always produces identical output.

## Limitations and possible improvements

- Data is processed in memory; very large inputs would need Spark or chunked reads.
- Amounts are floats; production code would use integer cents or `Decimal`.
- Rejected rows are only counted, not saved to a quarantine file.
- There are no automated tests yet. Good first tests: each validation rule, the success filter, the join, distinct buyer counting, and running one date twice.
- The folder swap is not fully atomic;
