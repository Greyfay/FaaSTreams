#!/usr/bin/env python3
"""Report duplicate rows in a CSV file.

Usage:
    python find_duplicates.py <input.csv>
"""
import sys

import pandas as pd


def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <input.csv>")
        sys.exit(1)

    csv_path = sys.argv[1]

    df = pd.read_csv(csv_path, dtype_backend="pyarrow", engine="pyarrow")
    total = len(df)

    # Hash each row to a single value so grouping/counting is a single vectorized
    # pass instead of a groupby over all columns (which is much slower for wide CSVs).
    row_hashes = pd.util.hash_pandas_object(df, index=False)
    hash_counts = row_hashes.value_counts()
    unique = len(hash_counts)
    dup_hash_counts = hash_counts[hash_counts > 1]

    print(f"File: {csv_path}")
    print(f"Total lines (excl. header): {total}")
    print(f"Unique lines: {unique}")
    print(f"Duplicate occurrences: {len(dup_hash_counts)}")
    print()
    print("Top 10 most duplicated rows:")

    top_hashes = dup_hash_counts.nlargest(10)
    for h, count in top_hashes.items():
        row = df.loc[(row_hashes == h).idxmax()]
        print(f"{count:>7} {','.join('' if pd.isna(v) else str(v) for v in row)}")


if __name__ == "__main__":
    main()
