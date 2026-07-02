#!/usr/bin/env python3
"""Remove exact duplicate rows from a CSV file.

Usage:
    python dedupe_csv.py <input.csv> [output.csv]

If output.csv is omitted, defaults to <input>.deduped.csv

Note: output fields are all quoted (valid CSV, just not "minimal" quoting
like the original file) - this is a side effect of using pyarrow's CSV
writer for speed on large files.
"""
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as pv


def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <input.csv> [output.csv]")
        sys.exit(1)

    csv_path = Path(sys.argv[1])
    out_path = Path(sys.argv[2]) if len(sys.argv) > 2 else csv_path.with_suffix(".deduped.csv")

    # Force every column to string so the parser preserves the original text
    # exactly (no numeric reformatting, no NaN/None literals).
    with open(csv_path) as f:
        header = next(f).rstrip("\n").split(",")
    convert_options = pv.ConvertOptions(column_types={c: pa.string() for c in header})
    table = pv.read_csv(csv_path, convert_options=convert_options)

    before = table.num_rows
    # group_by over every column with no aggregation == distinct rows, done
    # entirely in Arrow (no per-row Python objects), which is what makes this fast.
    deduped = table.group_by(header, use_threads=True).aggregate([])
    after = deduped.num_rows

    pv.write_csv(deduped, out_path)
    print(f"Read {before} rows, wrote {after} rows ({before - after} duplicates removed) to {out_path}")


if __name__ == "__main__":
    main()
