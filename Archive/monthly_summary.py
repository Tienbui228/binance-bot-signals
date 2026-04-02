#!/usr/bin/env python3
from pathlib import Path
import argparse
from weekly_summary import generate_period_report


def main():
    parser = argparse.ArgumentParser(description="Generate monthly summary outputs from bot partitioned CSV files.")
    parser.add_argument("--data-dir", type=Path, default=Path("."), help="Project root containing data/ and reports/ directories.")
    parser.add_argument("--output-dir", type=Path, default=None, help="Where to write monthly outputs. Defaults to data dir/reports/monthly.")
    parser.add_argument("--end-date", type=str, default=None, help="Optional end date in YYYY-MM-DD. Defaults to latest timestamp found.")
    parser.add_argument("--days", type=int, default=30, help="Monthly window size in days.")
    args = parser.parse_args()
    generate_period_report(data_dir=args.data_dir, days=args.days, label="monthly", output_dir=args.output_dir, end_date=args.end_date)


if __name__ == "__main__":
    main()
