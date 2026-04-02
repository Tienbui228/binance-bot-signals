#!/usr/bin/env python3
import argparse
import csv
import shutil
from pathlib import Path
from typing import List, Tuple

DEFAULT_MARKERS = [
    "SIMCASE",
    "TEST_CLOSE_SEED_V1",
    "SIMULATE_SIGNAL_CASE_V1",
    "TEST_SEED",
    "simulated_runtime_case",
    "seed_test_",
]

TARGET_DIRS = [
    ("pending", "*.csv"),
    ("signals", "*.csv"),
    ("results", "*.csv"),
]

def row_matches(row: dict, markers: List[str]) -> bool:
    joined = " ".join("" if v is None else str(v) for v in row.values())
    upper = joined.upper()
    for m in markers:
        if m.upper() in upper:
            return True
    return False

def clean_csv(path: Path, markers: List[str], dry_run: bool) -> Tuple[int, int, Path]:
    if not path.exists():
        return (0, 0, path)

    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        rows = list(reader)

    kept = []
    removed = 0
    for row in rows:
        if row_matches(row, markers):
            removed += 1
        else:
            kept.append(row)

    if removed > 0 and not dry_run:
        backup = path.with_suffix(path.suffix + ".bak_testclean")
        shutil.copy2(path, backup)
        tmp = path.with_suffix(path.suffix + ".tmp")
        with tmp.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            for row in kept:
                writer.writerow(row)
        tmp.replace(path)
        return (len(rows), removed, backup)

    return (len(rows), removed, Path(""))

def main():
    ap = argparse.ArgumentParser(description="Remove synthetic test rows from pending/signals/results CSVs.")
    ap.add_argument("--project-dir", default="/root/binance_bot_signals", help="Project root directory")
    ap.add_argument("--marker", action="append", default=[], help="Extra marker to remove (can pass multiple times)")
    ap.add_argument("--dry-run", action="store_true", help="Only report what would be removed")
    args = ap.parse_args()

    project_dir = Path(args.project_dir).resolve()
    data_dir = project_dir / "data"
    markers = DEFAULT_MARKERS + args.marker

    print("RESET TEST ROWS")
    print(f"project_dir={project_dir}")
    print(f"dry_run={args.dry_run}")
    print("markers=" + ", ".join(markers))

    total_removed = 0
    touched = 0

    for subdir, pattern in TARGET_DIRS:
        dir_path = data_dir / subdir
        if not dir_path.exists():
            print(f"[skip] missing dir: {dir_path}")
            continue
        for fp in sorted(dir_path.glob(pattern)):
            total, removed, backup = clean_csv(fp, markers, args.dry_run)
            if removed > 0:
                touched += 1
                total_removed += removed
                if args.dry_run:
                    print(f"[dry-run] {fp} | rows={total} | removed={removed}")
                else:
                    print(f"[cleaned] {fp} | rows={total} | removed={removed} | backup={backup}")
            else:
                print(f"[ok] {fp} | rows={total} | removed=0")

    print(f"files_touched={touched}")
    print(f"rows_removed={total_removed}")
    if not args.dry_run:
        print("DONE")
        print("Suggested next steps:")
        print(f"  /root/venv/bin/python {project_dir}/simulate_signal_case.py --case short_tp1 --project-dir {project_dir}")
        print(f"  /root/venv/bin/python {project_dir}/simulate_signal_case.py --case short_sl --project-dir {project_dir}")
        print(f"  /root/venv/bin/python {project_dir}/simulate_signal_case.py --case long_tp2 --project-dir {project_dir}")
        print(f"  cd {project_dir} && /root/venv/bin/python audit_pipeline.py --data-dir {project_dir}")

if __name__ == "__main__":
    main()
