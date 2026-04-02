import csv
import datetime as dt
import sys
from collections import Counter
from pathlib import Path


def read_csv(path: Path):
    if not path.exists():
        return []
    with open(path, "r", newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def to_float(x, default=0.0):
    try:
        return float(x)
    except Exception:
        return default


def window_name(ts_ms):
    d = dt.datetime.utcfromtimestamp(int(ts_ms) / 1000.0)
    return f"{d.year}-{d.month:02d}"


def pct(n, d):
    return 100.0 * n / d if d else 0.0


def main():
    base_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(".")
    results = read_csv(base_dir / "results.csv")
    if not results:
        print("No results.csv data.")
        return

    buckets = {}
    for r in results:
        bucket = window_name(r.get("timestamp_ms", "0"))
        buckets.setdefault(bucket, []).append(r)

    print("=== MONTHLY REVIEW ===")
    for bucket in sorted(buckets):
        rows = buckets[bucket]
        total = len(rows)
        outcomes = Counter(r.get("outcome", "UNKNOWN") for r in rows)
        r_values = [to_float(r.get("r_multiple")) for r in rows]
        avg_r = sum(r_values) / len(r_values) if r_values else 0.0
        wins = outcomes.get("WIN_TP1", 0) + outcomes.get("WIN_TP2", 0)
        print(
            f"{bucket}: trades={total}, wins={wins} ({pct(wins, total):.2f}%), "
            f"stops={outcomes.get('LOSS_STOP', 0)}, expired={outcomes.get('EXPIRED', 0)}, avg_r={avg_r:.4f}"
        )

    print("\nInterpretation:")
    print("- If a month has too few trades, do not overreact.")
    print("- If 2-3 windows in a row are weak, then review thresholds.")
    print("- Compare with pending stats and signal count before changing config.")


if __name__ == "__main__":
    main()
