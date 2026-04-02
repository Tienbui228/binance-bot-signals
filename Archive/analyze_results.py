import csv
import sys
from collections import Counter, defaultdict
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


def pct(n, d):
    return 100.0 * n / d if d else 0.0


def summarize_overall(results):
    total = len(results)
    outcomes = Counter(r.get("outcome", "UNKNOWN") for r in results)
    r_values = [to_float(r.get("r_multiple")) for r in results]
    avg_r = sum(r_values) / len(r_values) if r_values else 0.0
    wins = outcomes.get("WIN_TP1", 0) + outcomes.get("WIN_TP2", 0)
    losses = outcomes.get("LOSS_STOP", 0)
    expired = outcomes.get("EXPIRED", 0)

    print("=== OVERALL RESULTS ===")
    print(f"total_closed={total}")
    print(f"wins={wins} ({pct(wins, total):.2f}%)")
    print(f"stops={losses} ({pct(losses, total):.2f}%)")
    print(f"expired={expired} ({pct(expired, total):.2f}%)")
    print(f"avg_r={avg_r:.4f}")
    print(f"win_tp1={outcomes.get('WIN_TP1', 0)}")
    print(f"win_tp2={outcomes.get('WIN_TP2', 0)}")
    print(f"loss_stop={outcomes.get('LOSS_STOP', 0)}")
    print(f"expired_count={outcomes.get('EXPIRED', 0)}")
    print()

    print("=== BY SIDE ===")
    by_side = defaultdict(list)
    for r in results:
        by_side[r.get("side", "UNKNOWN")].append(r)
    for side, rows in sorted(by_side.items()):
        total_side = len(rows)
        side_out = Counter(r.get("outcome", "UNKNOWN") for r in rows)
        side_r = [to_float(r.get("r_multiple")) for r in rows]
        avg_side_r = sum(side_r) / len(side_r) if side_r else 0.0
        wins_side = side_out.get("WIN_TP1", 0) + side_out.get("WIN_TP2", 0)
        print(
            f"{side}: trades={total_side}, wins={wins_side} ({pct(wins_side, total_side):.2f}%), "
            f"stops={side_out.get('LOSS_STOP', 0)}, expired={side_out.get('EXPIRED', 0)}, avg_r={avg_side_r:.4f}"
        )
    print()

    print("=== BY SYMBOL (top 15) ===")
    by_symbol = defaultdict(list)
    for r in results:
        by_symbol[r.get("symbol", "UNKNOWN")].append(r)
    ranked = sorted(by_symbol.items(), key=lambda kv: len(kv[1]), reverse=True)[:15]
    for sym, rows in ranked:
        total_sym = len(rows)
        sym_out = Counter(r.get("outcome", "UNKNOWN") for r in rows)
        sym_r = [to_float(r.get("r_multiple")) for r in rows]
        avg_sym_r = sum(sym_r) / len(sym_r) if sym_r else 0.0
        wins_sym = sym_out.get("WIN_TP1", 0) + sym_out.get("WIN_TP2", 0)
        print(
            f"{sym}: trades={total_sym}, wins={wins_sym} ({pct(wins_sym, total_sym):.2f}%), "
            f"stops={sym_out.get('LOSS_STOP', 0)}, expired={sym_out.get('EXPIRED', 0)}, avg_r={avg_sym_r:.4f}"
        )
    print()


def summarize_signals(signals):
    print("=== SIGNALS SUMMARY ===")
    total = len(signals)
    print(f"total_signals={total}")
    if not total:
        print()
        return
    statuses = Counter(r.get("status", "UNKNOWN") for r in signals)
    sides = Counter(r.get("side", "UNKNOWN") for r in signals)
    for side, count in sorted(sides.items()):
        print(f"{side}_signals={count} ({pct(count, total):.2f}%)")
    print("statuses:")
    for status, count in sorted(statuses.items()):
        print(f"  {status}={count} ({pct(count, total):.2f}%)")
    conf = [to_float(r.get("confidence")) for r in signals]
    score = [to_float(r.get("score")) for r in signals]
    oi = [to_float(r.get("oi_jump_pct")) for r in signals]
    vol = [to_float(r.get("vol_ratio")) for r in signals]
    waited = [to_float(r.get("retest_bars_waited")) for r in signals]
    def avg(vals): return sum(vals) / len(vals) if vals else 0.0
    print(f"avg_confidence={avg(conf):.4f}")
    print(f"avg_score={avg(score):.4f}")
    print(f"avg_oi_jump_pct={avg(oi):.4f}")
    print(f"avg_vol_ratio={avg(vol):.4f}")
    print(f"avg_retest_wait_bars={avg(waited):.4f}")
    print()


def summarize_pending(pending):
    print("=== PENDING SUMMARY ===")
    if pending is None:
        print("pending_setups.csv not found")
        print()
        return
    total = len(pending)
    print(f"total_pending_rows={total}")
    if not total:
        print()
        return
    statuses = Counter(r.get("status", "UNKNOWN") for r in pending)
    sides = Counter(r.get("side", "UNKNOWN") for r in pending)
    for side, count in sorted(sides.items()):
        print(f"{side}_pending={count} ({pct(count, total):.2f}%)")
    print("statuses:")
    for status, count in sorted(statuses.items()):
        print(f"  {status}={count} ({pct(count, total):.2f}%)")
    print()


def main():
    base_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(".")
    results = read_csv(base_dir / "results.csv")
    signals = read_csv(base_dir / "signals.csv")
    pending_path = base_dir / "pending_setups.csv"
    pending = read_csv(pending_path) if pending_path.exists() else None

    print(f"DATA_DIR={base_dir.resolve()}")
    print()

    if results:
        summarize_overall(results)
    else:
        print("=== OVERALL RESULTS ===")
        print("No closed trades yet.\n")

    summarize_signals(signals)
    summarize_pending(pending)

    print("=== QUICK TAKE ===")
    if len(results) < 20:
        print("Sample still small. Do not retune aggressively yet.")
    else:
        print("Sample is becoming usable for threshold review.")


if __name__ == "__main__":
    main()
