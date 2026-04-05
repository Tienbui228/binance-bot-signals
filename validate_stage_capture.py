#!/usr/bin/env python3
"""
validate_stage_capture.py
--------------------------
Deferred validation: verify that fresh post-patch cases have
pre_pending and pending_open properly captured (not missing_unexpected).

Usage:
  # Step 1: get CUT_MS immediately after confirming patch is live
  python3 -c "import time; print(int(time.time() * 1000))"

  # Step 2: wait 2-3 scanner rounds

  # Step 3: run this script
  python3 validate_stage_capture.py --cut-ms 1775300000000

  # With custom paths
  python3 validate_stage_capture.py \
    --cut-ms 1775300000000 \
    --pending-dir data/pending \
    --workspace  review_workspace \
    --tz         Asia/Ho_Chi_Minh
"""

import argparse
import csv
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

STAGES_TO_CHECK = ("pre_pending", "pending_open")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def safe_int(v, d=0):
    try:
        return int(float(v)) if v not in (None, "", "NA") else d
    except Exception:
        return d


def owner_day(ts_ms: int, tz) -> str:
    if not ts_ms:
        return ""
    return (datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
            .astimezone(tz).strftime("%Y-%m-%d"))


def load_pending(pending_dir: Path) -> List[Dict]:
    rows = []
    for f in sorted(pending_dir.glob("pending_*.csv")):
        try:
            with open(f, newline="", encoding="utf-8-sig") as fh:
                rows.extend(csv.DictReader(fh))
        except Exception as e:
            print(f"  [warn] {f.name}: {e}")
    return rows


def load_case_meta(workspace: Path, case_day: str, case_id: str) -> Optional[Dict]:
    path = workspace / "cases" / case_day / case_id / "case_meta.json"
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Main check
# ---------------------------------------------------------------------------

def run(pending_dir: Path, workspace: Path, cut_ms: int, tz_name: str):
    tz = ZoneInfo(tz_name)

    print("=" * 60)
    print("Stage Capture Validation — pre_pending / pending_open")
    print("=" * 60)
    print(f"  CUT_MS     : {cut_ms}")
    print(f"  pending_dir: {pending_dir}")
    print(f"  workspace  : {workspace}")
    print(f"  tz         : {tz_name}")
    print()

    # Load all rows
    all_rows = load_pending(pending_dir)

    # Filter to fresh rows only (created after CUT_MS)
    fresh = [
        r for r in all_rows
        if safe_int(r.get("created_ts_ms")) >= cut_ms
    ]

    print(f"Total rows loaded    : {len(all_rows)}")
    print(f"Fresh rows (>CUT_MS) : {len(fresh)}")
    print()

    if not fresh:
        print("  No fresh rows found yet — wait longer and retry.")
        print("  Minimum: 2-3 scanner rounds after patch deploy.")
        return

    # Per-stage counters
    stage_results = {stage: Counter() for stage in STAGES_TO_CHECK}
    no_case_meta = 0
    detail_rows = []

    for row in fresh:
        case_id    = row.get("pending_id") or row.get("setup_id") or ""
        created_ms = safe_int(row.get("created_ts_ms"))
        case_day   = owner_day(created_ms, tz)

        if not case_id or not case_day:
            for stage in STAGES_TO_CHECK:
                stage_results[stage]["no_case_id"] += 1
            continue

        meta = load_case_meta(workspace, case_day, case_id)
        if meta is None:
            no_case_meta += 1
            for stage in STAGES_TO_CHECK:
                stage_results[stage]["no_case_meta"] += 1
            detail_rows.append({
                "case_id":   case_id,
                "symbol":    row.get("symbol", ""),
                "side":      row.get("side", ""),
                "pre":       "NO_CASE_META",
                "pend":      "NO_CASE_META",
            })
            continue

        stages = meta.get("stages", {})
        pre_status  = stages.get("pre_pending",  {}).get("stage_status", "missing")
        pend_status = stages.get("pending_open", {}).get("stage_status", "missing")

        stage_results["pre_pending"][pre_status]   += 1
        stage_results["pending_open"][pend_status] += 1

        detail_rows.append({
            "case_id": case_id,
            "symbol":  row.get("symbol", ""),
            "side":    row.get("side", ""),
            "pre":     pre_status,
            "pend":    pend_status,
        })

    # Print summary
    print("Stage status distribution (fresh rows only):")
    print()

    for stage in STAGES_TO_CHECK:
        counts = stage_results[stage]
        total  = sum(counts.values())
        print(f"  {stage}:")
        for status, cnt in sorted(counts.items(), key=lambda x: -x[1]):
            bar   = "█" * min(cnt, 30)
            mark  = "✓" if status == "captured" else ("~" if status == "capture_failed" else "✗")
            pct   = f"{cnt / max(total, 1) * 100:.0f}%"
            print(f"    {mark} {status:<25} {cnt:>4}  {pct:>5}  {bar}")
        print()

    # Verdict
    pre_captured   = stage_results["pre_pending"].get("captured", 0)
    pend_captured  = stage_results["pending_open"].get("captured", 0)
    pre_missing    = stage_results["pre_pending"].get("missing_unexpected", 0)
    pend_missing   = stage_results["pending_open"].get("missing_unexpected", 0)
    total_fresh    = len(fresh)

    print("=" * 60)
    if pre_missing == 0 and pend_missing == 0 and no_case_meta == 0:
        if pre_captured > 0 and pend_captured > 0:
            print("PASS ✓  Live stage capture is working.")
            print(f"  pre_pending  captured: {pre_captured}/{total_fresh}")
            print(f"  pending_open captured: {pend_captured}/{total_fresh}")
            print()
            print("  Stage coverage for pre_pending/pending_open is PROVEN on fresh cases.")
        else:
            print("PARTIAL ⚠  No missing_unexpected, but stages are capture_failed.")
            print("  This means review_snapshots.enabled=False in config.")
            print("  Stage registration is working; snapshots are just disabled.")
            print("  This is acceptable if snapshots are intentionally off.")
    else:
        print("FAIL ✗  Live stage capture is NOT fully working.")
        if pre_missing > 0:
            print(f"  pre_pending  missing_unexpected: {pre_missing}/{total_fresh}")
        if pend_missing > 0:
            print(f"  pending_open missing_unexpected: {pend_missing}/{total_fresh}")
        if no_case_meta > 0:
            print(f"  no case_meta found: {no_case_meta}/{total_fresh}")
        print()
        print("  The apply_stage_persist_fix.py patch may not be deployed.")
        print("  Or _review_register_pending_case is still failing silently.")
        print("  Check logs for: [review_case warn] register_pending_case")
    print("=" * 60)

    # Detail table (up to 20 rows)
    print()
    print("Case detail (fresh rows):")
    print(f"  {'Symbol':<12} {'Side':<6} {'pre_pending':<26} {'pending_open'}")
    print(f"  {'-'*12} {'-'*6} {'-'*26} {'-'*26}")
    for d in detail_rows[:20]:
        pre_icon  = "✓" if d["pre"]  == "captured" else ("~" if d["pre"]  == "capture_failed" else "✗")
        pend_icon = "✓" if d["pend"] == "captured" else ("~" if d["pend"] == "capture_failed" else "✗")
        print(f"  {d['symbol']:<12} {d['side']:<6} "
              f"{pre_icon} {d['pre']:<24} "
              f"{pend_icon} {d['pend']}")
    if len(detail_rows) > 20:
        print(f"  ... and {len(detail_rows) - 20} more")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Validate fresh pre_pending/pending_open stage capture after patch deploy."
    )
    parser.add_argument("--cut-ms",      type=int, required=True,
                        help="Timestamp captured immediately after confirming patch is live")
    parser.add_argument("--pending-dir", default="data/pending")
    parser.add_argument("--workspace",   default="review_workspace")
    parser.add_argument("--tz",          default="Asia/Ho_Chi_Minh")
    args = parser.parse_args()

    run(
        pending_dir = Path(args.pending_dir),
        workspace   = Path(args.workspace),
        cut_ms      = args.cut_ms,
        tz_name     = args.tz,
    )


if __name__ == "__main__":
    main()
