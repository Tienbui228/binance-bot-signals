#!/usr/bin/env python3
"""
Phase 4B patch — build_daily_review_pack.py
Fixes:
  Bug 1: closed_no_reason false positive (CONFIRMED/not_due_yet counted wrongly)
  Bug 2: dispatch_action shows 0%/OK when no confirmed rows
  Bug 3: cascade fix — semantic_health_ok false alarm

Usage (run on server):
  python3 apply_patch_4b.py
  python3 apply_patch_4b.py --check   # dry-run, shows what would change
  python3 apply_patch_4b.py --backup  # creates .bak before patching
"""

import argparse
import shutil
import sys
from pathlib import Path

TARGET = Path("build_daily_review_pack.py")

# ---------------------------------------------------------------------------
# Patch 1 — fix closed_no_reason counter in ReportData._compute()
# ---------------------------------------------------------------------------
PATCH1_OLD = """\
        self.closed_no_reason = sum(
            1 for r in rows
            if str(r.get("closed_ts_ms") or "").strip()
            and not str(r.get("close_reason") or "").strip()
        )"""

PATCH1_NEW = """\
        # Bug 1 fix: only count truly terminal rows (INVALIDATED / EXPIRED_WAIT)
        # that have a real closed_ts_ms but are missing close_reason.
        # CONFIRMED rows with case_close_type=not_due_yet must NOT be counted —
        # they have closed_ts_ms set at confirm time, but the case is not yet closed.
        _TERMINAL_STATUSES = {"INVALIDATED", "EXPIRED_WAIT"}
        self.closed_no_reason = sum(
            1 for r in rows
            if str(r.get("status", "")).strip().upper() in _TERMINAL_STATUSES
            and str(r.get("closed_ts_ms") or "").strip()
            and str(r.get("closed_ts_ms") or "").strip()
                not in ("not_reached_yet", "0", "")
            and not str(r.get("close_reason") or "").strip()
        )"""

# ---------------------------------------------------------------------------
# Patch 2 — fix dispatch display in s_h() when confirmed_rows == 0
# ---------------------------------------------------------------------------
PATCH2_OLD = """\
    tbl = make_table(doc, ["Field","Populated","Coverage","Status"],
                     [2.4, 0.9, 0.9, 1.0])
    for i, (field, pop, ok) in enumerate([
        ("regime_label (3A set)",    regime_ok,   data.legacy_count==0),
        ("regime_fit_for_strategy",  fit_ok,      data.not_eval_fit==0),
        ("dispatch_action",          dispatch_ok, dispatch_ok_for_confirmed),
        ("setup_quality_band",       setup_ok,    True),
    ]):
        bgs = [CLR_ROW_ALT if i%2 else CLR_WHITE]*3 + [CLR_GREEN_BG if ok else CLR_AMBER_BG]
        add_row(tbl, [field, str(pop), data.pct(pop), "OK" if ok else "GAPS"],
                [2.4,0.9,0.9,1.0], bgs=bgs)"""

PATCH2_NEW = """\
    # Bug 2 fix: when there are no confirmed rows, dispatch_action coverage
    # is N/A — not OK and not GAPS. Showing 0%/OK is misleading.
    if len(data.confirmed_rows) == 0:
        dispatch_status_label = "N/A"
        dispatch_status_bg    = CLR_AMBER_BG
    elif dispatch_ok_for_confirmed:
        dispatch_status_label = "OK"
        dispatch_status_bg    = CLR_GREEN_BG
    else:
        dispatch_status_label = "GAPS"
        dispatch_status_bg    = CLR_AMBER_BG

    tbl = make_table(doc, ["Field","Populated","Coverage","Status"],
                     [2.4, 0.9, 0.9, 1.0])
    for i, (field, pop, ok, status_label, status_bg) in enumerate([
        ("regime_label (3A set)",   regime_ok, data.legacy_count==0,        "OK" if data.legacy_count==0   else "GAPS", CLR_GREEN_BG if data.legacy_count==0   else CLR_AMBER_BG),
        ("regime_fit_for_strategy", fit_ok,    data.not_eval_fit==0,         "OK" if data.not_eval_fit==0   else "GAPS", CLR_GREEN_BG if data.not_eval_fit==0   else CLR_AMBER_BG),
        ("dispatch_action",         dispatch_ok, dispatch_ok_for_confirmed,  dispatch_status_label,                       dispatch_status_bg),
        ("setup_quality_band",      setup_ok,  True,                         "OK",                                        CLR_GREEN_BG),
    ]):
        bgs = [CLR_ROW_ALT if i%2 else CLR_WHITE]*3 + [status_bg]
        add_row(tbl, [field, str(pop), data.pct(pop), status_label],
                [2.4,0.9,0.9,1.0], bgs=bgs)"""


# ---------------------------------------------------------------------------
# Apply patches
# ---------------------------------------------------------------------------

def check_patches(content: str) -> dict:
    return {
        "patch1": PATCH1_OLD in content,
        "patch2": PATCH2_OLD in content,
    }


def apply(content: str, check_only: bool = False) -> str:
    results = check_patches(content)
    all_found = True

    for name, found in results.items():
        status = "FOUND" if found else "NOT FOUND — already applied or file mismatch"
        print(f"  {name}: {status}")
        if not found:
            all_found = False

    if check_only:
        return content

    if not all_found:
        print("\nWARN: some patches were not found. Aborting to avoid partial apply.")
        sys.exit(1)

    content = content.replace(PATCH1_OLD, PATCH1_NEW, 1)
    content = content.replace(PATCH2_OLD, PATCH2_NEW, 1)
    return content


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--check",  action="store_true", help="Dry-run only")
    parser.add_argument("--backup", action="store_true", help="Backup before patching")
    args = parser.parse_args()

    if not TARGET.exists():
        print(f"ERROR: {TARGET} not found in current directory.")
        print("Run this script from the same directory as build_daily_review_pack.py")
        sys.exit(1)

    content = TARGET.read_text(encoding="utf-8")
    print(f"Checking patches against {TARGET} ({len(content)} chars)...")
    print()

    patched = apply(content, check_only=args.check)

    if args.check:
        print("\nDry-run complete. No files written.")
        return

    if args.backup:
        bak = TARGET.with_suffix(".py.bak")
        shutil.copy2(TARGET, bak)
        print(f"Backup written: {bak}")

    TARGET.write_text(patched, encoding="utf-8")
    print(f"\nPatch applied successfully to {TARGET}")
    print("Next: rebuild the report to verify.")
    print("  python3 oi_scanner.py --build-review-pack 2026-04-05")


if __name__ == "__main__":
    main()
