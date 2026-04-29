#!/usr/bin/env python3
"""
scripts/validate_v4_1.py

V4-1 post-implementation validation. Run after a research day has been processed.

Usage:
  python3 scripts/validate_v4_1.py 2026-04-25

All checks must pass for V4-1 to be considered complete.
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    import pandas as pd
except ImportError:
    print("ERROR: pandas not installed — run: pip install pandas")
    sys.exit(1)

research_day = sys.argv[1] if len(sys.argv) > 1 else "2026-04-25"
csv_path = f"data/research_output/top_movers/{research_day}/csv/daily_case_dataset_{research_day}.csv"

if not os.path.exists(csv_path):
    print(f"ERROR: CSV not found at {csv_path}")
    print("Run the research pipeline first: python3 scripts/run_daily_top_movers_research.py --day {research_day}")
    sys.exit(1)

df = pd.read_csv(csv_path)
errors = []
print(f"\nValidating V4-1 output for {research_day} ({len(df)} rows)\n")

# 1. research_case_id format
expected_rcid = df.apply(
    lambda r: f"{r['symbol']}_{research_day}_{r['selection_horizon']}", axis=1
)
mismatch = (df["research_case_id"] != expected_rcid).sum()
if mismatch > 0:
    errors.append(f"FAIL: {mismatch} rows have wrong research_case_id format")
else:
    print("PASS: research_case_id format correct")

# 2. selection_horizon populated and valid
valid_horizons = {"1d", "7d", "1d_gainers", "1d_losers"}
bad_horizon = (
    df["selection_horizon"].isnull().sum()
    + (~df["selection_horizon"].isin(valid_horizons)).sum()
    if "selection_horizon" in df.columns else len(df)
)
if bad_horizon > 0:
    errors.append(f"FAIL: {bad_horizon} rows have null/invalid selection_horizon")
else:
    horizon_counts = df["selection_horizon"].value_counts().to_dict()
    print(f"PASS: selection_horizon populated — {horizon_counts}")

# 3. 7d cases present
n_7d = (df["selection_horizon"] == "7d").sum() if "selection_horizon" in df.columns else 0
if n_7d == 0:
    errors.append("FAIL: No 7d dump cases found in dataset")
else:
    print(f"PASS: {n_7d} 7d dump cases present")

# 4. Layer 0 blocking fields now populated
l0_fields = ["case_inclusion_reason", "semantic_clean_flag", "exclusion_reason", "dataset_batch"]
for field in l0_fields:
    if field not in df.columns:
        errors.append(f"FAIL: Layer 0 field '{field}' MISSING from schema")
    else:
        null_count = df[field].isnull().sum()
        if null_count == len(df):
            errors.append(f"FAIL: Layer 0 field '{field}' is all-null")
        else:
            print(f"PASS: {field} populated ({null_count} nulls of {len(df)})")

# 5. Layer 1 blocking fields now populated
l1_fields = ["day_range_pct", "intraday_expansion_pct", "rank_volume_24h",
             "notional_volume_usd", "rank_abs_change_24h"]
for field in l1_fields:
    if field not in df.columns:
        errors.append(f"FAIL: Layer 1 field '{field}' MISSING from schema")
    else:
        null_count = df[field].isnull().sum()
        if null_count == len(df):
            errors.append(f"FAIL: Layer 1 field '{field}' is all-null")
        else:
            print(f"PASS: {field} populated ({null_count} nulls of {len(df)})")

# 6. live_universe_eligible_flag populated
if "live_universe_eligible_flag" not in df.columns:
    errors.append("FAIL: live_universe_eligible_flag missing")
else:
    n_elig = df["live_universe_eligible_flag"].sum()
    n_not  = (~df["live_universe_eligible_flag"].astype(bool)).sum()
    print(f"PASS: live_universe_eligible_flag — eligible:{n_elig}, not_eligible:{n_not}")

# 7. Dedup: symbols in both 1d dump and 7d dump must have two separate rows
if "selection_horizon" in df.columns:
    df_1d = df[df["selection_horizon"] == "1d"]
    df_7d = df[df["selection_horizon"] == "7d"]
    overlap_syms = set(df_1d["symbol"]) & set(df_7d["symbol"])
    for sym in overlap_syms:
        count = len(df[df["symbol"] == sym])
        if count < 2:
            errors.append(f"FAIL: {sym} in both 1d and 7d but only {count} row — expected 2 separate rows")
    if overlap_syms:
        print(f"PASS: {len(overlap_syms)} overlap symbols have separate rows: {overlap_syms}")
    else:
        print("PASS: No overlap between 1d and 7d dump (or no 7d cases yet)")

# 8. also_in_7d_top10 flag on 1d rows when overlap
if "selection_horizon" in df.columns and overlap_syms:
    for sym in overlap_syms:
        row_1d = df[(df["symbol"] == sym) & (df["selection_horizon"] == "1d")]
        if not row_1d.empty:
            flag = row_1d.iloc[0].get("also_in_7d_top10", False)
            if str(flag).lower() not in ("true", "1", "yes"):
                errors.append(f"FAIL: {sym} 1d row missing also_in_7d_top10=True")
    print(f"PASS: also_in_7d_top10 flags correct on 1d overlap rows")

# 9. runtime_equivalent_case_id is null in V4-1
if "runtime_equivalent_case_id" in df.columns:
    non_null = df["runtime_equivalent_case_id"].notna().sum()
    if non_null > 0:
        errors.append(f"FAIL: runtime_equivalent_case_id should be null in V4-1 but {non_null} rows non-null")
    else:
        print("PASS: runtime_equivalent_case_id all null (expected in V4-1)")
else:
    errors.append("FAIL: runtime_equivalent_case_id field missing from schema")

# 10. case_id format unchanged
expected_case_id = df.apply(
    lambda r: f"{research_day.replace('-','')}_{r['symbol']}_{r['side']}", axis=1
)
id_ok = (df["case_id"] == expected_case_id).sum()
if id_ok < len(df):
    errors.append(f"FAIL: case_id format wrong — only {id_ok}/{len(df)} match expected format")
else:
    print(f"PASS: case_id unchanged — all {id_ok} rows match expected format")

# 11. Existing Phase 2A-2E fields still present
phase2_fields = ["resolution_label", "data_quality_flag", "decision_grade",
                 "future_1h_max_favor_pct", "future_4h_max_favor_pct",
                 "research_eligible_YN"]
for field in phase2_fields:
    if field not in df.columns:
        errors.append(f"FAIL: Existing Phase 2 field '{field}' disappeared from schema")
    else:
        print(f"PASS: Existing field '{field}' still present")

# 12. Import isolation (no live runtime imports in R1 modules)
import subprocess
checks = [
    ("grep -r 'from scanner' research/top_movers/daily_selector.py", "daily_selector imports scanner"),
    ("grep -r 'import oi_scanner' research/top_movers/", "research imports oi_scanner"),
]
for cmd, label in checks:
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.stdout.strip():
        errors.append(f"FAIL: Import isolation — {label}: {result.stdout.strip()}")
    else:
        print(f"PASS: Import isolation — no {label}")

# Final result
print()
if errors:
    print("=== VALIDATION FAILED ===")
    for e in errors:
        print(f"  {e}")
    sys.exit(1)
else:
    print(f"=== ALL VALIDATION CHECKS PASSED ({research_day}) ===")
    sys.exit(0)
