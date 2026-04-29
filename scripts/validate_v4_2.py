#!/bin/sh
"exec" "python3" "$0" "$@"
"""
scripts/validate_v4_2.py

V4-2 post-implementation validation. Run after a research day has been processed.

Usage:
  python3 scripts/validate_v4_2.py 2026-04-25

All hard-gate checks must pass for V4-2 to be considered complete.
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
    print(f"Run the research pipeline first: python3 scripts/run_daily_top_movers_research.py --day {research_day}")
    sys.exit(1)

df = pd.read_csv(csv_path)
errors = []
print(f"Loaded {len(df)} rows from {csv_path}\n")


def check_field(name, null_ok=False):
    if name not in df.columns:
        errors.append(f"FAIL: field '{name}' MISSING from schema")
        return False
    n_null = df[name].isnull().sum()
    if not null_ok and n_null == len(df):
        errors.append(f"FAIL: field '{name}' is all-null ({len(df)} rows)")
        return False
    print(f"PASS: {name} — {n_null} null / {len(df)} rows")
    return True


# 1. New Layer 2 fields present and not all-null
check_field("p1_price",        null_ok=False)
check_field("peak_close",      null_ok=False)
check_field("p3_price",        null_ok=True)   # null when no retest
check_field("bars_p0_to_p1",   null_ok=False)
check_field("bars_p1_to_p2",   null_ok=False)
check_field("bars_p2_to_p3",   null_ok=True)   # null when no p3
check_field("bars_p3_to_p4",   null_ok=True)   # null when no p3/p4
check_field("anchor_reason_code", null_ok=False)
check_field("peak_age_hours",  null_ok=True)
check_field("case_spans_days", null_ok=False)

# 2. p1_price must be HIGH >= peak_close (close) for all non-null cases
if "p1_price" in df.columns and "peak_close" in df.columns:
    df_both = df[df["p1_price"].notna() & df["peak_close"].notna()]
    bad = (df_both["p1_price"] < df_both["peak_close"]).sum()
    if bad > 0:
        errors.append(f"FAIL: {bad} cases have p1_price < peak_close (p1_price must be HIGH >= close)")
    else:
        print(f"PASS: p1_price >= peak_close for all non-null cases ({len(df_both)} rows)")

# 3. p1_price vs range_high sanity check (warn only, not hard fail)
if "p1_price" in df.columns and "range_high" in df.columns:
    df_both = df[df["p1_price"].notna() & df["range_high"].notna()]
    if len(df_both) > 0:
        diff_pct = ((df_both["p1_price"] - df_both["range_high"]).abs() / df_both["range_high"] * 100)
        bad = (diff_pct > 5.0).sum()
        if bad > len(df_both) * 0.3:
            print(f"WARN: {bad}/{len(df_both)} cases have p1_price >5% away from range_high — review anchor logic")
        else:
            print(f"PASS: p1_price vs range_high within 5% for {len(df_both) - bad}/{len(df_both)} cases")

# 4. bar counts: non-negative
if "bars_p0_to_p1" in df.columns:
    neg = (df["bars_p0_to_p1"].dropna() < 0).sum()
    if neg > 0:
        errors.append(f"FAIL: {neg} cases have negative bars_p0_to_p1")
    else:
        print(f"PASS: bars_p0_to_p1 all >= 0")
    zeros = (df["bars_p0_to_p1"] == 0).sum()
    print(f"INFO: bars_p0_to_p1 == 0 in {zeros} cases (expected — sub-hour P0→P1)")

if "bars_p2_to_p3" in df.columns:
    df_nn = df[df["bars_p2_to_p3"].notna()]
    if len(df_nn) > 0:
        bad = (df_nn["bars_p2_to_p3"] < 1).sum()
        if bad > 0:
            errors.append(f"FAIL: {bad} cases have bars_p2_to_p3 < 1 (P3 must be at least 1 bar after P2)")
        else:
            print(f"PASS: bars_p2_to_p3 >= 1 for all non-null cases ({len(df_nn)} rows)")

# 5. anchor_reason_code: allowed values only
VALID_CODES = {"clean", "p1_fallback", "p1_p2_fallback"}
if "anchor_reason_code" in df.columns:
    codes = df["anchor_reason_code"].dropna().unique()
    unknown = [c for c in codes if not (c in VALID_CODES or str(c).startswith("unknown:"))]
    if unknown:
        errors.append(f"FAIL: Unexpected anchor_reason_code values: {unknown}")
    else:
        print(f"PASS: anchor_reason_code values all valid: {sorted(codes)}")

# 6. anchor_reason_code must correlate with anchor_validity_reason
if "anchor_reason_code" in df.columns and "anchor_validity_reason" in df.columns:
    mask = df["anchor_validity_reason"] == "all_anchors_auto_detected"
    if mask.sum() > 0:
        wrong = (df.loc[mask, "anchor_reason_code"] != "clean").sum()
        if wrong > 0:
            errors.append(f"FAIL: {wrong} cases with all_anchors_auto_detected but anchor_reason_code != clean")
        else:
            print("PASS: anchor_reason_code 'clean' correctly mapped from all_anchors_auto_detected")

# 7. peak_age_hours: positive for current research day
if "peak_age_hours" in df.columns:
    df_nn = df[df["peak_age_hours"].notna()]
    if len(df_nn) > 0:
        neg = (df_nn["peak_age_hours"] < 0).sum()
        if neg > 0:
            errors.append(f"FAIL: {neg} cases have negative peak_age_hours")
        else:
            print(f"PASS: peak_age_hours all positive ({len(df_nn)} non-null rows)")
        reasonable = (df_nn["peak_age_hours"] <= 72).sum()
        print(f"INFO: {reasonable}/{len(df_nn)} cases have peak_age_hours <= 72h")

# 8. case_spans_days: bool, no nulls
if "case_spans_days" in df.columns:
    if df["case_spans_days"].isnull().any():
        errors.append("FAIL: case_spans_days has nulls")
    else:
        n_spans = df["case_spans_days"].sum()
        print(f"PASS: case_spans_days — {n_spans}/{len(df)} cases span multiple days")

# 9. Layer 2 full coverage audit
L2_REQUIRED = [
    "p0_ts_ms", "p1_ts_ms", "p2_ts_ms", "p3_ts_ms", "p4_ts_ms",
    "p0_price", "p2_price", "p4_price",
    "anchor_detect_method", "anchor_quality_flag",
    "p1_price", "peak_close", "p3_price",
    "bars_p0_to_p1", "bars_p1_to_p2", "bars_p2_to_p3", "bars_p3_to_p4",
    "anchor_reason_code", "peak_age_hours", "case_spans_days",
]
missing_l2 = [f for f in L2_REQUIRED if f not in df.columns]
if missing_l2:
    errors.append(f"FAIL: Layer 2 still missing fields: {missing_l2}")
else:
    print(f"PASS: All Layer 2 required fields present in schema ({len(L2_REQUIRED)} fields)")

# 10. Phase 2 fields unchanged (backward compat check)
PHASE2_FIELDS = [
    "case_id", "resolution_label", "anchor_validity_reason",
    "anchor_conflict_flag", "decision_grade",
    "future_1h_max_favor_pct", "research_eligible_YN",
]
for f in PHASE2_FIELDS:
    if f not in df.columns:
        errors.append(f"FAIL: existing field '{f}' disappeared")
    else:
        print(f"PASS: existing field '{f}' still present")

# 11. V4-1 fields still present
V41_FIELDS = ["research_case_id", "selection_horizon", "live_universe_eligible_flag",
              "case_inclusion_reason", "semantic_clean_flag", "dataset_batch"]
for f in V41_FIELDS:
    if f not in df.columns:
        errors.append(f"FAIL: V4-1 field '{f}' disappeared — V4-2 broke backward compat")
    else:
        print(f"PASS: V4-1 field '{f}' still present")

# Result
print()
if errors:
    print("=== VALIDATION FAILED ===")
    for e in errors:
        print(f"  {e}")
    sys.exit(1)
else:
    print("=== ALL V4-2 VALIDATION CHECKS PASSED ===")
    sys.exit(0)
