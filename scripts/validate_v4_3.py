#!/bin/sh
"exec" "python3" "$0" "$@"
"""
scripts/validate_v4_3.py

V4-3 post-implementation validation. Run after a research day has been processed.

Usage:
  python3 scripts/validate_v4_3.py 2026-04-25

All 14 checks must pass for V4-3 to be considered complete.
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
csv_path = (
    f"data/research_output/top_movers/{research_day}/csv/"
    f"daily_case_dataset_{research_day}.csv"
)

if not os.path.exists(csv_path):
    print(f"ERROR: CSV not found at {csv_path}")
    print(f"Run: python3 scripts/run_daily_top_movers_research.py --day {research_day}")
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


# ---------------------------------------------------------------------------
# Check 1: p1_low present and not all-null
# ---------------------------------------------------------------------------
check_field("p1_low", null_ok=False)

# ---------------------------------------------------------------------------
# Check 2: p1_low <= peak_close <= p1_price for all non-null cases
# ---------------------------------------------------------------------------
if all(f in df.columns for f in ["p1_low", "peak_close", "p1_price"]):
    mask = df["p1_low"].notna() & df["peak_close"].notna() & df["p1_price"].notna()
    df_tri = df[mask]
    bad_low = (df_tri["p1_low"] > df_tri["peak_close"]).sum()
    bad_high = (df_tri["peak_close"] > df_tri["p1_price"]).sum()
    if bad_low > 0:
        errors.append(f"FAIL: {bad_low} cases have p1_low > peak_close")
    else:
        print(f"PASS: p1_low <= peak_close for all non-null cases ({len(df_tri)} rows)")
    if bad_high > 0:
        errors.append(f"FAIL: {bad_high} cases have peak_close > p1_price")
    else:
        print(f"PASS: peak_close <= p1_price for all non-null cases ({len(df_tri)} rows)")

# ---------------------------------------------------------------------------
# Check 3: All 10 Layer 3 fields present in schema
# ---------------------------------------------------------------------------
L3_FIELDS = [
    "pre_break_extension_pct_from_local_base",
    "pre_break_extension_pct_from_vwap",
    "pre_break_extension_pct_from_ema",
    "peak_upper_wick_ratio",
    "wick_cluster_count_last_n_bars",
    "blowoff_volume_ratio",
    "close_location_near_low_score",
    "failed_continuation_count",
    "exhaustion_strength_raw",
    "exhaustion_strength_bucket",
]
missing_l3 = [f for f in L3_FIELDS if f not in df.columns]
if missing_l3:
    errors.append(f"FAIL: Layer 3 fields missing from schema: {missing_l3}")
else:
    print("PASS: All 10 Layer 3 fields present in schema")

# ---------------------------------------------------------------------------
# Check 4: wick_cluster_count_last_n_bars — never null, range [0, 5]
# ---------------------------------------------------------------------------
if "wick_cluster_count_last_n_bars" in df.columns:
    n_null = df["wick_cluster_count_last_n_bars"].isnull().sum()
    if n_null > 0:
        errors.append(
            f"FAIL: wick_cluster_count_last_n_bars has {n_null} nulls — must be always int"
        )
    else:
        bad = ((df["wick_cluster_count_last_n_bars"] < 0) |
               (df["wick_cluster_count_last_n_bars"] > 5)).sum()
        if bad > 0:
            errors.append(f"FAIL: {bad} cases have wick_cluster_count_last_n_bars outside [0, 5]")
        else:
            print(
                f"PASS: wick_cluster_count_last_n_bars always int, range [0,5] "
                f"(max={df['wick_cluster_count_last_n_bars'].max()})"
            )

# ---------------------------------------------------------------------------
# Check 5: failed_continuation_count — never null, >= 0
# ---------------------------------------------------------------------------
if "failed_continuation_count" in df.columns:
    n_null = df["failed_continuation_count"].isnull().sum()
    if n_null > 0:
        errors.append(
            f"FAIL: failed_continuation_count has {n_null} nulls — must be always int"
        )
    else:
        bad = (df["failed_continuation_count"] < 0).sum()
        if bad > 0:
            errors.append("FAIL: failed_continuation_count has negative values")
        else:
            print(
                f"PASS: failed_continuation_count always int, non-negative "
                f"(max={df['failed_continuation_count'].max()})"
            )

# ---------------------------------------------------------------------------
# Check 6: peak_upper_wick_ratio in [0, 1] for non-null cases
# ---------------------------------------------------------------------------
if "peak_upper_wick_ratio" in df.columns:
    df_nn = df[df["peak_upper_wick_ratio"].notna()]
    if len(df_nn) > 0:
        bad = ((df_nn["peak_upper_wick_ratio"] < 0) |
               (df_nn["peak_upper_wick_ratio"] > 1)).sum()
        if bad > 0:
            errors.append(f"FAIL: {bad} cases have peak_upper_wick_ratio outside [0, 1]")
        else:
            print(
                f"PASS: peak_upper_wick_ratio in [0, 1] for all non-null cases "
                f"({len(df_nn)} rows)"
            )

# ---------------------------------------------------------------------------
# Check 7: close_location_near_low_score in [0, 1] for non-null cases
# ---------------------------------------------------------------------------
if "close_location_near_low_score" in df.columns:
    df_nn = df[df["close_location_near_low_score"].notna()]
    if len(df_nn) > 0:
        bad = ((df_nn["close_location_near_low_score"] < 0) |
               (df_nn["close_location_near_low_score"] > 1)).sum()
        if bad > 0:
            errors.append(
                f"FAIL: {bad} cases have close_location_near_low_score outside [0, 1]"
            )
        else:
            print(
                f"PASS: close_location_near_low_score in [0, 1] for all non-null cases "
                f"({len(df_nn)} rows)"
            )

# ---------------------------------------------------------------------------
# Check 8: exhaustion_strength_raw in [0, 1] for non-null cases
# ---------------------------------------------------------------------------
if "exhaustion_strength_raw" in df.columns:
    df_nn = df[df["exhaustion_strength_raw"].notna()]
    if len(df_nn) > 0:
        bad = ((df_nn["exhaustion_strength_raw"] < 0) |
               (df_nn["exhaustion_strength_raw"] > 1)).sum()
        if bad > 0:
            errors.append(
                f"FAIL: {bad} cases have exhaustion_strength_raw outside [0, 1]"
            )
        else:
            print(
                f"PASS: exhaustion_strength_raw in [0, 1] for all non-null cases "
                f"({len(df_nn)} rows, mean={df_nn['exhaustion_strength_raw'].mean():.3f})"
            )

# ---------------------------------------------------------------------------
# Check 9: exhaustion_strength_bucket — only STRONG/MODERATE/WEAK
# ---------------------------------------------------------------------------
VALID_BUCKETS = {"STRONG", "MODERATE", "WEAK"}
if "exhaustion_strength_bucket" in df.columns:
    df_nn = df[df["exhaustion_strength_bucket"].notna()]
    if len(df_nn) > 0:
        bad_vals = set(df_nn["exhaustion_strength_bucket"].unique()) - VALID_BUCKETS
        if bad_vals:
            errors.append(
                f"FAIL: exhaustion_strength_bucket has invalid values: {bad_vals}"
            )
        else:
            counts = df_nn["exhaustion_strength_bucket"].value_counts().to_dict()
            print(f"PASS: exhaustion_strength_bucket valid — {counts}")

# ---------------------------------------------------------------------------
# Check 10: exhaustion_strength_raw and bucket are null-aligned
# ---------------------------------------------------------------------------
if all(f in df.columns for f in ["exhaustion_strength_raw", "exhaustion_strength_bucket"]):
    raw_null = df["exhaustion_strength_raw"].isnull()
    bkt_null = df["exhaustion_strength_bucket"].isnull()
    mismatch = (raw_null != bkt_null).sum()
    if mismatch > 0:
        errors.append(
            f"FAIL: {mismatch} cases where exhaustion_strength_raw null-ness "
            f"!= exhaustion_strength_bucket null-ness"
        )
    else:
        print(
            f"PASS: exhaustion_strength_raw and exhaustion_strength_bucket "
            f"null-aligned ({raw_null.sum()} nulls)"
        )

# ---------------------------------------------------------------------------
# Check 11: >= 50% of rows have non-null exhaustion_strength_raw
#            (guard against bars_1h not being wired)
# ---------------------------------------------------------------------------
if "exhaustion_strength_raw" in df.columns:
    n_non_null = df["exhaustion_strength_raw"].notna().sum()
    pct = n_non_null / len(df) * 100 if len(df) > 0 else 0
    if pct < 50.0:
        errors.append(
            f"FAIL: exhaustion_strength_raw is non-null in only {pct:.1f}% of rows "
            f"(expected >=50%) — bars_1h likely not wired correctly"
        )
    else:
        print(
            f"PASS: exhaustion_strength_raw non-null in {pct:.1f}% of rows "
            f"({n_non_null}/{len(df)})"
        )

# ---------------------------------------------------------------------------
# Check 12: V4-2 backward compat — no V4-2 fields disappeared
# ---------------------------------------------------------------------------
V42_FIELDS = [
    "p1_price", "peak_close", "p3_price",
    "bars_p0_to_p1", "bars_p1_to_p2", "bars_p2_to_p3", "bars_p3_to_p4",
    "anchor_reason_code", "peak_age_hours", "case_spans_days",
]
for f in V42_FIELDS:
    if f not in df.columns:
        errors.append(f"FAIL: V4-2 field '{f}' disappeared — V4-3 broke backward compat")
    else:
        print(f"PASS: V4-2 field '{f}' still present")

# ---------------------------------------------------------------------------
# Check 13: blowoff_volume_ratio >= 0 for non-null cases
# ---------------------------------------------------------------------------
if "blowoff_volume_ratio" in df.columns:
    df_nn = df[df["blowoff_volume_ratio"].notna()]
    if len(df_nn) > 0:
        bad = (df_nn["blowoff_volume_ratio"] < 0).sum()
        if bad > 0:
            errors.append(f"FAIL: {bad} cases have negative blowoff_volume_ratio")
        else:
            print(
                f"PASS: blowoff_volume_ratio >= 0 for all non-null cases "
                f"(mean={df_nn['blowoff_volume_ratio'].mean():.2f})"
            )

# ---------------------------------------------------------------------------
# Check 14: Full V4-3 schema — 11 fields (p1_low + 10 Layer 3) all present
# ---------------------------------------------------------------------------
L3_ALL = ["p1_low"] + L3_FIELDS
schema_missing = [f for f in L3_ALL if f not in df.columns]
if schema_missing:
    errors.append(f"FAIL: V4-3 schema incomplete. Missing: {schema_missing}")
else:
    print(f"PASS: All 11 V4-3 fields (p1_low + 10 Layer 3) present in schema")

# ---------------------------------------------------------------------------
# Result
# ---------------------------------------------------------------------------
print()
if errors:
    print("=== VALIDATION FAILED ===")
    for e in errors:
        print(f"  {e}")
    sys.exit(1)
else:
    print("=== ALL V4-3 VALIDATION CHECKS PASSED ===")
    sys.exit(0)
