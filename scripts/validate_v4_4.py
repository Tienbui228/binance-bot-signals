#!/bin/sh
"exec" "python3" "$0" "$@"
"""
scripts/validate_v4_4.py

V4-4 post-implementation validation. Run after a research day has been processed.

Usage:
  python3 scripts/validate_v4_4.py 2026-04-25

All hard-gate checks must pass for V4-4 to be considered complete.
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
    print(f"Run the research pipeline first: python3 scripts/run_daily_top_movers_research.py --day {research_day}")
    sys.exit(1)

df = pd.read_csv(csv_path, keep_default_na=False, na_values=["None", "NaN", "nan", "N/A"])
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


# ── Check 1: all 9 new V4-4 fields present ────────────────────────────────
NEW_FIELDS = [
    "quote_vol_5m_median_at_p3_usdt",
    "live_liquidity_gate_pass",
    "p3_detectable_in_live_mode_flag",
    "live_signal_age_min",
    "live_entry_price_proxy",
    "live_stop_price_proxy",
    "live_rr_conservative",
    "entry_feasible_flag",
    "entry_feasible_reason",
]
print("── Check 1: new V4-4 fields present ──")
for f in NEW_FIELDS:
    check_field(f, null_ok=True)
print()

# ── Check 2: live_signal_age_min == bars_to_retest × 5 ────────────────────
print("── Check 2: live_signal_age_min == bars_to_retest × 5 ──")
if "live_signal_age_min" in df.columns and "bars_to_retest" in df.columns:
    df_nn = df[df["bars_to_retest"].notna() & df["live_signal_age_min"].notna()].copy()
    if len(df_nn) > 0:
        expected = df_nn["bars_to_retest"].astype(int) * 5
        mismatch = (df_nn["live_signal_age_min"].astype(int) != expected).sum()
        if mismatch > 0:
            errors.append(f"FAIL: {mismatch} cases have live_signal_age_min != bars_to_retest × 5")
        else:
            print(f"PASS: live_signal_age_min == bars_to_retest × 5 for all {len(df_nn)} non-null cases")
    else:
        print("INFO: no non-null bars_to_retest rows — check skipped (expected if no retest cases)")
else:
    print("SKIP: required columns not present")
print()

# ── Check 3: live_stop_price_proxy > live_entry_price_proxy for SHORT ─────
print("── Check 3: live_stop > live_entry for SHORT non-null cases ──")
if "live_stop_price_proxy" in df.columns and "live_entry_price_proxy" in df.columns:
    df_sh = df[
        (df["side"] == "SHORT")
        & df["live_stop_price_proxy"].notna()
        & df["live_entry_price_proxy"].notna()
    ]
    if len(df_sh) > 0:
        bad = (
            df_sh["live_stop_price_proxy"].astype(float)
            <= df_sh["live_entry_price_proxy"].astype(float)
        ).sum()
        if bad > 0:
            errors.append(
                f"FAIL: {bad} SHORT cases have live_stop_price_proxy <= live_entry_price_proxy "
                f"(stop must be above entry for shorts)"
            )
        else:
            print(f"PASS: live_stop > live_entry for all {len(df_sh)} SHORT non-null cases")
    else:
        print("INFO: no non-null SHORT retest cases — check skipped")
else:
    print("SKIP: required columns not present")
print()

# ── Check 4: live_rr_conservative > 0 when populated ─────────────────────
print("── Check 4: live_rr_conservative > 0 when populated ──")
if "live_rr_conservative" in df.columns:
    df_nn = df[df["live_rr_conservative"].notna()]
    if len(df_nn) > 0:
        bad = (df_nn["live_rr_conservative"].astype(float) <= 0).sum()
        if bad > 0:
            errors.append(f"FAIL: {bad} cases have live_rr_conservative <= 0")
        else:
            print(f"PASS: live_rr_conservative > 0 for all {len(df_nn)} non-null cases")
        mean_rr = df_nn["live_rr_conservative"].astype(float).mean()
        if abs(mean_rr - 1.5) > 0.1:
            print(f"WARN: mean live_rr_conservative = {mean_rr:.3f}, expected ~1.5")
        else:
            print(f"PASS: mean live_rr_conservative = {mean_rr:.3f} (expected ~1.5)")
    else:
        print("INFO: no non-null live_rr_conservative rows")
else:
    print("SKIP: column not present")
print()

# ── Check 5: p3_detectable_in_live_mode_flag distribution info ────────────
print("── Check 5: p3_detectable_in_live_mode_flag distribution (mostly False expected) ──")
if "p3_detectable_in_live_mode_flag" in df.columns:
    df_sh = df[df["side"] == "SHORT"]
    df_nn = df_sh[df_sh["p3_detectable_in_live_mode_flag"].notna()]
    n_true  = (df_nn["p3_detectable_in_live_mode_flag"].astype(str).str.upper() == "TRUE").sum()
    n_false = (df_nn["p3_detectable_in_live_mode_flag"].astype(str).str.upper() == "FALSE").sum()
    print(
        f"INFO: p3_detectable_in_live_mode_flag — True:{n_true} False:{n_false} "
        f"(mostly False expected for current data)"
    )
else:
    print("SKIP: column not present")
print()

# ── Check 6: entry_feasible_reason never null ──────────────────────────────
print("── Check 6: entry_feasible_reason never null ──")
if "entry_feasible_reason" in df.columns:
    n_null = df["entry_feasible_reason"].isnull().sum()
    if n_null > 0:
        errors.append(
            f"FAIL: entry_feasible_reason has {n_null} nulls — should be a string ('no_retest', 'feasible', or a reason)"
        )
    else:
        dist = df["entry_feasible_reason"].value_counts().to_dict()
        print(f"PASS: entry_feasible_reason no nulls — distribution: {dist}")
else:
    errors.append("FAIL: entry_feasible_reason column missing")
print()

# ── Check 7: entry_feasible_reason == "feasible" when entry_feasible_flag True ──
print('── Check 7: entry_feasible_reason == "feasible" when entry_feasible_flag = True ──')
if "entry_feasible_flag" in df.columns and "entry_feasible_reason" in df.columns:
    df_feasible = df[df["entry_feasible_flag"].astype(str).str.upper() == "TRUE"]
    if len(df_feasible) > 0:
        bad = (df_feasible["entry_feasible_reason"] != "feasible").sum()
        if bad > 0:
            errors.append(
                f"FAIL: {bad} cases have entry_feasible_flag=True but entry_feasible_reason != 'feasible'"
            )
        else:
            print(f"PASS: entry_feasible_reason == 'feasible' for all {len(df_feasible)} feasible cases")
    else:
        print("INFO: no cases with entry_feasible_flag=True — expected given current retest data")
else:
    print("SKIP: required columns not present")
print()

# ── Check 8: outcome_measured_from field exists and has valid values ───────
print("── Check 8: outcome_measured_from field ──")
VALID_MEASURED_FROM = {"live_entry_price_proxy", "p3_price_fallback"}
if "outcome_measured_from" not in df.columns:
    print("WARN: outcome_measured_from field not present — check case_builder wiring")
else:
    bad_vals = (
        ~df["outcome_measured_from"].isin(VALID_MEASURED_FROM)
        & df["outcome_measured_from"].notna()
    ).sum()
    if bad_vals > 0:
        errors.append(f"FAIL: {bad_vals} unexpected outcome_measured_from values")
    else:
        dist = df["outcome_measured_from"].value_counts(dropna=False).to_dict()
        print(f"PASS: outcome_measured_from values valid: {dist}")
print()

# ── Check 9: existing Layer 5 fields still present ────────────────────────
print("── Check 9: existing Layer 5 fields unchanged ──")
EXISTING_L5 = [
    "retest_pack_available", "reclaim_pct",
    "retest_depth_vs_break_pct", "retest_duration_bars",
    "retest_volume_decay_ratio", "retest_reject_wick_ratio",
]
for f in EXISTING_L5:
    if f not in df.columns:
        errors.append(f"FAIL: existing Layer 5 field '{f}' disappeared")
    else:
        print(f"PASS: existing field '{f}' still present")
print()

# ── Check 10: quote_vol_5m_median_at_p3_usdt liquidity gate distribution ──
print("── Check 10: quote_vol_5m_median_at_p3_usdt distribution ──")
if "quote_vol_5m_median_at_p3_usdt" in df.columns:
    df_nn = df[df["quote_vol_5m_median_at_p3_usdt"].notna()]
    if len(df_nn) > 0:
        pct_below = (df_nn["quote_vol_5m_median_at_p3_usdt"].astype(float) < 10_000).sum()
        pct = pct_below / len(df_nn) * 100
        if pct > 80:
            print(
                f"WARN: {pct:.0f}% of cases have quote_vol_5m < 10K USDT — "
                f"most will have live_liquidity_gate_pass=False"
            )
        else:
            print(f"PASS: quote_vol_5m populated for {len(df_nn)}/{len(df)} cases")
    else:
        print("INFO: no non-null quote_vol_5m values — all cases had no retest or fetch failed")
else:
    print("SKIP: column not present")
print()

# ── Check 11: V4-1/V4-2/V4-3 fields still present (backward compat) ──────
print("── Check 11: backward compat — V4-1/2/3 fields ──")
PREV_FIELDS = [
    "research_case_id", "selection_horizon",            # V4-1
    "p1_price", "bars_p0_to_p1", "anchor_reason_code",  # V4-2
    "exhaustion_strength_bucket",                        # V4-3
]
for f in PREV_FIELDS:
    if f not in df.columns:
        errors.append(f"FAIL: previous phase field '{f}' disappeared")
    else:
        print(f"PASS: {f} still present (backward compat)")
print()

# ── Result ─────────────────────────────────────────────────────────────────
print()
if errors:
    print("=== VALIDATION FAILED ===")
    for e in errors:
        print(f"  {e}")
    sys.exit(1)
else:
    print("=== ALL V4-4 VALIDATION CHECKS PASSED ===")
    sys.exit(0)
