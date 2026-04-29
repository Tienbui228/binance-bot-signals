"""
scripts/validate_v4_5.py

Validate V4-5 output: 8 Layer 7 OI/funding fields.
Run after pipeline: python3 scripts/validate_v4_5.py YYYY-MM-DD

Exits 0 on all pass, exits 1 on any failure.
"""

import sys
import pandas as pd

research_day = sys.argv[1] if len(sys.argv) > 1 else "2026-04-25"
csv_path = (
    f"data/research_output/top_movers/{research_day}/csv/"
    f"daily_case_dataset_{research_day}.csv"
)

df = pd.read_csv(csv_path, keep_default_na=False)
errors = []


def check_field(name, null_ok=False):
    if name not in df.columns:
        errors.append(f"FAIL: field '{name}' MISSING from schema")
        return False
    n_null = df[name].isin(["", "None", "nan", None]).sum() + df[name].isnull().sum()
    if not null_ok and n_null == len(df):
        errors.append(f"FAIL: field '{name}' is all-null")
        return False
    print(f"PASS: {name} — {n_null} null / {len(df)} rows")
    return True


# 1. All 8 new fields present (null_ok because many cases have no P3)
NEW_FIELDS = [
    "oi_regime_label", "oi_change_1h_pct", "oi_change_4h_pct",
    "oi_data_unavailable", "oi_context_note",
    "latest_funding_rate_raw", "latest_funding_rate_pct", "funding_rate_bucket",
]
for f in NEW_FIELDS:
    check_field(f, null_ok=True)

# 2. oi_regime_label: never null, valid values only
VALID_REGIME = {
    "OI_EXPANSION_PRICE_DOWN",
    "OI_CONTRACTION_AFTER_DUMP",
    "OI_STABLE_PRICE_FAILS",
    "OI_UNCLEAR_OR_MISSING",
}
if "oi_regime_label" in df.columns:
    n_null = df["oi_regime_label"].isin(["", "None", "nan"]).sum() + df["oi_regime_label"].isnull().sum()
    if n_null > 0:
        errors.append(f"FAIL: oi_regime_label has {n_null} nulls — must never be null")
    invalid = (~df["oi_regime_label"].isin(VALID_REGIME)).sum()
    if invalid > 0:
        vals = df["oi_regime_label"].unique().tolist()
        errors.append(f"FAIL: {invalid} invalid oi_regime_label values: {vals}")
    else:
        dist = df["oi_regime_label"].value_counts().to_dict()
        print(f"PASS: oi_regime_label distribution: {dist}")

# 3. oi_data_unavailable: never null
if "oi_data_unavailable" in df.columns:
    n_null = df["oi_data_unavailable"].isin(["", "None", "nan"]).sum() + df["oi_data_unavailable"].isnull().sum()
    if n_null > 0:
        errors.append(f"FAIL: oi_data_unavailable has {n_null} nulls — must never be null")
    else:
        n_unavail = (df["oi_data_unavailable"].astype(str).str.lower() == "true").sum()
        print(f"PASS: oi_data_unavailable — {n_unavail} cases unavailable (expected 0 for recent data)")

# 4. oi_context_note: never null (empty string ok)
if "oi_context_note" in df.columns:
    n_null = df["oi_context_note"].isnull().sum()
    if n_null > 0:
        errors.append(f"FAIL: oi_context_note has {n_null} nulls — should be empty string not null")
    else:
        print(f"PASS: oi_context_note no nulls")

# 5. oi_change_1h_pct != oi_delta_3bar_pct (must be different computations)
if "oi_change_1h_pct" in df.columns and "oi_delta_3bar_pct" in df.columns:
    df_1h = pd.to_numeric(df["oi_change_1h_pct"], errors="coerce")
    df_3b = pd.to_numeric(df["oi_delta_3bar_pct"], errors="coerce")
    mask = df_1h.notna() & df_3b.notna()
    df_both_count = mask.sum()
    if df_both_count > 0:
        identical = (df_1h[mask] == df_3b[mask]).sum()
        if identical == df_both_count:
            errors.append(
                "FAIL: oi_change_1h_pct is identical to oi_delta_3bar_pct for all cases "
                "— likely using wrong field or wrong computation"
            )
        else:
            print(f"PASS: oi_change_1h_pct differs from oi_delta_3bar_pct "
                  f"({identical}/{df_both_count} same)")

# 6. funding_rate_bucket valid values when not null
VALID_BUCKETS = {"negative", "neutral", "slightly_long", "crowded_long"}
if "funding_rate_bucket" in df.columns:
    df_nn = df[df["funding_rate_bucket"].notna() & (df["funding_rate_bucket"] != "")]
    if len(df_nn) > 0:
        invalid = (~df_nn["funding_rate_bucket"].isin(VALID_BUCKETS)).sum()
        if invalid > 0:
            errors.append(
                f"FAIL: {invalid} invalid funding_rate_bucket values: "
                f"{df_nn['funding_rate_bucket'].unique().tolist()}"
            )
        else:
            dist = df_nn["funding_rate_bucket"].value_counts().to_dict()
            print(f"PASS: funding_rate_bucket distribution: {dist}")
    else:
        print("WARN: funding_rate_bucket all null — check funding fetch")

# 7. latest_funding_rate_pct == latest_funding_rate_raw × 100
if "latest_funding_rate_raw" in df.columns and "latest_funding_rate_pct" in df.columns:
    raw_num = pd.to_numeric(df["latest_funding_rate_raw"], errors="coerce")
    pct_num = pd.to_numeric(df["latest_funding_rate_pct"], errors="coerce")
    mask = raw_num.notna() & pct_num.notna()
    df_both_count = mask.sum()
    if df_both_count > 0:
        expected_pct = (raw_num[mask] * 100).round(6)
        actual_pct   = pct_num[mask].round(6)
        mismatch = (abs(expected_pct - actual_pct) > 1e-8).sum()
        if mismatch > 0:
            errors.append(f"FAIL: {mismatch} cases have latest_funding_rate_pct != raw × 100")
        else:
            print(f"PASS: latest_funding_rate_pct == raw × 100 for all {df_both_count} non-null cases")

# 8. funding bucket consistent with raw threshold
if "latest_funding_rate_raw" in df.columns and "funding_rate_bucket" in df.columns:
    raw_num = pd.to_numeric(df["latest_funding_rate_raw"], errors="coerce")
    mask = raw_num.notna() & df["funding_rate_bucket"].notna() & (df["funding_rate_bucket"] != "")
    bucket_errors = 0
    for idx in df[mask].index:
        raw = raw_num[idx]
        bucket = df.at[idx, "funding_rate_bucket"]
        expected = (
            "negative"      if raw < -0.0001 else
            "neutral"       if raw <= 0.0001 else
            "slightly_long" if raw <= 0.0005 else
            "crowded_long"
        )
        if bucket != expected:
            bucket_errors += 1
    if bucket_errors > 0:
        errors.append(
            f"FAIL: {bucket_errors} cases have funding_rate_bucket inconsistent "
            f"with latest_funding_rate_raw thresholds"
        )
    else:
        print(f"PASS: funding_rate_bucket consistent with raw for all {mask.sum()} non-null cases")

# 9. oi_data_unavailable=True → oi_change_1h_pct must be null
if "oi_data_unavailable" in df.columns and "oi_change_1h_pct" in df.columns:
    df_unavail = df[df["oi_data_unavailable"].astype(str).str.lower() == "true"]
    if len(df_unavail) > 0:
        df_1h_unavail = pd.to_numeric(df_unavail["oi_change_1h_pct"], errors="coerce")
        bad = df_1h_unavail.notna().sum()
        if bad > 0:
            errors.append(
                f"FAIL: {bad} cases have oi_data_unavailable=True "
                f"but non-null oi_change_1h_pct"
            )
        else:
            print(f"PASS: oi_change_1h_pct null when oi_data_unavailable=True")

# 10. Population check on oi_change_1h_pct
if "oi_change_1h_pct" in df.columns:
    df_1h = pd.to_numeric(df["oi_change_1h_pct"], errors="coerce")
    n_populated = df_1h.notna().sum()
    pct_populated = n_populated / len(df) * 100 if len(df) > 0 else 0
    if pct_populated < 50:
        print(f"WARN: oi_change_1h_pct only {pct_populated:.0f}% populated — "
              f"check OI history fetch for cases without p3_ts_ms")
    else:
        print(f"PASS: oi_change_1h_pct {pct_populated:.0f}% populated ({n_populated}/{len(df)})")

# 11. Existing Layer 7 fields unchanged
EXISTING_L7 = [
    "oi_delta_3bar_pct", "research_regime", "btc_24h_change_pct",
    "alt_breadth_pct", "flow_phase_code", "participation_pattern",
]
for f in EXISTING_L7:
    if f not in df.columns:
        errors.append(f"FAIL: existing Layer 7 field '{f}' disappeared")
    else:
        print(f"PASS: existing field '{f}' still present")

# 12. V4-1 through V4-4 fields backward compat
PREV_FIELDS = [
    "research_case_id", "selection_horizon",         # V4-1
    "p1_price", "anchor_reason_code",                # V4-2
    "exhaustion_strength_bucket",                    # V4-3
    "entry_feasible_flag",                           # V4-4
]
for f in PREV_FIELDS:
    if f not in df.columns:
        errors.append(f"FAIL: previous phase field '{f}' disappeared")
    else:
        print(f"PASS: {f} still present (backward compat)")

# Result
print()
if errors:
    print("=== VALIDATION FAILED ===")
    for e in errors:
        print(f"  {e}")
    sys.exit(1)
else:
    print("=== ALL V4-5 VALIDATION CHECKS PASSED ===")
    sys.exit(0)
