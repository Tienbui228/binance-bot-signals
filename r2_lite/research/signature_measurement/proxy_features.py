"""
proxy_features.py
Implements Section 9 proxy formulas from PHASE_R2_LITE_MEASUREMENT_SPEC_V1_1.

All z-scores use strictly past data (no lookahead):
  z(x_t) = clip( (x_t - mean(x[t-N:t-1])) / max(std(x[t-N:t-1]), EPS), -Z_CLIP, Z_CLIP )

Implemented via rolling + shift(1) in pandas.

Offline research only — not imported by live runtime.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

# Section 9.1.1 constants
EPS = 1e-9
Z_CLIP = 3.0
ROLL_Z_N = 96        # 96 x 5m = 8h
ROLL_BASELINE_N = 288  # 288 x 5m = 24h


# ---------------------------------------------------------------------------
# Rolling z-score (no lookahead)
# ---------------------------------------------------------------------------

def rolling_zscore(series: pd.Series, n: int = ROLL_Z_N,
                   clip_val: float = Z_CLIP) -> pd.Series:
    """
    Rolling z-score using strictly past data (shift(1) ensures no lookahead).
    Returns NaN where < n historical rows exist.
    """
    past_mean = series.rolling(n).mean().shift(1)
    past_std = series.rolling(n).std().shift(1)
    z = (series - past_mean) / (past_std.clip(lower=EPS))
    return z.clip(-clip_val, clip_val)


# ---------------------------------------------------------------------------
# Section 9.1.3 — OI and price delta features
# ---------------------------------------------------------------------------

def compute_oi_price_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds:
      price_return_1bar_pct, price_return_3bar_pct
      oi_delta_1bar_pct, oi_delta_3bar_pct, oi_delta_6bar_pct
      oi_price_alignment_code
    """
    df = df.copy()
    close = df["close"]
    oi = df.get("oi_value", pd.Series(dtype=float))

    df["price_return_1bar_pct"] = 100 * (close / close.shift(1).clip(lower=EPS) - 1)
    df["price_return_3bar_pct"] = 100 * (close / close.shift(3).clip(lower=EPS) - 1)

    if not oi.isna().all():
        df["oi_delta_1bar_pct"] = 100 * (oi / oi.shift(1).clip(lower=EPS) - 1)
        df["oi_delta_3bar_pct"] = 100 * (oi / oi.shift(3).clip(lower=EPS) - 1)
        df["oi_delta_6bar_pct"] = 100 * (oi / oi.shift(6).clip(lower=EPS) - 1)
    else:
        df["oi_delta_1bar_pct"] = float("nan")
        df["oi_delta_3bar_pct"] = float("nan")
        df["oi_delta_6bar_pct"] = float("nan")

    # Section 9.1.6 — OI-price alignment code
    p3 = df["price_return_3bar_pct"]
    o3 = df["oi_delta_3bar_pct"]

    conditions = [
        (p3 > 0) & (o3 > 0),
        (p3 > 0) & (o3 < 0),
        (p3 < 0) & (o3 > 0),
        (p3 < 0) & (o3 < 0),
    ]
    choices = ["PRICE_UP_OI_UP", "PRICE_UP_OI_DOWN", "PRICE_DOWN_OI_UP", "PRICE_DOWN_OI_DOWN"]
    df["oi_price_alignment_code"] = np.select(conditions, choices, default="MIXED_FLAT")

    return df


# ---------------------------------------------------------------------------
# Section 9.1.4 & 9.1.5 — Taker + imbalance features
# ---------------------------------------------------------------------------

def compute_raw_imbalance_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds raw imbalance fields from percentage columns and taker volumes.
    Taker data sourced from klines fields taker_buy_base_vol / taker_sell_base_vol.

    Section 9.1.4:  taker_imbalance = (buy - sell) / max(buy + sell, EPS)
    Section 9.1.5:  top_acc_imbalance = top_long_account_pct - top_short_account_pct
                    top_pos_imbalance = top_long_position_pct - top_short_position_pct
                    global_acc_imbalance = global_long_account_pct - global_short_account_pct
                    global_acc_imbalance_delta = global_acc_imbalance_t - global_acc_imbalance_t-1
                    top_vs_global_divergence = z(top_pos_imbalance) - z(global_acc_imbalance)
    """
    df = df.copy()

    # Taker imbalance
    buy = df.get("taker_buy_base_vol", pd.Series(dtype=float))
    sell = df.get("taker_sell_base_vol", pd.Series(dtype=float))
    if not buy.isna().all():
        total = (buy + sell).clip(lower=EPS)
        df["taker_imbalance"] = (buy - sell) / total
    else:
        df["taker_imbalance"] = float("nan")

    # Top / global imbalances
    for needed, long_col, short_col, out_col in [
        ("top_long_account_pct", "top_long_account_pct", "top_short_account_pct", "top_acc_imbalance"),
        ("top_long_position_pct", "top_long_position_pct", "top_short_position_pct", "top_pos_imbalance"),
        ("global_long_account_pct", "global_long_account_pct", "global_short_account_pct", "global_acc_imbalance"),
    ]:
        if needed in df.columns and not df[needed].isna().all():
            df[out_col] = df[long_col] - df[short_col]
        else:
            df[out_col] = float("nan")

    # Global delta
    if "global_acc_imbalance" in df.columns:
        df["global_acc_imbalance_delta"] = df["global_acc_imbalance"] - df["global_acc_imbalance"].shift(1)
    else:
        df["global_acc_imbalance_delta"] = float("nan")

    return df


# ---------------------------------------------------------------------------
# Volume features
# ---------------------------------------------------------------------------

def compute_volume_features(df: pd.DataFrame,
                              roll_baseline_n: int = ROLL_BASELINE_N) -> pd.DataFrame:
    """
    Adds:
      volume_ratio_1bar  : quote_volume[t] / mean(quote_volume[t-7:t-1])
      volume_ratio_3bar  : mean(quote_volume[t-2:t]) / mean(quote_volume[t-8:t-3])
      volume_ratio_vs_baseline : quote_volume[t] / mean(quote_volume[t-288:t-1])
    """
    df = df.copy()
    qv = df["quote_volume"]

    # 1-bar: current vs prior 6-bar mean (same as market_math.py convention)
    baseline_1 = qv.rolling(6).mean().shift(1)
    df["volume_ratio_1bar"] = qv / baseline_1.clip(lower=EPS)

    # 3-bar: mean of last 3 / mean of prior 6
    recent_3 = qv.rolling(3).mean()
    prior_6 = qv.rolling(6).mean().shift(3)
    df["volume_ratio_3bar"] = recent_3 / prior_6.clip(lower=EPS)

    # vs baseline: current / 24h mean
    baseline_long = qv.rolling(roll_baseline_n).mean().shift(1)
    df["volume_ratio_vs_baseline"] = qv / baseline_long.clip(lower=EPS)

    return df


# ---------------------------------------------------------------------------
# Section 9.2 — large_participant_proxy
# ---------------------------------------------------------------------------

def compute_large_participant_proxy(df: pd.DataFrame,
                                     roll_n: int = ROLL_Z_N) -> pd.Series:
    """
    large_participant_proxy_base =
      0.45 * z(top_pos_imbalance)
      + 0.20 * z(top_acc_imbalance)
      + 0.20 * z(oi_delta_3bar_pct)
      + 0.15 * z(taker_imbalance)

    alignment_bonus:
      +0.20 if PRICE_UP_OI_UP
      -0.20 if PRICE_DOWN_OI_UP
       0.00 otherwise

    Final: clip(base + alignment_bonus, -3, 3)
    """
    z_top_pos = rolling_zscore(df["top_pos_imbalance"].fillna(0), roll_n)
    z_top_acc = rolling_zscore(df["top_acc_imbalance"].fillna(0), roll_n)
    z_oi3 = rolling_zscore(df["oi_delta_3bar_pct"].fillna(0), roll_n)
    z_taker = rolling_zscore(df["taker_imbalance"].fillna(0), roll_n)

    base = (0.45 * z_top_pos
            + 0.20 * z_top_acc
            + 0.20 * z_oi3
            + 0.15 * z_taker)

    alignment = df["oi_price_alignment_code"].map({
        "PRICE_UP_OI_UP": 0.20,
        "PRICE_DOWN_OI_UP": -0.20,
    }).fillna(0.0)

    return (base + alignment).clip(-3.0, 3.0)


# ---------------------------------------------------------------------------
# Section 9.3 — crowd_participation_proxy
# ---------------------------------------------------------------------------

def compute_crowd_participation_proxy(df: pd.DataFrame,
                                       roll_n: int = ROLL_Z_N) -> pd.Series:
    """
    crowd_participation_proxy =
      0.45 * z(global_acc_imbalance)
      + 0.20 * z(global_acc_imbalance_delta)
      + 0.20 * z(taker_imbalance)
      + 0.15 * z(basis_rate)

    clip(-3, 3)
    """
    z_global = rolling_zscore(df["global_acc_imbalance"].fillna(0), roll_n)
    z_global_d = rolling_zscore(df["global_acc_imbalance_delta"].fillna(0), roll_n)
    z_taker = rolling_zscore(df["taker_imbalance"].fillna(0), roll_n)
    basis = df.get("basis_rate", pd.Series(0.0, index=df.index)).fillna(0)
    z_basis = rolling_zscore(basis, roll_n)

    result = (0.45 * z_global
              + 0.20 * z_global_d
              + 0.20 * z_taker
              + 0.15 * z_basis)
    return result.clip(-3.0, 3.0)


# ---------------------------------------------------------------------------
# Section 9.4 — crowd_overheat_proxy
# ---------------------------------------------------------------------------

def compute_crowd_overheat_proxy(df: pd.DataFrame,
                                  roll_n: int = ROLL_Z_N) -> pd.Series:
    """
    crowd_overheat_proxy =
      0.55 * max(0, z(global_acc_imbalance))
      + 0.25 * max(0, z(basis_rate))
      + 0.20 * max(0, z(taker_imbalance))

    clip(0, 3)
    """
    z_global = rolling_zscore(df["global_acc_imbalance"].fillna(0), roll_n)
    basis = df.get("basis_rate", pd.Series(0.0, index=df.index)).fillna(0)
    z_basis = rolling_zscore(basis, roll_n)
    z_taker = rolling_zscore(df["taker_imbalance"].fillna(0), roll_n)

    result = (0.55 * z_global.clip(lower=0)
              + 0.25 * z_basis.clip(lower=0)
              + 0.20 * z_taker.clip(lower=0))
    return result.clip(0.0, 3.0)


# ---------------------------------------------------------------------------
# Section 9.5 — flow_composite_signal
# ---------------------------------------------------------------------------

def compute_flow_composite_signal(df: pd.DataFrame,
                                   roll_n: int = ROLL_Z_N) -> pd.Series:
    """
    flow_composite_signal =
      0.40 * large_participant_proxy
      + 0.20 * z(oi_delta_3bar_pct)
      + 0.15 * z(taker_imbalance)
      + 0.10 * breakout_quality_score_numeric
      - 0.15 * crowd_overheat_proxy

    breakout_quality_score_numeric mapping (per spec Section 9.5):
      poor=-1.0, mixed=0.0, good=1.0, strong=1.5   -> clipped to [-2, 2]
    """
    lp = df["large_participant_proxy"]
    z_oi3 = rolling_zscore(df["oi_delta_3bar_pct"].fillna(0), roll_n)
    z_taker = rolling_zscore(df["taker_imbalance"].fillna(0), roll_n)
    overheat = df["crowd_overheat_proxy"]

    # Breakout quality numeric conversion
    bq_map = {"poor": -1.0, "weak": 0.0, "clean": 1.0, "strong": 1.5}
    bq_numeric = df.get("breakout_quality_band", pd.Series(dtype=str)).map(bq_map).fillna(0.0).clip(-2, 2)

    result = (0.40 * lp
              + 0.20 * z_oi3
              + 0.15 * z_taker
              + 0.10 * bq_numeric
              - 0.15 * overheat)
    return result.clip(-3.0, 3.0)


# ---------------------------------------------------------------------------
# Section 9.6 — flow_phase_code
# ---------------------------------------------------------------------------

def classify_flow_phase(df: pd.DataFrame) -> pd.Series:
    """
    Classify each row into a flow_phase_code per Section 9.6 mapping logic.
    Uses: large_participant_proxy, crowd_participation_proxy, crowd_overheat_proxy,
          top_vs_global_divergence, flow_composite_signal
    """
    LP_HIGH = 1.0
    LP_MID = 0.5
    CP_MID = 0.5
    CP_HIGH = 1.2
    OVERHEAT_HIGH = 1.5

    lp = df["large_participant_proxy"]
    cp = df["crowd_participation_proxy"]
    oh = df["crowd_overheat_proxy"]
    tvg = df["top_vs_global_divergence"]
    fcs = df["flow_composite_signal"]

    phase = pd.Series("UNCLEAR_FLOW", index=df.index)

    # Apply in priority order (first match wins)
    m8 = (lp < 0.25) & (cp < 0.25) & (df["oi_delta_3bar_pct"].fillna(0) < 0.25)
    m7 = tvg.abs() >= 1.25
    m6 = (lp < 0) & (cp >= CP_MID)
    m5 = (cp >= CP_HIGH) & (lp < LP_MID)
    m4 = oh >= OVERHEAT_HIGH
    m3 = (lp >= LP_MID) & (cp >= CP_MID) & (oh < OVERHEAT_HIGH)
    m2 = (lp >= LP_HIGH) & (cp >= 0.25) & (cp < CP_HIGH)
    m1 = (lp >= LP_HIGH) & (cp < 0.25)

    # Apply in reverse priority (later assignments overwrite earlier)
    phase[m8] = "LOW_PARTICIPATION_BREAKOUT"
    phase[m7 & (fcs <= 0.25)] = "FLOW_DIVERGENCE_WARNING"
    phase[m6] = "LARGE_FADE_CROWD_ENTER"
    phase[m5] = "CROWD_CHASE_LATE"
    phase[m4] = "CROWD_OVERHEATED"
    phase[m3] = "BOTH_ALIGNED_HEALTHY"
    phase[m2] = "LARGE_LEAD_CROWD_EARLY"
    phase[m1] = "LARGE_LEAD_CROWD_ABSENT"

    return phase


# ---------------------------------------------------------------------------
# Main entry point: enrich all proxy features for a symbol DataFrame
# ---------------------------------------------------------------------------

def enrich_proxy_features(df: pd.DataFrame,
                           roll_z_n: int = ROLL_Z_N,
                           roll_baseline_n: int = ROLL_BASELINE_N) -> pd.DataFrame:
    """
    Orchestrates all Section 9 feature computation on an aligned symbol DataFrame.
    Returns the DataFrame with all proxy feature columns added.
    Missing upstream data remains NaN; callers mark data_complete_YN accordingly.
    """
    df = compute_oi_price_features(df)
    df = compute_raw_imbalance_features(df)
    df = compute_volume_features(df, roll_baseline_n=roll_baseline_n)

    # z-scored divergence (needs top_pos_imbalance and global_acc_imbalance)
    z_tpi = rolling_zscore(df["top_pos_imbalance"].fillna(0), roll_z_n)
    z_gai = rolling_zscore(df["global_acc_imbalance"].fillna(0), roll_z_n)
    df["top_vs_global_divergence"] = z_tpi - z_gai

    df["large_participant_proxy"] = compute_large_participant_proxy(df, roll_z_n)
    df["crowd_participation_proxy"] = compute_crowd_participation_proxy(df, roll_z_n)
    df["crowd_overheat_proxy"] = compute_crowd_overheat_proxy(df, roll_z_n)

    # flow_composite requires breakout_quality_band to be present already
    # (computed in classifier.py before this step in the pipeline)
    df["flow_composite_signal"] = compute_flow_composite_signal(df, roll_z_n)
    df["flow_phase_code"] = classify_flow_phase(df)

    return df


# ---------------------------------------------------------------------------
# Data quality helper: identify which proxy fields are missing for each row
# ---------------------------------------------------------------------------

PROXY_REQUIRED_FIELDS = [
    "top_long_account_pct", "top_short_account_pct",
    "top_long_position_pct", "top_short_position_pct",
    "global_long_account_pct", "global_short_account_pct",
    "taker_buy_base_vol", "oi_value", "basis_rate",
]


def compute_data_quality_flags(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds data_complete_YN, missing_field_count, missing_field_list,
    eligible_for_measurement_YN, measurement_exclusion_reason.

    A row is ineligible if:
    - any required proxy source field is NaN (proxy_input_missing)
    - z-score lookback is not yet satisfied (insufficient_history)
    """
    df = df.copy()

    missing_fields = pd.Series([[] for _ in range(len(df))], index=df.index, dtype=object)
    for f in PROXY_REQUIRED_FIELDS:
        if f not in df.columns:
            for i in df.index:
                missing_fields[i] = missing_fields[i] + [f]
        else:
            nan_mask = df[f].isna()
            for i in df[nan_mask].index:
                missing_fields[i] = missing_fields[i] + [f]

    missing_count = missing_fields.apply(len)
    df["missing_field_count"] = missing_count
    df["missing_field_list"] = missing_fields.apply(lambda x: "|".join(x) if x else "")
    df["data_complete_YN"] = (missing_count == 0).map({True: "Y", False: "N"})

    # Ineligible if missing proxy fields
    proxy_missing = missing_count > 0
    # Ineligible if z-score NaN (insufficient history — need roll_z_n bars)
    z_nan = df.get("large_participant_proxy", pd.Series(dtype=float)).isna()

    df["eligible_for_measurement_YN"] = "Y"
    df.loc[proxy_missing, "eligible_for_measurement_YN"] = "N"
    df.loc[z_nan, "eligible_for_measurement_YN"] = "N"

    reason = pd.Series("", index=df.index)
    reason[proxy_missing] = "proxy_input_missing:" + df.loc[proxy_missing, "missing_field_list"]
    reason[z_nan & ~proxy_missing] = "insufficient_history_for_zscore"
    df["measurement_exclusion_reason"] = reason

    return df
