"""
rule_engine.py
Evaluates candidate and baseline scan rules against a feature-enriched DataFrame.

Rule config is read entirely from research_config.yaml (rule_v1 block).
No thresholds are hard-coded here.

Returns a boolean pandas Series (signal mask) — one True/False per bar.
Offline research only.
"""
from __future__ import annotations

import pandas as pd


# ---------------------------------------------------------------------------
# Candidate rule V1
# ---------------------------------------------------------------------------

def evaluate_candidate_rule(df: pd.DataFrame, rule_cfg: dict) -> pd.Series:
    """
    Evaluates the V1 candidate LONG rule against every bar in df.

    Required df columns (all computed upstream by classifier + proxy_features):
      compression_score, oi_delta_3bar_pct, top_vs_global_divergence,
      taker_imbalance, breakout_quality_band, volume_ratio_3bar,
      basis_rate, eligible_for_measurement_YN

    rule_cfg keys (from research_config.yaml -> research_measurement.rule_v1):
      compression_score_min
      oi_delta_3bar_pct_min
      top_vs_global_divergence_min
      taker_imbalance_min
      breakout_quality_band_allowed  (list of strings)
      volume_ratio_3bar_min
      basis_rate_max_for_long

    Returns boolean Series aligned to df.index.
    """
    mask = pd.Series(True, index=df.index)

    # Only fire on rows eligible for measurement
    eligible = df.get("eligible_for_measurement_YN", pd.Series("N", index=df.index))
    mask &= eligible == "Y"

    # 1. Compression: pre-break nesting must be tight
    comp_min = rule_cfg.get("compression_score_min", 0.65)
    mask &= df["compression_score"].fillna(0) >= comp_min

    # 2. OI expansion: open interest growing into the break
    oi_min = rule_cfg.get("oi_delta_3bar_pct_min", 2.0)
    mask &= df["oi_delta_3bar_pct"].fillna(0) >= oi_min

    # 3. Top-cohort divergence: top participants leading
    tvg_min = rule_cfg.get("top_vs_global_divergence_min", 0.50)
    mask &= df["top_vs_global_divergence"].fillna(0) >= tvg_min

    # 4. Taker imbalance: buyers dominate order flow
    taker_min = rule_cfg.get("taker_imbalance_min", 0.08)
    mask &= df["taker_imbalance"].fillna(0) >= taker_min

    # 5. Breakout quality: clean or strong only
    allowed_bands = set(rule_cfg.get("breakout_quality_band_allowed", ["clean", "strong"]))
    mask &= df["breakout_quality_band"].isin(allowed_bands)

    # 6. Volume participation
    vol_min = rule_cfg.get("volume_ratio_3bar_min", 1.20)
    mask &= df["volume_ratio_3bar"].fillna(0) >= vol_min

    # 7. Basis sanity: long setups should not fire into extreme contango
    basis_max = rule_cfg.get("basis_rate_max_for_long", 0.020)
    if "basis_rate" in df.columns:
        mask &= df["basis_rate"].fillna(0) <= basis_max

    return mask


# ---------------------------------------------------------------------------
# Baseline rules (Section 17)
# ---------------------------------------------------------------------------

def evaluate_baseline_rule(df: pd.DataFrame, baseline_cfg: dict) -> pd.Series:
    """
    Evaluate a simplified baseline rule from the baselines list in research_config.yaml.
    Only the keys present in baseline_cfg are applied.
    Ineligible rows are always excluded.
    """
    mask = pd.Series(True, index=df.index)

    eligible = df.get("eligible_for_measurement_YN", pd.Series("N", index=df.index))
    mask &= eligible == "Y"

    if "compression_score_min" in baseline_cfg:
        mask &= df["compression_score"].fillna(0) >= baseline_cfg["compression_score_min"]

    if "oi_delta_3bar_pct_min" in baseline_cfg:
        mask &= df["oi_delta_3bar_pct"].fillna(0) >= baseline_cfg["oi_delta_3bar_pct_min"]

    if "top_vs_global_divergence_min" in baseline_cfg:
        mask &= df["top_vs_global_divergence"].fillna(0) >= baseline_cfg["top_vs_global_divergence_min"]

    if "taker_imbalance_min" in baseline_cfg:
        mask &= df["taker_imbalance"].fillna(0) >= baseline_cfg["taker_imbalance_min"]

    if "breakout_quality_band_allowed" in baseline_cfg:
        allowed = set(baseline_cfg["breakout_quality_band_allowed"])
        mask &= df["breakout_quality_band"].isin(allowed)

    if "volume_ratio_3bar_min" in baseline_cfg:
        mask &= df["volume_ratio_3bar"].fillna(0) >= baseline_cfg["volume_ratio_3bar_min"]

    if "basis_rate_max_for_long" in baseline_cfg and "basis_rate" in df.columns:
        mask &= df["basis_rate"].fillna(0) <= baseline_cfg["basis_rate_max_for_long"]

    return mask
