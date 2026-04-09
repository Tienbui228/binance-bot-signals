"""
classifier.py
Price structure classifiers for the R2-lite research pipeline.

Implements:
  - compression_score (Q3.1 formula from PHASE_R2_LITE_MEASUREMENT_SPEC_V1_1)
  - breakout_quality_score and breakout_quality_band (Q3.2 formula)
  - regime_label_candidate (Q4 simplified research regime)
  - btc_24h_band, alt_breadth_band, volatility_band (Board D helpers)
  - breakout_level (rolling max of highs for research)

All formulas are research-only. No live runtime code is imported or modified.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

EPS = 1e-9


# ---------------------------------------------------------------------------
# Q3.1 — Compression score
# ---------------------------------------------------------------------------

def compute_compression_score(df: pd.DataFrame,
                               recent_bars: int = 6,
                               prev_bars: int = 12) -> pd.Series:
    """
    Measures pre-break range compression.

    Notation (at bar t):
      range = high - low
      median_range_last6   = median(range[t-5:t+1])   (last 6 bars incl. current)
      median_range_prev12  = median(range[t-17:t-6])   (12 bars before the last 6)
      close_span_last6     = max(close[t-5:t+1]) - min(close[t-5:t+1])
      mean_wick_sum_last6  = mean(upper_wick + lower_wick over last 6 bars)

    Sub-scores:
      range_contract  = 1 - clip(median_range_last6 / max(median_range_prev12, EPS), 0, 1.5) / 1.5
      close_cluster   = 1 - clip(close_span_last6 / max(median_range_prev12, EPS), 0, 1.5) / 1.5
      wick_cleanliness = 1 - clip(mean_wick_sum_last6, 0, 1)

    Final (weights: 0.45 / 0.35 / 0.20):
      compression_score = clip(0.45*range_contract + 0.35*close_cluster + 0.20*wick_cleanliness, 0, 1)
    """
    df = df.copy()
    high = df["high"]
    low = df["low"]
    close = df["close"]

    bar_range = high - low

    # Median of last `recent_bars` including current
    median_recent = bar_range.rolling(recent_bars).median()

    # Median of the prior `prev_bars` window (before the recent window)
    # = rolling(prev_bars).median() then shifted by recent_bars
    median_prev = bar_range.rolling(prev_bars).median().shift(recent_bars)

    # Close span over last recent_bars
    close_max = close.rolling(recent_bars).max()
    close_min = close.rolling(recent_bars).min()
    close_span = close_max - close_min

    # Wick ratios
    total_range = (bar_range).clip(lower=EPS)
    body_high = pd.concat([df["open"], close], axis=1).max(axis=1)
    body_low = pd.concat([df["open"], close], axis=1).min(axis=1)
    upper_wick = (high - body_high) / total_range
    lower_wick = (body_low - low) / total_range
    wick_sum = (upper_wick + lower_wick).clip(0, 1)
    mean_wick_sum = wick_sum.rolling(recent_bars).mean()

    # Sub-scores
    prev_floor = median_prev.clip(lower=EPS)
    range_contract = 1 - (median_recent / prev_floor).clip(0, 1.5) / 1.5
    close_cluster = 1 - (close_span / prev_floor).clip(0, 1.5) / 1.5
    wick_cleanliness = 1 - mean_wick_sum.clip(0, 1)

    score = (0.45 * range_contract
             + 0.35 * close_cluster
             + 0.20 * wick_cleanliness).clip(0, 1)

    return score


# ---------------------------------------------------------------------------
# Breakout level: rolling max of highs (research proxy for resistance)
# ---------------------------------------------------------------------------

def compute_breakout_level(df: pd.DataFrame,
                            lookback: int = 12) -> pd.Series:
    """
    breakout_level[t] = max(high[t-lookback:t])
    Uses shift(1) to exclude the current bar -> strictly past data, no lookahead.

    For a LONG setup:
      close_t > breakout_level[t] implies a breakout is occurring.
    """
    return df["high"].rolling(lookback).max().shift(1)


# ---------------------------------------------------------------------------
# Price structure features (per bar)
# ---------------------------------------------------------------------------

def compute_price_structure_features(df: pd.DataFrame,
                                      atr_proxy_bars: int = 20) -> pd.DataFrame:
    """
    Adds: body_ratio, upper_wick_ratio, lower_wick_ratio,
          close_location_in_range, range_expansion_ratio
    """
    df = df.copy()
    high = df["high"]
    low = df["low"]
    close = df["close"]
    open_ = df["open"]

    total_range = (high - low).clip(lower=EPS)
    body = (close - open_).abs()
    body_high = pd.concat([open_, close], axis=1).max(axis=1)
    body_low = pd.concat([open_, close], axis=1).min(axis=1)

    df["body_ratio"] = (body / total_range).clip(0, 1)
    df["upper_wick_ratio"] = ((high - body_high) / total_range).clip(0, 1)
    df["lower_wick_ratio"] = ((body_low - low) / total_range).clip(0, 1)
    df["close_location_in_range"] = ((close - low) / total_range).clip(0, 1)

    # Range expansion vs ATR proxy (median of past atr_proxy_bars ranges)
    atr_proxy = (high - low).rolling(atr_proxy_bars).median().shift(1).clip(lower=EPS)
    df["range_expansion_ratio"] = ((high - low) / atr_proxy).clip(0, 5)
    df["atr_proxy"] = atr_proxy

    return df


# ---------------------------------------------------------------------------
# Q3.2 — Breakout quality score and band
# ---------------------------------------------------------------------------

def compute_breakout_quality(df: pd.DataFrame) -> pd.DataFrame:
    """
    Requires columns already computed:
      breakout_level, body_ratio, upper_wick_ratio, volume_ratio_3bar,
      oi_delta_3bar_pct, atr_proxy

    Formula:
      break_distance = close_t - breakout_level
      break_strength = clip( (break_distance / max(atr_proxy, EPS)) / 1.5, 0, 1 )
      body_quality   = clip(body_ratio, 0, 1)
      wick_clean     = 1 - clip(upper_wick_ratio, 0, 1)
      volume_support = clip(volume_ratio_3bar / 2.0, 0, 1)
      oi_support     = clip(max(oi_delta_3bar_pct, 0) / 4.0, 0, 1)

      score = clip(
        0.30*break_strength + 0.20*body_quality + 0.15*wick_clean
        + 0.20*volume_support + 0.15*oi_support, 0, 1)

    Band mapping:
      >= 0.75 -> strong
      >= 0.60 -> clean
      >= 0.45 -> weak
      else    -> poor
    """
    df = df.copy()

    break_distance = (df["close"] - df["breakout_level"]).clip(lower=0)
    break_strength = (break_distance / df["atr_proxy"].clip(lower=EPS) / 1.5).clip(0, 1)
    body_quality = df["body_ratio"].clip(0, 1)
    wick_clean = (1 - df["upper_wick_ratio"].clip(0, 1))
    volume_support = (df["volume_ratio_3bar"] / 2.0).clip(0, 1)
    oi_support = (df["oi_delta_3bar_pct"].fillna(0).clip(lower=0) / 4.0).clip(0, 1)

    score = (0.30 * break_strength
             + 0.20 * body_quality
             + 0.15 * wick_clean
             + 0.20 * volume_support
             + 0.15 * oi_support).clip(0, 1)

    df["breakout_quality_score"] = score
    df["breakout_quality_band"] = pd.cut(
        score,
        bins=[-0.001, 0.45, 0.60, 0.75, 1.001],
        labels=["poor", "weak", "clean", "strong"],
    ).astype(str)

    df["distance_from_breakout_pct"] = (
        (df["close"] - df["breakout_level"]) / df["breakout_level"].clip(lower=EPS) * 100
    )

    return df


# ---------------------------------------------------------------------------
# Q4 — Research-only regime label candidate
# ---------------------------------------------------------------------------

def classify_regime_label_candidate(btc_24h_pct: float,
                                     alt_breadth_pct: float) -> str:
    """
    Simplified research-only regime. Does NOT use live classifier.

    if btc_24h_pct >= 1.5 and alt_breadth_pct >= 55 -> trend_continuation_friendly
    elif btc_24h_pct <= -1.5 and alt_breadth_pct <= 35 -> broad_weakness_sell_pressure
    else -> unclear_mixed
    """
    if btc_24h_pct >= 1.5 and alt_breadth_pct >= 55.0:
        return "trend_continuation_friendly"
    if btc_24h_pct <= -1.5 and alt_breadth_pct <= 35.0:
        return "broad_weakness_sell_pressure"
    return "unclear_mixed"


# ---------------------------------------------------------------------------
# Band classifiers (Board D slices)
# ---------------------------------------------------------------------------

def classify_btc_24h_band(pct: float) -> str:
    """Thresholds from research_config.yaml bands.btc_24h."""
    if pct >= 3.0:
        return "strong_bull"
    if pct >= 0.0:
        return "mild_bull"
    if pct >= -3.0:
        return "mild_bear"
    return "strong_bear"


def classify_alt_breadth_band(pct: float) -> str:
    """Thresholds from research_config.yaml bands.alt_breadth."""
    if pct >= 70.0:
        return "strong"
    if pct >= 50.0:
        return "moderate"
    if pct >= 30.0:
        return "weak"
    return "very_weak"


def classify_volatility_band(vol_5m_std_pct: float) -> str:
    """
    vol_5m_std_pct = std of 5m close-to-close return % over past 24h (288 bars).
    Thresholds from research_config.yaml bands.volatility.
    """
    if vol_5m_std_pct >= 0.5:
        return "high"
    if vol_5m_std_pct >= 0.25:
        return "medium"
    return "low"


def compute_symbol_volatility_band(df: pd.DataFrame,
                                    lookback: int = 288) -> pd.Series:
    """Per-bar rolling volatility band based on 24h (288 x 5m) std of returns."""
    ret_pct = df["close"].pct_change() * 100
    vol_std = ret_pct.rolling(lookback).std().shift(1)
    return vol_std.apply(lambda x: classify_volatility_band(x) if pd.notna(x) else "unknown")


# ---------------------------------------------------------------------------
# Cross-symbol market context (computed once per run, joined to events)
# ---------------------------------------------------------------------------

def compute_alt_breadth_series(all_klines: dict[str, pd.DataFrame]) -> pd.Series:
    """
    At each 5m timestamp, compute alt_breadth_pct =
      % of universe symbols with positive 24h return (close[t] / close[t-288] - 1 > 0).

    Returns a pd.Series indexed by open_time (ms).
    """
    LOOKBACK = 288  # 24h in 5m bars

    per_symbol: list[pd.Series] = []
    for sym, kdf in all_klines.items():
        if kdf.empty or "close" not in kdf.columns:
            continue
        kdf = kdf.set_index("open_time").sort_index()
        ret_24h = kdf["close"] / kdf["close"].shift(LOOKBACK) - 1
        positive_flag = (ret_24h > 0).astype(float)
        per_symbol.append(positive_flag)

    if not per_symbol:
        return pd.Series(dtype=float)

    combined = pd.concat(per_symbol, axis=1)
    breadth_pct = combined.mean(axis=1) * 100
    return breadth_pct


def compute_btc_24h_series(btc_klines: pd.DataFrame) -> pd.Series:
    """
    BTC 24h change per 5m bar.
    Returns Series indexed by open_time.
    """
    LOOKBACK = 288
    if btc_klines.empty:
        return pd.Series(dtype=float)
    kdf = btc_klines.set_index("open_time").sort_index()
    return (kdf["close"] / kdf["close"].shift(LOOKBACK) - 1) * 100
