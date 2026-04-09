"""
event_builder.py
Builds candidate event rows from a signal mask and applies the 60-minute
first-fire cooldown deduplication (Section 7).

event_id format: {rule_family}_{rule_version}_{symbol}_{trigger_ts_ms}

Offline research only.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

import pandas as pd

from research.signature_measurement.contracts import F, MEASUREMENT_DETAIL_FIELDS


# ---------------------------------------------------------------------------
# Build raw candidate events from signal mask
# ---------------------------------------------------------------------------

def build_candidate_events(
    df: pd.DataFrame,
    symbol: str,
    signal_mask: pd.Series,
    rule_family: str,
    rule_version: str,
    measurement_version: str,
    universe_version: str,
    side: str = "LONG",
    timeframe: str = "5m",
) -> List[Dict[str, Any]]:
    """
    For each True row in signal_mask, extract one candidate event dict.
    All Section 8 fields that are present in df are captured.
    Missing fields are set to empty string (for CSV compatibility).

    Returns list of dicts in MEASUREMENT_DETAIL_FIELDS order.
    """
    fired = df[signal_mask].copy()
    events: List[Dict[str, Any]] = []

    for _, row in fired.iterrows():
        ts_ms = int(row["open_time"])
        dt_str = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
        event_id = f"{rule_family}_{rule_version}_{symbol}_{ts_ms}"

        ev: Dict[str, Any] = {
            # Identity
            F.EVENT_ID: event_id,
            F.SYMBOL: symbol,
            F.SIDE: side,
            F.RULE_FAMILY: rule_family,
            F.RULE_VERSION: rule_version,
            F.MEASUREMENT_VERSION: measurement_version,
            F.TRIGGER_TS_MS: ts_ms,
            F.TRIGGER_DT: dt_str,
            F.TRIGGER_TF: timeframe,
            F.PRICE_AT_TRIGGER: _fv(row, "close"),
            F.UNIVERSE_VERSION: universe_version,
            # Price structure
            F.BREAKOUT_LEVEL: _fv(row, "breakout_level"),
            F.DISTANCE_FROM_BREAKOUT_PCT: _fv(row, "distance_from_breakout_pct"),
            F.COMPRESSION_SCORE: _fv(row, "compression_score"),
            F.BODY_RATIO: _fv(row, "body_ratio"),
            F.UPPER_WICK_RATIO: _fv(row, "upper_wick_ratio"),
            F.LOWER_WICK_RATIO: _fv(row, "lower_wick_ratio"),
            F.CLOSE_LOCATION_IN_RANGE: _fv(row, "close_location_in_range"),
            F.RANGE_EXPANSION_RATIO: _fv(row, "range_expansion_ratio"),
            F.BREAKOUT_QUALITY_BAND: _sv(row, "breakout_quality_band"),
            F.BREAKOUT_QUALITY_SCORE: _fv(row, "breakout_quality_score"),
            # Volume
            F.VOLUME: _fv(row, "quote_volume"),
            F.VOLUME_RATIO_1BAR: _fv(row, "volume_ratio_1bar"),
            F.VOLUME_RATIO_3BAR: _fv(row, "volume_ratio_3bar"),
            F.VOLUME_RATIO_VS_BASELINE: _fv(row, "volume_ratio_vs_baseline"),
            # OI
            F.OI_VALUE: _fv(row, "oi_value"),
            F.OI_DELTA_1BAR_PCT: _fv(row, "oi_delta_1bar_pct"),
            F.OI_DELTA_3BAR_PCT: _fv(row, "oi_delta_3bar_pct"),
            F.OI_DELTA_6BAR_PCT: _fv(row, "oi_delta_6bar_pct"),
            F.OI_PRICE_ALIGNMENT_CODE: _sv(row, "oi_price_alignment_code"),
            # Flow proxies
            F.TOP_ACC_IMBALANCE: _fv(row, "top_acc_imbalance"),
            F.TOP_POS_IMBALANCE: _fv(row, "top_pos_imbalance"),
            F.GLOBAL_ACC_IMBALANCE: _fv(row, "global_acc_imbalance"),
            F.GLOBAL_ACC_IMBALANCE_DELTA: _fv(row, "global_acc_imbalance_delta"),
            F.TAKER_IMBALANCE: _fv(row, "taker_imbalance"),
            F.BASIS_RATE: _fv(row, "basis_rate"),
            F.TOP_VS_GLOBAL_DIVERGENCE: _fv(row, "top_vs_global_divergence"),
            F.LARGE_PARTICIPANT_PROXY: _fv(row, "large_participant_proxy"),
            F.CROWD_PARTICIPATION_PROXY: _fv(row, "crowd_participation_proxy"),
            F.CROWD_OVERHEAT_PROXY: _fv(row, "crowd_overheat_proxy"),
            F.FLOW_COMPOSITE_SIGNAL: _fv(row, "flow_composite_signal"),
            F.FLOW_PHASE_CODE: _sv(row, "flow_phase_code"),
            # Market context
            F.BTC_24H_CHANGE_PCT: _fv(row, "btc_24h_change_pct"),
            F.ALT_BREADTH_PCT: _fv(row, "alt_breadth_pct"),
            F.FUNDING_RATE: _fv(row, "funding_rate"),
            F.VOLATILITY_BAND: _sv(row, "volatility_band"),
            F.REGIME_LABEL_CANDIDATE: _sv(row, "regime_label_candidate"),
            F.BTC_24H_BAND: _sv(row, "btc_24h_band"),
            F.ALT_BREADTH_BAND: _sv(row, "alt_breadth_band"),
            # Data quality
            F.DATA_COMPLETE_YN: _sv(row, "data_complete_YN"),
            F.MISSING_FIELD_COUNT: _iv(row, "missing_field_count"),
            F.MISSING_FIELD_LIST: _sv(row, "missing_field_list"),
            F.ELIGIBLE_FOR_MEASUREMENT_YN: _sv(row, "eligible_for_measurement_YN"),
            F.MEASUREMENT_EXCLUSION_REASON: _sv(row, "measurement_exclusion_reason"),
            # Outcome fields — populated later by outcome_engine
            F.OUTCOME_30M_AVAILABLE_YN: "",
            F.FUTURE_30M_MAX_FAVOR_PCT: "",
            F.FUTURE_30M_MAX_ADVERSE_PCT: "",
            F.CLOSE_ABOVE_BREAKOUT_AFTER_30M_YN: "",
            F.OUTCOME_1H_AVAILABLE_YN: "",
            F.FUTURE_1H_MAX_FAVOR_PCT: "",
            F.FUTURE_1H_MAX_ADVERSE_PCT: "",
            F.CLOSE_ABOVE_BREAKOUT_AFTER_1H_YN: "",
            F.RECLAIM_BREAKOUT_1H_YN: "",
            F.OUTCOME_4H_AVAILABLE_YN: "",
            F.FUTURE_4H_MAX_FAVOR_PCT: "",
            F.FUTURE_4H_MAX_ADVERSE_PCT: "",
            F.RECLAIM_BREAKOUT_4H_YN: "",
            F.OUTCOME_NOT_AVAILABLE_REASON: "",
            F.TIME_TO_1PCT_FAVOR_MIN: "",
            F.TIME_TO_2PCT_FAVOR_MIN: "",
            F.TIME_TO_3PCT_FAVOR_MIN: "",
            F.TIME_TO_1PCT_ADVERSE_MIN: "",
            F.TIME_TO_2PCT_ADVERSE_MIN: "",
            F.PAYOFF_30M: "",
            F.PAYOFF_1H: "",
            F.PAYOFF_4H: "",
            F.MOVE_PERSISTENCE_CODE: "",
            F.OUTCOME_CLASS: "",
        }
        events.append(ev)

    return events


# ---------------------------------------------------------------------------
# Section 7.2 — 60-minute first-fire cooldown deduplication
# ---------------------------------------------------------------------------

def apply_first_fire_cooldown(
    events: List[Dict[str, Any]],
    cooldown_minutes: int = 60,
) -> Tuple[List[Dict[str, Any]], Dict]:
    """
    For the same symbol + side + rule_family:
    - keep the FIRST fire inside each cooldown window
    - drop all subsequent fires within cooldown_minutes of the last kept event

    Returns:
      kept_events: list of events with kept_yn=Y
      stats: dict with raw_fire_count, kept_fire_count, dropped_overlap_count
    """
    if not events:
        return [], {"raw_fire_count": 0, "kept_fire_count": 0, "dropped_overlap_count": 0}

    cooldown_ms = cooldown_minutes * 60 * 1000
    raw_count = len(events)

    # Sort by symbol, then by timestamp
    sorted_events = sorted(events, key=lambda e: (e[F.SYMBOL], int(e[F.TRIGGER_TS_MS])))

    kept: List[Dict] = []
    dropped = 0
    # last kept timestamp per (symbol, side, rule_family) key
    last_kept_ts: Dict[Tuple, int] = {}

    for ev in sorted_events:
        key = (ev[F.SYMBOL], ev[F.SIDE], ev[F.RULE_FAMILY])
        ts = int(ev[F.TRIGGER_TS_MS])
        last_ts = last_kept_ts.get(key, -1)

        if last_ts < 0 or (ts - last_ts) >= cooldown_ms:
            kept.append(ev)
            last_kept_ts[key] = ts
        else:
            dropped += 1

    stats = {
        "raw_fire_count": raw_count,
        "kept_fire_count": len(kept),
        "dropped_overlap_count": dropped,
    }
    return kept, stats


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _fv(row: pd.Series, col: str) -> Any:
    """Get float value from row, return empty string if missing or NaN."""
    val = row.get(col)
    if val is None:
        return ""
    try:
        import math
        if math.isnan(float(val)):
            return ""
        return round(float(val), 6)
    except (TypeError, ValueError):
        return ""


def _sv(row: pd.Series, col: str) -> str:
    """Get string value from row."""
    val = row.get(col)
    if val is None or (isinstance(val, float) and str(val) == "nan"):
        return ""
    return str(val)


def _iv(row: pd.Series, col: str) -> Any:
    """Get int value from row."""
    val = row.get(col)
    if val is None:
        return ""
    try:
        return int(val)
    except (TypeError, ValueError):
        return ""
