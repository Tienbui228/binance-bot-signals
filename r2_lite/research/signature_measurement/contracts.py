"""
contracts.py
Typed schemas and constants for the R2-lite signature measurement pipeline.
Offline research only — not imported by live runtime.

Field names match Section 8 of PHASE_R2_LITE_MEASUREMENT_SPEC_V1_1.
"""
from __future__ import annotations

from enum import Enum
from typing import Optional


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class OutcomeClass(str, Enum):
    A_PLUS_MOVE = "A_PLUS_MOVE"
    A_MOVE = "A_MOVE"
    B_MOVE = "B_MOVE"
    NOISE = "NOISE"
    FAIL = "FAIL"
    NOT_AVAILABLE_YET = "NOT_AVAILABLE_YET"


class MovePersistenceCode(str, Enum):
    PERSISTENT_CONTINUATION = "PERSISTENT_CONTINUATION"
    FAST_THEN_STALL = "FAST_THEN_STALL"
    FAST_THEN_REVERSE = "FAST_THEN_REVERSE"
    SLOW_GRIND = "SLOW_GRIND"
    NO_FOLLOWTHROUGH = "NO_FOLLOWTHROUGH"
    FAILED_EARLY = "FAILED_EARLY"


class OIPriceAlignment(str, Enum):
    PRICE_UP_OI_UP = "PRICE_UP_OI_UP"
    PRICE_UP_OI_DOWN = "PRICE_UP_OI_DOWN"
    PRICE_DOWN_OI_UP = "PRICE_DOWN_OI_UP"
    PRICE_DOWN_OI_DOWN = "PRICE_DOWN_OI_DOWN"
    MIXED_FLAT = "MIXED_FLAT"


class FlowPhaseCode(str, Enum):
    LARGE_LEAD_CROWD_ABSENT = "LARGE_LEAD_CROWD_ABSENT"
    LARGE_LEAD_CROWD_EARLY = "LARGE_LEAD_CROWD_EARLY"
    BOTH_ALIGNED_HEALTHY = "BOTH_ALIGNED_HEALTHY"
    CROWD_CHASE_LATE = "CROWD_CHASE_LATE"
    CROWD_OVERHEATED = "CROWD_OVERHEATED"
    LARGE_FADE_CROWD_ENTER = "LARGE_FADE_CROWD_ENTER"
    FLOW_DIVERGENCE_WARNING = "FLOW_DIVERGENCE_WARNING"
    LOW_PARTICIPATION_BREAKOUT = "LOW_PARTICIPATION_BREAKOUT"
    UNCLEAR_FLOW = "UNCLEAR_FLOW"


class BreakoutQualityBand(str, Enum):
    STRONG = "strong"
    CLEAN = "clean"
    WEAK = "weak"
    POOR = "poor"


class RuleDecisionCode(str, Enum):
    PROMISING_KEEP_TESTING = "PROMISING_KEEP_TESTING"
    PROMISING_NEEDS_REFINEMENT = "PROMISING_NEEDS_REFINEMENT"
    TOO_FEW_EVENTS = "TOO_FEW_EVENTS"
    OUTLIER_DEPENDENT = "OUTLIER_DEPENDENT"
    REGIME_SPECIFIC_ONLY = "REGIME_SPECIFIC_ONLY"
    LOW_SIGNAL_REJECT = "LOW_SIGNAL_REJECT"


# ---------------------------------------------------------------------------
# Section 8 — Event row field names (canonical; avoids string typos in code)
# ---------------------------------------------------------------------------

class F:
    """Field name constants for event rows / measurement details CSV."""

    # 8.1 Identity
    EVENT_ID = "event_id"
    SYMBOL = "symbol"
    SIDE = "side"
    RULE_FAMILY = "rule_family"
    RULE_VERSION = "rule_version"
    MEASUREMENT_VERSION = "measurement_version"
    TRIGGER_TS_MS = "trigger_ts_ms"
    TRIGGER_DT = "trigger_dt"
    TRIGGER_TF = "trigger_tf"
    PRICE_AT_TRIGGER = "price_at_trigger"
    UNIVERSE_VERSION = "universe_version"

    # 8.2 Price structure
    BREAKOUT_LEVEL = "breakout_level"
    DISTANCE_FROM_BREAKOUT_PCT = "distance_from_breakout_pct"
    COMPRESSION_SCORE = "compression_score"
    BODY_RATIO = "body_ratio"
    UPPER_WICK_RATIO = "upper_wick_ratio"
    LOWER_WICK_RATIO = "lower_wick_ratio"
    CLOSE_LOCATION_IN_RANGE = "close_location_in_range"
    RANGE_EXPANSION_RATIO = "range_expansion_ratio"
    BREAKOUT_QUALITY_BAND = "breakout_quality_band"
    BREAKOUT_QUALITY_SCORE = "breakout_quality_score"

    # 8.2 Volume
    VOLUME = "volume"
    VOLUME_RATIO_1BAR = "volume_ratio_1bar"
    VOLUME_RATIO_3BAR = "volume_ratio_3bar"
    VOLUME_RATIO_VS_BASELINE = "volume_ratio_vs_baseline"

    # 8.2 OI
    OI_VALUE = "oi_value"
    OI_DELTA_1BAR_PCT = "oi_delta_1bar_pct"
    OI_DELTA_3BAR_PCT = "oi_delta_3bar_pct"
    OI_DELTA_6BAR_PCT = "oi_delta_6bar_pct"
    OI_PRICE_ALIGNMENT_CODE = "oi_price_alignment_code"

    # 8.2 Flow/participation proxies
    TOP_ACC_IMBALANCE = "top_acc_imbalance"
    TOP_POS_IMBALANCE = "top_pos_imbalance"
    GLOBAL_ACC_IMBALANCE = "global_acc_imbalance"
    GLOBAL_ACC_IMBALANCE_DELTA = "global_acc_imbalance_delta"
    TAKER_IMBALANCE = "taker_imbalance"
    BASIS_RATE = "basis_rate"
    TOP_VS_GLOBAL_DIVERGENCE = "top_vs_global_divergence"
    LARGE_PARTICIPANT_PROXY = "large_participant_proxy"
    CROWD_PARTICIPATION_PROXY = "crowd_participation_proxy"
    CROWD_OVERHEAT_PROXY = "crowd_overheat_proxy"
    FLOW_COMPOSITE_SIGNAL = "flow_composite_signal"
    FLOW_PHASE_CODE = "flow_phase_code"

    # 8.2 Market context
    BTC_24H_CHANGE_PCT = "btc_24h_change_pct"
    ALT_BREADTH_PCT = "alt_breadth_pct"
    FUNDING_RATE = "funding_rate"
    VOLATILITY_BAND = "volatility_band"
    REGIME_LABEL_CANDIDATE = "regime_label_candidate"

    # Derived bands (for Board D)
    BTC_24H_BAND = "btc_24h_band"
    ALT_BREADTH_BAND = "alt_breadth_band"

    # 8.3 Data quality
    DATA_COMPLETE_YN = "data_complete_YN"
    MISSING_FIELD_COUNT = "missing_field_count"
    MISSING_FIELD_LIST = "missing_field_list"
    ELIGIBLE_FOR_MEASUREMENT_YN = "eligible_for_measurement_YN"
    MEASUREMENT_EXCLUSION_REASON = "measurement_exclusion_reason"

    # Outcome fields (Section 10)
    FUTURE_30M_MAX_FAVOR_PCT = "future_30m_max_favor_pct"
    FUTURE_30M_MAX_ADVERSE_PCT = "future_30m_max_adverse_pct"
    FUTURE_1H_MAX_FAVOR_PCT = "future_1h_max_favor_pct"
    FUTURE_1H_MAX_ADVERSE_PCT = "future_1h_max_adverse_pct"
    FUTURE_4H_MAX_FAVOR_PCT = "future_4h_max_favor_pct"
    FUTURE_4H_MAX_ADVERSE_PCT = "future_4h_max_adverse_pct"

    CLOSE_ABOVE_BREAKOUT_AFTER_30M_YN = "close_above_breakout_after_30m_YN"
    CLOSE_ABOVE_BREAKOUT_AFTER_1H_YN = "close_above_breakout_after_1h_YN"
    RECLAIM_BREAKOUT_1H_YN = "reclaim_breakout_1h_YN"
    RECLAIM_BREAKOUT_4H_YN = "reclaim_breakout_4h_YN"

    TIME_TO_1PCT_FAVOR_MIN = "time_to_1pct_favor_min"
    TIME_TO_2PCT_FAVOR_MIN = "time_to_2pct_favor_min"
    TIME_TO_3PCT_FAVOR_MIN = "time_to_3pct_favor_min"
    TIME_TO_1PCT_ADVERSE_MIN = "time_to_1pct_adverse_min"
    TIME_TO_2PCT_ADVERSE_MIN = "time_to_2pct_adverse_min"

    OUTCOME_30M_AVAILABLE_YN = "outcome_30m_available_YN"
    OUTCOME_1H_AVAILABLE_YN = "outcome_1h_available_YN"
    OUTCOME_4H_AVAILABLE_YN = "outcome_4h_available_YN"
    OUTCOME_NOT_AVAILABLE_REASON = "outcome_not_available_reason"

    # Derived outcome (Section 11)
    PAYOFF_30M = "payoff_30m"
    PAYOFF_1H = "payoff_1h"
    PAYOFF_4H = "payoff_4h"
    MOVE_PERSISTENCE_CODE = "move_persistence_code"
    OUTCOME_CLASS = "outcome_class"

    # Overlap filter tracking (Section 7.2)
    RAW_FIRE_IDX = "raw_fire_idx"
    KEPT_YN = "kept_yn"
    OVERLAP_DROP_REASON = "overlap_drop_reason"

    # Baseline comparison
    BASELINE_NAME = "baseline_name"


# ---------------------------------------------------------------------------
# Canonical event row field ordering for CSV output
# ---------------------------------------------------------------------------

EVENT_LOG_FIELDS = [
    F.EVENT_ID, F.SYMBOL, F.SIDE, F.RULE_FAMILY, F.RULE_VERSION,
    F.MEASUREMENT_VERSION, F.TRIGGER_TS_MS, F.TRIGGER_DT, F.TRIGGER_TF,
    F.PRICE_AT_TRIGGER, F.UNIVERSE_VERSION, F.DATA_COMPLETE_YN,
    F.ELIGIBLE_FOR_MEASUREMENT_YN, F.OUTCOME_CLASS,
]

MEASUREMENT_DETAIL_FIELDS = [
    # Identity
    F.EVENT_ID, F.SYMBOL, F.SIDE, F.RULE_FAMILY, F.RULE_VERSION,
    F.MEASUREMENT_VERSION, F.TRIGGER_TS_MS, F.TRIGGER_DT, F.TRIGGER_TF,
    F.PRICE_AT_TRIGGER, F.UNIVERSE_VERSION,
    # Price structure
    F.BREAKOUT_LEVEL, F.DISTANCE_FROM_BREAKOUT_PCT, F.COMPRESSION_SCORE,
    F.BODY_RATIO, F.UPPER_WICK_RATIO, F.LOWER_WICK_RATIO,
    F.CLOSE_LOCATION_IN_RANGE, F.RANGE_EXPANSION_RATIO,
    F.BREAKOUT_QUALITY_BAND, F.BREAKOUT_QUALITY_SCORE,
    # Volume
    F.VOLUME, F.VOLUME_RATIO_1BAR, F.VOLUME_RATIO_3BAR, F.VOLUME_RATIO_VS_BASELINE,
    # OI
    F.OI_VALUE, F.OI_DELTA_1BAR_PCT, F.OI_DELTA_3BAR_PCT, F.OI_DELTA_6BAR_PCT,
    F.OI_PRICE_ALIGNMENT_CODE,
    # Flow
    F.TOP_ACC_IMBALANCE, F.TOP_POS_IMBALANCE, F.GLOBAL_ACC_IMBALANCE,
    F.GLOBAL_ACC_IMBALANCE_DELTA, F.TAKER_IMBALANCE, F.BASIS_RATE,
    F.TOP_VS_GLOBAL_DIVERGENCE, F.LARGE_PARTICIPANT_PROXY,
    F.CROWD_PARTICIPATION_PROXY, F.CROWD_OVERHEAT_PROXY,
    F.FLOW_COMPOSITE_SIGNAL, F.FLOW_PHASE_CODE,
    # Market context
    F.BTC_24H_CHANGE_PCT, F.ALT_BREADTH_PCT, F.FUNDING_RATE,
    F.VOLATILITY_BAND, F.REGIME_LABEL_CANDIDATE,
    F.BTC_24H_BAND, F.ALT_BREADTH_BAND,
    # Data quality
    F.DATA_COMPLETE_YN, F.MISSING_FIELD_COUNT, F.MISSING_FIELD_LIST,
    F.ELIGIBLE_FOR_MEASUREMENT_YN, F.MEASUREMENT_EXCLUSION_REASON,
    # Raw outcome
    F.OUTCOME_30M_AVAILABLE_YN, F.FUTURE_30M_MAX_FAVOR_PCT, F.FUTURE_30M_MAX_ADVERSE_PCT,
    F.CLOSE_ABOVE_BREAKOUT_AFTER_30M_YN,
    F.OUTCOME_1H_AVAILABLE_YN, F.FUTURE_1H_MAX_FAVOR_PCT, F.FUTURE_1H_MAX_ADVERSE_PCT,
    F.CLOSE_ABOVE_BREAKOUT_AFTER_1H_YN, F.RECLAIM_BREAKOUT_1H_YN,
    F.OUTCOME_4H_AVAILABLE_YN, F.FUTURE_4H_MAX_FAVOR_PCT, F.FUTURE_4H_MAX_ADVERSE_PCT,
    F.RECLAIM_BREAKOUT_4H_YN,
    F.OUTCOME_NOT_AVAILABLE_REASON,
    # Speed
    F.TIME_TO_1PCT_FAVOR_MIN, F.TIME_TO_2PCT_FAVOR_MIN, F.TIME_TO_3PCT_FAVOR_MIN,
    F.TIME_TO_1PCT_ADVERSE_MIN, F.TIME_TO_2PCT_ADVERSE_MIN,
    # Derived outcome
    F.PAYOFF_30M, F.PAYOFF_1H, F.PAYOFF_4H,
    F.MOVE_PERSISTENCE_CODE, F.OUTCOME_CLASS,
]
