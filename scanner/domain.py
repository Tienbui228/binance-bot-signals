"""
scanner/domain.py
-----------------
SHIM — do not add new contracts here.

This file is kept for backward compatibility with existing imports in
oi_scanner.py, scanner/lifecycle.py, and scanner/review_service.py.

New code must import from contracts/ instead:
    from contracts.regime_result import RegimeResult
    from contracts.thesis_result import ThesisResult
    from contracts.delivery_result import DeliveryResult
    from contracts.veto_result import VetoResult
    from contracts.dispatch_result import DispatchResult

This file will be deprecated once all import sites are migrated
(Phase B complete; full deprecation after Phase E).
"""
from dataclasses import dataclass
from typing import Optional


# ── Legacy contracts — still used by oi_scanner.py internals ──────────────

@dataclass
class RegimeVerdict:
    """Legacy. Prefer contracts.regime_result.RegimeResult for new code."""
    regime_label: str = "unknown"
    regime_confidence: str = "not_evaluated"
    regime_fit_long_breakout: str = "not_evaluated"
    regime_fit_short_exhaustion: str = "not_evaluated"
    regime_note: str = ""


@dataclass
class DeliveryMetadata:
    """Legacy. Prefer contracts.delivery_result.DeliveryResult for new code."""
    entry_state: str = "not_evaluated"
    delivery_band: str = "not_evaluated"
    entry_distance_pct: float = 0.0
    retest_depth_pct: float = 0.0
    risk_pct_real: float = 0.0
    sl_distance_pct: float = 0.0
    tp1_distance_pct: float = 0.0
    tp2_distance_pct: float = 0.0
    delivery_note: str = ""


@dataclass
class VetoVerdict:
    """Legacy. Prefer contracts.veto_result.VetoResult for new code."""
    veto_flag: bool = False
    veto_reason_code: str = "not_evaluated"
    veto_layer: str = "not_evaluated"
    veto_note: str = ""


@dataclass
class DispatchDecision:
    """Legacy. Prefer contracts.dispatch_result.DispatchResult for new code."""
    dispatch_action: str = "not_evaluated"
    dispatch_confidence_band: str = "not_evaluated"
    dispatch_reason: str = "not_evaluated"
    publish_priority: int = 0


@dataclass
class CaseDecision:
    case_id: str
    symbol: str
    side: str
    strategy_family: str
    regime_label: str = "unknown"
    regime_fit_for_strategy: str = "not_evaluated"
    setup_quality_band: str = "not_evaluated"
    delivery_band: str = "not_evaluated"
    veto_reason_code: str = "not_evaluated"
    dispatch_action: str = "not_evaluated"
    dispatch_confidence_band: str = "not_evaluated"
    dispatch_reason: str = "not_evaluated"


@dataclass
class Signal:
    signal_id: str
    timestamp_ms: int
    symbol: str
    side: str
    score: float
    confidence: float
    reason: str

    breakout_level: float
    entry_low: float
    entry_high: float
    entry_ref: float
    stop: float
    tp1: float
    tp2: float

    price: float
    oi_jump_pct: float
    funding_pct: float
    vol_ratio: float
    retest_bars_waited: int

    setup_id: str = ""
    config_version: str = ""
    strategy: str = "legacy_5m_retest"
    market_regime: str = "unknown"
    btc_4h_change_pct: float = 0.0
    btc_1h_change_pct: float = 0.0
    btc_24h_range_pct: float = 0.0
    btc_4h_range_pct: float = 0.0
    alt_market_breadth_pct: float = 0.0
    btc_price: float = 0.0
    btc_24h_change_pct: float = 0.0
    btc_regime: str = "unknown"
    risk_pct_real: float = 0.0
    sl_distance_pct: float = 0.0
    tp1_distance_pct: float = 0.0
    tp2_distance_pct: float = 0.0
    break_distance_pct: float = 0.0
    retest_depth_pct: float = 0.0
    score_oi: float = 0.0
    score_exhaustion: float = 0.0
    score_breakout: float = 0.0
    score_retest: float = 0.0
    reason_tags: str = ""
    stop_was_forced_min_risk: str = "no"
    manual_tradable: str = "yes"
    manual_trade_note: str = "good_for_manual"
    regime_label: str = "unclear_mixed"
    regime_fit_for_strategy: str = "MEDIUM"
    dispatch_action: str = "not_evaluated"
    dispatch_confidence_band: str = "not_evaluated"
    dispatch_reason: str = "not_evaluated"
    status: str = "OPEN"


@dataclass
class PendingSetup:
    pending_id: str
    created_ts_ms: int
    signal_open_time: int
    symbol: str
    side: str
    score: float
    confidence: float
    reason: str
    breakout_level: float
    signal_price: float
    signal_high: float
    signal_low: float
    oi_jump_pct: float
    funding_pct: float
    vol_ratio: float
    setup_id: str = ""
    strategy: str = "legacy_5m_retest"
    market_regime: str = "unknown"
    btc_4h_change_pct: float = 0.0
    btc_1h_change_pct: float = 0.0
    btc_24h_range_pct: float = 0.0
    btc_4h_range_pct: float = 0.0
    alt_market_breadth_pct: float = 0.0
    btc_price: float = 0.0
    btc_24h_change_pct: float = 0.0
    btc_regime: str = "unknown"
    score_oi: float = 0.0
    score_exhaustion: float = 0.0
    score_breakout: float = 0.0
    score_retest: float = 0.0
    reason_tags: str = ""
    status: str = "PENDING"
    close_reason: str = ""
    bars_waited: int = 0
    closed_ts_ms: int = 0
    regime_label: str = "unknown"
    regime_fit_for_strategy: str = "not_evaluated"
    setup_quality_band: str = "not_evaluated"
    delivery_band: str = "not_evaluated"
    veto_reason_code: str = "not_evaluated"
    dispatch_action: str = "not_evaluated"
    dispatch_confidence_band: str = "not_evaluated"
    dispatch_reason: str = "not_evaluated"


@dataclass
class StageSlot:
    stage_name: str
    stage_status: str
    stage_content_type: str
    image_path: str = ""
    capture_time_local: str = ""
    note: str = ""


@dataclass
class CaseRecord:
    case_id: str
    case_day: str
    symbol: str
    side: str
    strategy: str
    signal_time_local: str = ""
    created_time_local: str = ""
    confirmed_time_local: str = ""
    sent_time_local: str = ""
    close_time_local: str = ""
    fallback_close_due_time_local: str = ""
    status_final: str = ""
    close_reason: str = ""
    is_confirmed: bool = False
    is_sent_signal: bool = False
    case_close_type: str = "not_due_yet"
    pre_pending: Optional[StageSlot] = None
    pending_open: Optional[StageSlot] = None
    entry_or_confirm: Optional[StageSlot] = None
    case_close: Optional[StageSlot] = None
    slot_bundle_complete: bool = False
    evidence_ready_for_review: str = "none"
    lifecycle_complete: bool = False
    human_review_status: str = "PENDING"
    verdict_code: str = ""
    root_cause_code: str = ""
    action_candidate_code: str = ""
    review_notes_short: str = ""
