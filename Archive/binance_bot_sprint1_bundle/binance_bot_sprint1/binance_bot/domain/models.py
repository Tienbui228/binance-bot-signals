from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from .enums import (
    CaseCloseType,
    EvidenceReadiness,
    Side,
    StageContentType,
    StageName,
    StageStatus,
)


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


@dataclass
class StageSlot:
    stage_name: StageName
    stage_status: StageStatus
    stage_content_type: StageContentType
    image_path: Optional[str] = None
    capture_time_local: Optional[str] = None


@dataclass
class CaseRecord:
    case_id: str
    case_day: str
    symbol: str
    side: Side
    strategy: str
    signal_time_local: Optional[str] = None
    created_time_local: Optional[str] = None
    confirmed_time_local: Optional[str] = None
    sent_time_local: Optional[str] = None
    close_time_local: Optional[str] = None
    fallback_close_due_time_local: Optional[str] = None
    status_final: str = ""
    close_reason: str = ""
    is_confirmed: bool = False
    is_sent_signal: bool = False
    case_close_type: CaseCloseType = CaseCloseType.NOT_DUE_YET
    pre_pending: StageSlot = field(default_factory=lambda: StageSlot(StageName.PRE_PENDING, StageStatus.NOT_REACHED_YET, StageContentType.NONE))
    pending_open: StageSlot = field(default_factory=lambda: StageSlot(StageName.PENDING_OPEN, StageStatus.NOT_REACHED_YET, StageContentType.NONE))
    entry_or_confirm: StageSlot = field(default_factory=lambda: StageSlot(StageName.ENTRY_OR_CONFIRM, StageStatus.NOT_REACHED_YET, StageContentType.NONE))
    case_close: StageSlot = field(default_factory=lambda: StageSlot(StageName.CASE_CLOSE, StageStatus.NOT_REACHED_YET, StageContentType.NONE))
    slot_bundle_complete: bool = True
    evidence_ready_for_review: EvidenceReadiness = EvidenceReadiness.NONE
    lifecycle_complete: bool = False
    human_review_status: str = ""
    verdict_code: str = ""
    root_cause_code: str = ""
    action_candidate_code: str = ""
    review_notes_short: str = ""
