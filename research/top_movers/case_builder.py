"""
research/top_movers/case_builder.py

Builds:
  - CaseRow (daily_top_mover_cases.csv) — one per token
  - AnchorSnapshotRows (daily_top_mover_anchor_snapshots.csv) — five per token

v2 taxonomy:
  - move_class: 8 labels (no generic 'unclear' as dominant)
  - pre_move_signature: 6 labels derived from proxy (not compression geometry)
  - participation_pattern: 6 labels derived from flow_phase_code
  - structural_quality: 6 labels from compression + break quality + context

v2 eligibility fields:
  - research_eligible_YN
  - full_visual_complete_YN
  - proxy_complete_YN
  - outcome_complete_YN

v2 caution_flag: narrow, evidence-based only.
"""

from datetime import datetime, timezone as _utc
from typing import Dict, List, Optional

from research.top_movers.anchor_detector import (
    AnchorSet,
    compute_breakdown_pack,
    compute_retest_fail_pack,
)
from research.top_movers.canonical_move import CanonicalMove
from research.top_movers.decision_mapping import (
    classify_flow_phase_code,
    classify_resolution_label,
    classify_timing_flags,
    compute_post_p2_outcomes,
    compute_confidence_fields,
    compute_eligibility_fields,
    compute_caution_flag_narrow,
    compute_decision_grade,
    compute_range_expansion_ratio,
    flow_phase_to_participation_pattern,
    generate_case_takeaways,
    map_to_existing_strategy,
)
from research.top_movers.proxy_features import (
    build_proxy_features,
    compute_flow_composite,
    compute_btc_context_fields,
    compute_retest_volume_decay_ratio,
    compute_exhaustion_pack,
)
from research.top_movers.io import image_path, safe_round

# Image key per anchor (v2 standard set)
_IMAGE_KEYS: Dict[str, Optional[str]] = {
    "P0": "P0_context_1h",
    "P1": "P1_ignition_5m",
    "P2": "P2_P3_break_expansion_5m",
    "P3": None,   # shares P2_P3 image
    "P4": "P4_resolution_15m",
}


def make_case_id(research_day: str, symbol: str, side: str) -> str:
    return f"{research_day.replace('-','')}_{symbol}_{side}"


def _derive_case_inclusion_reason(v4_meta: Dict) -> str:
    horizon = v4_meta.get("selection_horizon", "")
    bucket  = v4_meta.get("top_mover_bucket", "")
    if horizon == "1d" and "dump" in bucket:
        return "top_dumper_1d"
    if horizon == "7d" and "dump" in bucket:
        return "top_dumper_7d"
    if "gainer" in bucket or horizon == "1d_gainers":
        return "top_gainer_1d"
    if "loser" in bucket or horizon == "1d_losers":
        return "top_loser_1d"
    return f"unknown_{horizon}"


# ---------------------------------------------------------------------------
# Classification helpers v2
# ---------------------------------------------------------------------------

def _classify_structural_quality_v2(bq_band: str, comp_score: float, side: str, overheat: float) -> str:
    """
    Allowed: clean_base_break | repeated_test_then_break | shallow_pullback_continuation
             | runaway_no_base | dirty_break | exhaustion_spike
    """
    bq = bq_band or "poor"; cs = comp_score or 0.0; oh = overheat or 0.0
    if side == "SHORT" and oh >= 0.50 and bq in ("strong",):
        return "exhaustion_spike"
    if bq in ("strong",) and cs >= 0.65:
        return "clean_base_break"
    if bq in ("strong","clean") and cs >= 0.55:
        return "repeated_test_then_break"
    if bq in ("clean",) and cs >= 0.40:
        return "shallow_pullback_continuation"
    if bq in ("strong","clean") and cs < 0.40:
        return "runaway_no_base"
    return "dirty_break"


def _classify_pre_move_signature_v2(px: Dict) -> str:
    """
    Allowed: oi_led | volume_led | top_cohort_led | crowd_led | mixed_led | low_signal_premove
    Derived from proxy fields, NOT from compression geometry.
    """
    all_none = all(px.get(f) is None for f in [
        "top_pos_imbalance","taker_imbalance","oi_delta_3bar_pct","global_acc_imbalance",
    ])
    if all_none:
        return "low_signal_premove"

    top_pos   = abs(px.get("top_pos_imbalance") or 0.0)
    taker     = abs(px.get("taker_imbalance") or 0.0)
    oi_delta  = abs(px.get("oi_delta_3bar_pct") or 0.0)
    global_ac = abs(px.get("global_acc_imbalance") or 0.0)
    large     = px.get("large_participant_proxy") or 0.0
    crowd     = px.get("crowd_participation_proxy") or 0.0

    signals = []
    if top_pos >= 5.0 or large >= 0.50:  signals.append("top_cohort_led")
    if oi_delta >= 2.0:                  signals.append("oi_led")
    if taker >= 0.20:                    signals.append("volume_led")
    if global_ac >= 5.0 or crowd >= 0.50: signals.append("crowd_led")

    if not signals:      return "low_signal_premove"
    if len(signals) >= 3: return "mixed_led"
    if len(signals) == 2:
        if "top_cohort_led" in signals: return "top_cohort_led"
        if "oi_led" in signals:         return "oi_led"
        return "mixed_led"
    return signals[0]


def _classify_move_class_v2(side: str, structural_quality: str, flow_phase_code: str, resolution_label: str) -> str:
    """
    Allowed: clean_continuation_pump | slow_build_then_break | squeeze_then_expansion
             | blowoff_pump | clean_breakdown_dump | exhaustion_dump
             | fake_break_then_reverse | noisy_unstructured_move
    """
    # Override: any fast reversal is a fake break
    if resolution_label == "FAST_THEN_REVERSE":
        return "fake_break_then_reverse"

    sq = structural_quality
    if side == "LONG":
        if sq == "clean_base_break":
            if flow_phase_code in ("BOTH_ALIGNED_HEALTHY","LARGE_LEAD_CROWD_EARLY"):
                return "squeeze_then_expansion"
            return "clean_continuation_pump"
        if sq in ("repeated_test_then_break","shallow_pullback_continuation"):
            return "slow_build_then_break"
        if sq == "runaway_no_base":
            return "blowoff_pump"
        return "noisy_unstructured_move"   # dirty_break

    if side == "SHORT":
        if sq == "exhaustion_spike":
            return "exhaustion_dump"
        if sq in ("clean_base_break","repeated_test_then_break","shallow_pullback_continuation"):
            return "clean_breakdown_dump"
        if sq == "runaway_no_base":
            return "exhaustion_dump"
        return "noisy_unstructured_move"

    return "noisy_unstructured_move"


# ---------------------------------------------------------------------------
# Wave 2 — L6 timing helpers (case_builder owns these: pure timestamp arithmetic)
# ---------------------------------------------------------------------------

# PROVISIONAL bucket labels — not grounded in backtest analysis
_TOD_BUCKETS = [
    (0,  5,  "asia_late"),
    (6,  9,  "london_open"),
    (10, 13, "ny_morning"),
    (14, 17, "ny_afternoon"),
    (18, 23, "asia_early"),
]


def _compute_time_of_day_bucket(ts_ms: Optional[int]) -> Optional[str]:
    """UTC hour of P2 → provisional time-of-day bucket label."""
    if ts_ms is None:
        return None
    hour = datetime.fromtimestamp(ts_ms / 1000.0, tz=_utc.utc).hour
    for lo, hi, label in _TOD_BUCKETS:
        if lo <= hour <= hi:
            return label
    return "unknown"


def _compute_time_since_day_high(
    bars_5m: List[Dict],
    p2_idx: int,
    day_start_idx: int,
    side: str,
) -> Optional[float]:
    """Minutes from the day extreme (high for LONG, low for SHORT) to P2.

    Scans bars from day_start_idx to p2_idx to locate the extreme bar.
    Returns positive value: how long before P2 the extreme was formed.
    Returns None if insufficient bars.
    """
    window = bars_5m[day_start_idx:p2_idx]
    if not window:
        return None
    if side == "LONG":
        ext_bar = max(window, key=lambda b: b["high"])
    else:
        ext_bar = min(window, key=lambda b: b["low"])
    p2_bar = bars_5m[p2_idx] if p2_idx < len(bars_5m) else None
    if p2_bar is None:
        return None
    delta_ms = p2_bar["open_time"] - ext_bar["open_time"]
    return round(delta_ms / 60000.0, 1)


# ---------------------------------------------------------------------------
# V4-2 anchor derived helpers
# ---------------------------------------------------------------------------

def _map_anchor_reason_code(validity_reason: str) -> str:
    mapping = {
        "all_anchors_auto_detected":        "clean",
        "p1_no_ignition":                   "p1_fallback",
        "p1_no_ignition_p2_no_clean_break": "p1_p2_fallback",
    }
    if validity_reason in mapping:
        return mapping[validity_reason]
    return "unknown" if not validity_reason else f"unknown:{validity_reason}"


def _compute_peak_age_hours(p1_ts_ms: int, research_day: str) -> Optional[float]:
    try:
        day_end_ms = int(
            datetime.strptime(research_day, "%Y-%m-%d")
            .replace(tzinfo=_utc.utc).timestamp() * 1000
        ) + 24 * 3600 * 1000
        return round((day_end_ms - p1_ts_ms) / (3600 * 1000), 2)
    except Exception:
        return None


def _compute_case_spans_days(p0_ts_ms: int, p4_ts_ms: int) -> bool:
    p0_day = datetime.fromtimestamp(p0_ts_ms / 1000, tz=_utc.utc).date()
    p4_day = datetime.fromtimestamp(p4_ts_ms / 1000, tz=_utc.utc).date()
    return p0_day != p4_day


# ---------------------------------------------------------------------------
# Main case row builder
# ---------------------------------------------------------------------------

def build_case_row(
    case_id: str,
    move,
    anchors: AnchorSet,
    proxy,
    bars_5m_extended: List[Dict],
    research_regime: str,
    btc_24h: float,
    alt_breadth_pct: float,
    top_mover_rank: int,
    top_mover_bucket: str,
    image_results: Dict,
    btc_bars_15m: Optional[List[Dict]] = None,
    btc_bars_1h: Optional[List[Dict]] = None,
    bars_1h: Optional[List[Dict]] = None,
    v4_selection_meta: Optional[Dict] = None,
) -> Dict:
    """Build one row for daily_top_mover_cases.csv."""

    p2_idx = anchors.p2.bar_idx
    px = proxy.at(p2_idx) if proxy else {}

    # --- BTC context fields (Layer 7) ---
    p2_ts_ms_val = anchors.p2.ts_ms if anchors.p2 else 0
    btc_ctx = compute_btc_context_fields(
        p2_ts_ms=p2_ts_ms_val or 0,
        btc_bars_15m=btc_bars_15m or [],
        btc_bars_1h=btc_bars_1h or [],
    )

    bq_score = anchors.p2.break_quality_score
    bq_band  = anchors.p2.break_quality_band
    comp_score = anchors.p0.compression_score

    large   = px.get("large_participant_proxy")
    crowd   = px.get("crowd_participation_proxy")
    overheat = px.get("crowd_overheat_proxy")
    z_oi   = px.get("z_oi_delta")
    z_tak  = px.get("z_taker")
    div    = px.get("top_vs_global_divergence")
    flow   = compute_flow_composite(large, z_oi, z_tak, bq_score, overheat)

    # --- flow_phase_code ---
    flow_phase = classify_flow_phase_code(large, crowd, overheat, div, flow)

    # --- participation_pattern (v2: derived from flow_phase) ---
    part_pat = flow_phase_to_participation_pattern(flow_phase)

    # --- structural_quality v2 ---
    struct_q = _classify_structural_quality_v2(bq_band, comp_score, move.side, overheat or 0.0)

    # --- pre_move_signature v2 ---
    pre_sig = _classify_pre_move_signature_v2(px)

    # --- outcomes (needs extended bars) ---
    p2_close = anchors.p2.bar.get("close", 0) if anchors.p2.bar else 0
    outcomes = compute_post_p2_outcomes(bars_5m_extended, anchors.p2.ts_ms, p2_close, move.side)

    # --- resolution_label ---
    resolution = classify_resolution_label(
        f30=outcomes.get("future_30m_max_favor_pct"),
        a30=outcomes.get("future_30m_max_adverse_pct"),
        f1h=outcomes.get("future_1h_max_favor_pct"),
        a1h=outcomes.get("future_1h_max_adverse_pct"),
        f4h=outcomes.get("future_4h_max_favor_pct"),
        a4h=outcomes.get("future_4h_max_adverse_pct"),
        close1h=outcomes.get("close_above_break_after_1h_YN","N"),
        reclaim4h=outcomes.get("reclaim_break_4h_YN","N"),
    )

    # --- move_class v2 (uses resolution for fake_break override) ---
    move_class = _classify_move_class_v2(move.side, struct_q, flow_phase, resolution)

    # --- research_verdict ---
    verdict = _classify_research_verdict(struct_q, part_pat, move.data_quality_ok)

    # --- strategy mapping ---
    strat = map_to_existing_strategy(
        side=move.side, move_class=move_class, pre_move_signature=pre_sig,
        participation_pattern=part_pat, structural_quality=struct_q,
        flow_phase_code=flow_phase, resolution_label=resolution,
    )

    # --- confidence ---
    conf = compute_confidence_fields(px, image_results, outcomes, move.data_quality_ok, move.daily_return_pct)

    # --- eligibility fields (v2) ---
    elig = compute_eligibility_fields(
        data_quality_ok=move.data_quality_ok,
        anchors=anchors,
        image_results=image_results,
        proxy_completeness=conf["proxy_completeness_score"],
        outcome_completeness=conf["outcome_completeness_score"],
    )
    # V4-1: exclusion_reason overrides research_eligible_YN for dump cases only.
    # Existing gainers/losers keep their computed eligibility unchanged.
    if (v4_selection_meta
            and v4_selection_meta.get("exclusion_reason", "")
            and v4_selection_meta.get("selection_horizon") in ("1d", "7d")):
        elig["research_eligible_YN"] = "N"

    # --- decision grade ---
    grade = compute_decision_grade(
        strategy_action_type=strat["strategy_action_type"],
        classification_confidence=conf["classification_confidence"],
        proxy_completeness=conf["proxy_completeness_score"],
        outcome_completeness=conf["outcome_completeness_score"],
    )

    # --- caution_flag (narrow, evidence-based) ---
    caution = compute_caution_flag_narrow(
        proxy_complete_yn=elig["proxy_complete_YN"],
        full_visual_complete_yn=elig["full_visual_complete_YN"],
        outlier_risk_flag=conf["outlier_risk_flag"],
        structural_quality=struct_q,
        participation_pattern=part_pat,
        decision_grade=grade,
    )

    # --- new canonical fields ---
    anc_quality  = _derive_anchor_quality_flag(anchors)
    anc_conflict = _derive_anchor_conflict_flag(anchors)
    anc_validity = _derive_anchor_validity_reason(anchors)
    data_conf    = conf["classification_confidence"]
    interv_conf  = _derive_intervention_confidence(
        grade, strat["existing_strategy_fit_confidence"], data_conf, caution, anc_conflict
    )
    p4_trigger   = _derive_p4_trigger(anchors.p4)

    # --- range expansion ---
    range_exp = compute_range_expansion_ratio(
        p3_bar=anchors.p3.bar if anchors.p3 else None,
        range_high=anchors.range_high, range_low=anchors.range_low, side=move.side,
    )

    # --- takeaways ---
    tkwy = generate_case_takeaways(
        flow_phase_code=flow_phase, resolution_label=resolution, move_class=move_class,
        strategy_action_type=strat["strategy_action_type"],
        maps_to=strat["maps_to_existing_strategy_family"],
        improvement_layer=strat["improvement_target_layer"],
    )

    # --- Wave 2: L4 Breakdown pack ---
    vol_series = proxy.volume_ratio_3bar if proxy else []
    bdp = compute_breakdown_pack(bars_5m_extended, anchors, vol_series)

    # --- Wave 2: L5 Retest fail / reclaim pack ---
    rfp = compute_retest_fail_pack(
        bars_5m_extended,
        anchors.p2, anchors.p3, anchors.p4,
        move.side, anchors.range_high, anchors.range_low,
    )
    # retest_volume_decay_ratio lives in proxy_features (volume-based field)
    rfp_vol_decay = compute_retest_volume_decay_ratio(
        bars_5m_extended,
        anchors.p2.bar_idx if anchors.p2 else None,
        rfp.retest_start_bar_idx,
        rfp.retest_extreme_bar_idx,
    )

    # --- Wave 2: L6 Timing / staleness ---
    timing_flags = classify_timing_flags(
        bars_to_retest=rfp.bars_to_retest,
        bars_to_fail=rfp.bars_to_fail,
        p2_ts_ms=anchors.p2.ts_ms if anchors.p2 else None,
        p4_ts_ms=anchors.p4.ts_ms if anchors.p4 else None,
    )
    time_bucket = _compute_time_of_day_bucket(anchors.p2.ts_ms if anchors.p2 else None)
    # day_start_idx: find first bar with open_time >= UTC day start
    try:
        _day_start_ms = int(datetime.strptime(move.research_day, "%Y-%m-%d")
                            .replace(tzinfo=_utc.utc).timestamp() * 1000)
    except Exception:
        _day_start_ms = 0
    _day_start_idx = 0
    for _i, _b in enumerate(bars_5m_extended):
        if _b["open_time"] >= _day_start_ms:
            _day_start_idx = _i
            break
    time_since_high = _compute_time_since_day_high(
        bars_5m_extended,
        anchors.p2.bar_idx if anchors.p2 else 0,
        _day_start_idx,
        move.side,
    )
    time_since_break = (
        round((anchors.p3.ts_ms - anchors.p2.ts_ms) / 60000.0, 1)
        if anchors.p3 and anchors.p2 else None
    )

    def ts_or(a): return a.ts_ms if a else None
    def pr_or(a): return a.bar.get("close") if a and a.bar else None

    row = {
        # Identity
        "case_id": case_id, "research_day": move.research_day,
        "symbol": move.symbol, "side": move.side,
        "top_mover_rank": top_mover_rank, "top_mover_bucket": top_mover_bucket,
        # Descriptive
        "daily_return_pct": safe_round(move.daily_return_pct, 4),
        "day_open": safe_round(move.day_open, 6), "day_close": safe_round(move.day_close, 6),
        "day_high": safe_round(move.day_high, 6),  "day_low": safe_round(move.day_low, 6),
        "move_magnitude_pct": safe_round(move.move_magnitude_pct, 4),
        # Anchors
        "p0_ts_ms": ts_or(anchors.p0), "p1_ts_ms": ts_or(anchors.p1),
        "p2_ts_ms": ts_or(anchors.p2), "p3_ts_ms": ts_or(anchors.p3), "p4_ts_ms": ts_or(anchors.p4),
        "p0_price": pr_or(anchors.p0), "p2_price": pr_or(anchors.p2), "p4_price": pr_or(anchors.p4),
        "range_high": safe_round(anchors.range_high, 6), "range_low": safe_round(anchors.range_low, 6),
        "range_expansion_ratio": range_exp,
        # V4-2/V4-3: Anchor price fields (from AnchorPoint.bar — no extra API calls)
        "p1_price":    float(anchors.p1.bar["high"])  if anchors.p1  else None,
        "peak_close":  float(anchors.p1.bar["close"]) if anchors.p1  else None,
        "p1_low":      float(anchors.p1.bar["low"])   if anchors.p1  else None,  # V4-3
        "p3_price":    float(anchors.p3.bar["close"]) if anchors.p3  else None,
        # V4-2: Bar counts (1h resolution for P0→P2, 5m for P2→P4)
        "bars_p0_to_p1": max(0, (anchors.p1.ts_ms - anchors.p0.ts_ms) // (60 * 60 * 1000))
                         if anchors.p0 and anchors.p1 else None,
        "bars_p1_to_p2": max(0, (anchors.p2.ts_ms - anchors.p1.ts_ms) // (60 * 60 * 1000))
                         if anchors.p1 and anchors.p2 else None,
        "bars_p2_to_p3": max(0, (anchors.p3.ts_ms - anchors.p2.ts_ms) // (5 * 60 * 1000))
                         if anchors.p2 and anchors.p3 else None,
        "bars_p3_to_p4": max(0, (anchors.p4.ts_ms - anchors.p3.ts_ms) // (5 * 60 * 1000))
                         if anchors.p3 and anchors.p4 else None,
        # Anchors quality
        "compression_score": safe_round(comp_score, 4),
        "break_quality_score": safe_round(bq_score, 4), "break_quality_band": bq_band or "",
        # Proxy fields
        "large_participant_proxy": safe_round(large, 4),
        "crowd_participation_proxy": safe_round(crowd, 4),
        "crowd_overheat_proxy": safe_round(overheat, 4),
        "flow_composite_signal": safe_round(flow, 4),
        "flow_phase_code": flow_phase,
        "oi_delta_3bar_pct": safe_round(px.get("oi_delta_3bar_pct"), 4),
        "volume_ratio_3bar": safe_round(px.get("volume_ratio_3bar"), 4),
        "top_acc_imbalance_at_p2": safe_round(px.get("top_acc_imbalance"), 4),
        "top_pos_imbalance_at_p2": safe_round(px.get("top_pos_imbalance"), 4),
        "global_acc_imbalance_at_p2": safe_round(px.get("global_acc_imbalance"), 4),
        "taker_imbalance_at_p2": safe_round(px.get("taker_imbalance"), 4),
        "btc_24h_change_pct": safe_round(btc_24h, 4),
        "btc_change_15m_pct": btc_ctx.get("btc_change_15m_pct"),
        "btc_change_1h_pct":  btc_ctx.get("btc_change_1h_pct"),
        "market_volatility_proxy": btc_ctx.get("market_volatility_proxy"),
        "alt_breadth_pct": safe_round(alt_breadth_pct, 2),
        "research_regime": research_regime,
        # Outcomes (post-P2, retrospective)
        "future_30m_max_favor_pct":   outcomes.get("future_30m_max_favor_pct"),
        "future_30m_max_adverse_pct": outcomes.get("future_30m_max_adverse_pct"),
        "future_1h_max_favor_pct":    outcomes.get("future_1h_max_favor_pct"),
        "future_1h_max_adverse_pct":  outcomes.get("future_1h_max_adverse_pct"),
        "future_4h_max_favor_pct":    outcomes.get("future_4h_max_favor_pct"),
        "future_4h_max_adverse_pct":  outcomes.get("future_4h_max_adverse_pct"),
        "time_to_2pct_favor_min":     outcomes.get("time_to_2pct_favor_min"),
        "time_to_3pct_favor_min":     outcomes.get("time_to_3pct_favor_min"),
        "close_above_break_after_1h_YN": outcomes.get("close_above_break_after_1h_YN","N"),
        "reclaim_break_4h_YN":           outcomes.get("reclaim_break_4h_YN","N"),
        "resolution_label": resolution,
        # v2 taxonomy
        "move_class": move_class, "pre_move_signature": pre_sig,
        "participation_pattern": part_pat, "structural_quality": struct_q,
        "research_verdict": verdict,
        "case_takeaway_1": tkwy["case_takeaway_1"], "case_takeaway_2": tkwy["case_takeaway_2"],
        # Strategy-action
        "maps_to_existing_strategy_family":          strat["maps_to_existing_strategy_family"],
        "existing_strategy_fit_confidence":          strat["existing_strategy_fit_confidence"],
        "existing_strategy_mapping_reason":          strat["existing_strategy_mapping_reason"],
        "improvement_target_layer":                  strat["improvement_target_layer"],
        "improvement_hypothesis":                    strat["improvement_hypothesis"],
        "existing_strategy_improvement_candidate_YN": strat["existing_strategy_improvement_candidate_YN"],
        "new_strategy_candidate_flag":               strat["new_strategy_candidate_flag"],
        "candidate_strategy_family_name":            strat["candidate_strategy_family_name"],
        "candidate_trigger_description":             strat["candidate_trigger_description"],
        "candidate_environment":                     strat["candidate_environment"],
        "candidate_invalid_pattern":                 strat["candidate_invalid_pattern"],
        "new_strategy_confidence":                   strat["new_strategy_confidence"],
        "strategy_action_type":                      strat["strategy_action_type"],
        # Decision grade
        "decision_grade": grade,
        # Eligibility (v2)
        "research_eligible_YN":      elig["research_eligible_YN"],
        "full_visual_complete_YN":   elig["full_visual_complete_YN"],
        "proxy_complete_YN":         elig["proxy_complete_YN"],
        "outcome_complete_YN":       elig["outcome_complete_YN"],
        # Confidence
        "classification_confidence":  conf["classification_confidence"],
        "proxy_completeness_score":   conf["proxy_completeness_score"],
        "image_completeness_score":   conf["image_completeness_score"],
        "outcome_completeness_score": conf["outcome_completeness_score"],
        "data_quality_flag":          conf["data_quality_flag"],
        "outlier_risk_flag":          conf["outlier_risk_flag"],
        "caution_flag":               caution,
        # Meta
        "anchor_detect_method": move.detection_method,
        "data_quality_ok":   "Y" if move.data_quality_ok else "N",
        "data_quality_note": move.data_quality_note or "",
        # Canonical new fields
        "anchor_quality_flag":     anc_quality,
        "anchor_conflict_flag":    anc_conflict,
        "anchor_validity_reason":  anc_validity,
        "anchor_reason_code":      _map_anchor_reason_code(anc_validity or ""),
        "peak_age_hours":          _compute_peak_age_hours(anchors.p1.ts_ms, move.research_day)
                                   if anchors.p1 else None,
        "p4_resolution_trigger":   p4_trigger,
        "data_confidence":         data_conf,
        "intervention_confidence": interv_conf,
        # Wave 2 — L4 Breakdown pack
        "break_distance_pct":               bdp.break_distance_pct,
        "break_close_strength":             bdp.break_close_strength,        # PROVISIONAL
        "break_bar_body_ratio":             bdp.break_bar_body_ratio,        # PROVEN
        "break_volume_ratio":               bdp.break_volume_ratio,          # PROVISIONAL
        "support_test_count":               bdp.support_test_count,
        "break_cleanliness_score":          bdp.break_cleanliness_score,     # PROVEN
        "immediate_followthrough_pct_1bar": bdp.immediate_followthrough_pct_1bar,
        "immediate_followthrough_pct_3bar": bdp.immediate_followthrough_pct_3bar,
        "false_break_reclaim_fast_flag":    bdp.false_break_reclaim_fast_flag,
        # Wave 2 — L5 Retest fail / reclaim pack
        "retest_pack_available":                rfp.pack_available,
        "reclaim_pct":                          rfp.reclaim_pct,
        "retest_depth_vs_break_pct":            rfp.retest_depth_vs_break_pct,
        "retest_duration_bars":                 rfp.retest_duration_bars,
        "retest_volume_decay_ratio":            rfp_vol_decay,
        "retest_reject_wick_ratio":             rfp.retest_reject_wick_ratio,
        "retest_rejection_quality":             rfp.retest_rejection_quality,
        "retest_close_back_above_break_flag":   rfp.retest_close_back_above_break_flag,
        "fail_strength":                        rfp.fail_strength,
        "fail_confirmation_bar_count":          rfp.fail_confirmation_bar_count,
        "max_reclaim_before_resolution_pct":    rfp.max_reclaim_before_resolution_pct,
        "second_rejection_present_flag":        rfp.second_rejection_present_flag,
        # Wave 2 — L6 Timing / staleness pack
        "bars_to_retest":               rfp.bars_to_retest,
        "bars_to_fail":                 rfp.bars_to_fail,
        "bars_fail_to_acceleration":    None,           # DEFERRED — definition unclear
        "late_retest_flag":             timing_flags["late_retest_flag"],   # PROVISIONAL threshold
        "stale_setup_flag":             timing_flags["stale_setup_flag"],   # PROVISIONAL threshold
        "time_of_day_bucket":           time_bucket,    # PROVISIONAL labels
        "time_since_day_high_minutes":  time_since_high,
        "time_since_break_minutes":     time_since_break,
    }

    # --- V4-1: Case identity (Layer 0) + Selection/Universe (Layer 1) ---
    _v4 = v4_selection_meta or {}
    _sel_horizon = _v4.get("selection_horizon", "1d_gainers")
    _excl_reason = _v4.get("exclusion_reason", "")
    if _sel_horizon in ("1d", "7d"):
        _case_identity_mode = "research_dump_event"
    elif _sel_horizon == "1d_gainers":
        _case_identity_mode = "research_gainer_event"
    elif _sel_horizon == "1d_losers":
        _case_identity_mode = "research_loser_event"
    else:
        _case_identity_mode = "research_daily_bar_event"

    row.update({
        # Layer 0 — Case identity
        "research_case_id":             f"{move.symbol}_{move.research_day}_{_sel_horizon}",
        "case_identity_mode":           _case_identity_mode,
        "selection_horizon":            _sel_horizon,
        "selection_window":             _v4.get("selection_window", "rolling_24h"),
        "case_inclusion_reason":        _derive_case_inclusion_reason(_v4) if _v4 else "",
        "semantic_clean_flag":          True,
        "exclusion_reason":             _excl_reason,
        "dataset_batch":                move.research_day,
        "also_in_7d_top10":             _v4.get("also_in_7d_top10", False),
        "also_in_1d_top10":             _v4.get("also_in_1d_top10", False),
        "runtime_equivalent_case_id":   None,
        "runtime_linkage_status":       "linkage_not_attempted",
        "case_spans_days":              _compute_case_spans_days(anchors.p0.ts_ms, anchors.p4.ts_ms)
                                        if anchors.p0 and anchors.p4 else False,
        # Layer 1 — Selection / universe
        "rank_abs_change_24h":          _v4.get("rank_abs_change_24h"),
        "rank_volume_24h":              _v4.get("rank_volume_24h"),
        "quote_vol_24h_usdt":           _v4.get("quote_vol_24h_usdt"),
        "notional_volume_usd":          _v4.get("notional_volume_usd"),
        "notional_liquidity_band":      _v4.get("notional_liquidity_band"),
        "week_change_pct":              _v4.get("week_change_pct"),
        "day_range_pct":                _v4.get("day_range_pct"),
        "intraday_expansion_pct":       _v4.get("intraday_expansion_pct"),
        "market_cap_usd":               _v4.get("market_cap_usd"),
        "market_cap_verified":          _v4.get("market_cap_verified", False),
        "market_cap_source":            _v4.get("market_cap_source", "unknown"),
        "live_universe_eligible_flag":  _v4.get("live_universe_eligible_flag", False),
        "universe_mismatch_reason":     _v4.get("universe_mismatch_reason", ""),
    })

    # --- V4-3: Layer 3 Exhaustion Pack ---
    _exhaust = compute_exhaustion_pack(
        symbol=move.symbol,
        p0_price=float(anchors.p0.bar["close"]) if anchors.p0 and anchors.p0.bar else None,
        p0_ts_ms=anchors.p0.ts_ms if anchors.p0 else None,
        p1_ts_ms=anchors.p1.ts_ms if anchors.p1 else None,
        p1_price=row.get("p1_price"),
        peak_close=row.get("peak_close"),
        p1_low=row.get("p1_low"),
        p2_ts_ms=anchors.p2.ts_ms if anchors.p2 else None,
        bars_1h=bars_1h or [],
    )
    row.update(_exhaust)

    return row


# ---------------------------------------------------------------------------
# Anchor snapshot rows
# ---------------------------------------------------------------------------

def build_anchor_snapshot_rows(case_id, research_day, symbol, anchors, proxy) -> List[Dict]:
    rows = []
    for code, ap in zip(["P0","P1","P2","P3","P4"],
                         [anchors.p0, anchors.p1, anchors.p2, anchors.p3, anchors.p4]):
        img_key = _IMAGE_KEYS.get(code)
        img_fp  = image_path(research_day, case_id, img_key) if img_key else ""
        if ap is None:
            rows.append({"case_id":case_id,"symbol":symbol,"research_day":research_day,
                         "anchor_code":code,"anchor_ts_ms":"not_reached_yet",
                         "bar_open":"","bar_high":"","bar_low":"","bar_close":"","bar_volume":"",
                         "anchor_detect_method":"AUTO_V1","anchor_auto_confidence":"",
                         "compression_score":"","break_quality_score":"","break_quality_band":"",
                         "directional_extension_pct":"",
                         "oi_delta_3bar_at_anchor":"","volume_ratio_3bar_at_anchor":"",
                         "image_path":img_fp,"image_created_YN":"N",
                         "image_missing_reason":"anchor_not_reached_yet"})
            continue
        bar = ap.bar or {}
        px  = proxy.at(ap.bar_idx) if proxy else {}
        rows.append({
            "case_id":case_id,"symbol":symbol,"research_day":research_day,
            "anchor_code":code,"anchor_ts_ms":ap.ts_ms,
            "bar_open": safe_round(bar.get("open"),6),
            "bar_high": safe_round(bar.get("high"),6),
            "bar_low":  safe_round(bar.get("low"),6),
            "bar_close":safe_round(bar.get("close"),6),
            "bar_volume":safe_round(bar.get("volume"),2),
            "anchor_detect_method":ap.detect_method,
            "anchor_auto_confidence":safe_round(ap.auto_confidence,3),
            "compression_score":safe_round(ap.compression_score,4) if code=="P0" else "",
            "break_quality_score":safe_round(ap.break_quality_score,4) if code=="P2" else "",
            "break_quality_band":(ap.break_quality_band or "") if code=="P2" else "",
            "directional_extension_pct":safe_round(ap.directional_extension_pct,4) if code=="P3" else "",
            "oi_delta_3bar_at_anchor":safe_round(px.get("oi_delta_3bar_pct"),4),
            "volume_ratio_3bar_at_anchor":safe_round(px.get("volume_ratio_3bar"),4),
            "image_path":img_fp,"image_created_YN":"N","image_missing_reason":"not_yet_rendered" if img_key else "no_image_for_this_anchor",
        })
    return rows


def apply_image_results(anchor_rows, image_results) -> List[Dict]:
    for row in anchor_rows:
        img_key = _IMAGE_KEYS.get(row.get("anchor_code",""))
        if img_key is None: continue
        r = image_results.get(img_key)
        if r is None: continue
        if r.get("created"):
            row["image_created_YN"] = "Y"; row["image_missing_reason"] = ""
        else:
            row["image_created_YN"] = "N"; row["image_missing_reason"] = r.get("reason","render_failed")
    return anchor_rows


# ---------------------------------------------------------------------------
# Research verdict helper
# ---------------------------------------------------------------------------

def _classify_research_verdict(struct_q, part_pat, data_ok):
    if not data_ok: return "unclear"
    if struct_q in ("clean_base_break","exhaustion_spike") and part_pat in ("top_leads_crowd_late","top_and_crowd_align"):
        return "setup_signature_candidate"
    if struct_q in ("repeated_test_then_break","shallow_pullback_continuation"):
        return "worth_monitoring"
    return "skip"


# ---------------------------------------------------------------------------
# New helpers for canonical case dataset fields
# ---------------------------------------------------------------------------

def _derive_anchor_quality_flag(anchors) -> str:
    p2 = anchors.p2
    if p2 is None:
        return "LOW"
    if (p2.break_quality_band in ("clean", "strong")
            and (p2.auto_confidence or 0) >= 0.6
            and not p2.fallback_used
            and anchors.p3 and not anchors.p3.fallback_used):
        return "HIGH"
    if (p2.break_quality_band in ("clean", "strong")
            or ((p2.auto_confidence or 0) >= 0.4 and not p2.fallback_used)):
        return "MEDIUM"
    return "LOW"


def _derive_anchor_conflict_flag(anchors) -> str:
    for ap in [anchors.p0, anchors.p1, anchors.p2, anchors.p3]:
        if ap and ap.fallback_used:
            return "Y"
    return "N"


def _derive_anchor_validity_reason(anchors) -> str:
    notes = []
    if anchors.p1 and anchors.p1.fallback_used:
        notes.append("p1_no_ignition")
    if anchors.p2 and anchors.p2.fallback_used:
        notes.append("p2_no_clean_break")
    if anchors.p3 and anchors.p3.fallback_used:
        notes.append("p3_at_p2")
    return "_".join(notes) if notes else "all_anchors_auto_detected"


def _derive_p4_trigger(p4) -> str:
    if p4 is None:
        return "not_reached"
    if p4.fallback_used:
        return "timeout"
    conf = p4.auto_confidence or 0
    if conf >= 0.75:
        return "structure_loss"
    if conf >= 0.65:
        return "expansion_cool"
    return "timeout"


def _derive_intervention_confidence(
    decision_grade: str,
    fit_confidence: str,
    data_confidence: str,
    caution_flag: str,
    anchor_conflict_flag: str,
) -> str:
    grade = decision_grade or ""
    fit   = fit_confidence or ""
    _ord  = ["HIGH", "MEDIUM", "LOW"]

    base = fit if (grade == "OLD_STRATEGY_IMPROVEMENT_CANDIDATE" and fit in _ord) else "LOW"
    if grade == "OLD_STRATEGY_IMPROVEMENT_CANDIDATE" and base == "LOW":
        base = "MEDIUM"  # default for improvement candidates without explicit fit

    downgrade = (
        data_confidence == "LOW"
        or caution_flag == "Y"
        or anchor_conflict_flag == "Y"
    )
    if downgrade:
        idx  = _ord.index(base) if base in _ord else 2
        base = _ord[min(idx + 1, 2)]

    # Cap at MEDIUM unless explicit OLD_IMPROVEMENT + HIGH fit + no downgrade triggers
    if base == "HIGH" and not (
        grade == "OLD_STRATEGY_IMPROVEMENT_CANDIDATE"
        and fit == "HIGH"
        and not downgrade
    ):
        base = "MEDIUM"

    return base
