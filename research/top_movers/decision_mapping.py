"""
research/top_movers/decision_mapping.py

Deterministic classification and mapping logic:
  - flow_phase_code (9 values, exact order from spec)
  - resolution_label (6 values, exact order from spec)
  - flow_phase → participation_pattern mapping
  - post-P2 outcome computation (extends into next UTC day as needed)
  - strategy mapping
  - eligibility fields (research_eligible_YN, full_visual_complete_YN, etc.)
  - caution_flag (narrow, evidence-based)
  - decision_grade
  - confidence fields
"""

from typing import Dict, List, Optional

_EPS = 1e-12

# ---------------------------------------------------------------------------
# flow_phase_code — 9 values, exact classification order from spec
# ---------------------------------------------------------------------------

FLOW_PHASE_CODES = [
    "LARGE_LEAD_CROWD_ABSENT", "LARGE_LEAD_CROWD_EARLY", "BOTH_ALIGNED_HEALTHY",
    "CROWD_CHASE_LATE", "CROWD_OVERHEATED", "LARGE_FADE_CROWD_ENTER",
    "FLOW_DIVERGENCE_WARNING", "LOW_PARTICIPATION_BREAK", "UNCLEAR_FLOW",
]


def classify_flow_phase_code(large, crowd, overheat, div, flow) -> str:
    if all(v is None for v in [large, crowd, overheat, div, flow]):
        return "UNCLEAR_FLOW"
    large = large or 0.0; crowd = crowd or 0.0; overheat = overheat or 0.0
    div = div or 0.0; flow = flow or 0.0
    if overheat >= 0.80:                                        return "CROWD_OVERHEATED"
    elif abs(large) < 0.25 and abs(crowd) < 0.25 and abs(flow) < 0.25: return "LOW_PARTICIPATION_BREAK"
    elif (div <= -0.75) or (large * crowd < 0):                return "FLOW_DIVERGENCE_WARNING"
    elif large >= 0.75 and crowd < 0.25:                       return "LARGE_LEAD_CROWD_ABSENT"
    elif large >= 0.75 and 0.25 <= crowd < 0.75 and overheat < 0.80: return "LARGE_LEAD_CROWD_EARLY"
    elif large >= 0.50 and crowd >= 0.50 and overheat < 0.80:  return "BOTH_ALIGNED_HEALTHY"
    elif crowd >= 0.75 and div <= 0 and overheat < 0.80:       return "CROWD_CHASE_LATE"
    elif large <= -0.25 and crowd >= 0.25:                     return "LARGE_FADE_CROWD_ENTER"
    else:                                                       return "UNCLEAR_FLOW"


# ---------------------------------------------------------------------------
# flow_phase → participation_pattern mapping (v2 taxonomy)
# ---------------------------------------------------------------------------

_FLOW_TO_PARTICIPATION: Dict[str, str] = {
    "LARGE_LEAD_CROWD_ABSENT":  "top_leads_crowd_late",
    "LARGE_LEAD_CROWD_EARLY":   "top_leads_crowd_late",
    "BOTH_ALIGNED_HEALTHY":     "top_and_crowd_align",
    "CROWD_CHASE_LATE":         "crowd_chase_dominant",
    "CROWD_OVERHEATED":         "crowd_chase_dominant",
    "LARGE_FADE_CROWD_ENTER":   "divergence_warning",
    "FLOW_DIVERGENCE_WARNING":  "divergence_warning",
    "LOW_PARTICIPATION_BREAK":  "low_participation_move",
    "UNCLEAR_FLOW":             "unclear_participation",
}

PARTICIPATION_PATTERNS = [
    "top_leads_crowd_late", "top_and_crowd_align", "crowd_chase_dominant",
    "divergence_warning", "low_participation_move", "unclear_participation",
]


def flow_phase_to_participation_pattern(flow_phase_code: str) -> str:
    return _FLOW_TO_PARTICIPATION.get(flow_phase_code, "unclear_participation")


# ---------------------------------------------------------------------------
# resolution_label — 6 values, exact order from spec
# ---------------------------------------------------------------------------

RESOLUTION_LABELS = [
    "PERSISTENT_CONTINUATION", "FAST_THEN_STALL", "FAST_THEN_REVERSE",
    "SLOW_GRIND", "NO_FOLLOWTHROUGH", "FAILED_EARLY",
]


def classify_resolution_label(f30, a30, f1h, a1h, f4h, a4h, close1h="N", reclaim4h="N") -> str:
    if all(v is None for v in [f30, a30, f1h, a1h, f4h, a4h]):
        return "NO_FOLLOWTHROUGH"
    f30 = f30 or 0.0; a30 = a30 or 0.0; f1h = f1h or 0.0
    a1h = a1h or 0.0; f4h = f4h or 0.0; a4h = a4h or 0.0
    if (a30 >= f30 and close1h == "N") or (f1h < 0.8 and a1h >= 1.5):  return "FAILED_EARLY"
    elif f30 >= 2.0 and f1h >= 2.5 and (reclaim4h == "Y" or a4h >= max(2.0, 0.6 * f4h)): return "FAST_THEN_REVERSE"
    elif f30 >= 2.0 and f1h >= 2.5 and f4h <= 1.20 * f1h:              return "FAST_THEN_STALL"
    elif f1h >= 2.5 and f4h >= 1.20 * f1h and close1h == "Y" and a4h <= max(2.0, 0.5 * f4h): return "PERSISTENT_CONTINUATION"
    elif f30 < 2.0 and f4h >= 2.5 and a4h <= max(2.0, 0.6 * f4h):      return "SLOW_GRIND"
    else:                                                                 return "NO_FOLLOWTHROUGH"


# ---------------------------------------------------------------------------
# Post-P2 outcome computation
# ---------------------------------------------------------------------------

def compute_post_p2_outcomes(bars_5m_all, p2_ts_ms, p2_close, side) -> Dict:
    """
    Retrospective measurement from P2 on historical bars.
    Bars may extend into next UTC day when P2 is late in the research day.
    """
    if not bars_5m_all or p2_close <= 0:
        return _empty_outcomes()

    ms_30m = 30 * 60 * 1000; ms_1h = 60 * 60 * 1000; ms_4h = 4 * 60 * 60 * 1000
    end_30m = p2_ts_ms + ms_30m; end_1h = p2_ts_ms + ms_1h; end_4h = p2_ts_ms + ms_4h

    post = [b for b in bars_5m_all if b["open_time"] > p2_ts_ms]
    b30 = [b for b in post if b["open_time"] <= end_30m]
    b1h = [b for b in post if b["open_time"] <= end_1h]
    b4h = [b for b in post if b["open_time"] <= end_4h]

    def fav(bars): 
        if not bars: return None
        return max((b["high"]-p2_close)/p2_close*100 if side=="LONG" else (p2_close-b["low"])/p2_close*100 for b in bars)
    def adv(bars):
        if not bars: return None
        return max((p2_close-b["low"])/p2_close*100 if side=="LONG" else (b["high"]-p2_close)/p2_close*100 for b in bars)

    f30=fav(b30); a30=adv(b30); f1h=fav(b1h); a1h=adv(b1h); f4h=fav(b4h); a4h=adv(b4h)

    t2=t3=None
    for b in post:
        if b["open_time"] > end_4h: break
        el = (b["open_time"] - p2_ts_ms) / 60000.0
        mv = (b["high"]-p2_close)/p2_close*100 if side=="LONG" else (p2_close-b["low"])/p2_close*100
        if t2 is None and mv >= 2.0: t2 = round(el, 1)
        if t3 is None and mv >= 3.0: t3 = round(el, 1)

    bar1h = next((b for b in reversed(b1h) if b["open_time"] <= end_1h), None)
    if bar1h:
        close1h = "Y" if (bar1h["close"]>p2_close if side=="LONG" else bar1h["close"]<p2_close) else "N"
    else: close1h = "N"

    reclaim4h = "N"; seen_fav = False
    for b in b4h:
        if side=="LONG":
            if b["high"] > p2_close*1.015: seen_fav = True
            if seen_fav and b["close"] < p2_close: reclaim4h = "Y"; break
        else:
            if b["low"] < p2_close*0.985: seen_fav = True
            if seen_fav and b["close"] > p2_close: reclaim4h = "Y"; break

    r = lambda v: round(v,4) if v is not None else None
    return {
        "future_30m_max_favor_pct": r(f30), "future_30m_max_adverse_pct": r(a30),
        "future_1h_max_favor_pct": r(f1h),  "future_1h_max_adverse_pct": r(a1h),
        "future_4h_max_favor_pct": r(f4h),  "future_4h_max_adverse_pct": r(a4h),
        "time_to_2pct_favor_min": t2,         "time_to_3pct_favor_min": t3,
        "close_above_break_after_1h_YN": close1h, "reclaim_break_4h_YN": reclaim4h,
    }


def _empty_outcomes() -> Dict:
    return {k: None for k in [
        "future_30m_max_favor_pct","future_30m_max_adverse_pct",
        "future_1h_max_favor_pct","future_1h_max_adverse_pct",
        "future_4h_max_favor_pct","future_4h_max_adverse_pct",
        "time_to_2pct_favor_min","time_to_3pct_favor_min",
    ]} | {"close_above_break_after_1h_YN":"N", "reclaim_break_4h_YN":"N"}


# ---------------------------------------------------------------------------
# Strategy mapping
# ---------------------------------------------------------------------------

def map_to_existing_strategy(side, move_class, pre_move_signature, participation_pattern,
                              structural_quality, flow_phase_code, resolution_label) -> Dict:
    maps_to = "unclear"; fit = "LOW"; reason = ""; layer = "not_applicable"
    hyp = "do_not_change_existing_strategy"; imp_yn = "N"
    new_cand = "N"; cand_fam = ""; cand_trig = ""; cand_env = ""; cand_inv = ""
    new_conf = "LOW"; action = "descriptive_only"

    if side == "LONG":
        if move_class in ("clean_continuation_pump","squeeze_then_expansion","slow_build_then_break"):
            if structural_quality in ("clean_base_break","repeated_test_then_break","shallow_pullback_continuation") \
                    and participation_pattern in ("top_leads_crowd_late","top_and_crowd_align"):
                maps_to = "long_breakout_retest"
                fit = "HIGH" if structural_quality == "clean_base_break" else "MEDIUM"
                reason = f"{move_class}_{participation_pattern}"
                layer = _imp_layer(flow_phase_code, pre_move_signature, participation_pattern)
                hyp = _imp_hyp(flow_phase_code, pre_move_signature)
                imp_yn = "Y" if layer != "not_strategy_problem" else "N"
                action = "improve_existing" if imp_yn == "Y" else "keep_observing"
            elif participation_pattern == "crowd_chase_dominant":
                maps_to = "long_breakout_retest"; fit = "LOW"
                reason = "crowd_chase_lacks_large_confirmation"; layer = "delivery_filter"
                hyp = "tighten_filter_for_crowd_chase_dominant"; imp_yn = "Y"; action = "improve_existing"
            else:
                maps_to = "none"; reason = "no_consistent_setup_structure"
        elif move_class in ("blowoff_pump","noisy_unstructured_move","fake_break_then_reverse"):
            maps_to = "none"; reason = f"{move_class}_does_not_map"

    elif side == "SHORT":
        if move_class in ("clean_breakdown_dump","exhaustion_dump"):
            if structural_quality in ("clean_base_break","repeated_test_then_break","exhaustion_spike") \
                    and participation_pattern in ("top_leads_crowd_late","top_and_crowd_align","crowd_chase_dominant"):
                maps_to = "short_exhaustion_retest"
                fit = "HIGH" if structural_quality in ("clean_base_break","exhaustion_spike") else "MEDIUM"
                reason = f"{move_class}_{participation_pattern}"
                layer = _imp_layer(flow_phase_code, pre_move_signature, participation_pattern)
                hyp = _imp_hyp(flow_phase_code, pre_move_signature)
                imp_yn = "Y" if layer != "not_strategy_problem" else "N"
                action = "improve_existing" if imp_yn == "Y" else "keep_observing"
            else:
                maps_to = "none"; reason = "insufficient_exhaustion_structure"
        else:
            maps_to = "none"; reason = f"{move_class}_does_not_map"

    # New strategy candidate
    if maps_to in ("none","unclear") and structural_quality not in ("dirty_break","runaway_no_base") \
            and flow_phase_code not in ("UNCLEAR_FLOW","LOW_PARTICIPATION_BREAK"):
        new_cand = "Y"
        cand_fam, cand_trig, cand_env, cand_inv = _new_strat_fields(side, move_class, flow_phase_code)
        new_conf = "MEDIUM" if participation_pattern in ("top_leads_crowd_late","top_and_crowd_align") else "LOW"
        action = "create_new" if new_conf == "MEDIUM" else "keep_observing"

    return {
        "maps_to_existing_strategy_family": maps_to,
        "existing_strategy_fit_confidence": fit,
        "existing_strategy_mapping_reason": reason,
        "improvement_target_layer": layer,
        "improvement_hypothesis": hyp,
        "existing_strategy_improvement_candidate_YN": imp_yn,
        "new_strategy_candidate_flag": new_cand,
        "candidate_strategy_family_name": cand_fam,
        "candidate_trigger_description": cand_trig,
        "candidate_environment": cand_env,
        "candidate_invalid_pattern": cand_inv,
        "new_strategy_confidence": new_conf,
        "strategy_action_type": action,
    }


def _imp_layer(fp, pre, part):
    if fp == "LARGE_LEAD_CROWD_EARLY":  return "wait_window"
    if fp in ("CROWD_OVERHEATED","CROWD_CHASE_LATE"): return "regime_filter"
    if fp == "BOTH_ALIGNED_HEALTHY":    return "not_strategy_problem"
    if fp == "FLOW_DIVERGENCE_WARNING": return "delivery_filter"
    if fp == "LARGE_FADE_CROWD_ENTER":  return "invalidation"
    if pre == "low_signal_premove":     return "detect"
    if part == "crowd_chase_dominant":  return "delivery_filter"
    return "not_applicable"


def _imp_hyp(fp, pre):
    if fp == "LARGE_LEAD_CROWD_EARLY":  return "extend_wait_when_top_leads_crowd_late"
    if fp in ("CROWD_OVERHEATED","CROWD_CHASE_LATE"): return "tighten_filter_for_crowd_chase_dominant"
    if fp == "BOTH_ALIGNED_HEALTHY":    return "do_not_change_existing_strategy"
    if fp == "FLOW_DIVERGENCE_WARNING": return "tighten_filter_for_crowd_chase_dominant"
    if fp == "LARGE_FADE_CROWD_ENTER":  return "relax_invalidation_for_clean_base_break"
    if pre == "low_signal_premove":     return "relax_invalidation_for_clean_base_break"
    return "do_not_change_existing_strategy"


def _new_strat_fields(side, move_class, fp):
    if side == "LONG" and fp in ("LARGE_LEAD_CROWD_ABSENT","LARGE_LEAD_CROWD_EARLY"):
        return ("long_runaway_oi_expansion",
                "tight_base_with_top_led_oi_expansion_into_break",
                "trend_continuation_or_unclear_mixed",
                "crowd_overheated_or_divergence_warning")
    if side == "SHORT" and fp in ("CROWD_OVERHEATED","CROWD_CHASE_LATE"):
        return ("short_crowd_trap_breakdown",
                "crowd_overheated_top_exhaustion_into_breakdown",
                "post_pump_or_unclear_mixed",
                "large_participant_supporting_crowd")
    if fp == "BOTH_ALIGNED_HEALTHY":
        nm = f"{'long' if side=='LONG' else 'short'}_momentum_continuation"
        return (nm, "both_large_and_crowd_aligned_at_break",
                "trend_continuation_friendly", "flow_divergence_or_overheat")
    nm = f"research_only_{side.lower()}_{move_class[:12]}"
    return (nm, "under_investigation", "unclear_mixed", "weak_proxy_data")


# ---------------------------------------------------------------------------
# Eligibility fields (4 new case-level fields — fix 4)
# ---------------------------------------------------------------------------

def compute_eligibility_fields(
    data_quality_ok: bool,
    anchors,
    image_results: Dict,
    proxy_completeness: int,
    outcome_completeness: int,
) -> Dict:
    """
    research_eligible_YN  = Y if data_quality_ok AND P0+P2+P3 anchors detected
    full_visual_complete_YN = Y if all 5 images created
    proxy_complete_YN     = Y if proxy_completeness >= 50
    outcome_complete_YN   = Y if all 3 horizon fields present (outcome_completeness >= 67)
    """
    has_anchors = (
        getattr(anchors, "p0", None) is not None
        and getattr(anchors, "p2", None) is not None
        and getattr(anchors, "p3", None) is not None
    )
    eligible = "Y" if (data_quality_ok and has_anchors) else "N"
    imgs_ok  = sum(1 for r in image_results.values() if r.get("created"))
    full_vis = "Y" if imgs_ok == 5 else "N"
    proxy_ok = "Y" if proxy_completeness >= 50 else "N"
    outcome_ok = "Y" if outcome_completeness >= 67 else "N"

    return {
        "research_eligible_YN": eligible,
        "full_visual_complete_YN": full_vis,
        "proxy_complete_YN": proxy_ok,
        "outcome_complete_YN": outcome_ok,
    }


# ---------------------------------------------------------------------------
# Caution flag — narrow, evidence-based (fix 5)
# ---------------------------------------------------------------------------

def compute_caution_flag_narrow(
    proxy_complete_yn: str,
    full_visual_complete_yn: str,
    outlier_risk_flag: str,
    structural_quality: str,
    participation_pattern: str,
    decision_grade: str,
) -> str:
    """
    caution_flag = Y only if:
      1. proxy_complete_YN = N
      2. full_visual_complete_YN = N
      3. outlier_dependency_flag = Y
      4. structural_quality = runaway_no_base
      5. participation_pattern = low_participation_move
      6. decision_grade = NOT_RELIABLE_YET
    """
    if (
        proxy_complete_yn == "N"
        or full_visual_complete_yn == "N"
        or outlier_risk_flag == "Y"
        or structural_quality == "runaway_no_base"
        or participation_pattern == "low_participation_move"
        or decision_grade == "NOT_RELIABLE_YET"
    ):
        return "Y"
    return "N"


# ---------------------------------------------------------------------------
# Confidence fields
# ---------------------------------------------------------------------------

def compute_confidence_fields(proxy_snapshot, image_results, outcomes, data_quality_ok, daily_return_pct) -> Dict:
    proxy_fields = [
        "large_participant_proxy","crowd_participation_proxy","crowd_overheat_proxy",
        "top_acc_imbalance","top_pos_imbalance","global_acc_imbalance",
        "taker_imbalance","basis_rate","oi_delta_3bar_pct",
    ]
    proxy_avail = sum(1 for f in proxy_fields if proxy_snapshot.get(f) is not None)
    proxy_pct = round(100.0 * proxy_avail / len(proxy_fields))

    imgs_ok = sum(1 for r in image_results.values() if r.get("created"))
    img_pct = round(100.0 * imgs_ok / max(len(image_results), 1))

    out_flds = ["future_30m_max_favor_pct","future_1h_max_favor_pct","future_4h_max_favor_pct"]
    out_avail = sum(1 for f in out_flds if outcomes.get(f) is not None)
    out_pct = round(100.0 * out_avail / len(out_flds))

    if proxy_pct >= 60 and out_pct >= 67 and data_quality_ok:
        conf = "HIGH"
    elif proxy_pct < 30 or out_pct < 33 or not data_quality_ok:
        conf = "LOW"
    else:
        conf = "MEDIUM"

    if data_quality_ok and proxy_pct >= 60:   dq = "CLEAN"
    elif data_quality_ok and proxy_pct >= 30: dq = "PARTIAL"
    else:                                     dq = "WEAK"

    outlier = "Y" if abs(daily_return_pct) > 20 or dq == "WEAK" else "N"

    return {
        "proxy_completeness_score": proxy_pct,
        "image_completeness_score": img_pct,
        "outcome_completeness_score": out_pct,
        "classification_confidence": conf,
        "data_quality_flag": dq,
        "outlier_risk_flag": outlier,
    }


# ---------------------------------------------------------------------------
# Decision grade
# ---------------------------------------------------------------------------

def compute_decision_grade(strategy_action_type, classification_confidence, proxy_completeness, outcome_completeness) -> str:
    if classification_confidence == "LOW" or proxy_completeness < 30 or outcome_completeness < 33:
        return "NOT_RELIABLE_YET"
    return {
        "improve_existing": "OLD_STRATEGY_IMPROVEMENT_CANDIDATE",
        "create_new":       "NEW_STRATEGY_THESIS_CANDIDATE",
        "keep_observing":   "KEEP_TRACKING",
    }.get(strategy_action_type, "DESCRIPTIVE_ONLY")


# ---------------------------------------------------------------------------
# Case takeaways
# ---------------------------------------------------------------------------

def generate_case_takeaways(flow_phase_code, resolution_label, move_class, strategy_action_type,
                             maps_to, improvement_layer) -> Dict:
    t1 = f"{flow_phase_code} → {resolution_label} | {move_class}"
    if strategy_action_type == "improve_existing":
        t2 = f"Maps to {maps_to} — target layer: {improvement_layer}"
    elif strategy_action_type == "create_new":
        t2 = "Potential new strategy thesis — needs multi-day support"
    else:
        t2 = ""
    return {"case_takeaway_1": t1, "case_takeaway_2": t2}


# ---------------------------------------------------------------------------
# Wave 2 — L6 timing / staleness flags (classify_timing_flags)
# ---------------------------------------------------------------------------

def classify_timing_flags(
    bars_to_retest: Optional[int],
    bars_to_fail: Optional[int],
    p2_ts_ms: Optional[int],
    p4_ts_ms: Optional[int],
) -> Dict:
    """Classify late_retest_flag and stale_setup_flag from timing data.

    PROVISIONAL thresholds (first-pass, not grounded in backtest):
      late_retest_flag: Y if bars_to_retest > 24  (= 2h on 5m bars)
      stale_setup_flag: Y if P2→P4 elapsed > 72 bars (= 6h on 5m bars)

    Both flags default to "N" when input data is unavailable.
    """
    _5M_MS = 5 * 60 * 1000

    # late_retest_flag — PROVISIONAL threshold: 24 bars = 2h
    late_retest = "N"
    if bars_to_retest is not None and bars_to_retest > 24:
        late_retest = "Y"

    # stale_setup_flag — PROVISIONAL threshold: 72 bars = 6h from P2 to resolution
    stale = "N"
    if p2_ts_ms is not None and p4_ts_ms is not None and _5M_MS > 0:
        elapsed_bars = (p4_ts_ms - p2_ts_ms) / _5M_MS
        if elapsed_bars > 72:
            stale = "Y"

    return {
        "late_retest_flag": late_retest,
        "stale_setup_flag": stale,
    }


# ---------------------------------------------------------------------------
# Range expansion ratio
# ---------------------------------------------------------------------------

def compute_range_expansion_ratio(p3_bar, range_high, range_low, side) -> Optional[float]:
    if p3_bar is None: return None
    span = max(range_high - range_low, _EPS)
    ext = (p3_bar.get("high", range_high) - range_high) if side == "LONG" else (range_low - p3_bar.get("low", range_low))
    return round(max(ext, 0.0) / span, 4)
