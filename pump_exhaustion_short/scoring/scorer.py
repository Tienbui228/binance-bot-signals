from typing import Dict, Optional


def score_case(case: Dict) -> Dict:
    """Score a FAILED_RETEST_CONFIRMED case /10.

    First checks hard gates H1-H8. If any fail: return excluded result.
    Otherwise compute subscores A-E per spec section 7.
    Returns Group 10 fields.
    """
    # --- Hard gates (H1-H8) ---
    # H1: base_validity_flag
    if not case.get("base_validity_flag"):
        return _excluded("base_invalid")

    # H2: breakdown_confirmed
    if not case.get("breakdown_confirmed"):
        return _excluded("breakdown_not_confirmed")

    # H3: no false break reclaim
    if case.get("false_break_reclaim_fast_flag"):
        return _excluded("false_break_reclaim")

    # H4: room >= 15%
    room_pct = case.get("room_pct", 0.0) or 0.0
    if room_pct < 0.15:
        return _excluded("room_too_small")

    # H5: rr_conservative — only check if conservative_target_valid
    if case.get("conservative_target_valid"):
        rr = case.get("rr_conservative") or 0.0
        if rr < 1.5:
            return _excluded("rr_too_low")
    # if conservative_target_valid=False: rr_conservative is not_applicable, skip H5

    # H6: anchor_order_valid (computed by scan_loop before calling scorer)
    if not case.get("anchor_order_valid", False):
        return _excluded("invalid_anchor_order")

    # H7: signal_age_min <= 15
    age = case.get("signal_age_min")
    if age is not None and age > 15:
        return _excluded("insufficient_data")

    # H8: liquidity quote_vol_5m_median >= 10000 (checked in scan loop gate G_S4)
    # scorer trusts scan_loop already passed this

    # --- Scoring A-E ---
    score_pump = _score_pump(case)
    score_exhaustion = _score_exhaustion(case)
    score_breakdown = _score_breakdown(case)
    score_market = _score_market(case)
    score_delivery = _score_delivery(case)

    total = score_pump + score_exhaustion + score_breakdown + score_market + score_delivery
    score_max = 10

    if total >= 7:
        confidence = "HIGH"
    elif total >= 5:
        confidence = "MEDIUM"
    else:
        confidence = "LOW"

    return {
        "excluded": False,
        "exclusion_reason": None,
        "score_total": total,
        "score_max": score_max,
        "confidence_label": confidence,
        "score_pump_context": score_pump,
        "score_exhaustion": score_exhaustion,
        "score_breakdown_quality": score_breakdown,
        "score_market_pressure": score_market,
        "score_delivery": score_delivery,
    }


def _score_pump(case: Dict) -> int:
    s = 0
    if (case.get("pump_pct") or 0) >= 0.50:
        s += 1
    if (case.get("pump_vol_ratio") or 0) >= 5.0:
        s += 1
    return s


def _score_exhaustion(case: Dict) -> int:
    s = 0
    # post-peak avg vol < 30% of peak vol
    post_peak_avg = case.get("post_peak_avg_vol_12h")
    peak_vol = case.get("peak_vol")
    if post_peak_avg is not None and peak_vol and peak_vol > 0:
        if post_peak_avg < 0.30 * peak_vol:
            s += 1
    # cvd_proxy declining in 6h post-peak
    if case.get("cvd_proxy_1h_trend") == "declining":
        s += 1
    return s


def _score_breakdown(case: Dict) -> int:
    s = 0
    if (case.get("break_body_ratio") or 0) >= 0.45:
        s += 1
    if (case.get("break_volume_ratio") or 0) >= 1.5:
        s += 1
    if (case.get("immediate_followthrough_pct_3bar") or 0) >= 0.01:
        s += 1
    return s


def _score_market(case: Dict) -> int:
    s = 0
    if case.get("oi_regime_label") == "OI_EXPANSION_PRICE_DOWN":
        s += 1
    if (case.get("latest_funding_rate") or 0) > 0.0001:
        s += 1
    return s


def _score_delivery(case: Dict) -> int:
    entry = case.get("entry_price") or 0
    breakdown = case.get("breakdown_level") or 0
    if breakdown > 0 and entry > 0:
        if entry <= breakdown * 1.02:
            return 1
    return 0


def _excluded(reason: str) -> Dict:
    return {
        "excluded": True,
        "exclusion_reason": reason,
        "score_total": 0,
        "score_max": 10,
        "confidence_label": None,
        "score_pump_context": 0,
        "score_exhaustion": 0,
        "score_breakdown_quality": 0,
        "score_market_pressure": 0,
        "score_delivery": 0,
    }
