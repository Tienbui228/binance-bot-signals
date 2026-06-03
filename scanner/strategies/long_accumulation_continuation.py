"""Strategy detection for long_accumulation_continuation.

Detects concentrated top-trader positioning with OI confirmation and retail cap.
All 5 hard gates must pass for setup_detected=True. Score is gate4.
"""

import logging

from scanner.strategies._accumulation_features import compute_accumulation_features

_logger = logging.getLogger("strategy.long_accumulation_continuation")

# Canonical regime label strings emitted by scanner/regime/classifier.py
_REGIME_BULL     = "trend_continuation_friendly"
_REGIME_BEARISH  = "broad_weakness_sell_pressure"
_REGIME_NEUTRAL  = "unclear_mixed"

_BULL_SCORE    = 30
_NEUTRAL_SCORE = 10
_BEARISH_SCORE = 0


def _score_regime(regime_label: str, config: dict) -> int:
    """Map regime_label to BTC regime score component (0, 10, or 30)."""
    score_map = config.get("btc_regime_score_map", {})
    bull_labels    = score_map.get("bull", [_REGIME_BULL])
    bearish_labels = score_map.get("bearish", [_REGIME_BEARISH])

    if not isinstance(bull_labels, list):
        bull_labels = [_REGIME_BULL]
    if not isinstance(bearish_labels, list):
        bearish_labels = [_REGIME_BEARISH]

    if regime_label in bull_labels:
        return _BULL_SCORE
    if regime_label in bearish_labels:
        return _BEARISH_SCORE
    if regime_label not in ("", None):
        if regime_label not in (_REGIME_NEUTRAL,):
            _logger.warning("[acc_cont] unrecognized regime_label=%r — using neutral score", regime_label)
    return _NEUTRAL_SCORE


def _score_gap(gap_now: float) -> int:
    if gap_now >= 0.20:  return 30
    if gap_now >= 0.15:  return 22
    if gap_now >= 0.10:  return 14
    if gap_now >= 0.05:  return 7
    return 0


def _score_pos_now(pos_now: float) -> int:
    if pos_now >= 0.70:  return 20
    if pos_now >= 0.65:  return 15
    if pos_now >= 0.60:  return 10
    if pos_now >= 0.55:  return 5
    return 0


def _score_pos_trend(pos_trend_3v14: float, pos_recovery_from_min: float) -> int:
    if pos_trend_3v14 > 0.02 and pos_recovery_from_min >= 0.03:  return 20
    if pos_trend_3v14 > 0.01:                                     return 15
    if pos_trend_3v14 > 0.0:                                      return 10
    if pos_trend_3v14 > -0.01:                                    return 5
    return 0


def _participation_tag(pos_now: float, gap_now: float, pos_trend_3v14: float) -> str:
    """Map positioning structure to a participation label for format_signal."""
    if pos_now >= 0.68 and gap_now >= 0.15:
        return "concentrated_loaded"
    if pos_now >= 0.68:
        return "loaded"
    if gap_now >= 0.15 and pos_trend_3v14 > 0:
        return "concentrated_building"
    if pos_trend_3v14 > 0:
        return "building"
    return "weak_structure"


def detect_long_accumulation_continuation(
    symbol: str,
    series_pos: list,
    series_acct: list,
    series_retail: list,
    series_taker: list,
    series_oi: list,
    oi_delta_1h_pct: float,
    regime_label: str,
    config: dict,
    series_close: list | None = None,
    funding_8h_pct: float = 0.0,
) -> dict:
    """Detect long_accumulation_continuation setup.

    Returns the full output contract dict regardless of gate outcome.
    setup_detected=False when insufficient_history or any of the 5 gates fails.
    Score is computed even when gates fail (diagnostic).
    """
    pos_trend_min   = float(config.get("pos_trend_min",  0.0))
    pos_now_high    = float(config.get("pos_now_high",   0.68))
    pos_now_min     = float(config.get("pos_now_min",    0.65))
    oi_delta_min    = float(config.get("oi_delta_min_pct", 5.0))
    retail_max      = float(config.get("retail_max",     0.70))
    strong_min      = int(config.get("quality_band_strong_min",   70))
    moderate_min    = int(config.get("quality_band_moderate_min", 40))
    gate_min_score  = int((config.get("gate") or {}).get("min_score", 60))
    gates_v201_enabled   = bool(config.get("gates_v201_enabled", False))
    price_vs_base_min    = float(config.get("price_vs_baseline_min", -0.02))
    price_trend_7v30_min = float(config.get("price_trend_7v30_min",  -0.02))
    funding_8h_pct_max   = float(config.get("funding_8h_pct_max",    0.03))

    # ── Feature computation ─────────────────────────────────────────────────
    feat_result = compute_accumulation_features(
        series_pos, series_acct, series_retail, series_taker, series_oi,
        series_close=series_close,
    )

    insufficient = feat_result["insufficient_history"]
    partial      = feat_result["partial_history"]

    if insufficient:
        return {
            "strategy":          "long_accumulation_continuation",
            "strategy_family":   "long_accumulation_continuation",
            "setup_detected":    False,
            "setup_quality_band": "WEAK",
            "setup_reason_tags": ["insufficient_history"],
            "accumulation_score": 0,
            "score_breakdown":   {"gap": 0, "pos_now": 0, "pos_trend": 0, "btc_regime": 0},
            "btc_regime_label":  regime_label,
            "insufficient_history": True,
            "partial_history":   False,
            # All feature fields zeroed — not computed
            "pos_now": 0.0, "acct_now": 0.0, "gap_now": 0.0,
            "retail_now": 0.0, "taker_now": 0.0,
            "pos_slope_30d": 0.0, "gap_slope_30d": 0.0, "retail_slope_30d": 0.0,
            "pos_trend_3v14": 0.0, "oi_trend_3v14": 0.0,
            "pos_min_30d": 0.0, "pos_recovery_from_min": 0.0,
            "retail_min_30d": 0.0, "retail_recovery": 0.0,
            "taker_7d_avg": 0.0,
            "oi_delta_1h_pct": oi_delta_1h_pct,
            "price_vs_baseline": 0.0,
            "price_trend_7v30":  0.0,
            "funding_8h_pct":    funding_8h_pct,
        }

    f = feat_result["features"]
    pos_now            = f["pos_now"]
    acct_now           = f["acct_now"]
    gap_now            = f["gap_now"]
    retail_now         = f["retail_now"]
    taker_now          = f["taker_now"]
    pos_slope_30d      = f["pos_slope_30d"]
    gap_slope_30d      = f["gap_slope_30d"]
    retail_slope_30d   = f["retail_slope_30d"]
    pos_trend_3v14     = f["pos_trend_3v14"]
    oi_trend_3v14      = f["oi_trend_3v14"]
    pos_min_30d        = f["pos_min_30d"]
    pos_recovery_from_min = f["pos_recovery_from_min"]
    retail_min_30d     = f["retail_min_30d"]
    retail_recovery    = f["retail_recovery"]
    taker_7d_avg       = f["taker_7d_avg"]
    price_vs_baseline  = f.get("price_vs_baseline", 0.0)
    price_trend_7v30   = f.get("price_trend_7v30",  0.0)

    # ── Score (always computed) ─────────────────────────────────────────────
    s_gap      = _score_gap(gap_now)
    s_pos_now  = _score_pos_now(pos_now)
    s_pos_trnd = _score_pos_trend(pos_trend_3v14, pos_recovery_from_min)
    s_regime   = _score_regime(regime_label, config)

    score = s_gap + s_pos_now + s_pos_trnd + s_regime
    score = max(0, min(100, score))

    score_breakdown = {
        "gap":        s_gap,
        "pos_now":    s_pos_now,
        "pos_trend":  s_pos_trnd,
        "btc_regime": s_regime,
    }

    if score >= strong_min:
        quality_band = "STRONG"
    elif score >= moderate_min:
        quality_band = "MODERATE"
    else:
        quality_band = "WEAK"

    # ── Hard gates ──────────────────────────────────────────────────────────
    gate1 = (pos_trend_3v14 > pos_trend_min) or (pos_now >= pos_now_high)
    gate2 = oi_delta_1h_pct >= oi_delta_min
    gate3 = retail_now < retail_max
    gate4 = score >= gate_min_score  # score >= gate.min_score from config (default 60)
    gate5 = pos_now >= pos_now_min   # whale position floor (default 0.65)

    reason_tags: list[str] = []
    failed_gates: list[str] = []

    if gate1:
        reason_tags.append("gate1_pass")
    else:
        failed_gates.append("gate1_fail_whale")

    if gate2:
        reason_tags.append("gate2_pass")
    else:
        failed_gates.append("gate2_fail_oi")

    if gate3:
        reason_tags.append("gate3_pass")
    else:
        failed_gates.append("gate3_fail_retail_fomo")

    if gate4:
        reason_tags.append("gate4_pass")
    else:
        failed_gates.append("gate4_fail_score_too_low")

    if gate5:
        reason_tags.append("gate5_pass")
    else:
        failed_gates.append("gate5_fail_pos_too_low")

    reason_tags.extend(failed_gates)

    # ── v2.0.1 gates (Gate T: price-trend-floor, Gate F: funding-neutral) ───
    gate6 = price_vs_baseline >= price_vs_base_min     # Gate T: 30d trend floor
    gate7 = price_trend_7v30  >= price_trend_7v30_min  # Gate T: 7d momentum floor
    gate8 = funding_8h_pct    <  funding_8h_pct_max    # Gate F: crowded-long cap

    # Tags always written (shadow + live) so CSV can diff would-be-blocked set
    if gate6:
        reason_tags.append("gate6_pass")
    else:
        reason_tags.append("gate6_fail_v201_price_30d")
    if gate7:
        reason_tags.append("gate7_pass")
    else:
        reason_tags.append("gate7_fail_v201_price_7d")
    if gate8:
        reason_tags.append("gate8_pass")
    else:
        reason_tags.append("gate8_fail_v201_funding_crowded")

    if gates_v201_enabled:
        setup_detected = gate1 and gate2 and gate3 and gate4 and gate5 and gate8
    else:
        setup_detected = gate1 and gate2 and gate3 and gate4 and gate5

    if setup_detected:
        # Key=value tags required by format_signal (parsed from reason_tags string)
        participation = _participation_tag(pos_now, gap_now, pos_trend_3v14)
        reason_tags.append(f"participation={participation}")
        # oi_vs_baseline: no leading + (format_signal adds it at line 1314)
        reason_tags.append(f"oi_vs_baseline={oi_delta_1h_pct:.1f}%")
        reason_tags.append("vol_vs_baseline=n/a")
        reason_tags.append(f"structure_score={score}")
        # OI multi-day trend shadow tag — diagnostic only, never blocks setup_detected
        if oi_trend_3v14 > 1.0:
            reason_tags.append("oi_trend_pass")
        else:
            reason_tags.append("oi_trend_fail")

    return {
        "strategy":          "long_accumulation_continuation",
        "strategy_family":   "long_accumulation_continuation",
        "setup_detected":    setup_detected,
        "setup_quality_band": quality_band,
        "setup_reason_tags": reason_tags,
        "accumulation_score": score,
        "score_breakdown":   score_breakdown,
        "btc_regime_label":  regime_label,
        "insufficient_history": False,
        "partial_history":   partial,
        # Raw feature fields
        "pos_now":            pos_now,
        "acct_now":           acct_now,
        "gap_now":            gap_now,
        "retail_now":         retail_now,
        "taker_now":          taker_now,
        "pos_slope_30d":      pos_slope_30d,
        "gap_slope_30d":      gap_slope_30d,
        "retail_slope_30d":   retail_slope_30d,
        "pos_trend_3v14":     pos_trend_3v14,
        "oi_trend_3v14":      oi_trend_3v14,
        "pos_min_30d":        pos_min_30d,
        "pos_recovery_from_min": pos_recovery_from_min,
        "retail_min_30d":     retail_min_30d,
        "retail_recovery":    retail_recovery,
        "taker_7d_avg":       taker_7d_avg,
        "oi_delta_1h_pct":    oi_delta_1h_pct,
        "price_vs_baseline":  price_vs_baseline,
        "price_trend_7v30":   price_trend_7v30,
        "funding_8h_pct":     funding_8h_pct,
    }
