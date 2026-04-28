from typing import Dict, List, Optional, Tuple


def _vwap(bars: List[Dict]) -> float:
    total_vol = sum(b["volume"] for b in bars)
    if total_vol <= 0:
        return 0.0
    return sum(((b["high"] + b["low"] + b["close"]) / 3) * b["volume"] for b in bars) / total_vol


def _count_tests(bars: List[Dict], level: float, tol_pct: float = 0.01) -> int:
    count = 0
    for b in bars:
        if abs(b["low"] - level) / level <= tol_pct:
            count += 1
    return count


def detect_breakdown(bars_5m: List[Dict],
                     peak_time_ms: int,
                     peak_high: float,
                     cfg: Dict) -> Dict:
    """Detect breakdown below support level in post-peak 5m candles.

    Returns Group 5 fields + breakdown_confirmed, breakdown_candle_time.
    """
    scan_cfg = cfg.get("scan", {})
    body_ratio_min = scan_cfg.get("breakdown_body_ratio_min", 0.45)
    vol_ratio_min = scan_cfg.get("breakdown_vol_ratio_min", 1.0)
    taker_sell_min = scan_cfg.get("breakdown_taker_sell_ratio_min", 0.52)
    false_break_bars = scan_cfg.get("false_break_reclaim_bars", 3)

    # Filter: bars after peak within 24h
    cutoff_ms = peak_time_ms + 24 * 3_600_000
    post_peak = [b for b in bars_5m if b["open_time"] > peak_time_ms and b["open_time"] <= cutoff_ms]

    if len(post_peak) < 6:
        return _no_breakdown()

    # --- Find breakdown_level with priority ---
    breakdown_level, level_source, level_confidence = _find_breakdown_level(post_peak, peak_high)
    if breakdown_level is None:
        return _no_breakdown()

    # Count support tests before any break
    support_test_count = _count_tests(post_peak, breakdown_level)
    if level_source == "RANGE_LOW" and support_test_count >= 3:
        level_confidence = "HIGH"
    elif level_source in ("RANGE_LOW", "SWING_LOW"):
        level_confidence = "MEDIUM"
    else:
        level_confidence = "LOW"

    # --- Scan for breakdown candle ---
    breakdown_candle = None
    breakdown_candle_idx = None
    for i, bar in enumerate(post_peak):
        if bar["close"] >= breakdown_level:
            continue
        break_distance_pct = (breakdown_level - bar["close"]) / breakdown_level
        if break_distance_pct < 0.003:
            continue
        range_ = bar["high"] - bar["low"]
        body = bar["open"] - bar["close"]  # bearish: open > close
        body_ratio = body / range_ if range_ > 1e-10 else 0.0
        if body_ratio < body_ratio_min:
            continue
        # Volume vs prior 20
        prior_vols = [post_peak[j]["volume"] for j in range(max(0, i - 20), i)]
        avg_vol = sum(prior_vols) / len(prior_vols) if prior_vols else 0.0
        vol_ratio = bar["volume"] / avg_vol if avg_vol > 0 else 0.0
        if vol_ratio < vol_ratio_min:
            continue
        if bar.get("taker_sell_ratio", 0.5) < taker_sell_min:
            continue
        breakdown_candle = bar
        breakdown_candle_idx = i
        break

    if breakdown_candle is None:
        return {**_no_breakdown(),
                "breakdown_level": breakdown_level,
                "breakdown_level_source": level_source,
                "breakdown_level_confidence": level_confidence,
                "support_test_count_before_break": support_test_count}

    breakdown_time = breakdown_candle["open_time"]
    break_distance_pct = (breakdown_level - breakdown_candle["close"]) / breakdown_level
    range_ = breakdown_candle["high"] - breakdown_candle["low"]
    body = breakdown_candle["open"] - breakdown_candle["close"]
    break_body_ratio = body / range_ if range_ > 1e-10 else 0.0
    prior_vols = [post_peak[j]["volume"] for j in range(max(0, breakdown_candle_idx - 20), breakdown_candle_idx)]
    avg_vol = sum(prior_vols) / len(prior_vols) if prior_vols else 0.0
    break_volume_ratio = breakdown_candle["volume"] / avg_vol if avg_vol > 0 else 0.0

    # Follow-through
    after = post_peak[breakdown_candle_idx + 1:]
    ft_1bar = (breakdown_level - after[0]["close"]) / breakdown_level if after else None
    ft_3bar = (breakdown_level - min(b["close"] for b in after[:3])) / breakdown_level if len(after) >= 3 else None

    # False break reclaim
    false_break = any(b["close"] > breakdown_level for b in after[:false_break_bars])

    return {
        "breakdown_confirmed": True,
        "breakdown_level": breakdown_level,
        "breakdown_level_source": level_source,
        "breakdown_level_confidence": level_confidence,
        "support_test_count_before_break": support_test_count,
        "break_distance_pct": break_distance_pct,
        "break_body_ratio": break_body_ratio,
        "break_volume_ratio": break_volume_ratio,
        "immediate_followthrough_pct_1bar": ft_1bar,
        "immediate_followthrough_pct_3bar": ft_3bar,
        "false_break_reclaim_fast_flag": false_break,
        "breakdown_candle_time": breakdown_time,
    }


def _find_breakdown_level(post_peak: List[Dict],
                           peak_high: float) -> Tuple[Optional[float], str, str]:
    # Priority 1: RANGE_LOW — first 12 candles
    if len(post_peak) >= 6:
        range_low = min(b["low"] for b in post_peak[:12])
        if range_low < peak_high:
            return range_low, "RANGE_LOW", "MEDIUM"

    # Priority 2: SWING_LOW — candles 13-36
    if len(post_peak) >= 13:
        range_low = min(b["low"] for b in post_peak[:12])
        swing_window = post_peak[12:36]
        if swing_window:
            swing_low = min(b["low"] for b in swing_window)
            if swing_low != range_low and swing_low < peak_high:
                return swing_low, "SWING_LOW", "MEDIUM"

    # Priority 3: VWAP_BAND
    vwap_window = post_peak[:24] if len(post_peak) >= 24 else post_peak
    vwap = _vwap(vwap_window)
    if vwap > 0 and vwap < peak_high:
        return vwap * 0.995, "VWAP_BAND", "LOW"

    # Priority 4: FALLBACK
    fallback = peak_high * 0.92
    return fallback, "FALLBACK", "LOW"


def _no_breakdown() -> Dict:
    return {
        "breakdown_confirmed": False,
        "breakdown_level": None,
        "breakdown_level_source": None,
        "breakdown_level_confidence": None,
        "support_test_count_before_break": None,
        "break_distance_pct": None,
        "break_body_ratio": None,
        "break_volume_ratio": None,
        "immediate_followthrough_pct_1bar": None,
        "immediate_followthrough_pct_3bar": None,
        "false_break_reclaim_fast_flag": False,
        "breakdown_candle_time": None,
    }
