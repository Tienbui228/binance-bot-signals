import time
from typing import Dict, List, Optional


def detect_retest(bars_5m: List[Dict],
                  breakdown_time_ms: int,
                  breakdown_level: float,
                  base_price_median: float,
                  peak_high: float,
                  cfg: Dict) -> Dict:
    """Detect failed retest of breakdown level in post-breakdown 5m candles.

    Returns Group 6 + Group 7 fields.
    """
    scan_cfg = cfg.get("scan", {})
    retest_tol = scan_cfg.get("retest_tol", 0.02)
    taker_buy_max = scan_cfg.get("retest_taker_buy_max", 0.45)
    max_age_min = scan_cfg.get("signal_max_age_min", 15)

    cutoff_ms = breakdown_time_ms + 24 * 3_600_000
    post_breakdown = [b for b in bars_5m
                      if b["open_time"] > breakdown_time_ms and b["open_time"] <= cutoff_ms]

    if not post_breakdown:
        return _no_retest()

    now_ms = int(time.time() * 1000)

    retest_candle: Optional[Dict] = None
    retest_idx: Optional[int] = None
    for i, bar in enumerate(post_breakdown):
        # wick touches breakdown from below
        if bar["high"] < breakdown_level * (1 - retest_tol):
            continue
        # close stays below breakdown (fail)
        if bar["close"] >= breakdown_level:
            continue
        # buyers weak at retest
        if bar.get("taker_buy_ratio", 1.0) >= taker_buy_max:
            continue
        # signal not stale
        signal_age_min = (now_ms - bar["open_time"]) / 60_000
        if signal_age_min > max_age_min:
            continue
        retest_candle = bar
        retest_idx = i
        break

    if retest_candle is None:
        return _no_retest()

    retest_high = retest_candle["high"]
    retest_time = retest_candle["open_time"]
    entry_price = retest_candle["close"]
    stop_price = retest_high * 1.005

    # Targets
    target_conservative = base_price_median + 0.5 * (peak_high - base_price_median)
    target_extreme = base_price_median

    conservative_target_valid = target_conservative < entry_price
    rr_conservative: Optional[float] = None
    if conservative_target_valid and (stop_price - entry_price) > 0:
        rr_conservative = (entry_price - target_conservative) / (stop_price - entry_price)

    rr_extreme: Optional[float] = None
    if (stop_price - entry_price) > 0:
        rr_extreme = (entry_price - target_extreme) / (stop_price - entry_price)

    # Additional fields
    retest_depth_pct = (retest_high - breakdown_level) / breakdown_level if breakdown_level > 0 else None
    fail_strength = (breakdown_level - entry_price) / breakdown_level if breakdown_level > 0 else None

    # max_reclaim_above_break_pct: max(high) in post-breakdown bars before retest, above breakdown_level
    prior_bars = post_breakdown[:retest_idx]
    max_high_above = max((b["high"] for b in prior_bars if b["high"] > breakdown_level), default=breakdown_level)
    max_reclaim_above_break_pct = (max_high_above - breakdown_level) / breakdown_level

    # retest_duration_bars: candles from breakdown to retest
    retest_duration_bars = retest_idx + 1

    # fail_confirmation_bar_count: bars after retest with close < breakdown_level
    after_retest = post_breakdown[retest_idx + 1:]
    fail_conf = sum(1 for b in after_retest[:5] if b["close"] < breakdown_level)

    signal_age_min = (now_ms - retest_time) / 60_000

    return {
        "retest_confirmed": True,
        "retest_high": retest_high,
        "retest_candle_time": retest_time,
        "retest_depth_vs_break_pct": retest_depth_pct,
        "max_reclaim_above_break_pct": max_reclaim_above_break_pct,
        "retest_duration_bars": retest_duration_bars,
        "fail_strength": fail_strength,
        "fail_confirmation_bar_count": fail_conf,
        "signal_age_min": signal_age_min,
        "taker_buy_ratio_at_retest": retest_candle.get("taker_buy_ratio"),
        # Entry fields
        "entry_price": entry_price,
        "stop_price": stop_price,
        "target_conservative": target_conservative,
        "target_extreme": target_extreme,
        "conservative_target_valid": conservative_target_valid,
        "rr_conservative": rr_conservative,
        "rr_extreme": rr_extreme,
        "pattern_type": "failed_retest",
    }


def _no_retest() -> Dict:
    return {
        "retest_confirmed": False,
        "retest_high": None,
        "retest_candle_time": None,
        "retest_depth_vs_break_pct": None,
        "max_reclaim_above_break_pct": None,
        "retest_duration_bars": None,
        "fail_strength": None,
        "fail_confirmation_bar_count": None,
        "signal_age_min": None,
        "taker_buy_ratio_at_retest": None,
        "entry_price": None,
        "stop_price": None,
        "target_conservative": None,
        "target_extreme": None,
        "conservative_target_valid": None,
        "rr_conservative": None,
        "rr_extreme": None,
        "pattern_type": None,
    }
