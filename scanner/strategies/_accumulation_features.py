"""Pure feature computation for long_accumulation_continuation strategy.

No I/O, no global state. All inputs are pre-sorted lists of floats passed by caller.
"""

import numpy as np


def _slope(series: list[float]) -> float:
    """Linear slope per period via polyfit. Returns 0.0 on degenerate input."""
    n = len(series)
    if n < 2:
        return 0.0
    try:
        return float(np.polyfit(range(n), series, 1)[0])
    except Exception:
        return 0.0


def _mean(series: list[float]) -> float:
    if not series:
        return 0.0
    return sum(series) / len(series)


def compute_accumulation_features(
    series_pos: list[float],
    series_acct: list[float],
    series_retail: list[float],
    series_taker: list[float],
    series_oi: list[float],
    series_close: list[float] | None = None,
) -> dict:
    """Compute all scalar features for the accumulation strategy.

    All series must be sorted ascending by time (oldest first, newest last).
    series_oi is optional — caller provides daily OI bars (≥14 pts) for oi_trend_3v14.
    oi_delta_1h_pct is computed separately by the caller from the 5m OI source.
    oi_trend_3v14 is set to 0.0 when series_oi has fewer than 14 points or base=0.

    Returns:
        {
            "insufficient_history": bool,
            "partial_history": bool,
            "features": dict | None,
        }

    features is None when insufficient_history is True.
    """
    # series_oi excluded from required — caller provides daily bars; may be None if fetch fails
    required = [series_pos, series_acct, series_retail, series_taker]

    # Any None or empty → insufficient
    if any(s is None or len(s) == 0 for s in required):
        return {"insufficient_history": True, "partial_history": False, "features": None}

    min_len = min(len(s) for s in required)

    if min_len < 14:
        return {"insufficient_history": True, "partial_history": False, "features": None}

    partial = min_len < 30

    # Scalars from most-recent element
    pos_now    = series_pos[-1]
    acct_now   = series_acct[-1]
    gap_now    = pos_now - acct_now
    retail_now = series_retail[-1]
    taker_now  = series_taker[-1]

    # Gap series derived from pos and acct (aligned by index, shortest wins)
    n = min(len(series_pos), len(series_acct))
    gap_series = [series_pos[i] - series_acct[i] for i in range(n)]

    # Slopes over full available window
    pos_slope_30d    = _slope(series_pos)
    gap_slope_30d    = _slope(gap_series)
    retail_slope_30d = _slope(series_retail)

    # 3-period vs 14-period means (trend signals)
    pos_trend_3v14 = _mean(series_pos[-3:]) - _mean(series_pos[-14:])
    # oi_trend_3v14: relative % vs 14-day baseline; requires daily OI series (≥14 pts)
    oi_trend_3v14 = 0.0
    if series_oi is not None and len(series_oi) >= 14:
        _oi_base = _mean(series_oi[-14:])
        if _oi_base > 0:
            oi_trend_3v14 = (_mean(series_oi[-3:]) - _oi_base) / _oi_base * 100.0

    # Min-based recovery metrics
    pos_min_30d            = min(series_pos)
    pos_recovery_from_min  = pos_now - pos_min_30d
    retail_min_30d         = min(series_retail)
    retail_recovery        = retail_now - retail_min_30d

    # Taker 7-day average (last 7 observations)
    taker_7d_avg = _mean(series_taker[-7:])

    # Price trend fields for v2.0.1 Gate T (spec §2.3)
    price_vs_baseline = 0.0
    price_trend_7v30  = 0.0
    if series_close and len(series_close) >= 14:
        _base = _mean(series_close)
        if _base > 0:
            price_vs_baseline = series_close[-1] / _base - 1.0
        _n   = len(series_close)
        _m7  = _mean(series_close[-min(7, _n):])
        _m30 = _mean(series_close[-min(30, _n):])
        if _m30 > 0:
            price_trend_7v30 = _m7 / _m30 - 1.0

    return {
        "insufficient_history": False,
        "partial_history": partial,
        "features": {
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
            "price_vs_baseline":  price_vs_baseline,
            "price_trend_7v30":   price_trend_7v30,
        },
    }
