from typing import Dict, List, Optional, Tuple


def enrich_taker_fields(bars: List[Dict]) -> Tuple[List[Dict], Optional[str]]:
    """Add taker_sell_vol, taker_delta_proxy, taker_buy_ratio, taker_sell_ratio to each bar.
    Returns (enriched_bars, data_fetch_error) where error is set if quote_vol fallback was used."""
    has_fallback = False
    for bar in bars:
        volume = bar.get("volume", 0.0)
        taker_buy = bar.get("taker_buy_base_vol", 0.0)
        if bar.get("_quote_vol_fallback", False):
            has_fallback = True
        taker_sell = max(volume - taker_buy, 0.0)
        bar["taker_sell_vol"] = taker_sell
        bar["taker_delta_proxy"] = taker_buy - taker_sell
        if volume > 0:
            bar["taker_buy_ratio"] = taker_buy / volume
            bar["taker_sell_ratio"] = taker_sell / volume
        else:
            bar["taker_buy_ratio"] = 0.5
            bar["taker_sell_ratio"] = 0.5
    error = "quote_vol_fallback_used" if has_fallback else None
    return bars, error


def compute_cvd_proxy(bars: List[Dict]) -> List[float]:
    """Cumulative sum of taker_delta_proxy across bars."""
    cvd = []
    running = 0.0
    for bar in bars:
        running += bar.get("taker_delta_proxy", 0.0)
        cvd.append(running)
    return cvd


def cvd_trend(cvd_series: List[float], lookback: int = 6) -> str:
    """Determine trend direction of last `lookback` CVD values."""
    if len(cvd_series) < 2:
        return "flat"
    window = cvd_series[-lookback:]
    if len(window) < 2:
        return "flat"
    slope = (window[-1] - window[0]) / max(len(window) - 1, 1)
    threshold = abs(window[0]) * 0.001 if window[0] != 0 else 1.0
    if slope > threshold:
        return "rising"
    if slope < -threshold:
        return "declining"
    return "flat"


def avg_vol_baseline(bars: List[Dict], n: int = 100) -> float:
    """Mean volume of first n bars (baseline for spike detection)."""
    subset = bars[:n]
    if not subset:
        return 0.0
    total = sum(b.get("volume", 0.0) for b in subset)
    return total / len(subset)


def quote_vol_5m_median(bars_5m: List[Dict], last_n: int = 20) -> float:
    """Median of quote_asset_volume from the last_n 5m candles.
    Falls back to close * volume if quote_asset_volume is missing/zero."""
    window = bars_5m[-last_n:] if len(bars_5m) >= last_n else bars_5m
    if not window:
        return 0.0
    values = []
    for b in window:
        qv = b.get("quote_asset_volume", 0.0)
        if qv <= 0:
            qv = b.get("close", 0.0) * b.get("volume", 0.0)
        values.append(qv)
    values.sort()
    mid = len(values) // 2
    if len(values) % 2 == 0:
        return (values[mid - 1] + values[mid]) / 2.0
    return values[mid]


def post_peak_avg_vol(bars_1h: List[Dict], peak_idx: int, lookback: int = 12) -> float:
    """Average volume of `lookback` 1h candles immediately after peak."""
    start = peak_idx + 1
    end = start + lookback
    subset = bars_1h[start:end]
    if not subset:
        return 0.0
    return sum(b.get("volume", 0.0) for b in subset) / len(subset)
