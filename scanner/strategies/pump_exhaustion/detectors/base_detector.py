import statistics
from typing import Dict, List, Optional

from scanner.strategies.pump_exhaustion.volume_utils import avg_vol_baseline


def _linregress_slope(values: List[float]) -> float:
    """Simple linear regression slope."""
    n = len(values)
    if n < 2:
        return 0.0
    xs = list(range(n))
    xm = sum(xs) / n
    ym = sum(values) / n
    num = sum((xs[i] - xm) * (values[i] - ym) for i in range(n))
    den = sum((xs[i] - xm) ** 2 for i in range(n))
    if den == 0:
        return 0.0
    return num / den


def detect_base(bars_1h: List[Dict], cfg: Dict) -> Dict:
    """Detect pre-pump base from 1h klines.

    Returns dict with all Group 2 fields + peak_idx for use by pump_detector.
    Sets base_validity_flag=False with base_invalid_reason on any failure.
    """
    disc_cfg = cfg.get("discovery", {})
    vol_spike_multiplier = disc_cfg.get("vol_spike_multiplier", 2.5)

    if len(bars_1h) < 20:
        return _invalid_base("insufficient_data")

    # Step 1: baseline volume from first 100 candles
    baseline = avg_vol_baseline(bars_1h, n=100)
    if baseline <= 0:
        return _invalid_base("insufficient_data")

    # Step 2: find peak (argmax high)
    peak_idx = max(range(len(bars_1h)), key=lambda i: bars_1h[i]["high"])
    peak_high = bars_1h[peak_idx]["high"]
    peak_time = bars_1h[peak_idx]["open_time"]

    # Step 3: find ignition_idx — scan OLDER→NEWER [0, peak_idx-1]
    ignition_idx: Optional[int] = None
    for i in range(0, peak_idx):
        if bars_1h[i]["volume"] > baseline * vol_spike_multiplier:
            ignition_idx = i
            break  # take first (oldest) spike

    if ignition_idx is None:
        return {**_invalid_base("ignition_not_found_before_peak"),
                "peak_idx": peak_idx, "peak_high": peak_high, "peak_time": peak_time}

    # Guard: need at least 12 bars before ignition
    if ignition_idx < 12:
        return {**_invalid_base("insufficient_bars_before_ignition"),
                "peak_idx": peak_idx, "peak_high": peak_high, "peak_time": peak_time}

    # Step 4: base_window = 12 candles before ignition (exclusive)
    base_window = bars_1h[ignition_idx - 12: ignition_idx]

    closes = [b["close"] for b in base_window]
    base_price_median = statistics.median(closes)

    if base_price_median <= 0:
        return {**_invalid_base("insufficient_data"),
                "peak_idx": peak_idx, "peak_high": peak_high, "peak_time": peak_time}

    # Step 5: slope check (trending base)
    slope = _linregress_slope(closes) / base_price_median
    base_trending_flag = slope > 0.005
    if base_trending_flag:
        return {
            "base_validity_flag": False,
            "base_invalid_reason": "base_window_already_trending",
            "base_trending_flag": True,
            "base_price_median": base_price_median,
            "peak_idx": peak_idx,
            "peak_high": peak_high,
            "peak_time": peak_time,
            "base_window_start_ts": base_window[0]["open_time"],
            "base_window_end_ts": base_window[-1]["open_time"],
        }

    # Step 6: base fields
    highs = [b["high"] for b in base_window]
    lows = [b["low"] for b in base_window]
    base_range_pct = (max(highs) - min(lows)) / base_price_median if base_price_median > 0 else 0.0

    return {
        "base_validity_flag": True,
        "base_invalid_reason": None,
        "base_detection_method": "vol_spike_anchor",
        "base_window_start_ts": base_window[0]["open_time"],
        "base_window_end_ts": base_window[-1]["open_time"],
        "base_price_median": base_price_median,
        "base_range_pct": base_range_pct,
        "base_trending_flag": False,
        # internals for pump_detector
        "peak_idx": peak_idx,
        "peak_high": peak_high,
        "peak_time": peak_time,
        "ignition_idx": ignition_idx,
        "base_window": base_window,  # passed to pump_detector for pre_pump_low
    }


def _invalid_base(reason: str) -> Dict:
    return {
        "base_validity_flag": False,
        "base_invalid_reason": reason,
        "base_detection_method": "vol_spike_anchor",
        "base_window_start_ts": None,
        "base_window_end_ts": None,
        "base_price_median": None,
        "base_range_pct": None,
        "base_trending_flag": None,
        "peak_idx": None,
        "peak_high": None,
        "peak_time": None,
        "ignition_idx": None,
        "base_window": [],
    }
