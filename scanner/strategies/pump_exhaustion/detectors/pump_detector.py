import time
from typing import Dict, List


def detect_pump(bars_1h: List[Dict], base_result: Dict) -> Dict:
    """Detect pump metrics from base detection result.

    Requires base_result with base_validity_flag=True.
    Returns Group 3 fields.
    """
    if not base_result.get("base_validity_flag"):
        return {
            "pump_detected": False,
            "peak_high": base_result.get("peak_high"),
            "peak_time": base_result.get("peak_time"),
        }

    peak_idx = base_result["peak_idx"]
    peak_high = base_result["peak_high"]
    peak_time = base_result["peak_time"]
    base_price_median = base_result["base_price_median"]
    base_window = base_result.get("base_window", [])
    base_window_end_ts = base_result["base_window_end_ts"]

    # pump_pct
    pump_pct = (peak_high - base_price_median) / base_price_median if base_price_median > 0 else 0.0

    # pump_vol_ratio: peak vol vs avg of 20 candles before peak
    start = max(0, peak_idx - 20)
    prior_vols = [bars_1h[i]["volume"] for i in range(start, peak_idx)]
    avg_prior = sum(prior_vols) / len(prior_vols) if prior_vols else 0.0
    peak_vol = bars_1h[peak_idx]["volume"]
    pump_vol_ratio = peak_vol / avg_prior if avg_prior > 0 else 0.0

    # pump_speed_h: hours from base_window_end to peak
    pump_speed_h = 0.0
    if base_window_end_ts and peak_time:
        pump_speed_h = (peak_time - base_window_end_ts) / 3_600_000

    # pre_pump_low (reference only)
    pre_pump_low = min(b["low"] for b in base_window) if base_window else None

    # peak_age_h
    now_ms = int(time.time() * 1000)
    peak_age_h = (now_ms - peak_time) / 3_600_000 if peak_time else 9999.0

    return {
        "pump_detected": True,
        "peak_high": peak_high,
        "peak_time": peak_time,
        "peak_age_h": peak_age_h,
        "pump_pct": pump_pct,
        "pump_vol_ratio": pump_vol_ratio,
        "pump_speed_h": pump_speed_h,
        "pre_pump_low": pre_pump_low,
    }
