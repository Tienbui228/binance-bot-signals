import time
import threading
from typing import Dict, List, Optional

from pump_exhaustion_short.shared.r2_binance_client import R2BinanceClient
from pump_exhaustion_short.shared.volume_utils import (
    enrich_taker_fields, compute_cvd_proxy, cvd_trend, quote_vol_5m_median,
    post_peak_avg_vol,
)
from pump_exhaustion_short.shared.alert import send_alert
from pump_exhaustion_short.watchlist.watchlist_manager import WatchlistManager
from pump_exhaustion_short.detectors.breakdown_detector import detect_breakdown
from pump_exhaustion_short.detectors.retest_detector import detect_retest
from pump_exhaustion_short.classifiers.oi_regime_classifier import classify_oi_regime
from pump_exhaustion_short.scoring.scorer import score_case


def run_scan_loop(client: R2BinanceClient,
                  wl_manager: WatchlistManager,
                  cfg: Dict,
                  stop_event: threading.Event):
    interval_sec = cfg["runtime"]["scan_interval_minutes"] * 60
    print(f"[Scan] Loop started. Interval: {interval_sec}s")
    while not stop_event.is_set():
        _safe_run_scan(client, wl_manager, cfg)
        stop_event.wait(timeout=interval_sec)
    print("[Scan] Loop stopped.")


def _safe_run_scan(client, wl_manager, cfg):
    try:
        run_scan_once(client, wl_manager, cfg)
    except Exception as e:
        import traceback
        print(f"[Scan] run_once error: {e}")
        traceback.print_exc()


def run_scan_once(client: R2BinanceClient,
                  wl_manager: WatchlistManager,
                  cfg: Dict):
    active = wl_manager.get_active_cases()
    if not active:
        return

    now_ms = int(time.time() * 1000)
    updated_count = 0

    # Cleanup stale cases first — flush immediately if any were removed
    stale_removed = wl_manager.remove_stale(max_peak_age_h=72.0)
    if stale_removed:
        wl_manager.flush()
        wl_manager.sync_csvs()

    for case in active:
        case_id = case["case_id"]
        symbol = case["symbol"]
        state = case.get("case_state", "DISCOVERED")

        updates = _process_case(case, client, cfg, now_ms)
        if updates:
            wl_manager.update_case(case_id, updates)
            updated_count += 1

    if updated_count:
        wl_manager.flush()
        wl_manager.sync_csvs()


def _process_case(case: Dict, client: R2BinanceClient, cfg: Dict, now_ms: int) -> Optional[Dict]:
    """Process one case through state machine. Returns dict of updates, or None."""
    symbol = case["symbol"]
    state = case.get("case_state", "DISCOVERED")
    scan_cfg = cfg.get("scan", {})

    # Fetch fresh data
    bars_5m = client.get_klines(symbol, "5m", scan_cfg.get("scan_5m_candles", 288))
    bars_1h = client.get_klines(symbol, "1h", 80)  # 80h covers max peak_age_h=72 + buffer
    oi_hist = client.get_oi_hist(symbol, period="1h", limit=24)
    funding = client.get_funding_rate(symbol, limit=1)

    if not bars_5m or not bars_1h:
        return None

    # Enrich taker fields
    bars_5m, fetch_err_5m = enrich_taker_fields(bars_5m)
    bars_1h, fetch_err_1h = enrich_taker_fields(bars_1h)

    data_fetch_error = fetch_err_5m or fetch_err_1h or case.get("data_fetch_error")

    current_price = bars_5m[-1]["close"] if bars_5m else 0.0
    peak_high = case.get("peak_high") or 0.0
    peak_time = case.get("peak_time") or 0
    base_price_median = case.get("base_price_median") or 0.0

    # Scan gate G_S1: room_pct >= 15%
    room_pct = (current_price - base_price_median) / base_price_median if base_price_median > 0 else 0.0
    if room_pct < 0.15:
        return {"data_fetch_error": data_fetch_error, "room_pct": room_pct,
                "current_price_at_scan": current_price}

    # Scan gate G_S2: peak_age <= 72h
    peak_age_h = (now_ms - int(peak_time)) / 3_600_000 if peak_time else 9999
    if peak_age_h > 72:
        return {"case_state": "EXCLUDED", "exclusion_reason": "peak_too_old",
                "data_fetch_error": data_fetch_error}

    # Scan gate G_S4: quote_vol_5m_median >= 10K
    qvol_median = quote_vol_5m_median(bars_5m, last_n=20)
    min_qvol = scan_cfg.get("quote_vol_5m_min_usdt", 10000)
    if qvol_median < min_qvol:
        # Use fallback estimate but mark error
        data_fetch_error = data_fetch_error or "quote_vol_fallback_used"

    # OI regime
    oi_result = classify_oi_regime(oi_hist, current_price, peak_high)

    # Funding rate
    latest_funding_rate = funding[0]["fundingRate"] if funding else None

    # CVD/taker on 1h
    cvd_1h = compute_cvd_proxy(bars_1h)
    cvd_1h_trend = cvd_trend(cvd_1h, lookback=6)
    taker_sell_1h = bars_1h[-1].get("taker_sell_ratio", 0.5) if bars_1h else None

    # CVD/taker on 5m
    cvd_5m = compute_cvd_proxy(bars_5m[-72:])  # last 6h
    cvd_5m_trend = cvd_trend(cvd_5m, lookback=6)
    taker_sell_5m = bars_5m[-1].get("taker_sell_ratio", 0.5) if bars_5m else None

    # post-peak avg vol (for scoring)
    peak_idx = _find_peak_idx(bars_1h, peak_high)
    post_avg_vol = post_peak_avg_vol(bars_1h, peak_idx) if peak_idx is not None else None

    context_updates = {
        "room_pct": room_pct,
        "current_price_at_scan": current_price,
        "data_fetch_error": data_fetch_error,
        "oi_regime_label": oi_result["oi_regime_label"],
        "oi_change_from_peak_pct": oi_result["oi_change_from_peak_pct"],
        "oi_change_1h_pct": oi_result["oi_change_1h_pct"],
        "oi_change_4h_pct": oi_result["oi_change_4h_pct"],
        "oi_context_note": oi_result["oi_context_note"],
        "latest_funding_rate": latest_funding_rate,
        "cvd_proxy_1h_trend": cvd_1h_trend,
        "taker_sell_ratio_1h": taker_sell_1h,
        "cvd_proxy_5m_trend": cvd_5m_trend,
        "taker_sell_ratio_5m": taker_sell_5m,
        "post_peak_avg_vol_12h": post_avg_vol,
    }

    # --- State machine ---
    if state == "DISCOVERED":
        return _handle_discovered(case, bars_5m, cfg, context_updates, now_ms, peak_time)

    if state in ("BREAKDOWN_CONFIRMED", "RETEST_WAITING"):
        return _handle_retest_stage(case, bars_5m, cfg, context_updates, now_ms,
                                    base_price_median, peak_high)

    if state == "FAILED_RETEST_CONFIRMED":
        return _handle_scoring(case, cfg, context_updates)

    return context_updates


def _handle_discovered(case: Dict, bars_5m: List[Dict], cfg: Dict,
                        context_updates: Dict, now_ms: int, peak_time) -> Dict:
    peak_high = case.get("peak_high") or 0.0
    peak_age_h = (now_ms - int(peak_time)) / 3_600_000 if peak_time else 9999

    bd = detect_breakdown(bars_5m, int(peak_time), peak_high, cfg)

    if bd.get("false_break_reclaim_fast_flag") and bd.get("breakdown_confirmed"):
        return {**context_updates, "case_state": "EXCLUDED",
                "exclusion_reason": "false_break_reclaim",
                "breakdown_level": bd.get("breakdown_level"),
                "breakdown_candle_time": bd.get("breakdown_candle_time")}

    if bd.get("breakdown_confirmed"):
        p2_ts = bd["breakdown_candle_time"]
        p2_price = bd["breakdown_level"]
        p0_ts = case.get("p0_ts") or 0
        p1_ts = case.get("p1_ts") or 0
        anchor_order_valid = bool(p0_ts and p1_ts and p2_ts and p0_ts < p1_ts < p2_ts)
        return {
            **context_updates,
            **{k: v for k, v in bd.items() if k not in ("breakdown_confirmed",)},
            "case_state": "BREAKDOWN_CONFIRMED",
            "anchor_order_valid": anchor_order_valid,
            "p2_ts": p2_ts,
            "p2_price": p2_price,
        }

    # Check straight dump: state=DISCOVERED, peak_age_h<=12, no breakdown in 12h
    if peak_age_h <= 12:
        current_price = bars_5m[-1]["close"] if bars_5m else 0.0
        dump_pct = (peak_high - current_price) / peak_high if peak_high > 0 else 0.0
        # Check if price dumped >= 20% without breakdown detection
        if dump_pct >= 0.20:
            # Check taker delta proxy negative for last 6h (72 × 5m)
            last_6h = bars_5m[-72:]
            neg_count = sum(1 for b in last_6h if b.get("taker_delta_proxy", 0) < 0)
            if neg_count >= 60:
                return {**context_updates, "case_state": "WATCHLIST_ONLY_STRAIGHT_DUMP"}

    return context_updates


def _handle_retest_stage(case: Dict, bars_5m: List[Dict], cfg: Dict,
                          context_updates: Dict, now_ms: int,
                          base_price_median: float, peak_high: float) -> Dict:
    breakdown_time = case.get("breakdown_candle_time") or case.get("p2_ts")
    breakdown_level = case.get("breakdown_level")

    if not breakdown_time or not breakdown_level:
        return {**context_updates, "case_state": "RETEST_WAITING"}

    retest = detect_retest(bars_5m, int(breakdown_time), float(breakdown_level),
                            base_price_median, peak_high, cfg)

    if not retest.get("retest_confirmed"):
        return {**context_updates, "case_state": "RETEST_WAITING"}

    # Retest confirmed — compute anchor_order_valid with P3
    p0_ts = case.get("p0_ts") or 0
    p1_ts = case.get("p1_ts") or 0
    p2_ts = case.get("p2_ts") or 0
    p3_ts = retest["retest_candle_time"]
    anchor_order_valid = bool(p0_ts and p1_ts and p2_ts and p3_ts
                               and p0_ts < p1_ts < p2_ts < p3_ts)

    return {
        **context_updates,
        **{k: v for k, v in retest.items() if k not in ("retest_confirmed",)},
        "case_state": "FAILED_RETEST_CONFIRMED",
        "anchor_order_valid": anchor_order_valid,
        "p3_ts": p3_ts,
        "p3_price": retest["retest_high"],
    }


def _handle_scoring(case: Dict, cfg: Dict, context_updates: Dict) -> Dict:
    # Merge context into case for scoring
    merged = {**case, **context_updates}
    score_result = score_case(merged)

    if score_result.get("excluded"):
        return {
            **context_updates,
            "case_state": "EXCLUDED",
            "exclusion_reason": score_result["exclusion_reason"],
            **{k: v for k, v in score_result.items() if k not in ("excluded", "exclusion_reason")},
        }

    min_score = cfg.get("scoring", {}).get("min_score_to_alert", 5)
    if score_result.get("score_total", 0) >= min_score:
        _fire_alert(merged, score_result, cfg)

    return {
        **context_updates,
        "case_state": "OUTCOME_PENDING",
        "outcome_status": "pending",
        **{k: v for k, v in score_result.items() if k not in ("excluded", "exclusion_reason")},
    }


def _fire_alert(case: Dict, score: Dict, cfg: Dict):
    sym = case.get("symbol", "?")
    entry = case.get("entry_price")
    stop = case.get("stop_price")
    rr = case.get("rr_conservative")
    total = score.get("score_total", 0)
    confidence = score.get("confidence_label", "?") or "?"
    entry_str = f"{entry:.4f}" if isinstance(entry, (int, float)) else str(entry)
    stop_str = f"{stop:.4f}" if isinstance(stop, (int, float)) else str(stop)
    rr_str = f"{rr:.2f}" if isinstance(rr, (int, float)) else str(rr)
    msg = (f"[R2] pump_exhaustion_short ALERT\n"
           f"Symbol: {sym}\n"
           f"Entry: {entry_str}  Stop: {stop_str}\n"
           f"R:R (conservative): {rr_str}\n"
           f"Score: {total}/10  Confidence: {confidence}\n"
           f"OI: {case.get('oi_regime_label', '?')}")
    send_alert(msg, confidence, cfg)


def _find_peak_idx(bars_1h: List[Dict], peak_high: float) -> Optional[int]:
    if not bars_1h or not peak_high:
        return None
    for i, b in enumerate(bars_1h):
        if abs(b["high"] - peak_high) / peak_high < 0.001:
            return i
    return max(range(len(bars_1h)), key=lambda i: bars_1h[i]["high"])


def scan_symbol_once(symbol: str, client: R2BinanceClient, cfg: Dict) -> Dict:
    """Fetch fresh data + run all detectors for symbol. Returns fields dict.
    Standalone helper for testing and debugging — does not update watchlist.
    """
    scan_cfg = cfg.get("scan", {})
    bars_5m = client.get_klines(symbol, "5m", scan_cfg.get("scan_5m_candles", 288))
    bars_1h = client.get_klines(symbol, "1h", 80)
    oi_hist = client.get_oi_hist(symbol, period="1h", limit=24)
    funding = client.get_funding_rate(symbol, limit=1)

    result: Dict = {"symbol": symbol}

    if bars_5m:
        bars_5m, fetch_err = enrich_taker_fields(bars_5m)
        result["data_fetch_error"] = fetch_err
        result["current_price"] = bars_5m[-1]["close"]
        cvd_5m = compute_cvd_proxy(bars_5m[-72:])
        result["cvd_5m_trend"] = cvd_trend(cvd_5m)
        result["quote_vol_5m_median"] = quote_vol_5m_median(bars_5m)

    if bars_1h:
        bars_1h, _ = enrich_taker_fields(bars_1h)
        from pump_exhaustion_short.detectors.base_detector import detect_base
        from pump_exhaustion_short.detectors.pump_detector import detect_pump
        base = detect_base(bars_1h, cfg)
        result["base_validity_flag"] = base.get("base_validity_flag")
        result["base_invalid_reason"] = base.get("base_invalid_reason")
        if base.get("base_validity_flag"):
            pump = detect_pump(bars_1h, base)
            result["pump_pct"] = pump.get("pump_pct")
            result["pump_vol_ratio"] = pump.get("pump_vol_ratio")
            result["peak_age_h"] = pump.get("peak_age_h")
            result["peak_high"] = pump.get("peak_high")
            if bars_5m and pump.get("peak_time"):
                bd = detect_breakdown(bars_5m, pump["peak_time"],
                                       pump["peak_high"], cfg)
                result["breakdown_confirmed"] = bd.get("breakdown_confirmed")
                result["breakdown_level"] = bd.get("breakdown_level")

    if oi_hist and result.get("current_price") and result.get("peak_high"):
        oi_result = classify_oi_regime(oi_hist, result["current_price"], result["peak_high"])
        result.update(oi_result)

    if funding:
        result["latest_funding_rate"] = funding[0]["fundingRate"]

    return result
