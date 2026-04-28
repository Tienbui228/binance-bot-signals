import time
from typing import Dict, List, Optional

from scanner.strategies.pump_exhaustion.config import get_pump_cfg
from scanner.strategies.pump_exhaustion._client import _BinanceHttp
from scanner.strategies.pump_exhaustion.watchlist.watchlist_manager import WatchlistManager
from scanner.strategies.pump_exhaustion.volume_utils import (
    enrich_taker_fields, compute_cvd_proxy, cvd_trend,
    quote_vol_5m_median, post_peak_avg_vol,
)
from scanner.strategies.pump_exhaustion.detectors.breakdown_detector import detect_breakdown
from scanner.strategies.pump_exhaustion.detectors.retest_detector import detect_retest
from scanner.strategies.pump_exhaustion.classifiers.oi_regime_classifier import classify_oi_regime
from scanner.strategies.pump_exhaustion.classifiers.market_regime import classify_market_regime
from scanner.strategies.pump_exhaustion.scoring.scorer import score_case
from scanner.strategies.pump_exhaustion.alert import send_alert


class PumpScanner:
    def __init__(self, cfg: Dict, client, wl_manager: Optional[WatchlistManager] = None):
        self._cfg = cfg
        self._client_app = client  # BinanceScanner — used only for market_regime BTC klines
        self._wl_manager = wl_manager if wl_manager is not None else WatchlistManager(cfg)
        self._http = _BinanceHttp(
            delay_sec=cfg.get("binance", {}).get("request_delay_sec", 0.2),
            timeout_sec=cfg.get("binance", {}).get("request_timeout_sec", 10),
        )

    def scan_once(self, eligible_symbols: List[str]):
        """Called from oi_scanner.scan_once() in the main thread every scan cycle."""
        pump_cfg = get_pump_cfg(self._cfg)
        if not pump_cfg.get("enabled", False):
            return

        active = self._wl_manager.get_active_cases()
        if not active:
            return

        # Compute market regime once per cycle
        try:
            regime = classify_market_regime(self._http, active)
        except Exception:
            regime = {"regime_label": "NEUTRAL_MIXED", "regime_score": 0,
                      "btc_vs_ema20_pct": 0.0, "alts_declining_pct": 0.0, "regime_note": ""}

        stale_removed = self._wl_manager.remove_stale(max_peak_age_h=72.0)
        if stale_removed:
            self._wl_manager.flush()
            self._wl_manager.sync_csvs()

        updated_count = 0
        for case in active:
            try:
                updates = self._process_case(case, regime, pump_cfg)
            except Exception as e:
                import traceback
                print(f"[PumpScanner] {case.get('symbol')} error: {e}")
                traceback.print_exc()
                updates = None
            if updates:
                self._wl_manager.update_case(case["case_id"], updates)
                updated_count += 1

        if updated_count:
            self._wl_manager.flush()
            self._wl_manager.sync_csvs()

    def _process_case(self, case: Dict, regime: Dict, pump_cfg: Dict) -> Optional[Dict]:
        symbol = case["symbol"]
        state = case.get("case_state", "DISCOVERED")
        scan_cfg = pump_cfg.get("scan", {})
        now_ms = int(time.time() * 1000)

        bars_5m = self._http.klines(symbol, "5m", scan_cfg.get("candles_5m", 288))
        bars_1h = self._http.klines(symbol, "1h", 80)
        oi_hist = self._http.oi_hist(symbol, period="1h", limit=24)
        funding_rate = self._http.funding(symbol)

        if not bars_5m or not bars_1h:
            return None

        bars_5m, fetch_err_5m = enrich_taker_fields(bars_5m)
        bars_1h, fetch_err_1h = enrich_taker_fields(bars_1h)

        data_fetch_error = fetch_err_5m or fetch_err_1h or case.get("data_fetch_error")

        current_price = bars_5m[-1]["close"] if bars_5m else 0.0
        peak_high = case.get("peak_high") or 0.0
        peak_time = case.get("peak_time") or 0
        base_price_median = case.get("base_price_median") or 0.0

        room_pct = (current_price - base_price_median) / base_price_median if base_price_median > 0 else 0.0
        if room_pct < 0.15:
            return {"data_fetch_error": data_fetch_error, "room_pct": room_pct,
                    "current_price_at_scan": current_price}

        try:
            peak_age_h = (now_ms - int(peak_time)) / 3_600_000 if peak_time else 9999
        except (TypeError, ValueError):
            peak_age_h = 9999
        if peak_age_h > 72:
            return {"case_state": "EXCLUDED", "exclusion_reason": "peak_too_old",
                    "data_fetch_error": data_fetch_error}

        qvol_median = quote_vol_5m_median(bars_5m, last_n=20)
        min_qvol = scan_cfg.get("quote_vol_5m_min_usdt", 10000)
        if qvol_median < min_qvol:
            data_fetch_error = data_fetch_error or "quote_vol_fallback_used"

        oi_result = classify_oi_regime(oi_hist, current_price, peak_high)

        cvd_1h = compute_cvd_proxy(bars_1h)
        cvd_1h_trend = cvd_trend(cvd_1h, lookback=6)
        taker_sell_1h = bars_1h[-1].get("taker_sell_ratio", 0.5) if bars_1h else None

        cvd_5m = compute_cvd_proxy(bars_5m[-72:])
        cvd_5m_trend = cvd_trend(cvd_5m, lookback=6)
        taker_sell_5m = bars_5m[-1].get("taker_sell_ratio", 0.5) if bars_5m else None

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
            "latest_funding_rate": funding_rate,
            "cvd_proxy_1h_trend": cvd_1h_trend,
            "taker_sell_ratio_1h": taker_sell_1h,
            "cvd_proxy_5m_trend": cvd_5m_trend,
            "taker_sell_ratio_5m": taker_sell_5m,
            "post_peak_avg_vol_12h": post_avg_vol,
            # Regime fields (updated every scan)
            "regime_label": regime.get("regime_label"),
            "regime_score": regime.get("regime_score"),
            "btc_vs_ema20_pct": regime.get("btc_vs_ema20_pct"),
            "alts_declining_pct": regime.get("alts_declining_pct"),
            "regime_note": regime.get("regime_note"),
        }

        if state == "DISCOVERED":
            return _handle_discovered(case, bars_5m, {"scan": scan_cfg}, context_updates, now_ms, peak_time)

        if state in ("BREAKDOWN_CONFIRMED", "RETEST_WAITING"):
            return _handle_retest_stage(case, bars_5m, {"scan": scan_cfg}, context_updates, now_ms,
                                        base_price_median, peak_high)

        if state == "FAILED_RETEST_CONFIRMED":
            return _handle_scoring(case, self._cfg, context_updates)

        return context_updates


def _handle_discovered(case: Dict, bars_5m: List[Dict], cfg: Dict,
                        context_updates: Dict, now_ms: int, peak_time) -> Dict:
    peak_high = case.get("peak_high") or 0.0
    try:
        peak_age_h = (now_ms - int(peak_time)) / 3_600_000 if peak_time else 9999
    except (TypeError, ValueError):
        peak_age_h = 9999

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
            **{k: v for k, v in bd.items() if k != "breakdown_confirmed"},
            "case_state": "BREAKDOWN_CONFIRMED",
            "anchor_order_valid": anchor_order_valid,
            "p2_ts": p2_ts,
            "p2_price": p2_price,
        }

    if peak_age_h <= 12:
        current_price = bars_5m[-1]["close"] if bars_5m else 0.0
        dump_pct = (peak_high - current_price) / peak_high if peak_high > 0 else 0.0
        if dump_pct >= 0.20:
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

    _sentinel = ("not_reached_yet", "not_evaluated", None, "")
    if breakdown_time in _sentinel or breakdown_level in _sentinel:
        return {**context_updates, "case_state": "RETEST_WAITING"}

    try:
        retest = detect_retest(bars_5m, int(breakdown_time), float(breakdown_level),
                                base_price_median, peak_high, cfg)
    except (TypeError, ValueError):
        return {**context_updates, "case_state": "RETEST_WAITING"}

    if not retest.get("retest_confirmed"):
        return {**context_updates, "case_state": "RETEST_WAITING"}

    p0_ts = case.get("p0_ts") or 0
    p1_ts = case.get("p1_ts") or 0
    p2_ts = case.get("p2_ts") or 0
    p3_ts = retest["retest_candle_time"]
    anchor_order_valid = bool(p0_ts and p1_ts and p2_ts and p3_ts
                               and p0_ts < p1_ts < p2_ts < p3_ts)

    return {
        **context_updates,
        **{k: v for k, v in retest.items() if k != "retest_confirmed"},
        "case_state": "FAILED_RETEST_CONFIRMED",
        "anchor_order_valid": anchor_order_valid,
        "p3_ts": p3_ts,
        "p3_price": retest["retest_high"],
    }


def _handle_scoring(case: Dict, cfg: Dict, context_updates: Dict) -> Dict:
    merged = {**case, **context_updates}
    score_result = score_case(merged)

    if score_result.get("excluded"):
        return {
            **context_updates,
            "case_state": "EXCLUDED",
            "exclusion_reason": score_result["exclusion_reason"],
            **{k: v for k, v in score_result.items() if k not in ("excluded", "exclusion_reason")},
        }

    pump_cfg = get_pump_cfg(cfg)
    min_score = pump_cfg.get("scoring", {}).get("min_score_to_alert", 6)
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
    score_max = score.get("score_max", 12)
    confidence = score.get("confidence_label", "?") or "?"
    regime = case.get("regime_label", "?")
    entry_str = f"{entry:.4f}" if isinstance(entry, (int, float)) else str(entry)
    stop_str = f"{stop:.4f}" if isinstance(stop, (int, float)) else str(stop)
    rr_str = f"{rr:.2f}" if isinstance(rr, (int, float)) else str(rr)
    msg = (f"pump_exhaustion_short SIGNAL\n"
           f"Symbol: {sym}\n"
           f"Entry: {entry_str}  Stop: {stop_str}\n"
           f"R:R (conservative): {rr_str}\n"
           f"Score: {total}/{score_max}  Confidence: {confidence}\n"
           f"OI: {case.get('oi_regime_label', '?')}  Regime: {regime}")
    send_alert(msg, confidence, cfg)


def _find_peak_idx(bars_1h: List[Dict], peak_high: float) -> Optional[int]:
    if not bars_1h or not peak_high:
        return None
    for i, b in enumerate(bars_1h):
        if abs(b["high"] - peak_high) / peak_high < 0.001:
            return i
    return max(range(len(bars_1h)), key=lambda i: bars_1h[i]["high"])
