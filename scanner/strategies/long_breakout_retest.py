
from __future__ import annotations

import time
from typing import Optional, Type


def _setup_quality_band(score: float) -> str:
    if score >= 80.0:
        return "STRONG"
    if score >= 60.0:
        return "OK"
    return "WEAK"


def build_pending_long_setup(scanner, symbol: str, pending_cls: Type) -> Optional[object]:
    scanner._funnel_hit("long_breakout_retest", "symbols_seen")
    th = scanner.cfg["thresholds"]
    scanner_cfg = scanner.cfg["scanner"]
    bars_5m_all = scanner.klines(symbol, scanner_cfg["interval_5m"], limit=80)
    bars_15m_all = scanner.klines(symbol, scanner_cfg["interval_15m"], limit=20)
    bars_1h_all = scanner.klines(symbol, scanner_cfg["interval_1h"], limit=12)
    if len(bars_5m_all) < 25 or len(bars_15m_all) < 10 or len(bars_1h_all) < 8:
        scanner._funnel_hit("long_breakout_retest", "fail_data")
        return None
    bars_5m = bars_5m_all[:-1]
    bars_15m = bars_15m_all[:-1]
    bars_1h = bars_1h_all[:-1]
    if len(bars_5m) < 25:
        scanner._funnel_hit("long_breakout_retest", "fail_data")
        return None
    scanner._funnel_hit("long_breakout_retest", "data_ok")
    signal_bar = bars_5m[-1]
    oi_jump = scanner.calc_oi_jump_pct(symbol)
    if oi_jump is None or oi_jump < float(th["oi_jump_pct_5m"]):
        scanner._funnel_hit("long_breakout_retest", "fail_oi")
        return None
    scanner._funnel_hit("long_breakout_retest", "oi_ok")
    body_ratio = scanner.candle_body_ratio(signal_bar)
    vol_ratio = scanner.volume_ratio(bars_5m)
    wick = scanner.wick_ratio(signal_bar)
    funding_pct = scanner.funding(symbol)
    trend15 = scanner.trend_15m(bars_15m)
    trend1h = scanner.trend_1h(bars_1h)
    breakout_lookback = int(th["breakout_lookback_bars"])
    prev_hh_window = bars_5m[-(breakout_lookback + 1):-1]
    if len(prev_hh_window) < 5:
        scanner._funnel_hit("long_breakout_retest", "fail_data")
        return None
    hh = max(x["high"] for x in prev_hh_window)
    price = signal_bar["close"]
    min_break_pct = float(th["min_break_distance_pct"])
    min_vol_ratio = float(th["min_volume_ratio"])
    max_wick_ratio = float(th["max_wick_ratio"])
    min_body_ratio = float(th["min_body_ratio"])
    max_funding_abs_pct = float(th["max_funding_abs_pct"])
    long_break = price > hh * (1 + min_break_pct / 100.0)
    if not long_break:
        scanner._funnel_hit("long_breakout_retest", "fail_breakout")
        return None
    scanner._funnel_hit("long_breakout_retest", "breakout_ok")
    if vol_ratio < min_vol_ratio or wick > max_wick_ratio or body_ratio < min_body_ratio:
        scanner._funnel_hit("long_breakout_retest", "fail_candle")
        return None
    scanner._funnel_hit("long_breakout_retest", "candle_ok")
    if scanner.already_open_signal(symbol, "LONG") or scanner.already_pending_setup(symbol, "LONG"):
        scanner._funnel_hit("long_breakout_retest", "blocked_duplicate")
        return None
    if trend15 not in ("up", "flat") or trend1h == "down":
        scanner._funnel_hit("long_breakout_retest", "fail_regime")
        return None
    scanner._funnel_hit("long_breakout_retest", "regime_ok")
    if funding_pct > max_funding_abs_pct:
        scanner._funnel_hit("long_breakout_retest", "fail_funding")
        return None
    scanner._funnel_hit("long_breakout_retest", "funding_ok")
    reason = f"OI jump {oi_jump:.2f}% + breakout pending + 15m {trend15} + 1h {trend1h}"
    score_oi = max(0.0, min(35.0, 18.0 + max(oi_jump - float(th["oi_jump_pct_5m"]), 0.0) * 6.0))
    breakout_dist_pct = max((price - hh) / max(hh, 1e-12) * 100.0, 0.0)
    score_breakout = max(0.0, min(40.0, 16.0 + breakout_dist_pct * 18.0 + max(vol_ratio - min_vol_ratio, 0.0) * 6.0 + body_ratio * 10.0 - wick * 10.0))
    score_retest = 0.0
    score = min(100.0, 50 + oi_jump * 6 + (vol_ratio - 1) * 20 + body_ratio * 10 - max(funding_pct, 0) * 120 - wick * 22)
    confidence = max(0.0, min(0.99, score / 100.0))
    reason_tags = ["oi_jump", "5m_breakout", f"15m_{trend15}", f"1h_{trend1h}"]
    pending_id = f"{symbol}-LONG-{signal_bar['open_time']}"
    ts = int(time.time() * 1000)
    btc_ctx = scanner.get_btc_context()
    scanner._funnel_hit("long_breakout_retest", "new_pending")
    return pending_cls(
        pending_id=pending_id,
        created_ts_ms=ts,
        signal_open_time=signal_bar["open_time"],
        symbol=symbol,
        side="LONG",
        score=score,
        confidence=confidence,
        reason=reason,
        breakout_level=hh,
        signal_price=price,
        signal_high=signal_bar["high"],
        signal_low=signal_bar["low"],
        oi_jump_pct=oi_jump,
        funding_pct=funding_pct,
        vol_ratio=vol_ratio,
        strategy="long_breakout_retest",
        market_regime=btc_ctx["market_regime"],
        btc_price=btc_ctx["btc_price"],
        btc_24h_change_pct=btc_ctx["btc_24h_change_pct"],
        btc_4h_change_pct=btc_ctx["btc_4h_change_pct"],
        btc_1h_change_pct=btc_ctx["btc_1h_change_pct"],
        btc_24h_range_pct=btc_ctx["btc_24h_range_pct"],
        btc_4h_range_pct=btc_ctx["btc_4h_range_pct"],
        alt_market_breadth_pct=btc_ctx["alt_market_breadth_pct"],
        btc_regime=btc_ctx["btc_regime"],
        score_oi=score_oi,
        score_exhaustion=0.0,
        score_breakout=score_breakout,
        score_retest=score_retest,
        reason_tags=";".join(reason_tags),
        regime_label="unknown",
        regime_fit_for_strategy="not_evaluated",
        setup_quality_band=_setup_quality_band(score),
        delivery_band="not_evaluated",
        veto_reason_code="not_evaluated",
        dispatch_action="not_evaluated",
        dispatch_confidence_band="not_evaluated",
        status="PENDING",
    )
