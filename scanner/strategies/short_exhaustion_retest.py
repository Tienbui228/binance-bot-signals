
from __future__ import annotations

import time
from typing import Optional, Type


def _setup_quality_band(score: float) -> str:
    if score >= 80.0:
        return "STRONG"
    if score >= 60.0:
        return "OK"
    return "WEAK"


def build_pending_short_exhaustion_setup(scanner, symbol: str, pending_cls: Type) -> Optional[object]:
    scanner._funnel_hit("short_exhaustion_retest", "symbols_seen")
    strategy_cfg = scanner.cfg.get("strategy", {})
    if not strategy_cfg.get("short_exhaustion_retest", {}).get("enabled", False):
        scanner._funnel_hit("short_exhaustion_retest", "fail_disabled")
        return None
    scanner_cfg = scanner.cfg["scanner"]
    bars_15m_all = scanner.klines(symbol, scanner_cfg["interval_15m"], limit=60)
    bars_1h_all = scanner.klines(symbol, scanner_cfg["interval_1h"], limit=24)
    if len(bars_15m_all) < 20 or len(bars_1h_all) < 14:
        scanner._funnel_hit("short_exhaustion_retest", "fail_data")
        return None
    bars_15m = bars_15m_all[:-1]
    bars_1h = bars_1h_all[:-1]
    if len(bars_15m) < 16 or len(bars_1h) < 10:
        scanner._funnel_hit("short_exhaustion_retest", "fail_data")
        return None
    scanner._funnel_hit("short_exhaustion_retest", "data_ok")
    if scanner.already_open_signal(symbol, "SHORT") or scanner.already_pending_setup(symbol, "SHORT"):
        scanner._funnel_hit("short_exhaustion_retest", "blocked_duplicate")
        return None
    exhaustion = scanner.detect_1h_exhaustion(bars_1h)
    if not exhaustion:
        scanner._funnel_hit("short_exhaustion_retest", "fail_exhaustion")
        return None
    scanner._funnel_hit("short_exhaustion_retest", "exhaustion_ok")
    breakdown = scanner.detect_15m_breakdown_after_exhaustion(bars_15m)
    if not breakdown:
        scanner._funnel_hit("short_exhaustion_retest", "fail_breakdown")
        return None
    scanner._funnel_hit("short_exhaustion_retest", "breakdown_ok")
    funding_pct = scanner.funding(symbol)
    oi_change_15m = scanner.calc_oi_change_pct(symbol, period="15m", limit=3)
    oi_jump_pct = oi_change_15m if oi_change_15m is not None else 0.0
    signal_bar = breakdown["signal_bar"]
    score_exhaustion = exhaustion["score_1h_exhaustion"]
    score_breakout = breakdown["score_15m_breakdown"]
    score_retest = 0.0
    score = min(100.0, score_exhaustion + score_breakout)
    confidence = max(0.0, min(0.99, score / 100.0))
    reason = f"1h exhaustion {score_exhaustion:.0f}/35 + 15m breakdown {score_breakout:.0f}/30 -> retest fail pending"
    reason_tags = list(exhaustion.get("reason_tags", [])) + list(breakdown.get("reason_tags", []))
    pending_id = f"{symbol}-SHORT-EXH-{signal_bar['open_time']}"
    ts = int(time.time() * 1000)
    btc_ctx = scanner.get_btc_context()
    scanner._funnel_hit("short_exhaustion_retest", "new_pending")
    return pending_cls(
        pending_id=pending_id,
        created_ts_ms=ts,
        signal_open_time=signal_bar["open_time"],
        symbol=symbol,
        side="SHORT",
        score=score,
        confidence=confidence,
        reason=reason,
        breakout_level=breakdown["breakdown_level"],
        signal_price=signal_bar["close"],
        signal_high=signal_bar["high"],
        signal_low=signal_bar["low"],
        oi_jump_pct=oi_jump_pct,
        funding_pct=funding_pct,
        vol_ratio=breakdown["vol_ratio"],
        strategy="short_exhaustion_retest",
        market_regime=btc_ctx["market_regime"],
        btc_price=btc_ctx["btc_price"],
        btc_24h_change_pct=btc_ctx["btc_24h_change_pct"],
        btc_4h_change_pct=btc_ctx["btc_4h_change_pct"],
        btc_1h_change_pct=btc_ctx["btc_1h_change_pct"],
        btc_24h_range_pct=btc_ctx["btc_24h_range_pct"],
        btc_4h_range_pct=btc_ctx["btc_4h_range_pct"],
        alt_market_breadth_pct=btc_ctx["alt_market_breadth_pct"],
        btc_regime=btc_ctx["btc_regime"],
        score_oi=0.0,
        score_exhaustion=score_exhaustion,
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
