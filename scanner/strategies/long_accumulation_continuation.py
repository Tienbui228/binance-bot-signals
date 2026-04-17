from __future__ import annotations

import time
from statistics import median
from typing import Optional, Type


# ── EMA / ATR helpers ─────────────────────────────────────────────────────────

def _ema(values: list, period: int) -> list:
    """Standard EMA seeded with SMA of first `period` values.

    Returns a list of the same length as `values`. Positions before the first
    computable EMA value are padded with None.
    """
    if len(values) < period:
        return [None] * len(values)
    seed = sum(values[:period]) / period
    result: list = [seed]
    k = 2.0 / (period + 1)
    for v in values[period:]:
        result.append(v * k + result[-1] * (1 - k))
    return [None] * (period - 1) + result


def _atr(bars: list, period: int) -> float:
    """Average of the last `period` true-range values (simple average, not Wilder)."""
    if len(bars) < 2:
        return 0.0
    trs = []
    for i in range(1, len(bars)):
        h, l, pc = bars[i]["high"], bars[i]["low"], bars[i - 1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    use = trs[-period:] if len(trs) >= period else trs
    return sum(use) / len(use) if use else 0.0


def _tr_series(bars: list) -> list:
    """True-range value for each bar from index 1 onward."""
    result = []
    for i in range(1, len(bars)):
        h, l, pc = bars[i]["high"], bars[i]["low"], bars[i - 1]["close"]
        result.append(max(h - l, abs(h - pc), abs(l - pc)))
    return result


def _pivot_lows(bars: list, window: int = 2) -> list:
    """Values of pivot lows within `bars`.

    Bar i is a pivot low when its low equals the minimum across
    [i-window, i+window].
    """
    n = len(bars)
    result = []
    for i in range(window, n - window):
        lo = bars[i]["low"]
        if all(bars[j]["low"] >= lo for j in range(i - window, i + window + 1) if j != i):
            result.append(lo)
    return result


# ── Structure score ───────────────────────────────────────────────────────────

def _compute_structure_score(bars_1h_completed: list, ema50_series: list,
                              atr18: float, cfg: dict) -> dict:
    """Score-based structure quality over the last 18 completed 1h bars (0–6 total).

    Component A (0–2): swing-low deterioration
    Component B (0–2): EMA50 integrity
    Component C (0–2): base compression
    """
    window = int(cfg.get("structure_window_bars_1h", 18))
    w = bars_1h_completed[-window:]
    ema50_w = ema50_series[-window:]

    if atr18 <= 0:
        return {"score": 0, "band": "MESSY", "swing_score": 0,
                "ema50_score": 0, "compression_score": 0, "compression_ratio": 1.0}

    # Component A: swing-low deterioration
    pivots = _pivot_lows(w, window=2)
    if len(pivots) >= 2:
        deterioration = (pivots[-2] - pivots[-1]) / atr18
    else:
        half = len(w) // 2
        fl = min(b["low"] for b in w[:half]) if w[:half] else 0.0
        sl = min(b["low"] for b in w[half:]) if w[half:] else 0.0
        deterioration = (fl - sl) / atr18

    swing_score = 2 if deterioration <= 0.25 else (1 if deterioration <= 0.50 else 0)

    # Component B: EMA50 integrity
    below_count = 0
    max_depth_atr = 0.0
    prev_deep = False
    max_consec_deep = 0
    consec_deep = 0
    for i, bar in enumerate(w):
        e50 = ema50_w[i]
        if e50 is None:
            prev_deep = False
            continue
        if bar["close"] < e50:
            below_count += 1
            depth = (e50 - bar["close"]) / atr18
            max_depth_atr = max(max_depth_atr, depth)
            is_deep = depth > 0.40
            if is_deep and prev_deep:
                consec_deep += 1
                max_consec_deep = max(max_consec_deep, consec_deep + 1)
            else:
                consec_deep = 0
            prev_deep = is_deep
        else:
            prev_deep = False
            consec_deep = 0

    if below_count == 0:
        ema50_score = 2
    elif below_count <= 2 and max_depth_atr <= 0.40 and max_consec_deep < 2:
        ema50_score = 1
    else:
        ema50_score = 0

    # Component C: compression
    trs = _tr_series(w)
    if len(trs) >= 6:
        compression_ratio = median(trs[-6:]) / median(trs) if median(trs) > 0 else 1.0
    else:
        compression_ratio = 1.0

    compression_score = 2 if compression_ratio < 0.75 else (1 if compression_ratio < 0.85 else 0)

    total = swing_score + ema50_score + compression_score
    band = "CLEAN" if total >= 5 else ("USABLE" if total == 4 else "MESSY")

    return {
        "score": total, "band": band,
        "swing_score": swing_score, "ema50_score": ema50_score,
        "compression_score": compression_score,
        "compression_ratio": round(compression_ratio, 4),
    }


# ── 1h context ────────────────────────────────────────────────────────────────

def _compute_1h_context(scanner, symbol: str, cfg: dict) -> dict:
    """Evaluate 1h price trend, OI/volume participation, structure, and volume gate."""

    def _fail(reason: str, extra_tags: list = None) -> dict:
        return {
            "pass": False, "fail_reason": reason,
            "reason_tags": (extra_tags or []),
            "participation_label": "NO_SUPPORT",
            "structure_quality_score": 0, "structure_quality_band": "MESSY",
            "oi_distance_pct": 0.0,
            "vol_ratio_1h": 0.0,
            "compression_ratio": 1.0,
        }

    tags: list = []
    scanner_cfg = scanner.cfg.get("scanner", {})

    # Fetch 1h bars — need 65 to reliably compute EMA50 on completed bars
    bars_1h_all = scanner.klines(symbol, scanner_cfg.get("interval_1h", "1h"), limit=65)
    if len(bars_1h_all) < 55:
        scanner._funnel_hit("long_accumulation_continuation", "fail_data")
        return _fail("insufficient_1h_bars", ["fail=1h_bars_insufficient"])

    completed_1h = bars_1h_all[:-1]  # exclude live bar
    closes = [b["close"] for b in completed_1h]

    # EMA20 / EMA50
    ema_fast = int(cfg.get("price_ema_fast", 20))
    ema_slow = int(cfg.get("price_ema_slow", 50))
    ema20_series = _ema(closes, ema_fast)
    ema50_series = _ema(closes, ema_slow)

    ema20_now = ema20_series[-1]
    ema50_now = ema50_series[-1]
    close_now = closes[-1]

    if ema20_now is None or ema50_now is None:
        return _fail("ema_compute_failed", ["fail=ema_compute_failed"])

    # Price trend gate
    if close_now <= ema20_now:
        return _fail("close_below_ema20", tags + ["fail=close_not_above_ema20"])
    if ema20_now <= ema50_now:
        return _fail("ema20_not_above_ema50", tags + ["fail=ema20_not_above_ema50"])

    ema20_6ago = ema20_series[-7]
    if ema20_6ago is None or ema20_6ago <= 0:
        return _fail("ema20_slope_unavailable", tags + ["fail=ema20_slope_unavailable"])
    if ema20_now / ema20_6ago - 1 <= 0:
        return _fail("ema20_slope_flat_or_down", tags + ["fail=ema20_slope_flat_or_down"])

    min_above = int(cfg.get("min_closes_above_fast_ma_12", 8))
    recent12_closes = closes[-12:]
    recent12_ema20 = ema20_series[-12:]
    closes_above = sum(1 for c, e in zip(recent12_closes, recent12_ema20)
                       if e is not None and c > e)
    if closes_above < min_above:
        return _fail("closes_above_ema20_insufficient",
                     tags + [f"fail=closes_above_ema20_{closes_above}_of_12"])

    max_ext = float(cfg.get("max_extension_from_ema50_pct", 0.12))
    ext_pct = close_now / ema50_now - 1 if ema50_now > 0 else 0.0
    if ext_pct > max_ext:
        return _fail("too_extended_from_ema50",
                     tags + [f"extension={ext_pct:.3f}", "fail=too_extended_from_ema50"])

    tags += ["price_above_ema20", "ema20_above_ema50", "ema20_uptrend",
             f"closes_above_ema20={closes_above}"]

    # Volume proxy gate (quote_volume from 1h bars, no perp_volume_24h available)
    min_vol = float(cfg.get("min_quote_volume_4h_usd", 2_000_000))
    vol_mode = cfg.get("volume_gate_mode", "SOFT_TAG")
    try:
        qv4h = sum(b.get("quote_volume", 0.0) for b in completed_1h[-4:])
        if qv4h > 0:
            tag_val = f"{int(qv4h):d}"
            if qv4h >= min_vol:
                tags.append(f"volume_gate=quote_volume_proxy_{tag_val}")
            else:
                tags.append(f"volume_gate=below_proxy_threshold_{tag_val}")
                if vol_mode == "REQUIRE":
                    return _fail("volume_below_proxy_threshold",
                                 tags + ["fail=volume_gate_blocked"])
        else:
            tags.append("volume_gate=unavailable")
    except Exception:
        tags.append("volume_gate=unavailable")

    # OI participation (1h history) — MA20 baseline, 1.50x ratio, not below 12h ago
    oi_ma_period = int(cfg.get("oi_ma_period", 20))
    oi_support_ratio = float(cfg.get("oi_support_ratio", 1.50))
    oi_support = False
    oi_distance_pct = 0.0
    try:
        oi_hist = scanner.oi_hist(symbol, period="1h", limit=32)
        if len(oi_hist) >= oi_ma_period:
            oi_vals = [r["oi_value"] for r in oi_hist]
            oi_baseline_ma20 = sum(oi_vals[-oi_ma_period:]) / oi_ma_period
            oi_now = oi_vals[-1]
            oi_12h_ago = oi_vals[-13] if len(oi_vals) >= 13 else oi_vals[0]
            if oi_baseline_ma20 > 0 and oi_now > 0:
                oi_distance_pct = oi_now / oi_baseline_ma20 - 1
                if (oi_now >= oi_baseline_ma20 * oi_support_ratio
                        and oi_now >= oi_12h_ago):
                    oi_support = True
                    tags += ["oi_support_strong",
                             f"oi_now={oi_now:.0f}",
                             f"oi_baseline={oi_baseline_ma20:.0f}",
                             f"oi_vs_baseline={oi_distance_pct:.3f}"]
                else:
                    tags.append(f"oi_weak_ratio={oi_distance_pct:.3f}")
        else:
            tags.append("oi_data_insufficient")
    except Exception as exc:
        tags.append(f"oi_fetch_error={type(exc).__name__}")

    # Volume participation (1h futures quote_volume — MA20 baseline, 2.00x ratio, not below 12h ago)
    vol_participation_ma_period = int(cfg.get("vol_participation_ma_period", 20))
    vol_participation_ratio = float(cfg.get("vol_participation_ratio", 2.00))
    vol_support = False
    vol_ratio_1h = 0.0
    try:
        vol_vals = [b["quote_volume"] for b in completed_1h]
        if len(vol_vals) >= vol_participation_ma_period:
            vol_baseline = sum(vol_vals[-vol_participation_ma_period:]) / vol_participation_ma_period
            vol_now = vol_vals[-1]
            vol_12h_ago = vol_vals[-13] if len(vol_vals) >= 13 else vol_vals[0]
            if vol_baseline > 0 and vol_now > 0:
                vol_ratio_1h = vol_now / vol_baseline
                if (vol_now >= vol_baseline * vol_participation_ratio
                        and vol_now >= vol_12h_ago):
                    vol_support = True
                    tags += ["vol_support_strong",
                             f"vol_now={vol_now:.0f}",
                             f"vol_baseline={vol_baseline:.0f}",
                             f"vol_vs_baseline={vol_ratio_1h:.3f}"]
                else:
                    tags.append(f"vol_weak_ratio={vol_ratio_1h:.3f}")
        else:
            tags.append("vol_data_insufficient")
    except Exception as exc:
        tags.append(f"vol_fetch_error={type(exc).__name__}")

    # Participation label
    if oi_support and vol_support:
        participation_label = "DUAL_SUPPORT"
        tags.append("participation=DUAL_SUPPORT")
    elif oi_support:
        participation_label = "OI_SUPPORT_STRONG"
        tags.append("participation=OI_SUPPORT_STRONG")
    elif vol_support:
        participation_label = "VOLUME_SUPPORT_STRONG"
        tags.append("participation=VOLUME_SUPPORT_STRONG")
    else:
        tags.append("participation=NO_SUPPORT")
        return _fail("no_participation_support", tags)

    # Structure score on last 18 completed 1h bars
    atr18 = _atr(completed_1h[-19:], 18) if len(completed_1h) >= 19 else 0.0
    struct = _compute_structure_score(completed_1h, ema50_series, atr18, cfg)
    min_struct = int(cfg.get("min_structure_quality_score", 4))

    if struct["score"] < min_struct:
        return _fail("structure_quality_too_low",
                     tags + [f"structure_score={struct['score']}",
                             f"fail=structure_{struct['band']}"])

    tags += [f"structure_score={struct['score']}", f"structure_label={struct['band']}",
             f"compression_ratio={struct['compression_ratio']}"]
    if struct["band"] == "CLEAN":
        tags.append("structure_clean")
    elif struct["band"] == "USABLE":
        tags.append("structure_usable")
    if struct["compression_score"] >= 1:
        tags.append("compression_present")

    scanner._funnel_hit("long_accumulation_continuation", "context_pass")

    return {
        "pass": True, "fail_reason": "",
        "reason_tags": tags,
        "participation_label": participation_label,
        "structure_quality_score": struct["score"],
        "structure_quality_band": struct["band"],
        "oi_distance_pct": oi_distance_pct,
        "vol_ratio_1h": vol_ratio_1h,
        "compression_ratio": struct["compression_ratio"],
    }


# ── 5m breakout-continuation trigger (no retest) ─────────────────────────────

def _compute_5m_trigger(scanner, symbol: str, cfg: dict) -> dict:
    """Evaluate 5m breakout-continuation bar quality. No retest logic.

    Entry = breakout bar close.
    Stop = min(breakout bar low, micro base low) * (1 - stop_buffer_pct).
    """

    def _fail(reason: str, extra_tags: list = None) -> dict:
        return {
            "pass": False, "fail_reason": reason,
            "reason_tags": (extra_tags or []),
            "breakout_level": 0.0, "breakout_distance_pct": 0.0,
            "breakout_body_ratio": 0.0, "breakout_vol_ratio": 0.0,
            "breakout_close_location": 0.0, "breakout_upper_wick": 0.0,
            "entry_price": 0.0, "stop_price": 0.0,
            "signal_bar_high": 0.0, "signal_bar_low": 0.0,
            "signal_bar_open_time": 0,
        }

    tags: list = []
    scanner_cfg = scanner.cfg.get("scanner", {})
    bars_5m_all = scanner.klines(symbol, scanner_cfg.get("interval_5m", "5m"), limit=90)
    if len(bars_5m_all) < 50:
        scanner._funnel_hit("long_accumulation_continuation", "fail_trigger_data")
        return _fail("insufficient_5m_bars", ["fail=5m_bars_insufficient"])

    completed_5m = bars_5m_all[:-1]  # exclude live bar
    signal_bar = completed_5m[-1]

    lookback = int(cfg.get("breakout_lookback_bars_5m", 12))
    if len(completed_5m) < lookback + 2:
        return _fail("insufficient_5m_lookback", ["fail=5m_lookback_insufficient"])

    prior_bars = completed_5m[-(lookback + 1):-1]
    breakout_level = max(b["high"] for b in prior_bars)

    close_s = signal_bar["close"]
    open_s = signal_bar["open"]
    high_s = signal_bar["high"]
    low_s = signal_bar["low"]
    bar_range = max(high_s - low_s, 1e-12)

    min_dist = float(cfg.get("min_breakout_distance_pct", 0.0015))
    min_body = float(cfg.get("min_breakout_body_ratio", 0.45))
    min_vol = float(cfg.get("min_breakout_vol_ratio", 1.20))
    min_close_loc = float(cfg.get("min_close_location", 0.60))
    max_wick = float(cfg.get("max_upper_wick_ratio", 0.30))

    if close_s <= breakout_level:
        scanner._funnel_hit("long_accumulation_continuation", "fail_trigger_breakout")
        return _fail("close_not_above_breakout_level",
                     [f"breakout_level={breakout_level:.6f}", "fail=no_breakout"])

    breakout_dist = close_s / breakout_level - 1
    if breakout_dist < min_dist:
        scanner._funnel_hit("long_accumulation_continuation", "fail_trigger_breakout")
        return _fail("breakout_distance_too_small",
                     [f"breakout_dist={breakout_dist:.4f}", "fail=breakout_dist_too_small"])

    body_ratio = abs(close_s - open_s) / bar_range
    if body_ratio < min_body:
        scanner._funnel_hit("long_accumulation_continuation", "fail_trigger_candle")
        return _fail("body_ratio_too_small",
                     [f"body_ratio={body_ratio:.3f}", "fail=body_too_weak"])

    close_location = (close_s - low_s) / bar_range
    if close_location < min_close_loc:
        scanner._funnel_hit("long_accumulation_continuation", "fail_trigger_candle")
        return _fail("close_location_too_low",
                     [f"close_location={close_location:.3f}", "fail=close_not_near_high"])

    upper_wick = (high_s - close_s) / bar_range
    if upper_wick > max_wick:
        scanner._funnel_hit("long_accumulation_continuation", "fail_trigger_candle")
        return _fail("upper_wick_too_large",
                     [f"upper_wick={upper_wick:.3f}", "fail=heavy_upper_wick"])

    vol_recent = sum(b["volume"] for b in completed_5m[-3:]) / 3.0
    vol_base = sum(b["volume"] for b in prior_bars) / max(len(prior_bars), 1)
    vol_ratio = vol_recent / vol_base if vol_base > 0 else 0.0
    if vol_ratio < min_vol:
        scanner._funnel_hit("long_accumulation_continuation", "fail_trigger_vol")
        return _fail("volume_ratio_too_low",
                     [f"vol_ratio={vol_ratio:.2f}", "fail=vol_too_weak"])

    # Stop: min(signal bar low, micro base low of last 6 prior bars), with buffer
    micro_base_low = min(b["low"] for b in prior_bars[-6:]) if len(prior_bars) >= 6 \
        else min(b["low"] for b in prior_bars)
    stop_raw = min(low_s, micro_base_low)
    stop_buf = float(cfg.get("stop_buffer_pct", 0.0015))
    stop_price = stop_raw * (1 - stop_buf)

    tags += [
        "breakout_confirmed",
        f"breakout_level={breakout_level:.6f}",
        f"breakout_distance_pct={breakout_dist:.4f}",
        f"breakout_body_ratio={body_ratio:.3f}",
        f"breakout_vol_ratio={vol_ratio:.2f}",
        f"breakout_close_location={close_location:.3f}",
        f"breakout_upper_wick={upper_wick:.3f}",
    ]

    scanner._funnel_hit("long_accumulation_continuation", "trigger_pass")

    return {
        "pass": True, "fail_reason": "",
        "reason_tags": tags,
        "breakout_level": breakout_level,
        "breakout_distance_pct": breakout_dist,
        "breakout_body_ratio": body_ratio,
        "breakout_vol_ratio": vol_ratio,
        "breakout_close_location": close_location,
        "breakout_upper_wick": upper_wick,
        "entry_price": close_s,
        "stop_price": stop_price,
        "signal_bar_high": high_s,
        "signal_bar_low": low_s,
        "signal_bar_open_time": signal_bar["open_time"],
    }


# ── Setup quality band ────────────────────────────────────────────────────────

def _acc_cont_quality_band(participation_label: str, struct_score: int,
                           vol_ratio: float, close_location: float) -> str:
    """Derive A/B/C from context quality + structure score + breakout bar quality.

    No retest quality component — breakout-continuation only.
    """
    if (participation_label == "DUAL_SUPPORT"
            and struct_score >= 5
            and vol_ratio >= 1.5
            and close_location >= 0.75):
        return "A"
    if struct_score >= 4 and vol_ratio >= 1.2 and close_location >= 0.60:
        return "B"
    return "C"


# ── Main entry point ──────────────────────────────────────────────────────────

def build_pending_long_accumulation_continuation_setup(
    scanner, symbol: str, pending_cls: Type
) -> Optional[object]:
    """Detect a long_accumulation_continuation breakout-continuation setup.

    Identity: long_accumulation_continuation / LONG
    Thesis:   1h trend + OI/volume participation + controlled base → clean 5m breakout bar
    Entry:    breakout bar close (no retest wait — distinct from long_breakout_retest)
    Stop:     min(breakout bar low, micro base low) * (1 - stop_buffer_pct)
    """
    enabled = (scanner.cfg.get("strategy", {})
               .get("long_accumulation_continuation", {})
               .get("enabled", True))
    if not enabled:
        return None

    scanner._funnel_hit("long_accumulation_continuation", "symbols_seen")

    cfg = scanner.cfg.get("long_accumulation_continuation", {})

    # Duplicate check
    if scanner.already_open_signal(symbol, "LONG"):
        scanner._funnel_hit("long_accumulation_continuation", "blocked_duplicate")
        return None

    scanner._funnel_hit("long_accumulation_continuation", "data_ok")

    # 1h context: trend + participation + structure
    context = _compute_1h_context(scanner, symbol, cfg)
    if not context["pass"]:
        scanner._funnel_hit("long_accumulation_continuation", "context_fail")
        return None

    # 5m breakout-continuation trigger (no retest)
    trigger = _compute_5m_trigger(scanner, symbol, cfg)
    if not trigger["pass"]:
        scanner._funnel_hit("long_accumulation_continuation", "trigger_fail")
        return None

    # Quality band (context + structure + breakout bar quality, no retest)
    band = _acc_cont_quality_band(
        context["participation_label"],
        context["structure_quality_score"],
        trigger["breakout_vol_ratio"],
        trigger["breakout_close_location"],
    )

    # Score / confidence
    score = min(100.0,
                60.0
                + context["structure_quality_score"] * 5.0
                + (5.0 if context["participation_label"] == "DUAL_SUPPORT" else 0.0)
                + (5.0 if band == "A" else 0.0))
    confidence = {"A": 0.80, "B": 0.65, "C": 0.50}.get(band, 0.50)

    # Assemble reason_tags
    all_tags = ["family=long_accumulation_continuation"]
    all_tags += context["reason_tags"]
    all_tags += trigger["reason_tags"]
    all_tags.append(f"setup_quality_band={band}")

    reason = (
        f"acc_cont LONG {symbol}: {context['participation_label']} "
        f"struct={context['structure_quality_band']}({context['structure_quality_score']}) "
        f"bk_dist={trigger['breakout_distance_pct']:.4f} "
        f"vol={trigger['breakout_vol_ratio']:.2f} band={band}"
    )

    ts = int(time.time() * 1000)
    pending_id = f"{symbol}-LONG-{trigger['signal_bar_open_time']}"
    btc_ctx = scanner.get_btc_context()

    scanner._funnel_hit("long_accumulation_continuation", "new_pending")

    # NOTE on proxy field reuse:
    #   oi_jump_pct → carries oi_distance_pct (OI now / EMA20 baseline - 1).
    #                 This is NOT a 5m OI jump delta. Displayed as "OI vs EMA20" in formatter.
    #   score_oi    → carries oi_distance_pct * 100 for display consistency.
    #   score_retest → always 0.0 (no retest logic in this family).
    return pending_cls(
        pending_id=pending_id,
        created_ts_ms=ts,
        signal_open_time=trigger["signal_bar_open_time"],
        symbol=symbol,
        side="LONG",
        score=round(score, 2),
        confidence=round(confidence, 4),
        reason=reason,
        breakout_level=trigger["breakout_level"],
        signal_price=trigger["entry_price"],
        signal_high=trigger["signal_bar_high"],
        signal_low=trigger["signal_bar_low"],
        oi_jump_pct=round(context["oi_distance_pct"], 6),  # proxy: oi_distance_pct
        funding_pct=0.0,
        vol_ratio=round(trigger["breakout_vol_ratio"], 4),
        strategy="long_accumulation_continuation",
        market_regime=btc_ctx["market_regime"],
        btc_price=btc_ctx["btc_price"],
        btc_24h_change_pct=btc_ctx["btc_24h_change_pct"],
        btc_4h_change_pct=btc_ctx["btc_4h_change_pct"],
        btc_1h_change_pct=btc_ctx["btc_1h_change_pct"],
        btc_24h_range_pct=btc_ctx["btc_24h_range_pct"],
        btc_4h_range_pct=btc_ctx["btc_4h_range_pct"],
        alt_market_breadth_pct=btc_ctx["alt_market_breadth_pct"],
        btc_regime=btc_ctx["btc_regime"],
        score_oi=round(context["oi_distance_pct"] * 100, 4),  # proxy: oi_distance * 100
        score_exhaustion=0.0,
        score_breakout=round(trigger["breakout_distance_pct"] * 100, 4),
        score_retest=0.0,  # no retest in this family
        reason_tags=";".join(all_tags),
        regime_label="unknown",
        regime_fit_for_strategy="not_evaluated",
        setup_quality_band=band,
        delivery_band="not_evaluated",
        veto_reason_code="not_evaluated",
        dispatch_action="not_evaluated",
        dispatch_confidence_band="not_evaluated",
        status="PENDING",
    )
