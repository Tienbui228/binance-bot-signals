"""
OI Range Breakout Strategy — V1 Long Only

Detects smart money aggressive positioning in range via:
  - OI spike: ≥10% on 1h bar
  - Volume spike: ≥2.0× EMA20 on 1h bar
  - Range consolidation: confirmed by ATR ratio filter

Signal fires immediately when bar closes meeting both conditions.
SL/TP calculated from detected range height.
"""

import logging

import requests

logger = logging.getLogger(__name__)


def fetch_market_cap(symbol: str) -> float | None:
    base = symbol.lower().removesuffix("usdt")
    try:
        search = requests.get(
            "https://api.coingecko.com/api/v3/search",
            params={"query": base},
            timeout=5,
        ).json()
        coins = search.get("coins", [])
        if not coins:
            return None
        exact = [c for c in coins if c.get("symbol", "").lower() == base]
        cg_id = exact[0]["id"] if exact else coins[0]["id"]
        price = requests.get(
            "https://api.coingecko.com/api/v3/simple/price",
            params={"ids": cg_id, "vs_currencies": "usd", "include_market_cap": "true"},
            timeout=5,
        ).json()
        return float(price.get(cg_id, {}).get("usd_market_cap", 0)) or None
    except Exception:
        return None


def calc_oi_delta_pct(oi_current: float, oi_prev: float) -> float:
    """
    Calculate OI delta percentage.

    Returns float. Positive = OI increasing.
    """
    if oi_prev <= 0:
        return 0.0
    return (oi_current - oi_prev) / oi_prev * 100.0


def calc_ema(series: list[float], period: int) -> float:
    """
    Calculate exponential moving average.

    Multiplier = 2 / (period + 1)
    Seed = SMA of first 'period' values
    Returns EMA of the last element.
    """
    if len(series) < period:
        return float("nan")

    multiplier = 2.0 / (period + 1)
    ema = sum(series[:period]) / period  # SMA seed

    for val in series[period:]:
        ema = val * multiplier + ema * (1 - multiplier)

    return ema


def calc_atr(
    highs: list[float],
    lows: list[float],
    closes: list[float],
    period: int,
) -> tuple[float | None, float | None, float | None]:
    """
    Calculate ATR (current, average, all series).

    Returns: (atr_current, atr_avg, atr_series) or (None, None, None) if invalid

    Note: atr_avg = mean of ALL ATR values in the series (not final smoothed value).
    """
    if len(closes) < period + 1:
        return None, None, None

    # Compute true range for each bar
    trs = []
    for i in range(1, len(closes)):
        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i] - closes[i - 1]),
        )
        trs.append(tr)

    if len(trs) < period:
        return None, None, None

    # Wilder's smoothing for ATR series
    atr_series = []
    atr_val = sum(trs[:period]) / period
    atr_series.append(atr_val)

    for tr in trs[period:]:
        atr_val = (atr_val * (period - 1) + tr) / period
        atr_series.append(atr_val)

    atr_current = atr_series[-1]
    atr_avg = sum(atr_series) / len(atr_series)

    return atr_current, atr_avg, atr_series


def detect_range(
    high_series: list[float],
    low_series: list[float],
    close_series: list[float],
    window: int,
    max_range_width_pct: float,
    min_range_width_pct: float,
    atr_period: int,
    atr_ratio_max: float,
) -> dict | None:
    """
    Detect consolidation range with ATR ratio filter.

    Conditions:
      1. range_width_pct in [min, max]
      2. ATR current / ATR avg < atr_ratio_max (volatility contracting)

    Excludes signal bar (-1) to avoid lookahead bias.
    Returns range dict or None if invalid.
    """
    # Exclude signal bar
    highs = high_series[-window - 1 : -1]
    lows = low_series[-window - 1 : -1]
    closes = close_series[-window - 1 : -1]

    if len(highs) < atr_period + 1:
        return None

    # ── Range width check ──
    range_high = max(highs)
    range_low = min(lows)
    range_height = range_high - range_low

    if range_low <= 0:
        return None

    range_width_pct = (range_height / range_low) * 100.0

    if range_width_pct > max_range_width_pct:
        return None  # trending or too volatile
    if range_width_pct < min_range_width_pct:
        return None  # noise, range meaningless

    # ── ATR ratio check ──
    atr_current, atr_avg, atr_series = calc_atr(highs, lows, closes, atr_period)

    if atr_current is None or atr_avg is None or atr_avg == 0:
        return None

    atr_ratio = atr_current / atr_avg

    if atr_ratio > atr_ratio_max:
        return None  # volatility not contracting

    return {
        "range_high": range_high,
        "range_low": range_low,
        "range_height": range_height,
        "midpoint": (range_high + range_low) / 2.0,
        "range_width_pct": range_width_pct,
        "atr_current": atr_current,
        "atr_avg": atr_avg,
        "atr_ratio": atr_ratio,
    }


def calc_sl_tp(
    entry_price: float,
    range_dict: dict,
    stop_buffer_pct: float,
    tp1_multiplier: float,
    tp2_multiplier: float,
) -> dict | None:
    """
    Calculate SL and TP levels from range.

    SL: below midpoint - buffer
    TP1: entry + 1.0× range_height
    TP2: entry + 1.5× range_height

    Returns dict or None if invalid.
    """
    range_low = range_dict["range_low"]
    range_height = range_dict["range_height"]

    # SL below range_low with buffer — valid regardless of entry position
    sl_price = range_low * (1.0 - stop_buffer_pct / 100.0)
    sl_dist = entry_price - sl_price

    if sl_dist <= 0:
        return None  # entry already below range_low (breakdown)

    tp1_price = entry_price + tp1_multiplier * range_height
    tp2_price = entry_price + tp2_multiplier * range_height

    risk_pct = (sl_dist / entry_price) * 100.0

    return {
        "sl_price": sl_price,
        "tp1_price": tp1_price,
        "tp2_price": tp2_price,
        "risk_pct": risk_pct,
    }


def calc_score(
    oi_delta_pct: float,
    vol_ratio: float,
    range_width_pct: float,
) -> int:
    """
    Calculate signal score (0–100).

    - OI strength: max 40 pts
    - Volume strength: max 40 pts
    - Range quality: max 20 pts
    """
    score = 0

    # OI component (40 pts)
    if oi_delta_pct >= 30:
        score += 40
    elif oi_delta_pct >= 20:
        score += 30
    elif oi_delta_pct >= 15:
        score += 20
    elif oi_delta_pct >= 10:
        score += 10
    elif oi_delta_pct >= 5:
        score += 5

    # Volume component (40 pts)
    if vol_ratio >= 4.0:
        score += 40
    elif vol_ratio >= 3.0:
        score += 30
    elif vol_ratio >= 2.5:
        score += 20
    elif vol_ratio >= 2.0:
        score += 10

    # Range quality (20 pts) — tighter range = better
    if range_width_pct <= 1.0:
        score += 20
    elif range_width_pct <= 2.0:
        score += 15
    elif range_width_pct <= 3.5:
        score += 10
    else:
        score += 5

    return score


def quality_band(score: int) -> str:
    """Map score to quality band."""
    SCORE_HIGH_CONVICTION = 70
    SCORE_STRONG = 50

    if score >= SCORE_HIGH_CONVICTION:
        return "HIGH_CONVICTION"
    if score >= SCORE_STRONG:
        return "STRONG"
    return "WEAK"


def detect_oi_range_breakout(
    symbol: str,
    klines_1h: list[dict],
    oi_history_15m: list[dict],
    config: dict,
) -> dict | None:
    """
    Main detection function for oi_range_breakout strategy.

    Args:
        symbol: trading pair (e.g., "BTCUSDT")
        klines_1h: list of 1h klines, ≥22 bars, sorted ascending
        oi_history_15m: list of 5m OI history, ≥13 bars sorted ascending
        config: strategy config dict

    Returns:
        Signal dict or None if no setup detected.
    """
    # ── Extract config ──
    oi_spike_min_pct = float(config.get("oi_spike_min_pct", 10.0))
    vol_ema_period = int(config.get("vol_ema_period", 20))
    vol_ema_multiplier = float(config.get("vol_ema_multiplier", 2.0))

    range_lookback = int(config.get("range_lookback_bars_1h", 20))
    min_range_width_pct = float(config.get("min_range_width_pct", 0.5))
    max_range_width_pct = float(config.get("max_range_width_pct", 5.0))

    atr_period = int(config.get("range_atr_period", 14))
    atr_ratio_max = float(config.get("range_atr_ratio_max", 0.8))

    stop_buffer_pct = float(config.get("stop_buffer_pct", 0.2))
    tp1_mult = float(config.get("tp1_range_multiplier", 1.0))
    tp2_mult = float(config.get("tp2_range_multiplier", 1.5))

    min_risk_pct = float(config.get("min_risk_pct", 0.5))
    max_risk_pct = float(config.get("max_risk_pct", 8.0))

    score_min_send = int(config.get("score_min_send", 20))
    score_strong = int(config.get("score_strong_signal", 50))
    score_high_conv = int(config.get("score_high_conviction", 70))

    # ── Validate inputs ──
    if not klines_1h or len(klines_1h) < 22:
        logger.warning(f"[oi_range_breakout] {symbol} — insufficient klines (<22)")
        return None

    if not oi_history_15m or len(oi_history_15m) < 13:
        logger.warning(f"[oi_range_breakout] {symbol} — insufficient OI history (<13 bars)")
        return None

    # ── Step 1: OI delta — rolling 60 min via 5m bars ──
    # bar[-1] = now, bar[-13] = 60 min ago (12 × 5m = 60 min)
    oi_now    = float(oi_history_15m[-1].get("oi_value", 0))
    oi_1h_ago = float(oi_history_15m[-13].get("oi_value", 0))
    oi_delta_pct = calc_oi_delta_pct(oi_now, oi_1h_ago)

    if oi_delta_pct < oi_spike_min_pct:
        logger.debug(
            f"[oi_range_breakout] {symbol} — OI spike insufficient: {oi_delta_pct:.2f}% < {oi_spike_min_pct}%"
        )
        return None

    # ── Step 2: Volume EMA ──
    vol_series = [float(k.get("volume", 0)) for k in klines_1h]
    vol_ema20 = calc_ema(vol_series[:-1], vol_ema_period)  # exclude current bar

    if vol_ema20 is None or vol_ema20 <= 0:
        logger.debug(f"[oi_range_breakout] {symbol} — EMA20 computation failed")
        return None

    volume_1h = float(klines_1h[-1].get("volume", 0))
    vol_ratio = volume_1h / vol_ema20 if vol_ema20 > 0 else 0.0

    if vol_ratio < vol_ema_multiplier:
        logger.debug(
            f"[oi_range_breakout] {symbol} — volume spike insufficient: {vol_ratio:.2f}x < {vol_ema_multiplier}x"
        )
        return None

    # ── Step 2.5: Market cap filter ──
    max_market_cap_usd = float(config.get("max_market_cap_usd", 150_000_000))
    market_cap = fetch_market_cap(symbol)
    if market_cap is not None and market_cap >= max_market_cap_usd:
        logger.debug(
            f"[oi_range_breakout] {symbol} — market cap too large: ${market_cap:,.0f} >= ${max_market_cap_usd:,.0f}"
        )
        return None

    # ── Step 3: Range detection ──
    high_series = [float(k.get("high", 0)) for k in klines_1h]
    low_series = [float(k.get("low", 0)) for k in klines_1h]
    close_series = [float(k.get("close", 0)) for k in klines_1h]

    range_result = detect_range(
        high_series,
        low_series,
        close_series,
        range_lookback,
        max_range_width_pct,
        min_range_width_pct,
        atr_period,
        atr_ratio_max,
    )

    if range_result is None:
        logger.debug(
            f"[oi_range_breakout] {symbol} — range detection failed (width/ATR invalid)"
        )
        return None

    # ── Step 4: SL/TP calculation ──
    entry_price = close_series[-1]
    sl_tp = calc_sl_tp(
        entry_price,
        range_result,
        stop_buffer_pct,
        tp1_mult,
        tp2_mult,
    )

    if sl_tp is None:
        logger.debug(f"[oi_range_breakout] {symbol} — SL/TP calculation failed")
        return None

    # ── Risk validation ──
    risk_pct = sl_tp["risk_pct"]
    if risk_pct < min_risk_pct or risk_pct > max_risk_pct:
        logger.debug(
            f"[oi_range_breakout] {symbol} — risk {risk_pct:.2f}% outside [{min_risk_pct}, {max_risk_pct}]"
        )
        return None

    # ── Step 5: Score ──
    score = calc_score(oi_delta_pct, vol_ratio, range_result["range_width_pct"])

    if score < score_min_send:
        logger.debug(
            f"[oi_range_breakout] {symbol} — score {score} < min_send {score_min_send}"
        )
        return None

    band = quality_band(score)

    # ── Build signal dict ──
    now_ms = int(__import__("time").time() * 1000)
    signal_open_ts = int(klines_1h[-1].get("openTime", now_ms))

    return {
        "strategy_family": "oi_range_breakout",
        "side": "long",
        "symbol": symbol,
        "oi_current": oi_now,
        "oi_prev": oi_1h_ago,
        "oi_delta_pct": oi_delta_pct,
        "volume_1h": volume_1h,
        "vol_ema20": vol_ema20,
        "vol_ratio": vol_ratio,
        "range_high": range_result["range_high"],
        "range_low": range_result["range_low"],
        "range_height": range_result["range_height"],
        "range_width_pct": range_result["range_width_pct"],
        "midpoint": range_result["midpoint"],
        "atr_current": range_result["atr_current"],
        "atr_avg": range_result["atr_avg"],
        "atr_ratio": range_result["atr_ratio"],
        "entry_price": entry_price,
        "sl_price": sl_tp["sl_price"],
        "tp1_price": sl_tp["tp1_price"],
        "tp2_price": sl_tp["tp2_price"],
        "risk_pct": risk_pct,
        "score": score,
        "setup_quality_band": band,
        "signal_ts_ms": now_ms,
        "trigger_bar_open_ts": signal_open_ts,
    }
