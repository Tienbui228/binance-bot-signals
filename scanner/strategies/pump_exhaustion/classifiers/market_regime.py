from typing import Dict, List


def classify_market_regime(client, watchlist_cases: List[Dict]) -> Dict:
    """Compute market-level regime for scoring group F.

    client — object with .klines(symbol, interval, limit) method.
    watchlist_cases — active cases (for alts_declining_pct heuristic).

    Returns:
      regime_label:        BROAD_WEAKNESS | BTC_WEAK_ALTS_MIXED | NEUTRAL_MIXED | TRENDING_UP
      regime_score:        0 / 1 / 2
      btc_vs_ema20_pct:    float
      ema20_slope:         float
      alts_declining_pct:  float
      regime_note:         str
    """
    try:
        btc_bars = client.klines("BTCUSDT", "1h", 55)
    except Exception:
        return _regime_unclear()

    if not btc_bars or len(btc_bars) < 50:
        return _regime_unclear()

    closes = [b["close"] for b in btc_bars]
    ema20_series = _ema(closes, 20)
    ema50_series = _ema(closes, 50)

    btc_current = closes[-1]
    ema20_now = ema20_series[-1]
    ema50_now = ema50_series[-1]

    ema20_prev6 = ema20_series[-6] if len(ema20_series) >= 6 else ema20_series[0]
    ema20_slope = (ema20_now - ema20_prev6) / ema20_prev6 if ema20_prev6 != 0 else 0.0
    btc_vs_ema20_pct = (btc_current - ema20_now) / ema20_now if ema20_now != 0 else 0.0

    alts_declining_pct = _compute_alts_declining(watchlist_cases)

    # Classify
    if (btc_current < ema20_now < ema50_now
            and ema20_slope < -0.002
            and alts_declining_pct >= 0.60):
        label, score = "BROAD_WEAKNESS", 2
    elif btc_current < ema20_now and not (ema20_now < ema50_now):
        label, score = "BTC_WEAK_ALTS_MIXED", 1
    elif btc_current < ema20_now:
        label, score = "BTC_WEAK_ALTS_MIXED", 1
    elif btc_current > ema20_now > ema50_now and ema20_slope > 0.002:
        label, score = "TRENDING_UP", 0
    else:
        label, score = "NEUTRAL_MIXED", 0

    note = "REGIME BULLISH — squeeze risk" if label == "TRENDING_UP" else ""

    return {
        "regime_label": label,
        "regime_score": score,
        "btc_vs_ema20_pct": btc_vs_ema20_pct,
        "ema20_slope": ema20_slope,
        "alts_declining_pct": alts_declining_pct,
        "regime_note": note,
    }


def _ema(series: List[float], period: int) -> List[float]:
    if len(series) < period:
        return [series[-1]] * len(series) if series else []
    k = 2.0 / (period + 1)
    result = [sum(series[:period]) / period]
    for price in series[period:]:
        result.append(price * k + result[-1] * (1 - k))
    # Pad start with first EMA value so index aligns with series
    pad = [result[0]] * (period - 1)
    return pad + result


def _compute_alts_declining(cases: List[Dict]) -> float:
    """Fraction of active cases where current price is below their recorded current_price_at_scan."""
    if not cases:
        return 0.0
    declining = 0
    valid = 0
    for c in cases:
        try:
            ref = float(c.get("current_price_at_scan") or 0)
            cur = float(c.get("current_price_at_scan") or 0)
        except (TypeError, ValueError):
            continue
        if ref > 0:
            valid += 1
            if cur < ref:
                declining += 1
    return declining / valid if valid > 0 else 0.0


def _regime_unclear() -> Dict:
    return {
        "regime_label": "NEUTRAL_MIXED",
        "regime_score": 0,
        "btc_vs_ema20_pct": 0.0,
        "ema20_slope": 0.0,
        "alts_declining_pct": 0.0,
        "regime_note": "insufficient BTC data",
    }
