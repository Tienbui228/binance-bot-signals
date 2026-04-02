from __future__ import annotations

from typing import Dict, List, Optional


def avg(vals: List[float]) -> float:
    return sum(vals) / max(len(vals), 1)


def price_change_pct(bars: List[Dict], lookback_bars: int = 1) -> float:
    if not bars or len(bars) <= lookback_bars:
        return 0.0
    prev = float(bars[-(lookback_bars + 1)].get("close", 0.0) or 0.0)
    curr = float(bars[-1].get("close", 0.0) or 0.0)
    if prev <= 0:
        return 0.0
    return (curr - prev) / prev * 100.0


def range_pct(bar: Dict) -> float:
    high = float(bar.get("high", 0.0) or 0.0)
    low = float(bar.get("low", 0.0) or 0.0)
    close = float(bar.get("close", 0.0) or 0.0)
    base = close if close > 0 else high if high > 0 else 0.0
    if base <= 0:
        return 0.0
    return (high - low) / base * 100.0


def calc_oi_change_pct(hist: List[Dict]) -> Optional[float]:
    if len(hist) < 2:
        return None
    prev = float(hist[-2]["oi_value"])
    curr = float(hist[-1]["oi_value"])
    if prev <= 0:
        return None
    return (curr - prev) / prev * 100.0


def volume_ratio_generic(bars_closed: List[Dict], recent_bars: int = 1, base_bars: int = 6) -> float:
    needed = recent_bars + base_bars
    if len(bars_closed) < needed + 1:
        return 0.0
    recent = avg([b["quote_volume"] for b in bars_closed[-recent_bars:]])
    base = avg([b["quote_volume"] for b in bars_closed[-(recent_bars + base_bars):-recent_bars]])
    if base <= 0:
        return 0.0
    return recent / base


def wick_ratio(bar: Dict) -> float:
    high, low, op, cl = bar["high"], bar["low"], bar["open"], bar["close"]
    total = max(high - low, 1e-12)
    body_high = max(op, cl)
    body_low = min(op, cl)
    upper = high - body_high
    lower = body_low - low
    return max(upper, lower) / total


def upper_wick_ratio(bar: Dict) -> float:
    total = max(bar["high"] - bar["low"], 1e-12)
    body_high = max(bar["open"], bar["close"])
    return max(bar["high"] - body_high, 0.0) / total


def lower_wick_ratio(bar: Dict) -> float:
    total = max(bar["high"] - bar["low"], 1e-12)
    body_low = min(bar["open"], bar["close"])
    return max(body_low - bar["low"], 0.0) / total


def candle_body_ratio(bar: Dict) -> float:
    total = max(bar["high"] - bar["low"], 1e-12)
    body = abs(bar["close"] - bar["open"])
    return body / total


def trend_15m(bars_15m_closed: List[Dict]) -> str:
    if len(bars_15m_closed) < 8:
        return "flat"
    highs = [b["high"] for b in bars_15m_closed[-4:]]
    lows = [b["low"] for b in bars_15m_closed[-4:]]
    if highs[-1] >= highs[-2] >= highs[-3] and lows[-1] >= lows[-2] >= lows[-3]:
        return "up"
    if highs[-1] <= highs[-2] <= highs[-3] and lows[-1] <= lows[-2] <= lows[-3]:
        return "down"
    return "flat"


def trend_1h(bars_1h_closed: List[Dict]) -> str:
    if len(bars_1h_closed) < 6:
        return "flat"
    closes = [b["close"] for b in bars_1h_closed[-6:]]
    if closes[-1] > closes[-3] > closes[-5]:
        return "up"
    if closes[-1] < closes[-3] < closes[-5]:
        return "down"
    return "flat"
