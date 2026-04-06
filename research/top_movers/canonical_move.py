"""
research/top_movers/canonical_move.py

Builds one canonical move per token-day.

For gainers  (LONG):  dominant upward move of the day
For losers   (SHORT): dominant downward move of the day

AUTO_V1 break candidate detection:
  LONG:
    1. Filter to UTC day bars in the 5m series
    2. Scan for bars where close > max(high of prior 3 bars) with positive volume surge
    3. Among those, pick the one with highest volume
    4. Fallback: bar with max close in the UTC day

  SHORT:
    Mirror: close < min(low of prior 3 bars) with positive volume surge
    Fallback: bar with min close in the UTC day

The break_candidate_bar_idx is an index into the full bars_5m array
(which includes LOOKBACK_BARS_5M = 96 pre-day bars at the start).
"""

from dataclasses import dataclass
from typing import Dict, List, Optional

from research.top_movers.io import LOOKBACK_BARS_5M


@dataclass
class CanonicalMove:
    symbol: str
    research_day: str
    side: str            # LONG or SHORT
    daily_return_pct: float
    day_open: float
    day_close: float
    day_high: float
    day_low: float
    move_magnitude_pct: float
    break_candidate_bar_idx: int   # index in bars_5m (including pre-lookback)
    approximate_break_ts_ms: int
    detection_method: str = "AUTO_V1"
    data_quality_ok: bool = True
    data_quality_note: str = ""


def _find_break_candidate_long(
    bars_5m: List[Dict],
    day_start_idx: int,
) -> int:
    """Find the break candidate bar for a LONG move.

    Returns an index in bars_5m.
    """
    best_idx = None
    best_vol = -1.0

    for i in range(day_start_idx + 3, len(bars_5m)):
        bar = bars_5m[i]
        prior_high = max(bars_5m[j]["high"] for j in range(i - 3, i))

        # Breakout condition: close above prior 3-bar high
        if bar["close"] <= prior_high:
            continue

        # Volume check: above average
        avg_vol_start = max(0, i - 12)
        recent_bars = bars_5m[avg_vol_start:i]
        if recent_bars:
            avg_vol = sum(b["volume"] for b in recent_bars) / len(recent_bars)
        else:
            avg_vol = 0.0

        if bar["volume"] < avg_vol * 1.1:
            continue

        if bar["volume"] > best_vol:
            best_vol = bar["volume"]
            best_idx = i

    if best_idx is not None:
        return best_idx

    # Fallback: bar with max close in UTC day
    day_bars = bars_5m[day_start_idx:]
    if not day_bars:
        return len(bars_5m) - 1
    max_close_offset = max(range(len(day_bars)), key=lambda j: day_bars[j]["close"])
    return day_start_idx + max_close_offset


def _find_break_candidate_short(
    bars_5m: List[Dict],
    day_start_idx: int,
) -> int:
    """Find the break candidate bar for a SHORT move."""
    best_idx = None
    best_vol = -1.0

    for i in range(day_start_idx + 3, len(bars_5m)):
        bar = bars_5m[i]
        prior_low = min(bars_5m[j]["low"] for j in range(i - 3, i))

        if bar["close"] >= prior_low:
            continue

        avg_vol_start = max(0, i - 12)
        recent_bars = bars_5m[avg_vol_start:i]
        if recent_bars:
            avg_vol = sum(b["volume"] for b in recent_bars) / len(recent_bars)
        else:
            avg_vol = 0.0

        if bar["volume"] < avg_vol * 1.1:
            continue

        if bar["volume"] > best_vol:
            best_vol = bar["volume"]
            best_idx = i

    if best_idx is not None:
        return best_idx

    # Fallback: bar with min close in UTC day
    day_bars = bars_5m[day_start_idx:]
    if not day_bars:
        return len(bars_5m) - 1
    min_close_offset = min(range(len(day_bars)), key=lambda j: day_bars[j]["close"])
    return day_start_idx + min_close_offset


def build_canonical_move(
    symbol: str,
    side: str,
    research_day: str,
    bars_5m: List[Dict],
    token_bar: Dict,
    day_start_ms: int,
) -> CanonicalMove:
    """Build a CanonicalMove for one token.

    bars_5m: full series including pre-day lookback (first LOOKBACK_BARS_5M bars are pre-day).
    token_bar: the 1d kline bar for context.
    day_start_ms: UTC day start in ms.
    """
    if len(bars_5m) < LOOKBACK_BARS_5M + 3:
        return CanonicalMove(
            symbol=symbol,
            research_day=research_day,
            side=side,
            daily_return_pct=float(token_bar.get("daily_return_pct", 0)),
            day_open=float(token_bar.get("day_open", 0)),
            day_close=float(token_bar.get("day_close", 0)),
            day_high=float(token_bar.get("day_high", 0)),
            day_low=float(token_bar.get("day_low", 0)),
            move_magnitude_pct=0.0,
            break_candidate_bar_idx=LOOKBACK_BARS_5M,
            approximate_break_ts_ms=day_start_ms,
            data_quality_ok=False,
            data_quality_note="insufficient_5m_bars",
        )

    # Find the actual day start index in bars_5m (first bar with open_time >= day_start_ms)
    day_start_idx = LOOKBACK_BARS_5M  # default
    for i, b in enumerate(bars_5m):
        if b["open_time"] >= day_start_ms:
            day_start_idx = i
            break

    if side == "LONG":
        bc_idx = _find_break_candidate_long(bars_5m, day_start_idx)
    else:
        bc_idx = _find_break_candidate_short(bars_5m, day_start_idx)

    bc_bar = bars_5m[bc_idx]
    bc_ts = bc_bar["open_time"]

    # Move magnitude: from day low to day high (normalized by day low)
    day_bars = bars_5m[day_start_idx:]
    if day_bars:
        day_high = max(b["high"] for b in day_bars)
        day_low = min(b["low"] for b in day_bars)
        if day_low > 0:
            magnitude = (day_high - day_low) / day_low * 100.0
        else:
            magnitude = abs(float(token_bar.get("daily_return_pct", 0)))
    else:
        day_high = float(token_bar.get("day_high", 0))
        day_low = float(token_bar.get("day_low", 0))
        magnitude = abs(float(token_bar.get("daily_return_pct", 0)))

    return CanonicalMove(
        symbol=symbol,
        research_day=research_day,
        side=side,
        daily_return_pct=float(token_bar.get("daily_return_pct", 0)),
        day_open=float(token_bar.get("day_open", 0)),
        day_close=float(token_bar.get("day_close", 0)),
        day_high=day_high,
        day_low=day_low,
        move_magnitude_pct=round(magnitude, 4),
        break_candidate_bar_idx=bc_idx,
        approximate_break_ts_ms=bc_ts,
        detection_method="AUTO_V1",
        data_quality_ok=True,
    )
