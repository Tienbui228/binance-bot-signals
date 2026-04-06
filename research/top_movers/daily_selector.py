"""
research/top_movers/daily_selector.py

Selects top 10 gainers and top 10 losers from USDT-M futures
for a fixed UTC research day.

Daily return definition (per spec):
  daily_return = (last closed price in UTC day D / first closed price in UTC day D) - 1
  Implemented as: (1d_bar_close / 1d_bar_open - 1) * 100

Research regime (simplified, research-only):
  if btc_24h >= 1.5 and alt_breadth_pct >= 55 -> trend_continuation_friendly
  elif btc_24h <= -1.5 and alt_breadth_pct <= 35 -> broad_weakness_sell_pressure
  else -> unclear_mixed

alt_breadth_pct = % of eligible USDT-M alt symbols with positive 24h return.
Eligible = all USDT-M perpetual minus BTCUSDT, ETHUSDT, stablecoin pairs.

This module is downstream-only and does not write to live bot files.
"""

import sys
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from research.top_movers.io import (
    day_window_ms,
    load_or_fetch,
    save_cache,
    load_cache,
)
from research.top_movers.research_binance_client import ResearchBinanceClient

# Symbols always excluded from alt selection
_ALWAYS_EXCLUDE = {"BTCUSDT", "ETHUSDT"}

# Stablecoin base assets to filter out
_STABLECOIN_BASES = {
    "BUSD", "USDC", "DAI", "TUSD", "USDD", "USDP", "FDUSD",
    "PYUSD", "USDE", "FRAX", "SUSD",
}


def _is_stablecoin(symbol: str) -> bool:
    if not symbol.endswith("USDT"):
        return False
    base = symbol[:-4]
    return base in _STABLECOIN_BASES


@dataclass
class TokenDailyBar:
    symbol: str
    day_open: float
    day_close: float
    day_high: float
    day_low: float
    daily_return_pct: float
    quote_volume: float
    data_ok: bool = True
    data_note: str = ""


@dataclass
class DailySelectionResult:
    research_day: str
    gainers: List[TokenDailyBar]  # top 10, sorted best first
    losers: List[TokenDailyBar]   # top 10 losers, sorted worst first
    btc_24h_change_pct: float
    alt_breadth_pct: float
    research_regime: str
    total_eligible_alts: int
    positive_alts: int

    @property
    def all_tokens(self) -> List[Tuple[TokenDailyBar, str]]:
        """Return (token_bar, side) pairs for all 20 selected tokens."""
        result = [(t, "LONG") for t in self.gainers]
        result += [(t, "SHORT") for t in self.losers]
        return result


def _compute_research_regime(btc_24h: float, alt_breadth_pct: float) -> str:
    if btc_24h >= 1.5 and alt_breadth_pct >= 55:
        return "trend_continuation_friendly"
    if btc_24h <= -1.5 and alt_breadth_pct <= 35:
        return "broad_weakness_sell_pressure"
    return "unclear_mixed"


def _fetch_all_daily_bars(
    client: ResearchBinanceClient,
    research_day: str,
) -> List[Dict]:
    """Fetch 1d klines for all USDT-M perpetual symbols.

    Returns list of raw kline dicts with symbol added.
    This is cached in bulk to avoid re-fetching 300+ symbols.
    """
    day_start_ms, day_end_ms = day_window_ms(research_day)
    symbols = client.get_all_usdt_perpetual_symbols()
    print(f"[selector] fetching 1d klines for {len(symbols)} symbols (this may take ~{int(len(symbols)*0.12)}s)...")

    results = []
    for i, sym in enumerate(symbols):
        bars = client.get_klines(sym, "1d", day_start_ms, day_end_ms, limit=2)
        if not bars:
            continue
        # Find bar whose open_time matches the research day start
        bar = next(
            (b for b in bars if b["open_time"] == day_start_ms),
            bars[0] if bars else None,
        )
        if bar is None:
            continue
        row = {
            "symbol": sym,
            "open_time": bar["open_time"],
            "open": bar["open"],
            "high": bar["high"],
            "low": bar["low"],
            "close": bar["close"],
            "quote_volume": bar["quote_volume"],
        }
        results.append(row)
        if (i + 1) % 50 == 0:
            print(f"  {i+1}/{len(symbols)} done")

    return results


def select_daily_top_movers(
    client: ResearchBinanceClient,
    research_day: str,
    top_n: int = 10,
) -> DailySelectionResult:
    """Select top N gainers and losers for the research day.

    Uses cached 1d bar data if available.
    """
    cache_key = f"all_symbols_1d_klines_{research_day}"
    raw_bars = load_cache(research_day, cache_key)
    if raw_bars is None:
        raw_bars = _fetch_all_daily_bars(client, research_day)
        save_cache(research_day, cache_key, raw_bars)
    else:
        print(f"[selector] loaded {len(raw_bars)} symbols from cache")

    # Build TokenDailyBar list
    all_bars: List[TokenDailyBar] = []
    for row in raw_bars:
        sym = row["symbol"]
        op = float(row["open"])
        cl = float(row["close"])
        if op <= 0:
            all_bars.append(
                TokenDailyBar(sym, op, cl, float(row["high"]), float(row["low"]),
                              0.0, float(row["quote_volume"]),
                              data_ok=False, data_note="zero_open")
            )
            continue
        ret_pct = (cl / op - 1) * 100.0
        all_bars.append(
            TokenDailyBar(
                symbol=sym,
                day_open=op,
                day_close=cl,
                day_high=float(row["high"]),
                day_low=float(row["low"]),
                daily_return_pct=ret_pct,
                quote_volume=float(row["quote_volume"]),
                data_ok=True,
            )
        )

    # BTC bar
    btc_bar = next((b for b in all_bars if b.symbol == "BTCUSDT"), None)
    btc_24h = btc_bar.daily_return_pct if btc_bar else 0.0

    # Eligible alts
    eligible = [
        b for b in all_bars
        if b.symbol not in _ALWAYS_EXCLUDE
        and not _is_stablecoin(b.symbol)
        and b.data_ok
    ]

    positive_alts = sum(1 for b in eligible if b.daily_return_pct > 0)
    total_eligible = len(eligible)
    alt_breadth_pct = 100.0 * positive_alts / max(total_eligible, 1)

    # Sort by return
    sorted_eligible = sorted(eligible, key=lambda b: b.daily_return_pct, reverse=True)
    gainers = sorted_eligible[:top_n]
    losers = sorted_eligible[-(top_n):][::-1]  # worst first

    research_regime = _compute_research_regime(btc_24h, alt_breadth_pct)

    print(
        f"[selector] day={research_day} btc={btc_24h:.2f}% "
        f"breadth={alt_breadth_pct:.1f}% regime={research_regime}"
    )
    print(f"  gainers: {[g.symbol for g in gainers]}")
    print(f"  losers:  {[l.symbol for l in losers]}")

    return DailySelectionResult(
        research_day=research_day,
        gainers=gainers,
        losers=losers,
        btc_24h_change_pct=btc_24h,
        alt_breadth_pct=alt_breadth_pct,
        research_regime=research_regime,
        total_eligible_alts=total_eligible,
        positive_alts=positive_alts,
    )


def selection_to_movers_list_rows(result: DailySelectionResult) -> List[Dict]:
    """Build daily_top_movers_list.csv rows."""
    rows = []
    for rank, token in enumerate(result.gainers, 1):
        rows.append({
            "research_day": result.research_day,
            "rank": rank,
            "side": "LONG",
            "symbol": token.symbol,
            "daily_return_pct": round(token.daily_return_pct, 4),
            "day_open": token.day_open,
            "day_close": token.day_close,
            "day_high": token.day_high,
            "day_low": token.day_low,
            "quote_volume": round(token.quote_volume, 2),
            "selection_method": "AUTO_V1",
        })
    for rank, token in enumerate(result.losers, 1):
        rows.append({
            "research_day": result.research_day,
            "rank": rank,
            "side": "SHORT",
            "symbol": token.symbol,
            "daily_return_pct": round(token.daily_return_pct, 4),
            "day_open": token.day_open,
            "day_close": token.day_close,
            "day_high": token.day_high,
            "day_low": token.day_low,
            "quote_volume": round(token.quote_volume, 2),
            "selection_method": "AUTO_V1",
        })
    return rows
