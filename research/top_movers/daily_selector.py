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
from typing import Dict, List, Optional, Tuple, Any

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


# ---------------------------------------------------------------------------
# V4-1: Enriched selection helpers
# ---------------------------------------------------------------------------

def _assign_ranks_to_tickers(all_tickers: List[Dict]) -> None:
    """Assign rank_abs_change_24h and rank_volume_24h to each ticker dict in-place."""
    sorted_by_abs = sorted(all_tickers, key=lambda t: abs(t.get("price_change_pct", 0)), reverse=True)
    for rank_idx, t in enumerate(sorted_by_abs, start=1):
        t["rank_abs_change_24h"] = rank_idx

    sorted_by_vol = sorted(all_tickers, key=lambda t: t.get("quote_vol_24h_usdt", 0), reverse=True)
    for rank_idx, t in enumerate(sorted_by_vol, start=1):
        t["rank_volume_24h"] = rank_idx


def _existing_selection_to_v4_cases(
    result: DailySelectionResult,
    ticker_lookup: Dict[str, Dict],
) -> List[Dict]:
    """Convert gainers/losers from DailySelectionResult into V4 case dicts."""
    cases: List[Dict] = []

    def _make(token_bar: TokenDailyBar, side: str, rank: int) -> Dict:
        sym = token_bar.symbol
        ticker = ticker_lookup.get(sym, {})
        horizon = "1d_gainers" if side == "LONG" else "1d_losers"
        bucket  = "top_gainers" if side == "LONG" else "top_losers"
        vol = ticker.get("quote_vol_24h_usdt") or token_bar.quote_volume
        h = token_bar.day_high
        l = token_bar.day_low
        o = token_bar.day_open
        return {
            "symbol":               sym,
            "side":                 side,
            "selection_horizon":    horizon,
            "selection_window":     "rolling_24h",
            "top_mover_bucket":     bucket,
            "top_mover_rank":       rank,
            "rank_abs_change_24h":  ticker.get("rank_abs_change_24h"),
            "rank_volume_24h":      ticker.get("rank_volume_24h"),
            "daily_return_pct":     token_bar.daily_return_pct,
            "week_change_pct":      None,
            "quote_vol_24h_usdt":   vol,
            "notional_volume_usd":  vol,
            "day_high":             h,
            "day_low":              l,
            "day_open":             o,
            "day_close":            token_bar.day_close,
            "day_range_pct":        (h - l) / l * 100 if l > 0 else None,
            "intraday_expansion_pct": (h - l) / o * 100 if o > 0 else None,
            "also_in_7d_top10":     False,
            "also_in_1d_top10":     False,
            "liquidity_ok":         vol >= 1_000_000,
            "_token_bar":           token_bar,
        }

    for rank, token_bar in enumerate(result.gainers, start=1):
        cases.append(_make(token_bar, "LONG", rank))
    for rank, token_bar in enumerate(result.losers, start=1):
        cases.append(_make(token_bar, "SHORT", rank))
    return cases


def _build_1d_dump_cases(
    all_tickers: List[Dict],
    ticker_lookup: Dict[str, Dict],
    top_n: int = 10,
) -> List[Dict]:
    """Top N tokens by 24h price decline, excluding BTC/ETH and stablecoins."""
    candidates = [
        t for t in all_tickers
        if t["price_change_pct"] < 0
        and t["symbol"] not in _ALWAYS_EXCLUDE
        and not _is_stablecoin(t["symbol"])
    ]
    sorted_dump = sorted(candidates, key=lambda t: t["price_change_pct"])[:top_n]

    result: List[Dict] = []
    for rank_idx, t in enumerate(sorted_dump, start=1):
        sym = t["symbol"]
        h, l, o = t["day_high"], t["day_low"], t["day_open"]
        vol = t["quote_vol_24h_usdt"]
        result.append({
            "symbol":               sym,
            "side":                 "SHORT",
            "selection_horizon":    "1d",
            "selection_window":     "rolling_24h",
            "top_mover_bucket":     "top_dumpers_1d",
            "top_mover_rank":       rank_idx,
            "rank_abs_change_24h":  t.get("rank_abs_change_24h"),
            "rank_volume_24h":      t.get("rank_volume_24h"),
            "daily_return_pct":     t["price_change_pct"],
            "week_change_pct":      None,
            "quote_vol_24h_usdt":   vol,
            "notional_volume_usd":  vol,
            "day_high":             h,
            "day_low":              l,
            "day_open":             o,
            "day_close":            t["day_close"],
            "day_range_pct":        (h - l) / l * 100 if l > 0 else None,
            "intraday_expansion_pct": (h - l) / o * 100 if o > 0 else None,
            "also_in_7d_top10":     False,
            "also_in_1d_top10":     False,
            "liquidity_ok":         vol >= 1_000_000,
            "_token_bar": TokenDailyBar(
                symbol=sym,
                day_open=o,
                day_close=t["day_close"],
                day_high=h,
                day_low=l,
                daily_return_pct=t["price_change_pct"],
                quote_volume=vol,
                data_ok=True,
            ),
        })
    return result


def _build_7d_dump_cases(
    all_tickers: List[Dict],
    client: Any,
    ticker_lookup: Dict[str, Dict],
    top_n: int = 10,
) -> List[Dict]:
    """Top N tokens by 7-day price decline. Fetches daily klines per candidate."""
    candidates = [
        t for t in all_tickers
        if t["price_change_pct"] < -5.0
        and t["symbol"] not in _ALWAYS_EXCLUDE
        and not _is_stablecoin(t["symbol"])
    ]

    with_7d: List[Dict] = []
    for t in candidates:
        sym = t["symbol"]
        change_7d = client.fetch_7d_change_pct(sym)
        if change_7d is not None and change_7d < 0:
            with_7d.append({**t, "week_change_pct": change_7d})

    sorted_7d = sorted(with_7d, key=lambda t: t["week_change_pct"])[:top_n]

    result: List[Dict] = []
    for rank_idx, t in enumerate(sorted_7d, start=1):
        sym = t["symbol"]
        # Skip if not in ticker_lookup (suspended / missing 24h data)
        if sym not in ticker_lookup:
            continue
        h, l, o = t["day_high"], t["day_low"], t["day_open"]
        vol = t["quote_vol_24h_usdt"]
        result.append({
            "symbol":               sym,
            "side":                 "SHORT",
            "selection_horizon":    "7d",
            "selection_window":     "rolling_7d",
            "top_mover_bucket":     "top_dumpers_7d",
            "top_mover_rank":       rank_idx,
            "rank_abs_change_24h":  t.get("rank_abs_change_24h"),
            "rank_volume_24h":      t.get("rank_volume_24h"),
            "daily_return_pct":     t["price_change_pct"],
            "week_change_pct":      t["week_change_pct"],
            "quote_vol_24h_usdt":   vol,
            "notional_volume_usd":  vol,
            "day_high":             h,
            "day_low":              l,
            "day_open":             o,
            "day_close":            t["day_close"],
            "day_range_pct":        (h - l) / l * 100 if l > 0 else None,
            "intraday_expansion_pct": (h - l) / o * 100 if o > 0 else None,
            "also_in_7d_top10":     False,
            "also_in_1d_top10":     False,
            "liquidity_ok":         vol >= 1_000_000,
            "_token_bar": TokenDailyBar(
                symbol=sym,
                day_open=o,
                day_close=t["day_close"],
                day_high=h,
                day_low=l,
                daily_return_pct=t["price_change_pct"],
                quote_volume=vol,
                data_ok=True,
            ),
        })
    return result


def _apply_dedup_flags(
    dump_1d: List[Dict],
    dump_7d: List[Dict],
) -> Tuple[List[Dict], List[Dict]]:
    """Flag symbols that appear in both 1d and 7d dump lists. Both rows are kept."""
    syms_1d = {c["symbol"] for c in dump_1d}
    syms_7d = {c["symbol"] for c in dump_7d}
    overlap = syms_1d & syms_7d

    for c in dump_1d:
        if c["symbol"] in overlap:
            c["also_in_7d_top10"] = True
    for c in dump_7d:
        if c["symbol"] in overlap:
            c["also_in_1d_top10"] = True

    return dump_1d, dump_7d


def _enrich_with_market_cap(cases: List[Dict], client: Any) -> List[Dict]:
    """Batch CoinGecko market cap lookup; sets live_universe_eligible_flag and exclusion_reason."""
    all_symbols = [c["symbol"] for c in cases]
    mc_map = client.fetch_coingecko_market_caps(all_symbols)

    for case in cases:
        sym = case["symbol"]
        mc  = mc_map.get(sym)

        case["market_cap_usd"]      = mc
        case["market_cap_verified"] = mc is not None
        case["market_cap_source"]   = "coingecko" if mc is not None else "unknown"

        if mc is None:
            case["live_universe_eligible_flag"] = False
            case["universe_mismatch_reason"]    = "market_cap_unknown"
        elif mc > 500_000_000:
            case["live_universe_eligible_flag"] = False
            case["universe_mismatch_reason"]    = "market_cap_too_large"
        else:
            case["live_universe_eligible_flag"] = True
            case["universe_mismatch_reason"]    = ""

        if not case.get("liquidity_ok", True):
            case["exclusion_reason"] = "liquidity_too_low"
        elif not case["live_universe_eligible_flag"]:
            case["exclusion_reason"] = case["universe_mismatch_reason"]
        else:
            case["exclusion_reason"] = ""

        vol = case.get("quote_vol_24h_usdt") or 0
        if vol >= 10_000_000:
            case["notional_liquidity_band"] = "HIGH"
        elif vol >= 1_000_000:
            case["notional_liquidity_band"] = "MEDIUM"
        else:
            case["notional_liquidity_band"] = "LOW"

    return cases


def select_all_cases(
    research_day: str,
    client: Any,
    base_selection: Optional["DailySelectionResult"] = None,
) -> List[Dict]:
    """Return enriched case selection list for all horizons (gainers, losers, 1d dump, 7d dump).

    Each dict carries all Layer 0-1 V4 fields plus '_token_bar' (TokenDailyBar, internal key).
    If base_selection is provided it is reused, otherwise select_daily_top_movers() is called.
    """
    all_tickers = client.fetch_all_ticker_24h()
    if not all_tickers:
        raise RuntimeError("[select_all_cases] fetch_all_ticker_24h returned empty — cannot proceed")

    _assign_ranks_to_tickers(all_tickers)
    ticker_lookup: Dict[str, Dict] = {t["symbol"]: t for t in all_tickers}

    if base_selection is None:
        base_selection = select_daily_top_movers(client, research_day)

    existing_cases = _existing_selection_to_v4_cases(base_selection, ticker_lookup)

    dump_1d = _build_1d_dump_cases(all_tickers, ticker_lookup)
    print(f"[selector] 1d dump candidates: {len(dump_1d)}")

    dump_7d = _build_7d_dump_cases(all_tickers, client, ticker_lookup)
    print(f"[selector] 7d dump candidates: {len(dump_7d)}")

    dump_1d, dump_7d = _apply_dedup_flags(dump_1d, dump_7d)

    all_cases = existing_cases + dump_1d + dump_7d

    all_cases = _enrich_with_market_cap(all_cases, client)

    n_excluded = sum(1 for c in all_cases if c.get("exclusion_reason", ""))
    print(
        f"[selector] select_all_cases: {len(existing_cases)} existing + "
        f"{len(dump_1d)} 1d_dump + {len(dump_7d)} 7d_dump = {len(all_cases)} total "
        f"({n_excluded} excluded)"
    )
    return all_cases


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
