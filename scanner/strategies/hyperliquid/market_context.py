"""
Per-scan market context for all watchlist coins (spec §4, §5).

One batch call to metaAndAssetCtxs covers OI, funding, mark/oracle prices.
candleSnapshot called per coin for Gate T price trend.

Funding unit: VERIFIED 2026-06-01 via fundingHistory interval check.
  ctx["funding"] = hourly rate.
  funding_8h_equiv_pct = float(ctx["funding"]) * 8 * 100
"""

import time
from typing import Dict, List, Optional

from ._client import _HlHttp

# Confirmed hourly cadence from fundingHistory interval check (≈3600s).
FUNDING_UNIT_VERIFIED = True
FUNDING_HOURLY_TO_8H_FACTOR = 8 * 100  # → percent per 8h


class MarketContext:
    """Fetches and caches per-coin market data for one scan cycle."""

    def __init__(self, cfg: dict):
        hl_cfg = cfg.get("hl_whale_accum", {})
        self._client = _HlHttp(
            delay_sec=float(hl_cfg.get("http_delay_sec", 0.3)),
            timeout_sec=float(hl_cfg.get("http_timeout_sec", 10.0)),
        )
        # How many 1h candles to fetch for Gate T (need 30d + 7d window → 35 bars)
        self._candle_n = 35

    def fetch_all(self, coins: List[str]) -> Dict[str, dict]:
        """Returns coin → context dict for all watchlist coins.

        Keys per coin:
          funding_8h_pct   : float — 8h-equivalent funding rate in %
          mark_px          : float
          oracle_px        : float
          oi_coins         : float — open interest in coin units
          oi_usd           : float — oi_coins * mark_px
          day_ntl_vlm      : float — 24h notional volume USD
          prev_day_px      : float
          price_baseline   : float — mean(close[-30:]) for Gate T
          close_30d        : list[float] — 30 most recent 1h closes
          close_7d         : list[float] — 7 most recent 1h closes
        """
        ctx_map = self._client.meta_and_asset_ctxs()
        result: Dict[str, dict] = {}

        for coin in coins:
            raw = ctx_map.get(coin)
            if raw is None:
                continue

            try:
                mark_px = float(raw.get("markPx") or 0)
                oracle_px = float(raw.get("oraclePx") or 0)
                oi_coins = float(raw.get("openInterest") or 0)
                day_ntl_vlm = float(raw.get("dayNtlVlm") or 0)
                prev_day_px = float(raw.get("prevDayPx") or 0)

                funding_raw = float(raw.get("funding") or 0)
                if FUNDING_UNIT_VERIFIED:
                    funding_8h_pct = funding_raw * FUNDING_HOURLY_TO_8H_FACTOR
                else:
                    funding_8h_pct = None
                    print(f"[FUNDING-UNVERIFIED] Gate F disabled for {coin}")

                oi_usd = oi_coins * mark_px
            except (TypeError, ValueError) as e:
                print(f"[MarketContext] parse error for {coin}: {e}")
                continue

            # Gate T: fetch recent 1h candles for price trend
            candles = self._client.candle_snapshot_n(coin, "1h", self._candle_n)
            closes = _extract_closes(candles)
            close_30d = closes[-30:] if len(closes) >= 30 else closes
            close_7d = closes[-7:] if len(closes) >= 7 else closes
            price_baseline = (sum(close_30d) / len(close_30d)) if close_30d else mark_px

            result[coin] = {
                "funding_8h_pct": funding_8h_pct,
                "mark_px": mark_px,
                "oracle_px": oracle_px,
                "oi_coins": oi_coins,
                "oi_usd": oi_usd,
                "day_ntl_vlm": day_ntl_vlm,
                "prev_day_px": prev_day_px,
                "price_baseline": price_baseline,
                "close_30d": close_30d,
                "close_7d": close_7d,
            }

        return result


def _extract_closes(candles: List[dict]) -> List[float]:
    closes = []
    for c in candles:
        try:
            closes.append(float(c["c"]))
        except (KeyError, TypeError, ValueError):
            pass
    return closes
