"""
research/top_movers/research_binance_client.py

Research-only Binance USDT-M futures client.

IMPORTANT:
- Does NOT modify scanner/binance_client.py
- Does NOT share state with the live bot
- Handles proxy endpoints not available in the live bot client
- All API errors are caught and returned as empty lists (fail-soft)

Endpoints used:
  /fapi/v1/exchangeInfo
  /fapi/v1/klines
  /futures/data/openInterestHist
  /futures/data/topLongShortAccountRatio
  /futures/data/topLongShortPositionRatio
  /futures/data/globalLongShortAccountRatio
  /futures/data/takerlongshortRatio
  /futures/data/basis
"""

import time
from typing import Any, Dict, List, Optional

import requests

BASE_FAPI = "https://fapi.binance.com"
_HEADERS = {"User-Agent": "Mozilla/5.0"}

STABLECOIN_BASES = {
    "BUSD", "USDC", "DAI", "TUSD", "USDD", "USDP", "FDUSD",
    "PYUSD", "USDE", "FRAX", "SUSD",
}


class ResearchBinanceClient:
    """Research-only Binance USDT-M futures data client for Phase R1."""

    def __init__(self, rate_limit_sleep: float = 0.10):
        self.session = requests.Session()
        self.session.headers.update(_HEADERS)
        self.rate_limit_sleep = rate_limit_sleep

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _get(self, path: str, params: Optional[Dict] = None) -> Any:
        time.sleep(self.rate_limit_sleep)
        url = BASE_FAPI + path
        r = self.session.get(url, params=params or {}, timeout=25)
        r.raise_for_status()
        return r.json()

    # ------------------------------------------------------------------
    # Exchange info & symbols
    # ------------------------------------------------------------------

    def get_all_usdt_perpetual_symbols(self) -> List[str]:
        """Return all active USDT-M perpetual futures symbols."""
        info = self._get("/fapi/v1/exchangeInfo")
        result = []
        for item in info.get("symbols", []):
            if (
                item.get("contractType") == "PERPETUAL"
                and item.get("quoteAsset") == "USDT"
                and item.get("status") == "TRADING"
            ):
                result.append(item["symbol"])
        return result

    def is_stablecoin_pair(self, symbol: str) -> bool:
        """True if the base asset of a USDT pair is a stablecoin."""
        if not symbol.endswith("USDT"):
            return False
        base = symbol[:-4]
        return base in STABLECOIN_BASES

    # ------------------------------------------------------------------
    # Klines
    # ------------------------------------------------------------------

    def get_klines(
        self,
        symbol: str,
        interval: str,
        start_ms: int,
        end_ms: int,
        limit: int = 500,
    ) -> List[Dict]:
        """Fetch OHLCV klines for a symbol within a time window."""
        try:
            data = self._get(
                "/fapi/v1/klines",
                {
                    "symbol": symbol,
                    "interval": interval,
                    "startTime": int(start_ms),
                    "endTime": int(end_ms),
                    "limit": int(limit),
                },
            )
            return [
                {
                    "open_time": int(d[0]),
                    "open": float(d[1]),
                    "high": float(d[2]),
                    "low": float(d[3]),
                    "close": float(d[4]),
                    "volume": float(d[5]),
                    "close_time": int(d[6]),
                    "quote_volume": float(d[7]),
                }
                for d in data
            ]
        except Exception as e:
            return []

    # ------------------------------------------------------------------
    # OI history
    # ------------------------------------------------------------------

    def get_oi_hist(
        self,
        symbol: str,
        period: str,
        start_ms: int,
        end_ms: int,
        limit: int = 500,
    ) -> List[Dict]:
        """Fetch open interest history. Returns [] on failure."""
        try:
            data = self._get(
                "/futures/data/openInterestHist",
                {
                    "symbol": symbol,
                    "period": period,
                    "startTime": int(start_ms),
                    "endTime": int(end_ms),
                    "limit": int(limit),
                },
            )
            return [
                {
                    "ts": int(d["timestamp"]),
                    "oi_value": float(d.get("sumOpenInterestValue") or 0.0),
                }
                for d in data
            ]
        except Exception:
            return []

    # ------------------------------------------------------------------
    # Top trader long/short account ratio
    # ------------------------------------------------------------------

    def get_top_account_ratio(
        self,
        symbol: str,
        period: str,
        start_ms: int,
        end_ms: int,
        limit: int = 500,
    ) -> List[Dict]:
        """Top trader long/short ACCOUNT ratio. Returns [] on failure."""
        try:
            data = self._get(
                "/futures/data/topLongShortAccountRatio",
                {
                    "symbol": symbol,
                    "period": period,
                    "startTime": int(start_ms),
                    "endTime": int(end_ms),
                    "limit": int(limit),
                },
            )
            return [
                {
                    "ts": int(d["timestamp"]),
                    "long_pct": float(d.get("longAccount", 0)),
                    "short_pct": float(d.get("shortAccount", 0)),
                }
                for d in data
            ]
        except Exception:
            return []

    # ------------------------------------------------------------------
    # Top trader long/short position ratio
    # ------------------------------------------------------------------

    def get_top_position_ratio(
        self,
        symbol: str,
        period: str,
        start_ms: int,
        end_ms: int,
        limit: int = 500,
    ) -> List[Dict]:
        """Top trader long/short POSITION ratio. Returns [] on failure."""
        try:
            data = self._get(
                "/futures/data/topLongShortPositionRatio",
                {
                    "symbol": symbol,
                    "period": period,
                    "startTime": int(start_ms),
                    "endTime": int(end_ms),
                    "limit": int(limit),
                },
            )
            return [
                {
                    "ts": int(d["timestamp"]),
                    "long_pct": float(d.get("longAccount", 0)),
                    "short_pct": float(d.get("shortAccount", 0)),
                }
                for d in data
            ]
        except Exception:
            return []

    # ------------------------------------------------------------------
    # Global long/short account ratio
    # ------------------------------------------------------------------

    def get_global_account_ratio(
        self,
        symbol: str,
        period: str,
        start_ms: int,
        end_ms: int,
        limit: int = 500,
    ) -> List[Dict]:
        """Global long/short ACCOUNT ratio. Returns [] on failure."""
        try:
            data = self._get(
                "/futures/data/globalLongShortAccountRatio",
                {
                    "symbol": symbol,
                    "period": period,
                    "startTime": int(start_ms),
                    "endTime": int(end_ms),
                    "limit": int(limit),
                },
            )
            return [
                {
                    "ts": int(d["timestamp"]),
                    "long_pct": float(d.get("longAccount", 0)),
                    "short_pct": float(d.get("shortAccount", 0)),
                }
                for d in data
            ]
        except Exception:
            return []

    # ------------------------------------------------------------------
    # Taker buy/sell volume ratio
    # ------------------------------------------------------------------

    def get_taker_ratio(
        self,
        symbol: str,
        period: str,
        start_ms: int,
        end_ms: int,
        limit: int = 500,
    ) -> List[Dict]:
        """Taker buy/sell volume. Returns [] on failure."""
        try:
            data = self._get(
                "/futures/data/takerlongshortRatio",
                {
                    "symbol": symbol,
                    "period": period,
                    "startTime": int(start_ms),
                    "endTime": int(end_ms),
                    "limit": int(limit),
                },
            )
            return [
                {
                    "ts": int(d["timestamp"]),
                    "buy_vol": float(d.get("buyVol", 0)),
                    "sell_vol": float(d.get("sellVol", 0)),
                }
                for d in data
            ]
        except Exception:
            return []

    # ------------------------------------------------------------------
    # Basis (basisRate for PERPETUAL)
    # ------------------------------------------------------------------

    def get_basis(
        self,
        symbol: str,
        period: str,
        start_ms: int,
        end_ms: int,
        limit: int = 500,
    ) -> List[Dict]:
        """Basis rate for PERPETUAL contracts. Returns [] on failure.

        Note: endpoint uses 'pair' not 'symbol', and requires contractType.
        """
        try:
            data = self._get(
                "/futures/data/basis",
                {
                    "pair": symbol,
                    "contractType": "PERPETUAL",
                    "period": period,
                    "startTime": int(start_ms),
                    "endTime": int(end_ms),
                    "limit": int(limit),
                },
            )
            return [
                {
                    "ts": int(d["timestamp"]),
                    "basis_rate": float(d.get("basisRate", 0)),
                }
                for d in data
            ]
        except Exception:
            return []
