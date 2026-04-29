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

    # ------------------------------------------------------------------
    # V4-1: 24h ticker, 7d change, CoinGecko market cap
    # ------------------------------------------------------------------

    def _get_external(self, url: str, params: dict) -> Any:
        """Fetch from an external URL (non-Binance). No auth headers, no session."""
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        return resp.json()

    def fetch_all_ticker_24h(self) -> List[Dict]:
        """Fetch 24h rolling ticker for all USDT-ending symbols.

        Returns list of dicts with keys:
            symbol, price_change_pct, quote_vol_24h_usdt,
            day_high, day_low, day_open, day_close
        Returns [] on failure.
        """
        try:
            data = self._get("/fapi/v1/ticker/24hr")
            result = []
            for item in data:
                sym = item.get("symbol", "")
                if not sym.endswith("USDT"):
                    continue
                result.append({
                    "symbol":             sym,
                    "price_change_pct":   float(item.get("priceChangePercent", 0)),
                    "quote_vol_24h_usdt": float(item.get("quoteVolume", 0)),
                    "day_high":           float(item.get("highPrice", 0)),
                    "day_low":            float(item.get("lowPrice", 0)),
                    "day_open":           float(item.get("openPrice", 0)),
                    "day_close":          float(item.get("lastPrice", 0)),
                })
            return result
        except Exception as e:
            print(f"[research_client] fetch_all_ticker_24h failed: {e}")
            return []

    def fetch_7d_change_pct(self, symbol: str) -> Optional[float]:
        """Compute 7-day price change pct using last 8 daily bars.

        Returns (close[-1] - close[0]) / close[0] * 100, or None on failure.
        """
        try:
            data = self._get("/fapi/v1/klines", {
                "symbol": symbol, "interval": "1d", "limit": 8,
            })
            if not data or len(data) < 8:
                return None
            close_7d_ago = float(data[0][4])
            close_now    = float(data[-1][4])
            if close_7d_ago == 0:
                return None
            return (close_now - close_7d_ago) / close_7d_ago * 100
        except Exception:
            return None

    # ------------------------------------------------------------------
    # V4-5: OI history + funding rate (point-in-time fetches)
    # ------------------------------------------------------------------

    def fetch_oi_history(self, symbol: str, end_ts_ms: int, limit: int = 49) -> List[Dict]:
        """Fetch OI history ending at end_ts_ms.

        Uses sumOpenInterest (contract count), not sumOpenInterestValue (USD),
        so that OI change pct reflects actual position changes not price moves.
        Returns list of dicts ordered oldest→newest with keys: timestamp (int ms), oi_value (float).
        """
        try:
            data = self._get(
                "/futures/data/openInterestHist",
                {
                    "symbol":  symbol,
                    "period":  "5m",
                    "limit":   limit,
                    "endTime": end_ts_ms,
                },
            )
            return [
                {
                    "timestamp": int(item.get("timestamp", 0)),
                    "oi_value":  float(item.get("sumOpenInterest", 0)),
                }
                for item in data
            ]
        except Exception:
            return []

    def fetch_oi_changes(self, symbol: str, end_ts_ms: int) -> Dict:
        """Compute 1h and 4h OI change pct in a single API call (limit=49).

        Returns: {"oi_change_1h_pct": float|None, "oi_change_4h_pct": float|None}
        """
        bars = self.fetch_oi_history(symbol, end_ts_ms, limit=49)
        result = {"oi_change_1h_pct": None, "oi_change_4h_pct": None}

        if len(bars) < 13:
            return result

        oi_now    = bars[-1]["oi_value"]
        oi_1h_ago = bars[-13]["oi_value"]   # 12 × 5m = 60 min

        if oi_1h_ago and oi_1h_ago > 0:
            result["oi_change_1h_pct"] = round(
                (oi_now - oi_1h_ago) / oi_1h_ago * 100, 4
            )

        if len(bars) >= 49:
            oi_4h_ago = bars[-49]["oi_value"]   # 48 × 5m = 240 min
            if oi_4h_ago and oi_4h_ago > 0:
                result["oi_change_4h_pct"] = round(
                    (oi_now - oi_4h_ago) / oi_4h_ago * 100, 4
                )

        return result

    def fetch_funding_rate_at(self, symbol: str, end_ts_ms: int) -> Optional[float]:
        """Fetch the most recent funding rate at or before end_ts_ms.

        Returns raw decimal (e.g. 0.0003), NOT percentage. Returns None on failure.
        """
        try:
            raw = self._get(
                "/fapi/v1/fundingRate",
                {"symbol": symbol, "endTime": end_ts_ms, "limit": 1},
            )
            if raw and len(raw) > 0:
                return float(raw[0].get("fundingRate", 0))
            return None
        except Exception:
            return None

    def fetch_coingecko_market_caps(self, symbols: List[str]) -> Dict[str, Optional[float]]:
        """Batch CoinGecko market cap lookup for Binance symbols.

        Fetches pages 1-3 (up to 750 coins). On symbol conflict keeps higher market_cap.
        Returns {binance_symbol: market_cap_usd | None}.
        """
        cg_base = "https://api.coingecko.com/api/v3"
        coin_map: Dict[str, Dict] = {}

        for page in range(1, 4):
            try:
                resp = self._get_external(
                    f"{cg_base}/coins/markets",
                    {
                        "vs_currency": "usd",
                        "order":       "market_cap_desc",
                        "per_page":    250,
                        "page":        page,
                        "sparkline":   "false",
                    },
                )
                for coin in resp:
                    sym_lower = (coin.get("symbol") or "").lower()
                    mc = float(coin.get("market_cap") or 0)
                    if sym_lower in coin_map:
                        if mc > coin_map[sym_lower]["market_cap"]:
                            coin_map[sym_lower] = {"cg_id": coin.get("id", ""), "market_cap": mc}
                    else:
                        coin_map[sym_lower] = {"cg_id": coin.get("id", ""), "market_cap": mc}
                time.sleep(1.5)
            except Exception as e:
                print(f"[research_client] CoinGecko page {page} failed: {e}")
                break

        result: Dict[str, Optional[float]] = {}
        for binance_sym in symbols:
            base = binance_sym.replace("USDT", "").lower()
            if base in coin_map:
                result[binance_sym] = float(coin_map[base]["market_cap"])
            else:
                result[binance_sym] = None
        return result
