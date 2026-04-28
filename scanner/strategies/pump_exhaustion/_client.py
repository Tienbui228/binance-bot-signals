"""Thread-safe thin Binance Futures HTTP client for pump_exhaustion strategy.

Each instance creates its own requests.Session, making it safe to use
in background threads without sharing the main BinanceScanner session.
"""
import time
import requests
from typing import Dict, List, Optional


class _BinanceHttp:
    BASE_URL = "https://fapi.binance.com"

    def __init__(self, delay_sec: float = 0.2, timeout_sec: int = 10):
        self._delay = delay_sec
        self._timeout = timeout_sec
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": "Mozilla/5.0"})

    def _get(self, path: str, params: Optional[Dict] = None):
        time.sleep(self._delay)
        try:
            resp = self._session.get(self.BASE_URL + path, params=params, timeout=self._timeout)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            print(f"[PumpHttp] GET {path} failed: {e}")
            return None

    def klines(self, symbol: str, interval: str, limit: int,
               start_ms: Optional[int] = None) -> List[Dict]:
        params: Dict = {"symbol": symbol, "interval": interval, "limit": limit}
        if start_ms:
            params["startTime"] = start_ms
        data = self._get("/fapi/v1/klines", params)
        if not data:
            return []
        out = []
        for x in data:
            try:
                volume = float(x[5])
                quote_vol = float(x[7]) if x[7] else 0.0
                taker_buy_base = float(x[9]) if x[9] else 0.0
                out.append({
                    "open_time": int(x[0]),
                    "open": float(x[1]),
                    "high": float(x[2]),
                    "low": float(x[3]),
                    "close": float(x[4]),
                    "volume": volume,
                    "close_time": int(x[6]),
                    "quote_asset_volume": quote_vol,
                    "taker_buy_base_vol": taker_buy_base,
                    "taker_buy_quote_vol": float(x[10]) if x[10] else 0.0,
                    "_quote_vol_fallback": (quote_vol == 0.0),
                })
            except Exception:
                continue
        return out

    def oi_hist(self, symbol: str, period: str = "1h", limit: int = 24) -> List[Dict]:
        data = self._get("/futures/data/openInterestHist",
                         {"symbol": symbol, "period": period, "limit": limit})
        if not data:
            return []
        out = []
        for x in data:
            try:
                out.append({
                    "ts": int(x["timestamp"]),
                    "oi_value": float(x.get("sumOpenInterestValue") or 0.0),
                })
            except Exception:
                continue
        return out

    def funding(self, symbol: str) -> float:
        data = self._get("/fapi/v1/premiumIndex", {"symbol": symbol})
        if not data:
            return 0.0
        return float(data.get("lastFundingRate", 0.0)) * 100.0
