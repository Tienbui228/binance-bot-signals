from __future__ import annotations

from typing import Dict, List, Optional

import requests

BASE_FAPI = "https://fapi.binance.com"
HEADERS = {"User-Agent": "Mozilla/5.0"}


class BinanceClient:
    def __init__(self, base_url: str = BASE_FAPI, headers: Optional[Dict[str, str]] = None, timeout: int = 20):
        self.base_url = base_url
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(headers or HEADERS)

    def get(self, path: str, params: Optional[Dict] = None):
        response = self.session.get(self.base_url + path, params=params, timeout=self.timeout)
        response.raise_for_status()
        return response.json()

    def load_symbols(self, quote_asset: str, exclude_symbols: List[str] | None = None) -> List[str]:
        info = self.get("/fapi/v1/exchangeInfo")
        exclude = set(exclude_symbols or [])
        symbols: List[str] = []
        for item in info["symbols"]:
            if item.get("contractType") != "PERPETUAL":
                continue
            if item.get("quoteAsset") != quote_asset:
                continue
            if item.get("status") != "TRADING":
                continue
            symbol = item["symbol"]
            if symbol in exclude:
                continue
            symbols.append(symbol)
        return symbols

    def load_24h_tickers(self) -> Dict[str, Dict]:
        data = self.get("/fapi/v1/ticker/24hr")
        return {x["symbol"]: x for x in data}

    def klines(self, symbol: str, interval: str, limit: int = 50) -> List[Dict]:
        data = self.get("/fapi/v1/klines", {"symbol": symbol, "interval": interval, "limit": limit})
        return [
            {
                "open_time": int(x[0]),
                "open": float(x[1]),
                "high": float(x[2]),
                "low": float(x[3]),
                "close": float(x[4]),
                "volume": float(x[5]),
                "close_time": int(x[6]),
                "quote_volume": float(x[7]),
            }
            for x in data
        ]

    def oi_hist(self, symbol: str, period: str = "5m", limit: int = 10) -> List[Dict]:
        data = self.get("/futures/data/openInterestHist", {"symbol": symbol, "period": period, "limit": limit})
        return [{"ts": int(x["timestamp"]), "oi_value": float(x.get("sumOpenInterestValue") or 0.0)} for x in data]

    def funding_pct(self, symbol: str) -> float:
        data = self.get("/fapi/v1/premiumIndex", {"symbol": symbol})
        return float(data.get("lastFundingRate", 0.0)) * 100.0
