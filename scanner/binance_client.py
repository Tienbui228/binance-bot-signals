from typing import Dict, List, Optional

import requests

BASE_FAPI = "https://fapi.binance.com"
HEADERS = {"User-Agent": "Mozilla/5.0"}


class BinanceClient:
    def __init__(self, cfg: Dict):
        self.cfg = cfg
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def get(self, path: str, params: Optional[Dict] = None):
        url = BASE_FAPI + path
        response = self.session.get(url, params=params, timeout=20)
        response.raise_for_status()
        return response.json()

    def load_symbols(self) -> List[str]:
        info = self.get("/fapi/v1/exchangeInfo")
        quote = self.cfg["scanner"]["quote_asset"]
        exclude = set(self.cfg["scanner"].get("exclude_symbols", []))
        symbols: List[str] = []
        for item in info["symbols"]:
            if item.get("contractType") != "PERPETUAL":
                continue
            if item.get("quoteAsset") != quote:
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
        return {item["symbol"]: item for item in data}

    def filter_symbols(self, symbols: List[str], tickers: Dict[str, Dict]) -> List[str]:
        min_qv = float(self.cfg["scanner"]["min_quote_volume_usdt_24h"])
        min_price = float(self.cfg["scanner"]["min_price"])
        kept: List[str] = []
        for symbol in symbols:
            ticker = tickers.get(symbol)
            if not ticker:
                continue
            quote_volume = float(ticker.get("quoteVolume", 0))
            price = float(ticker.get("lastPrice", 0))
            if quote_volume >= min_qv and price >= min_price:
                kept.append(symbol)
        kept.sort(key=lambda sym: float(tickers[sym]["quoteVolume"]), reverse=True)
        return kept[: int(self.cfg["scanner"]["max_symbols"])]

    def klines(self, symbol: str, interval: str, limit: int = 50):
        data = self.get("/fapi/v1/klines", {"symbol": symbol, "interval": interval, "limit": limit})
        rows = []
        for item in data:
            rows.append({
                "open_time": int(item[0]),
                "open": float(item[1]),
                "high": float(item[2]),
                "low": float(item[3]),
                "close": float(item[4]),
                "volume": float(item[5]),
                "close_time": int(item[6]),
                "quote_volume": float(item[7]),
            })
        return rows

    def fetch_klines(self, symbol: str, interval: str, limit: int = 50):
        return self.klines(symbol, interval, limit=limit)

    def fetch_klines_range(self, symbol: str, interval: str, start_ms: int, end_ms: int, limit: int = 150):
        try:
            data = self.get(
                "/fapi/v1/klines",
                {
                    "symbol": symbol,
                    "interval": interval,
                    "startTime": int(start_ms),
                    "endTime": int(end_ms),
                    "limit": int(limit),
                },
            )
        except Exception:
            return []
        rows = []
        for item in data:
            rows.append({
                "open_time": int(item[0]),
                "open": float(item[1]),
                "high": float(item[2]),
                "low": float(item[3]),
                "close": float(item[4]),
                "volume": float(item[5]),
                "close_time": int(item[6]),
                "quote_volume": float(item[7]),
            })
        return rows

    def oi_hist(self, symbol: str, period: str = "5m", limit: int = 10):
        data = self.get("/futures/data/openInterestHist", {"symbol": symbol, "period": period, "limit": limit})
        rows = []
        for item in data:
            total = float(item.get("sumOpenInterestValue") or 0.0)
            rows.append({"ts": int(item["timestamp"]), "oi_value": total})
        return rows

    def funding(self, symbol: str) -> float:
        data = self.get("/fapi/v1/premiumIndex", {"symbol": symbol})
        return float(data.get("lastFundingRate", 0.0)) * 100.0

    def calc_oi_jump_pct(self, symbol: str):
        history = self.oi_hist(symbol, period="5m", limit=6)
        if len(history) < 2:
            return None
        previous = history[-2]["oi_value"]
        current = history[-1]["oi_value"]
        if previous <= 0:
            return None
        return (current - previous) / previous * 100.0

    def calc_oi_change_pct(self, symbol: str, period: str = "15m", limit: int = 3):
        history = self.oi_hist(symbol, period=period, limit=limit)
        if len(history) < 2:
            return None
        previous = history[-2]["oi_value"]
        current = history[-1]["oi_value"]
        if previous <= 0:
            return None
        return (current - previous) / previous * 100.0
