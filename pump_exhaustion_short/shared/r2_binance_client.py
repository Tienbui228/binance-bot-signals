import time
import requests
from typing import Dict, List, Optional


class R2BinanceClient:
    BASE_URL = "https://fapi.binance.com"

    def __init__(self, cfg: Dict):
        self._timeout = cfg.get("request_timeout_sec", 10)
        self._delay = cfg.get("request_delay_sec", 0.2)
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": "Mozilla/5.0"})

    def _get(self, path: str, params: Optional[Dict] = None) -> Optional[object]:
        time.sleep(self._delay)
        try:
            resp = self._session.get(self.BASE_URL + path, params=params, timeout=self._timeout)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            print(f"[R2Client] GET {path} failed: {e}")
            return None

    def get_all_usdt_perpetuals(self) -> List[Dict]:
        data = self._get("/fapi/v1/exchangeInfo")
        if not data:
            return []
        results = []
        for s in data.get("symbols", []):
            if (s.get("status") == "TRADING"
                    and s.get("contractType") == "PERPETUAL"
                    and s.get("quoteAsset") == "USDT"):
                onboard_ts = s.get("onboardDate", 0)
                results.append({"symbol": s["symbol"], "onboard_ts_ms": int(onboard_ts)})
        return results

    def get_klines(self, symbol: str, interval: str, limit: int,
                   start_ms: Optional[int] = None,
                   end_ms: Optional[int] = None) -> List[Dict]:
        params: Dict = {"symbol": symbol, "interval": interval, "limit": limit}
        if start_ms:
            params["startTime"] = start_ms
        if end_ms:
            params["endTime"] = end_ms
        data = self._get("/fapi/v1/klines", params)
        if not data:
            return []
        rows = []
        for item in data:
            try:
                volume = float(item[5])
                quote_vol = float(item[7]) if item[7] else 0.0
                taker_buy_base = float(item[9]) if item[9] else 0.0
                row = {
                    "open_time": int(item[0]),
                    "open": float(item[1]),
                    "high": float(item[2]),
                    "low": float(item[3]),
                    "close": float(item[4]),
                    "volume": volume,
                    "close_time": int(item[6]),
                    "quote_asset_volume": quote_vol,
                    "taker_buy_base_vol": taker_buy_base,
                    "taker_buy_quote_vol": float(item[10]) if item[10] else 0.0,
                    # flag for downstream: quote vol fallback needed
                    "_quote_vol_fallback": (quote_vol == 0.0),
                }
                rows.append(row)
            except Exception:
                continue
        return rows

    def get_oi_hist(self, symbol: str, period: str = "1h", limit: int = 24) -> List[Dict]:
        data = self._get("/futures/data/openInterestHist",
                         {"symbol": symbol, "period": period, "limit": limit})
        if not data:
            return []
        rows = []
        for item in data:
            try:
                rows.append({
                    "ts": int(item["timestamp"]),
                    "oi_value": float(item.get("sumOpenInterestValue") or 0.0),
                    "oi_contracts": float(item.get("sumOpenInterest") or 0.0),
                })
            except Exception:
                continue
        return rows

    def get_ticker_24h(self, symbol: str) -> Dict:
        data = self._get("/fapi/v1/ticker/24hr", {"symbol": symbol})
        if not data:
            return {}
        try:
            return {"quoteVolume": float(data.get("quoteVolume", 0))}
        except Exception:
            return {}

    def get_all_tickers_24h(self) -> List[Dict]:
        data = self._get("/fapi/v1/ticker/24hr")
        if not data:
            return []
        results = []
        for item in data:
            try:
                results.append({
                    "symbol": item["symbol"],
                    "quoteVolume": float(item.get("quoteVolume", 0)),
                    "lastPrice": float(item.get("lastPrice", 0)),
                })
            except Exception:
                continue
        return results

    def get_funding_rate(self, symbol: str, limit: int = 5) -> List[Dict]:
        data = self._get("/fapi/v1/fundingRate",
                         {"symbol": symbol, "limit": limit})
        if not data:
            return []
        rows = []
        for item in data:
            try:
                rows.append({
                    "fundingTime": int(item["fundingTime"]),
                    "fundingRate": float(item["fundingRate"]),
                })
            except Exception:
                continue
        return rows
