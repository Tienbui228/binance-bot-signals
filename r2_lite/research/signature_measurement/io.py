"""
io.py
Binance historical data fetcher + local CSV cache for the research pipeline.

Cache path schema:
  {base_dir}/{metric}/{symbol}_{period}_{start_ms}_{end_ms}.csv

Taker buy/sell volume is sourced directly from klines fields [9] and [10].
Top-account, top-position, global-ratio, OI history, and basis data are
fetched from Binance /futures/data/* historical endpoints with pagination.

Rate limit: configurable sleep between API calls.
Offline research only — not imported by live runtime.
"""
from __future__ import annotations

import csv
import logging
import time
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import requests

log = logging.getLogger(__name__)

BASE_FAPI = "https://fapi.binance.com"
BASE_FDATA = "https://fapi.binance.com"  # /futures/data/* lives under same host

KLINES_LIMIT = 1500   # Binance max per klines call
RATIO_LIMIT = 500     # max for top/global/OI/basis calls


# ---------------------------------------------------------------------------
# Cache manager
# ---------------------------------------------------------------------------

class ResearchCache:
    """Read/write CSV cache for raw Binance historical data."""

    def __init__(self, base_dir: str):
        self.base = Path(base_dir)

    def _path(self, metric: str, symbol: str, period: str,
               start_ms: int, end_ms: int) -> Path:
        folder = self.base / metric
        folder.mkdir(parents=True, exist_ok=True)
        fname = f"{symbol}_{period}_{start_ms}_{end_ms}.csv"
        return folder / fname

    def exists(self, metric: str, symbol: str, period: str,
                start_ms: int, end_ms: int) -> bool:
        return self._path(metric, symbol, period, start_ms, end_ms).exists()

    def read(self, metric: str, symbol: str, period: str,
              start_ms: int, end_ms: int) -> Optional[pd.DataFrame]:
        p = self._path(metric, symbol, period, start_ms, end_ms)
        if not p.exists():
            return None
        try:
            df = pd.read_csv(p)
            return df if not df.empty else None
        except Exception as exc:
            log.warning("cache read failed %s: %s", p, exc)
            return None

    def write(self, df: pd.DataFrame, metric: str, symbol: str,
               period: str, start_ms: int, end_ms: int) -> None:
        p = self._path(metric, symbol, period, start_ms, end_ms)
        df.to_csv(p, index=False)


# ---------------------------------------------------------------------------
# Binance historical fetcher
# ---------------------------------------------------------------------------

class ResearchFetcher:
    """
    Fetches historical Binance futures data for the research pipeline.
    All calls are cache-first: if a matching cache file exists it is returned
    without an API call.

    Taker volumes are extracted from klines (fields [9]/[10]) so no separate
    taker-volume endpoint is needed.
    """

    def __init__(self, cache: ResearchCache, rate_limit_sleep: float = 0.25,
                 max_retries: int = 3, retry_sleep: float = 2.0):
        self.cache = cache
        self.sleep = rate_limit_sleep
        self.retries = max_retries
        self.retry_sleep = retry_sleep
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "Mozilla/5.0"})

    # ------------------------------------------------------------------ low level

    def _get(self, path: str, params: Dict) -> List:
        url = BASE_FAPI + path
        for attempt in range(self.retries):
            try:
                resp = self.session.get(url, params=params, timeout=30)
                resp.raise_for_status()
                return resp.json()
            except requests.HTTPError as exc:
                status = exc.response.status_code if exc.response is not None else 0
                if status == 429:
                    log.warning("rate limited; sleeping 10s")
                    time.sleep(10)
                elif status in (400, 404, 418, 451):
                    # Symbol not supported by this endpoint — skip immediately, no retry
                    log.debug("HTTP %d for %s (symbol not supported), skipping", status, path)
                    raise
                else:
                    log.warning("HTTP error %s attempt %d: %s", path, attempt, exc)
                    if attempt < self.retries - 1:
                        time.sleep(self.retry_sleep)
                    else:
                        raise
            except Exception as exc:
                log.warning("fetch error %s attempt %d: %s", path, attempt, exc)
                if attempt < self.retries - 1:
                    time.sleep(self.retry_sleep)
                else:
                    raise
        return []

    def _paginate_ts(self, path: str, base_params: Dict,
                      ts_field: str, limit: int,
                      start_ms: int, end_ms: int) -> List[Dict]:
        """Page through a time-series endpoint using startTime/endTime."""
        rows: List[Dict] = []
        cur = start_ms
        while cur < end_ms:
            params = {**base_params, "startTime": cur, "endTime": end_ms, "limit": limit}
            batch = self._get(path, params)
            time.sleep(self.sleep)
            if not batch:
                break
            rows.extend(batch)
            last_ts = int(batch[-1][ts_field]) if isinstance(batch[-1], dict) else int(batch[-1][0])
            if last_ts <= cur or len(batch) < limit:
                break
            cur = last_ts + 1  # move past last returned timestamp
        return rows

    # ------------------------------------------------------------------ klines (with taker)

    def fetch_klines(self, symbol: str, period: str,
                      start_ms: int, end_ms: int) -> pd.DataFrame:
        """
        Fetch 5m klines including taker buy volume (fields [9] and [10]).
        Returns DataFrame with columns:
          open_time, open, high, low, close, volume (base), close_time,
          quote_volume, num_trades, taker_buy_base_vol, taker_buy_quote_vol,
          taker_sell_base_vol (derived), taker_imbalance_raw (derived)
        """
        cached = self.cache.read("klines", symbol, period, start_ms, end_ms)
        if cached is not None:
            log.debug("cache hit: klines %s %s", symbol, period)
            return cached

        log.info("fetching klines %s %s [%d..%d]", symbol, period, start_ms, end_ms)
        raw_rows = self._paginate_ts(
            "/fapi/v1/klines",
            {"symbol": symbol, "interval": period},
            ts_field="0",   # open_time is at index 0 (list not dict)
            limit=KLINES_LIMIT,
            start_ms=start_ms,
            end_ms=end_ms,
        )
        # Binance klines returns list of lists
        records = []
        for item in raw_rows:
            base_vol = float(item[5])
            taker_buy_base = float(item[9])
            taker_sell_base = base_vol - taker_buy_base
            total = taker_buy_base + taker_sell_base
            records.append({
                "open_time": int(item[0]),
                "open": float(item[1]),
                "high": float(item[2]),
                "low": float(item[3]),
                "close": float(item[4]),
                "volume": base_vol,
                "close_time": int(item[6]),
                "quote_volume": float(item[7]),
                "num_trades": int(item[8]),
                "taker_buy_base_vol": taker_buy_base,
                "taker_buy_quote_vol": float(item[10]),
                "taker_sell_base_vol": taker_sell_base,
            })
        df = pd.DataFrame(records)
        if not df.empty:
            df = df.drop_duplicates("open_time").sort_values("open_time").reset_index(drop=True)
            self.cache.write(df, "klines", symbol, period, start_ms, end_ms)
        return df

    # ------------------------------------------------------------------ top account ratio

    def fetch_top_account_ratio(self, symbol: str, period: str,
                                  start_ms: int, end_ms: int) -> pd.DataFrame:
        """
        /futures/data/topLongShortAccountRatio
        Returns: ts_ms, top_long_account_pct, top_short_account_pct
        """
        cached = self.cache.read("top_account_ratio", symbol, period, start_ms, end_ms)
        if cached is not None:
            return cached

        log.info("fetching top_account_ratio %s %s", symbol, period)
        try:
            raw = self._paginate_ts(
                "/futures/data/topLongShortAccountRatio",
                {"symbol": symbol, "period": period},
                ts_field="timestamp",
                limit=RATIO_LIMIT,
                start_ms=start_ms,
                end_ms=end_ms,
            )
        except Exception as exc:
            log.warning("top_account_ratio fetch failed %s: %s", symbol, exc)
            return pd.DataFrame()

        if not raw:
            return pd.DataFrame()

        records = [{
            "ts_ms": int(r["timestamp"]),
            "top_long_account_pct": float(r.get("longAccount", 0.5)) * 100,
            "top_short_account_pct": float(r.get("shortAccount", 0.5)) * 100,
        } for r in raw]
        df = pd.DataFrame(records).drop_duplicates("ts_ms").sort_values("ts_ms").reset_index(drop=True)
        self.cache.write(df, "top_account_ratio", symbol, period, start_ms, end_ms)
        return df

    # ------------------------------------------------------------------ top position ratio

    def fetch_top_position_ratio(self, symbol: str, period: str,
                                   start_ms: int, end_ms: int) -> pd.DataFrame:
        """
        /futures/data/topLongShortPositionRatio
        Returns: ts_ms, top_long_position_pct, top_short_position_pct
        """
        cached = self.cache.read("top_position_ratio", symbol, period, start_ms, end_ms)
        if cached is not None:
            return cached

        log.info("fetching top_position_ratio %s %s", symbol, period)
        try:
            raw = self._paginate_ts(
                "/futures/data/topLongShortPositionRatio",
                {"symbol": symbol, "period": period},
                ts_field="timestamp",
                limit=RATIO_LIMIT,
                start_ms=start_ms,
                end_ms=end_ms,
            )
        except Exception as exc:
            log.warning("top_position_ratio fetch failed %s: %s", symbol, exc)
            return pd.DataFrame()

        if not raw:
            return pd.DataFrame()

        records = [{
            "ts_ms": int(r["timestamp"]),
            "top_long_position_pct": float(r.get("longAccount", 0.5)) * 100,
            "top_short_position_pct": float(r.get("shortAccount", 0.5)) * 100,
        } for r in raw]
        df = pd.DataFrame(records).drop_duplicates("ts_ms").sort_values("ts_ms").reset_index(drop=True)
        self.cache.write(df, "top_position_ratio", symbol, period, start_ms, end_ms)
        return df

    # ------------------------------------------------------------------ global ratio

    def fetch_global_ratio(self, symbol: str, period: str,
                             start_ms: int, end_ms: int) -> pd.DataFrame:
        """
        /futures/data/globalLongShortAccountRatio
        Returns: ts_ms, global_long_account_pct, global_short_account_pct
        """
        cached = self.cache.read("global_ratio", symbol, period, start_ms, end_ms)
        if cached is not None:
            return cached

        log.info("fetching global_ratio %s %s", symbol, period)
        try:
            raw = self._paginate_ts(
                "/futures/data/globalLongShortAccountRatio",
                {"symbol": symbol, "period": period},
                ts_field="timestamp",
                limit=RATIO_LIMIT,
                start_ms=start_ms,
                end_ms=end_ms,
            )
        except Exception as exc:
            log.warning("global_ratio fetch failed %s: %s", symbol, exc)
            return pd.DataFrame()

        if not raw:
            return pd.DataFrame()

        records = [{
            "ts_ms": int(r["timestamp"]),
            "global_long_account_pct": float(r.get("longAccount", 0.5)) * 100,
            "global_short_account_pct": float(r.get("shortAccount", 0.5)) * 100,
        } for r in raw]
        df = pd.DataFrame(records).drop_duplicates("ts_ms").sort_values("ts_ms").reset_index(drop=True)
        self.cache.write(df, "global_ratio", symbol, period, start_ms, end_ms)
        return df

    # ------------------------------------------------------------------ OI history

    def fetch_oi_hist(self, symbol: str, period: str,
                       start_ms: int, end_ms: int) -> pd.DataFrame:
        """
        /futures/data/openInterestHist
        Returns: ts_ms, oi_value
        """
        cached = self.cache.read("open_interest", symbol, period, start_ms, end_ms)
        if cached is not None:
            return cached

        log.info("fetching oi_hist %s %s", symbol, period)
        try:
            raw = self._paginate_ts(
                "/futures/data/openInterestHist",
                {"symbol": symbol, "period": period},
                ts_field="timestamp",
                limit=RATIO_LIMIT,
                start_ms=start_ms,
                end_ms=end_ms,
            )
        except Exception as exc:
            log.warning("oi_hist fetch failed %s: %s", symbol, exc)
            return pd.DataFrame()

        if not raw:
            return pd.DataFrame()

        records = [{
            "ts_ms": int(r["timestamp"]),
            "oi_value": float(r.get("sumOpenInterestValue", 0.0)),
        } for r in raw]
        df = pd.DataFrame(records).drop_duplicates("ts_ms").sort_values("ts_ms").reset_index(drop=True)
        self.cache.write(df, "open_interest", symbol, period, start_ms, end_ms)
        return df

    # ------------------------------------------------------------------ basis

    def fetch_basis(self, symbol: str, period: str,
                     start_ms: int, end_ms: int) -> pd.DataFrame:
        """
        /futures/data/basis
        Returns: ts_ms, basis_rate
        Note: uses pair=symbol and contractType=PERPETUAL
        """
        cached = self.cache.read("basis", symbol, period, start_ms, end_ms)
        if cached is not None:
            return cached

        log.info("fetching basis %s %s", symbol, period)
        try:
            raw = self._paginate_ts(
                "/futures/data/basis",
                {"pair": symbol, "contractType": "PERPETUAL", "period": period},
                ts_field="timestamp",
                limit=RATIO_LIMIT,
                start_ms=start_ms,
                end_ms=end_ms,
            )
        except Exception as exc:
            log.warning("basis fetch failed %s: %s", symbol, exc)
            return pd.DataFrame()

        if not raw:
            return pd.DataFrame()

        records = [{
            "ts_ms": int(r["timestamp"]),
            "basis_rate": float(r.get("basisRate", 0.0)),
        } for r in raw]
        df = pd.DataFrame(records).drop_duplicates("ts_ms").sort_values("ts_ms").reset_index(drop=True)
        self.cache.write(df, "basis", symbol, period, start_ms, end_ms)
        return df

    # ------------------------------------------------------------------ combined fetch

    def fetch_symbol_data(self, symbol: str, period: str,
                           start_ms: int, end_ms: int) -> Dict[str, pd.DataFrame]:
        """
        Fetch and cache all data sources for one symbol.
        Returns dict keyed by metric name.
        Missing data sources return empty DataFrames (not errors).
        """
        return {
            "klines": self.fetch_klines(symbol, period, start_ms, end_ms),
            "top_account_ratio": self.fetch_top_account_ratio(symbol, period, start_ms, end_ms),
            "top_position_ratio": self.fetch_top_position_ratio(symbol, period, start_ms, end_ms),
            "global_ratio": self.fetch_global_ratio(symbol, period, start_ms, end_ms),
            "oi_hist": self.fetch_oi_hist(symbol, period, start_ms, end_ms),
            "basis": self.fetch_basis(symbol, period, start_ms, end_ms),
        }

    # ------------------------------------------------------------------ symbol universe

    def load_universe(self, quote_asset: str = "USDT",
                       exclude: Optional[list] = None) -> List[str]:
        """Load all USDT-M perpetual symbols from Binance."""
        info = self._get("/fapi/v1/exchangeInfo", {})
        exclude_set = set(exclude or [])
        symbols = []
        for item in info.get("symbols", []):
            if item.get("contractType") != "PERPETUAL":
                continue
            if item.get("quoteAsset") != quote_asset:
                continue
            if item.get("status") != "TRADING":
                continue
            sym = item["symbol"]
            if sym not in exclude_set:
                symbols.append(sym)
        return symbols

    def filter_by_volume(self, symbols: List[str],
                          min_qv: float, max_count: int) -> List[str]:
        """Filter symbols by 24h quote volume and return top N."""
        time.sleep(self.sleep)
        tickers = self._get("/fapi/v1/ticker/24hr", {})
        ticker_map = {t["symbol"]: t for t in tickers}
        qualified = []
        for sym in symbols:
            t = ticker_map.get(sym)
            if not t:
                continue
            if float(t.get("quoteVolume", 0)) >= min_qv:
                qualified.append((sym, float(t["quoteVolume"])))
        qualified.sort(key=lambda x: x[1], reverse=True)
        return [s for s, _ in qualified[:max_count]]


# ---------------------------------------------------------------------------
# Data alignment helper
# ---------------------------------------------------------------------------

def align_symbol_data(data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Left-join all data sources onto klines using open_time as the base.
    Klines open_time is the canonical timestamp for each 5m bar.

    Proxy data (top/global ratios, OI, basis) timestamps may not align
    perfectly with klines — we merge on nearest available using merge_asof.
    """
    klines = data.get("klines", pd.DataFrame())
    if klines.empty:
        return pd.DataFrame()

    base = klines.copy()
    base = base.sort_values("open_time").reset_index(drop=True)

    # Helper: merge_asof proxy df onto klines by timestamp
    def _merge(proxy_df: pd.DataFrame, ts_col: str,
                cols: List[str], suffix: str) -> pd.DataFrame:
        nonlocal base
        if proxy_df.empty:
            for c in cols:
                base[c] = float("nan")
            return base
        proxy_df = proxy_df.sort_values(ts_col).reset_index(drop=True)
        merged = pd.merge_asof(
            base[["open_time"]],
            proxy_df[[ts_col] + cols],
            left_on="open_time",
            right_on=ts_col,
            direction="nearest",
            tolerance=5 * 60 * 1000,  # 5m tolerance in ms
        )
        for c in cols:
            base[c] = merged[c].values
        return base

    _merge(data.get("top_account_ratio", pd.DataFrame()), "ts_ms",
           ["top_long_account_pct", "top_short_account_pct"], "tar")
    _merge(data.get("top_position_ratio", pd.DataFrame()), "ts_ms",
           ["top_long_position_pct", "top_short_position_pct"], "tpr")
    _merge(data.get("global_ratio", pd.DataFrame()), "ts_ms",
           ["global_long_account_pct", "global_short_account_pct"], "gr")
    _merge(data.get("oi_hist", pd.DataFrame()), "ts_ms", ["oi_value"], "oi")
    _merge(data.get("basis", pd.DataFrame()), "ts_ms", ["basis_rate"], "basis")

    return base
