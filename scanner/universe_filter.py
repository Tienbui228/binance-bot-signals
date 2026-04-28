import json
import os
import time
import requests
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple


class UniverseFilter:
    """Shared universe filter for all strategies.

    Fetches all USDT perpetuals from Binance, filters by market cap <= 500M
    using CoinGecko, and persists the result to data/eligible_universe.json.
    Cache survives process restarts; refreshed every cache_hours hours.
    """

    BINANCE_FUTURES_URL = "https://fapi.binance.com"

    def __init__(self, cfg: Dict):
        uf_cfg = cfg.get("universe_filter", {})
        self._enabled = uf_cfg.get("enabled", True)
        self._max_cap = uf_cfg.get("market_cap_max_usd", 500_000_000)
        self._cache_hours = uf_cfg.get("cache_hours", 4)
        self._pages = uf_cfg.get("pages_to_fetch", 3)
        self._delay = uf_cfg.get("request_delay_sec", 1.5)
        self._exclude_unknown = uf_cfg.get("exclude_if_unknown", True)
        self._base_url = uf_cfg.get("coingecko_base_url", "https://api.coingecko.com/api/v3")
        self._cache_path = uf_cfg.get("cache_path", "data/eligible_universe.json")
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": "Mozilla/5.0"})

        self._symbols: List[str] = []
        self._excluded: Dict[str, str] = {}
        self._built_at: Optional[datetime] = None

        os.makedirs(os.path.dirname(self._cache_path), exist_ok=True)
        self._load_from_disk()

    # ——— Public API ———

    def refresh_if_stale(self):
        if not self._enabled:
            return
        age_h = self._cache_age_hours()
        if age_h >= self._cache_hours:
            self.build_eligible_universe()

    def build_eligible_universe(self):
        """Fetch Binance perps + CoinGecko market caps, filter, and persist."""
        print(f"[UniverseFilter] Building eligible universe...")
        binance_symbols = self._fetch_binance_usdt_perps()
        if not binance_symbols:
            print("[UniverseFilter] Binance fetch failed — keeping existing cache")
            return

        mc_map = self._fetch_coingecko_market_caps()
        if not mc_map:
            print("[UniverseFilter] CoinGecko fetch failed — keeping existing cache")
            return

        eligible: List[str] = []
        excluded: Dict[str, str] = {}
        for sym in binance_symbols:
            cap = mc_map.get(_to_cg_symbol(sym))
            if cap is None:
                if self._exclude_unknown:
                    excluded[sym] = "market_cap_unknown"
                else:
                    eligible.append(sym)
            elif cap > self._max_cap:
                excluded[sym] = "market_cap_too_large"
            else:
                eligible.append(sym)

        self._symbols = eligible
        self._excluded = excluded
        self._built_at = datetime.now(timezone.utc)
        self._save_to_disk(binance_symbols)
        print(f"[UniverseFilter] Done: {len(eligible)} eligible, {len(excluded)} excluded "
              f"(total checked: {len(binance_symbols)})")

    def get_eligible_symbols(self) -> List[str]:
        self.refresh_if_stale()
        return list(self._symbols)

    def is_eligible(self, symbol: str) -> Tuple[bool, str]:
        """Returns (True, '') or (False, exclusion_reason)."""
        if symbol in self._symbols:
            return True, ""
        reason = self._excluded.get(symbol, "market_cap_unknown")
        return False, reason

    def has_data(self) -> bool:
        return bool(self._symbols)

    # ——— Private ———

    def _cache_age_hours(self) -> float:
        if self._built_at is None:
            return 9999.0
        now = datetime.now(timezone.utc)
        built = self._built_at
        if built.tzinfo is None:
            built = built.replace(tzinfo=timezone.utc)
        return (now - built).total_seconds() / 3600

    def _fetch_binance_usdt_perps(self) -> List[str]:
        try:
            resp = self._session.get(
                f"{self.BINANCE_FUTURES_URL}/fapi/v1/exchangeInfo",
                timeout=10,
            )
            resp.raise_for_status()
            symbols = []
            for s in resp.json().get("symbols", []):
                if (s.get("status") == "TRADING"
                        and s.get("contractType") == "PERPETUAL"
                        and s.get("quoteAsset") == "USDT"):
                    symbols.append(s["symbol"])
            return symbols
        except Exception as e:
            print(f"[UniverseFilter] Binance fetch error: {e}")
            return []

    def _fetch_coingecko_market_caps(self) -> Dict[str, float]:
        """Returns {cg_symbol: market_cap_usd}. Duplicate tickers → keep higher cap."""
        mc_map: Dict[str, float] = {}
        for page in range(1, self._pages + 1):
            try:
                resp = self._session.get(
                    f"{self._base_url}/coins/markets",
                    params={
                        "vs_currency": "usd",
                        "order": "market_cap_desc",
                        "per_page": 250,
                        "page": page,
                    },
                    timeout=15,
                )
                resp.raise_for_status()
                for coin in resp.json():
                    sym = (coin.get("symbol") or "").lower()
                    cap = coin.get("market_cap") or 0  # market_cap, NOT fdv
                    if sym and float(cap) > mc_map.get(sym, 0):
                        mc_map[sym] = float(cap)
                if page < self._pages:
                    time.sleep(self._delay)
            except Exception as e:
                print(f"[UniverseFilter] CoinGecko page {page} error: {e}")
                break
        return mc_map

    def _save_to_disk(self, all_symbols: List[str]):
        data = {
            "built_at": self._built_at.strftime("%Y-%m-%dT%H:%M:%SZ") if self._built_at else "",
            "total_checked": len(all_symbols),
            "total_eligible": len(self._symbols),
            "symbols": self._symbols,
            "excluded": self._excluded,
        }
        try:
            with open(self._cache_path, "w") as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            print(f"[UniverseFilter] Save to disk failed: {e}")

    def _load_from_disk(self):
        if not os.path.exists(self._cache_path):
            return
        try:
            with open(self._cache_path, "r") as f:
                data = json.load(f)
            built_at_str = data.get("built_at", "")
            if built_at_str:
                self._built_at = datetime.strptime(built_at_str, "%Y-%m-%dT%H:%M:%SZ").replace(
                    tzinfo=timezone.utc
                )
            self._symbols = data.get("symbols", [])
            self._excluded = data.get("excluded", {})
            print(f"[UniverseFilter] Loaded from disk: {len(self._symbols)} eligible symbols "
                  f"(built {built_at_str})")
        except Exception as e:
            print(f"[UniverseFilter] Load from disk failed: {e}")


def _to_cg_symbol(binance_symbol: str) -> str:
    """GWEIUSDT → gwei, BTCUSDT → btc"""
    sym = binance_symbol
    if sym.endswith("USDT"):
        sym = sym[:-4]
    return sym.lower()
