import time
import requests
from datetime import datetime
from typing import Dict, Optional, Tuple


class MarketCapCache:
    def __init__(self, cfg: Dict):
        self._base_url = cfg.get("coingecko_base_url", "https://api.coingecko.com/api/v3")
        self._cache_hours = cfg.get("cache_hours", 4)
        self._pages = cfg.get("pages_to_fetch", 3)
        self._delay = cfg.get("request_delay_sec", 1.5)
        self._cache: Dict[str, float] = {}   # {"btc": 1200000000000.0, "gwei": 12500000.0, ...}
        self._cache_built_at: Optional[datetime] = None

    def refresh_if_stale(self):
        now = datetime.utcnow()
        age_h = (now - self._cache_built_at).total_seconds() / 3600 if self._cache_built_at else 9999
        if age_h >= self._cache_hours:
            self._fetch()

    def _fetch(self):
        new_cache: Dict[str, float] = {}
        for page in range(1, self._pages + 1):
            try:
                resp = requests.get(
                    f"{self._base_url}/coins/markets",
                    params={
                        "vs_currency": "usd",
                        "order": "market_cap_desc",
                        "per_page": 250,
                        "page": page,
                    },
                    timeout=15,
                    headers={"User-Agent": "Mozilla/5.0"},
                )
                resp.raise_for_status()
                for coin in resp.json():
                    sym = (coin.get("symbol") or "").lower()
                    # Use market_cap (circulating supply × price), NOT fully_diluted_valuation
                    cap = coin.get("market_cap") or 0
                    # Duplicate symbol → keep higher cap (conservative: avoid passing large-cap through gate)
                    if sym and cap > new_cache.get(sym, 0):
                        new_cache[sym] = float(cap)
                if page < self._pages:
                    time.sleep(self._delay)
            except Exception as e:
                print(f"[MarketCapCache] page {page} failed: {e}")
                break   # fail-soft: stop fetching, use whatever we collected

        if new_cache:
            self._cache = new_cache
            self._cache_built_at = datetime.utcnow()
            print(f"[MarketCapCache] Refreshed: {len(self._cache)} coins")
        else:
            print("[MarketCapCache] Fetch failed, keeping existing cache")

    def get_market_cap(self, binance_symbol: str) -> Optional[float]:
        """Returns market_cap_usd or None if symbol not mapped."""
        return self._cache.get(_to_cg_symbol(binance_symbol))

    def is_eligible(self, binance_symbol: str, max_cap: float) -> Tuple[bool, str]:
        """Returns (True, "") or (False, exclusion_reason)."""
        cap = self.get_market_cap(binance_symbol)
        if cap is None:
            return False, "market_cap_unknown"
        if cap > max_cap:
            return False, "market_cap_too_large"
        return True, ""

    def has_data(self) -> bool:
        return bool(self._cache)


def _to_cg_symbol(binance_symbol: str) -> str:
    """GWEIUSDT → gwei, BTCUSDT → btc"""
    sym = binance_symbol
    if sym.endswith("USDT"):
        sym = sym[:-4]
    return sym.lower()
