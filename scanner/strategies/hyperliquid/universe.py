"""
Weekly MC-filtered watchlist for Hyperliquid coins (spec §3).

CoinGecko mapping: HL returns coin names ("BTC", "ETH", "PURR").
CoinGecko /coins/markets uses id slugs ("bitcoin", "ethereum").
We resolve via /coins/list (symbol → [slug]) and rank by market-cap to disambiguate.
Ambiguous matches (multiple slugs for same symbol) are logged — not silently dropped.
Unknown MC → exclude (fail-closed, §3.3).
"""

import json
import os
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set

import requests

from ._client import _HlHttp


_COINGECKO_BASE = "https://api.coingecko.com/api/v3"


class HlUniverse:
    def __init__(self, cfg: dict):
        hl_cfg = cfg.get("hl_whale_accum", {})
        u_cfg = hl_cfg.get("universe", {})
        self._mc_cap = float(u_cfg.get("mc_cap_usd", 500_000_000))
        self._log_excluded = bool(u_cfg.get("log_excluded", True))
        data_dir = hl_cfg.get("data_dir", "data/hl_whale_accum")
        self._watchlist_dir = data_dir
        os.makedirs(self._watchlist_dir, exist_ok=True)

        self._client = _HlHttp(
            delay_sec=float(hl_cfg.get("http_delay_sec", 0.5)),
            timeout_sec=float(hl_cfg.get("http_timeout_sec", 15.0)),
        )
        self._cg_session = requests.Session()
        self._cg_session.headers.update({"User-Agent": "Mozilla/5.0"})

        self._coins: List[str] = []
        self._built_date: Optional[str] = None
        self._load_latest_watchlist()

    # ── Public API ──────────────────────────────────────────────────────────

    def refresh_if_stale(self) -> None:
        """Rebuild watchlist if it's from a previous week."""
        if self._is_stale():
            self.build_watchlist()

    def build_watchlist(self) -> List[str]:
        print("[HlUniverse] Building watchlist...")
        hl_coins = self._fetch_hl_coins()
        if not hl_coins:
            print("[HlUniverse] ERROR: no coins from metaAndAssetCtxs")
            return self._coins

        symbol_to_slug, ambiguous = self._build_coingecko_slug_map()
        mc_by_slug = self._fetch_market_caps()

        included: List[str] = []
        excluded: Dict[str, str] = {}
        ambiguous_log: Dict[str, List[str]] = {}

        for coin in hl_coins:
            sym = coin.upper()
            if sym in ambiguous:
                # Multiple slugs — pick highest-cap one (already resolved in slug map)
                ambiguous_log[coin] = ambiguous[sym]
                slug = symbol_to_slug.get(sym)
            else:
                slug = symbol_to_slug.get(sym)

            if slug is None:
                excluded[coin] = "coingecko_unknown"
                continue

            mc = mc_by_slug.get(slug)
            if mc is None:
                excluded[coin] = "market_cap_unknown"
                continue
            if mc > self._mc_cap:
                excluded[coin] = f"market_cap_too_large:{mc:.0f}"
                continue

            included.append(coin)

        today = datetime.utcnow().strftime("%Y-%m-%d")
        data = {
            "built_date": today,
            "total_hl_coins": len(hl_coins),
            "total_eligible": len(included),
            "coins": included,
        }
        path = os.path.join(self._watchlist_dir, f"watchlist_{today}.json")
        _atomic_write_json(path, data)

        self._coins = included
        self._built_date = today

        if self._log_excluded:
            exc_path = os.path.join(self._watchlist_dir, f"watchlist_excluded_{today}.json")
            _atomic_write_json(exc_path, {"excluded": excluded, "ambiguous_match": ambiguous_log})

        print(f"[HlUniverse] Watchlist built: {len(included)} eligible / {len(hl_coins)} total HL coins")
        if ambiguous_log:
            print(f"[HlUniverse] Ambiguous CoinGecko matches: {list(ambiguous_log.keys())[:10]}")

        return included

    def get_watchlist_coins(self) -> List[str]:
        self.refresh_if_stale()
        return list(self._coins)

    def is_eligible(self, coin: str) -> bool:
        return coin in self._coins

    # ── Internal ────────────────────────────────────────────────────────────

    def _is_stale(self) -> bool:
        if not self._built_date:
            return True
        today = datetime.utcnow()
        # Stale if built_date is before the most recent Monday
        most_recent_monday = today - timedelta(days=today.weekday())
        try:
            built = datetime.strptime(self._built_date, "%Y-%m-%d")
            return built.date() < most_recent_monday.date()
        except ValueError:
            return True

    def _load_latest_watchlist(self) -> None:
        """Load the most recently built watchlist file from disk."""
        try:
            files = sorted(
                f for f in os.listdir(self._watchlist_dir)
                if f.startswith("watchlist_") and f.endswith(".json") and "excluded" not in f
            )
            if not files:
                return
            path = os.path.join(self._watchlist_dir, files[-1])
            with open(path, "r") as fh:
                data = json.load(fh)
            self._coins = data.get("coins", [])
            self._built_date = data.get("built_date")
            print(f"[HlUniverse] Loaded watchlist from {files[-1]}: {len(self._coins)} coins")
        except Exception as e:
            print(f"[HlUniverse] Could not load watchlist: {e}")

    def _fetch_hl_coins(self) -> List[str]:
        ctx_map = self._client.meta_and_asset_ctxs()
        return list(ctx_map.keys())

    def _build_coingecko_slug_map(self) -> tuple:
        """Returns (symbol→best_slug dict, symbol→[all_slugs] for ambiguous ones)."""
        try:
            r = self._cg_session.get(f"{_COINGECKO_BASE}/coins/list", timeout=20)
            r.raise_for_status()
            coins_list = r.json()
        except Exception as e:
            print(f"[HlUniverse] CoinGecko /coins/list failed: {e}")
            return {}, {}

        # symbol → [id] — symbols are NOT unique
        sym_map: Dict[str, List[str]] = {}
        for c in coins_list:
            sym = c.get("symbol", "").upper()
            if sym:
                sym_map.setdefault(sym, []).append(c["id"])

        # For ambiguous symbols, use top-MC ranking to pick best slug
        # Fetch top 500 by MC to resolve most common ones
        top_mc_slugs = self._fetch_top_mc_slugs(500)

        result: Dict[str, str] = {}
        ambiguous: Dict[str, List[str]] = {}

        for sym, slugs in sym_map.items():
            if len(slugs) == 1:
                result[sym] = slugs[0]
            else:
                # Pick the slug with highest MC rank
                best = None
                for slug in slugs:
                    if slug in top_mc_slugs:
                        if best is None or top_mc_slugs[slug] < top_mc_slugs[best]:
                            best = slug
                if best is None:
                    best = slugs[0]
                result[sym] = best
                ambiguous[sym] = slugs

        return result, ambiguous

    def _fetch_top_mc_slugs(self, n: int) -> Dict[str, int]:
        """Returns {slug: rank} for top n coins by market cap."""
        ranks: Dict[str, int] = {}
        per_page = 250
        pages = (n + per_page - 1) // per_page
        rank = 1
        for page in range(1, pages + 1):
            try:
                r = self._cg_session.get(
                    f"{_COINGECKO_BASE}/coins/markets",
                    params={"vs_currency": "usd", "order": "market_cap_desc", "per_page": per_page, "page": page},
                    timeout=20,
                )
                r.raise_for_status()
                for coin in r.json():
                    ranks[coin["id"]] = rank
                    rank += 1
                time.sleep(0.5)
            except Exception as e:
                print(f"[HlUniverse] CoinGecko /coins/markets page {page} failed: {e}")
        return ranks

    def _fetch_market_caps(self) -> Dict[str, Optional[float]]:
        """Returns {slug: market_cap_usd} for top 750 coins."""
        mc: Dict[str, float] = {}
        per_page = 250
        for page in range(1, 4):
            try:
                r = self._cg_session.get(
                    f"{_COINGECKO_BASE}/coins/markets",
                    params={
                        "vs_currency": "usd",
                        "order": "market_cap_desc",
                        "per_page": per_page,
                        "page": page,
                    },
                    timeout=20,
                )
                r.raise_for_status()
                for coin in r.json():
                    cap = coin.get("market_cap")
                    if cap is not None:
                        try:
                            mc[coin["id"]] = float(cap)
                        except (TypeError, ValueError):
                            pass
                time.sleep(0.5)
            except Exception as e:
                print(f"[HlUniverse] CoinGecko markets page {page} failed: {e}")
        return mc


def _atomic_write_json(path: str, data: object) -> None:
    tmp = path + ".tmp"
    try:
        with open(tmp, "w") as fh:
            json.dump(data, fh, indent=2)
        os.replace(tmp, path)
    except Exception as e:
        print(f"[HlUniverse] write failed {path}: {e}")
