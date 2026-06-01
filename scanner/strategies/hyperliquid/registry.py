"""
Address registry — per-coin tracking of whale addresses (spec §6).

Entry conditions (3-tier floor, ANY):
  single fill notional >= 100k
  rolling 1h notional  >= 150k
  rolling 24h notional >= 300k
  current position value >= 100k

Atomic writes: registry is forward-only stateful — losing it means restarting warm-up.
Eviction ordering: evict AFTER churn is computed (caller responsibility — see scanner.py).
"""

import json
import os
import threading
import time
from typing import Dict, List, Optional, Set

from ._client import _HlHttp


class AddressRegistry:
    def __init__(self, cfg: dict):
        hl_cfg = cfg.get("hl_whale_accum", {})
        re_cfg = hl_cfg.get("registry_entry", {})
        self._floor_single = float(re_cfg.get("single_fill_notional_min", 100_000))
        self._floor_1h = float(re_cfg.get("rolling_1h_notional_min", 150_000))
        self._floor_24h = float(re_cfg.get("rolling_24h_notional_min", 300_000))
        self._floor_pos = float(re_cfg.get("position_value_min", 100_000))
        self._stale_days = int(hl_cfg.get("registry_stale_days", 5))

        data_dir = hl_cfg.get("data_dir", "data/hl_whale_accum")
        self._reg_dir = os.path.join(data_dir, "registry")
        os.makedirs(self._reg_dir, exist_ok=True)

        self._client = _HlHttp(
            delay_sec=float(hl_cfg.get("http_delay_sec", 0.3)),
            timeout_sec=float(hl_cfg.get("http_timeout_sec", 10.0)),
        )

        # registry[coin][addr] = entry dict
        self._registry: Dict[str, Dict[str, dict]] = {}
        self._locks: Dict[str, threading.Lock] = {}

    # ── Public API ──────────────────────────────────────────────────────────

    def get_addresses(self, coin: str) -> List[str]:
        return list(self._get_coin_registry(coin).keys())

    def get_entry(self, coin: str, addr: str) -> Optional[dict]:
        return self._get_coin_registry(coin).get(addr)

    def get_all_entries(self, coin: str) -> Dict[str, dict]:
        return dict(self._get_coin_registry(coin))

    def ingest_fills(self, coin: str) -> int:
        """Discover new addresses via recent trades. Returns count of new entries added."""
        trades = self._client.recent_trades(coin)
        now_ms = int(time.time() * 1000)
        lock = self._get_lock(coin)
        reg = self._get_coin_registry(coin)

        # Pre-fetch userFills for known addresses OUTSIDE the lock (HTTP calls take 1–10s)
        one_hour_ms = 3_600_000
        addrs_to_fetch: List[str] = []
        with lock:
            for addr in list(reg.keys()):
                last_seen = reg[addr].get("last_seen_ms", 0)
                if now_ms - last_seen >= 60_000:
                    addrs_to_fetch.append(addr)

        fills_by_addr: Dict[str, list] = {}
        for addr in addrs_to_fetch:
            fills_by_addr[addr] = self._client.user_fills(addr, start_ms=now_ms - one_hour_ms)

        # Apply all updates under a single lock acquisition
        added = 0
        changed = False
        with lock:
            for trade in trades:
                for addr in trade.get("users", []):
                    if not addr or addr in reg:
                        continue
                    notional = _safe_float(trade.get("px")) * _safe_float(trade.get("sz"))
                    if notional < self._floor_single:
                        continue
                    reg[addr] = _new_entry(addr, now_ms, notional)
                    added += 1
                    changed = True

            for addr, fills in fills_by_addr.items():
                entry = reg.get(addr)
                if entry is None:
                    continue
                for fill in fills:
                    notional = _safe_float(fill.get("px")) * _safe_float(fill.get("sz"))
                    if fill.get("coin") == coin:
                        entry["rolling_1h_notional"] = entry.get("rolling_1h_notional", 0) + notional
                        changed = True
                entry["last_seen_ms"] = now_ms

            if changed:
                self._persist(coin, reg)

        return added

    def refresh_positions(self, coin: str) -> None:
        """Update position values via clearinghouse_state per tracked address."""
        lock = self._get_lock(coin)
        with lock:
            addresses = list(self._get_coin_registry(coin).keys())

        # Fetch positions OUTSIDE the lock (HTTP calls take 1–10s each)
        now_ms = int(time.time() * 1000)
        pos_by_addr: Dict[str, Optional[dict]] = {}
        for addr in addresses:
            pos_by_addr[addr] = self._client.clearinghouse_state(addr, coin)

        # Apply updates and persist under a single lock acquisition
        with lock:
            reg = self._get_coin_registry(coin)
            changed = False
            for addr, pos in pos_by_addr.items():
                entry = reg.get(addr)
                if entry is None:
                    continue
                if pos is None:
                    entry["last_szi"] = 0.0
                    entry["last_position_value"] = 0.0
                else:
                    entry["last_szi"] = _safe_float(pos.get("szi"))
                    entry["last_position_value"] = _safe_float(pos.get("positionValue"))
                    entry["last_entry_px"] = _safe_float(pos.get("entryPx"))
                entry["last_seen_ms"] = now_ms
                changed = True
            if changed:
                self._persist(coin, reg)

    def evict_stale(self, coin: str, prev_top_n: Set[str]) -> List[str]:
        """Remove addresses with flat position > stale_days.

        Defers eviction of addresses still in prev_top_n until the NEXT cycle
        (caller computes churn BEFORE calling this — see scanner.py).
        Returns list of evicted addresses.
        """
        reg = self._get_coin_registry(coin)
        lock = self._get_lock(coin)
        stale_ms = self._stale_days * 86_400_000
        now_ms = int(time.time() * 1000)
        evicted: List[str] = []

        with lock:
            for addr in list(reg.keys()):
                entry = reg[addr]
                if entry.get("last_szi", 0) != 0:
                    continue  # still has position
                if entry.get("cluster_id", addr) in prev_top_n:
                    continue  # defer — churn already captured this cycle
                flat_since = entry.get("last_seen_ms", now_ms)
                if now_ms - flat_since >= stale_ms:
                    del reg[addr]
                    evicted.append(addr)
            if evicted:
                self._persist(coin, reg)

        return evicted

    def add_entry(self, coin: str, addr: str, notional: float) -> None:
        """Explicitly add an address that just crossed the entry floor."""
        now_ms = int(time.time() * 1000)
        lock = self._get_lock(coin)
        reg = self._get_coin_registry(coin)
        with lock:
            if addr not in reg:
                reg[addr] = _new_entry(addr, now_ms, notional)
                self._persist(coin, reg)

    def meets_entry_floor(self, single_notional: float = 0.0, pos_value: float = 0.0,
                          rolling_1h: float = 0.0, rolling_24h: float = 0.0) -> bool:
        return (
            single_notional >= self._floor_single
            or rolling_1h >= self._floor_1h
            or rolling_24h >= self._floor_24h
            or pos_value >= self._floor_pos
        )

    def load_coin(self, coin: str) -> None:
        """Explicitly load registry from disk for a coin (called on startup)."""
        self._get_coin_registry(coin)

    # ── Internal ────────────────────────────────────────────────────────────

    def _get_lock(self, coin: str) -> threading.Lock:
        if coin not in self._locks:
            self._locks[coin] = threading.Lock()
        return self._locks[coin]

    def _get_coin_registry(self, coin: str) -> Dict[str, dict]:
        if coin not in self._registry:
            self._registry[coin] = self._load_from_disk(coin)
        return self._registry[coin]

    def _load_from_disk(self, coin: str) -> Dict[str, dict]:
        path = self._reg_path(coin)
        if not os.path.exists(path):
            return {}
        try:
            with open(path, "r") as fh:
                data = json.load(fh)
            return data if isinstance(data, dict) else {}
        except Exception as e:
            print(f"[Registry] load failed {coin}: {e}")
            return {}

    def _persist(self, coin: str, reg: Dict[str, dict]) -> None:
        path = self._reg_path(coin)
        tmp = path + ".tmp"
        try:
            with open(tmp, "w") as fh:
                json.dump(reg, fh)
            os.replace(tmp, path)
        except Exception as e:
            print(f"[Registry] persist failed {coin}: {e}")

    def _reg_path(self, coin: str) -> str:
        safe = coin.replace("/", "_")
        return os.path.join(self._reg_dir, f"{safe}.json")


def _new_entry(addr: str, now_ms: int, notional: float) -> dict:
    return {
        "first_seen_ms": now_ms,
        "last_seen_ms": now_ms,
        "last_szi": 0.0,
        "last_position_value": 0.0,
        "last_entry_px": 0.0,
        "rolling_1h_notional": notional,
        "rolling_24h_notional": notional,
        "cluster_id": addr,  # default until clustering assigns canonical id
    }


def _safe_float(v) -> float:
    try:
        return float(v) if v not in (None, "", "N/A") else 0.0
    except (TypeError, ValueError):
        return 0.0
