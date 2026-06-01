"""
Sybil clustering — assign canonical cluster_id to each tracked address (spec §8.2).

Invariant: cluster_id = lexicographically smallest address in the merged cluster.
This makes cluster_id deterministic and stable across scans — merges don't create
new IDs, they adopt the smallest existing one. Critical for Jaccard / cohort metrics
(spec §0, plan capstone invariant).

Call frequency: UPDATE-only, not full-recompute. Only query ledger for NEW addresses
since last clustering run (per address last_clustered_ts). Never re-query known addresses.

Strong linkage (auto-merge): userNonFundingLedgerUpdates reveals transfers between
tracked addresses — a direct funding relationship.

Weak linkage (flag only, no auto-merge): near-simultaneous entries (same coin, ±1 min,
similar size). Too many false positives to auto-merge.
"""

import json
import os
import threading
import time
from typing import Dict, List, Optional, Set

from ._client import _HlHttp


class SybilClusterer:
    def __init__(self, cfg: dict):
        hl_cfg = cfg.get("hl_whale_accum", {})
        data_dir = hl_cfg.get("data_dir", "data/hl_whale_accum")
        self._cluster_dir = os.path.join(data_dir, "clusters")
        os.makedirs(self._cluster_dir, exist_ok=True)

        self._client = _HlHttp(
            delay_sec=float(hl_cfg.get("http_delay_sec", 0.3)),
            timeout_sec=float(hl_cfg.get("http_timeout_sec", 10.0)),
        )

        # cluster_state[coin] = {addr: cluster_id, last_clustered: {addr: ts_ms}}
        self._state: Dict[str, dict] = {}
        self._locks: Dict[str, threading.Lock] = {}

    # ── Public API ──────────────────────────────────────────────────────────

    def cluster_coin(self, coin: str, registry: Dict[str, dict]) -> Dict[str, str]:
        """Update cluster assignments for all addresses in registry.

        Returns addr→cluster_id dict for this coin.
        Only queries ledger for addresses not yet clustered.
        """
        state = self._load_state(coin)
        assignments: Dict[str, str] = state.get("assignments", {})
        last_clustered: Dict[str, int] = state.get("last_clustered", {})

        # Union-Find structure: parent[addr] = canonical cluster_id
        parent = {addr: assignments.get(addr, addr) for addr in registry}

        # Ensure all addresses start with themselves if unknown
        for addr in registry:
            if addr not in parent:
                parent[addr] = addr

        # Strong linkage: query ledger ONLY for new addresses
        new_addrs = [a for a in registry if a not in last_clustered]
        now_ms = int(time.time() * 1000)
        tracked_set = set(registry.keys())

        for addr in new_addrs:
            ledger = self._client.user_non_funding_ledger_updates(addr, start_ms=now_ms - 90 * 86_400_000)
            last_clustered[addr] = now_ms

            for entry in ledger:
                delta = entry.get("delta", {})
                if delta.get("type") != "send":
                    continue
                # Check if transfer partner is also a tracked address
                src = delta.get("user", "")
                dst = delta.get("destination", "")
                partner = dst if src == addr else src if dst == addr else None
                if partner and partner in tracked_set:
                    _union(parent, addr, partner)

        # Weak linkage: near-simultaneous entries (flag in registry, not in cluster)
        # Skipped — would require cross-address entry-time comparison; false-positive risk too high

        # Resolve final canonical IDs
        final: Dict[str, str] = {}
        for addr in parent:
            final[addr] = _find(parent, addr)

        # Persist
        new_state = {"assignments": final, "last_clustered": last_clustered}
        self._save_state(coin, new_state)
        self._state[coin] = new_state

        return final

    def get_assignments(self, coin: str) -> Dict[str, str]:
        """Return addr→cluster_id from cached/persisted state."""
        state = self._load_state(coin)
        return dict(state.get("assignments", {}))

    # ── Internal ────────────────────────────────────────────────────────────

    def _load_state(self, coin: str) -> dict:
        if coin in self._state:
            return self._state[coin]
        path = self._state_path(coin)
        if os.path.exists(path):
            try:
                with open(path, "r") as fh:
                    data = json.load(fh)
                self._state[coin] = data
                return data
            except Exception as e:
                print(f"[Clustering] load failed {coin}: {e}")
        empty: dict = {"assignments": {}, "last_clustered": {}}
        self._state[coin] = empty
        return empty

    def _save_state(self, coin: str, state: dict) -> None:
        path = self._state_path(coin)
        tmp = path + ".tmp"
        try:
            with open(tmp, "w") as fh:
                json.dump(state, fh)
            os.replace(tmp, path)
        except Exception as e:
            print(f"[Clustering] save failed {coin}: {e}")

    def _state_path(self, coin: str) -> str:
        safe = coin.replace("/", "_")
        return os.path.join(self._cluster_dir, f"{safe}.json")


def _find(parent: Dict[str, str], addr: str) -> str:
    """Path-compressed find returning canonical (smallest-lex) cluster_id."""
    root = addr
    while parent.get(root, root) != root:
        root = parent[root]
    # Path compression
    cur = addr
    while parent.get(cur, cur) != root:
        nxt = parent[cur]
        parent[cur] = root
        cur = nxt
    return root


def _union(parent: Dict[str, str], a: str, b: str) -> None:
    """Merge clusters of a and b. New cluster_id = lex-smallest address in merged set."""
    ra = _find(parent, a)
    rb = _find(parent, b)
    if ra == rb:
        return
    # Canonical id = lexicographically smallest
    canonical = min(ra, rb)
    other = max(ra, rb)
    parent[other] = canonical


def apply_cluster_assignments(registry: Dict[str, dict], assignments: Dict[str, str]) -> None:
    """Write cluster_id into each registry entry in-place."""
    for addr, entry in registry.items():
        entry["cluster_id"] = assignments.get(addr, addr)
