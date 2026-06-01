"""
Persistent state for HL whale accumulation scanner.

Per-coin scan state: data/hl_whale_accum/state/{coin}.json (atomic write)
Scan metrics CSV: data/hl_whale_accum/logs/{coin}_metrics.csv (append-only)

concentration_history stored as [[ts_ms, value], ...] (not just values) to enable
time-based analysis in §13 even if snapshot-count windowing is used.
"""

import csv
import json
import os
from typing import Dict, List, Optional, Tuple


_CSV_COLUMNS = [
    "scan_ts",
    "coverage_estimate",
    "active_addresses",
    "total_addresses",
    "real_concentration",
    "top_n_long_notional",
    "distinct_clusters",
    "same_cohort_delta",
    "topn_jaccard",
    "g1", "g2", "g3", "g4", "g5", "g6",
    "gates_pass",
    "veto",
    "veto_reason",
    "detected",
    "shadow_would_fire",
    "throttle_count",
    "new_address_rate",
    "registry_churn_rate",
    "insufficient_history",
    "blocked_reason",
    "window_time_h",
    "snapshots_available",
    "price_vs_baseline",
    "price_trend_7v30",
    "funding_8h_pct",
]


class WatchlistManager:
    def __init__(self, cfg: dict):
        hl_cfg = cfg.get("hl_whale_accum", {})
        data_dir = hl_cfg.get("data_dir", "data/hl_whale_accum")
        self._state_dir = os.path.join(data_dir, "state")
        self._log_dir = os.path.join(data_dir, "logs")
        os.makedirs(self._state_dir, exist_ok=True)
        os.makedirs(self._log_dir, exist_ok=True)

        self._state_cache: Dict[str, dict] = {}

    # ── Scan state ────────────────────────────────────────────────────────────

    def load_state(self, coin: str) -> dict:
        if coin in self._state_cache:
            return self._state_cache[coin]
        path = self._state_path(coin)
        if os.path.exists(path):
            try:
                with open(path, "r") as fh:
                    data = json.load(fh)
                self._state_cache[coin] = data
                return data
            except Exception as e:
                print(f"[WatchlistManager] load_state failed {coin}: {e}")
        empty: dict = {
            "last_scan_ts": None,
            "prev_top_n": [],
            "prev_cluster_notionals": {},
            "concentration_history": [],
            "oi_history": [],
            "top_n_notional_history": [],
            "warmup_start_ts": None,
            "coverage_adequate_since_ts": None,
        }
        self._state_cache[coin] = empty
        return empty

    def save_state(self, coin: str, state: dict) -> None:
        self._state_cache[coin] = state
        path = self._state_path(coin)
        tmp = path + ".tmp"
        try:
            with open(tmp, "w") as fh:
                json.dump(state, fh)
            os.replace(tmp, path)
        except Exception as e:
            print(f"[WatchlistManager] save_state failed {coin}: {e}")

    # ── Scan log ─────────────────────────────────────────────────────────────

    def log_scan(
        self,
        coin: str,
        sig: dict,
        churn: dict,
        health: dict,
        anti: dict,
        conc: dict,
    ) -> None:
        """Append one row to the coin's scan metrics CSV."""
        row = {
            "scan_ts": _now_ts(),
            "coverage_estimate": health.get("coverage_estimate", ""),
            "active_addresses": health.get("active_address_count", ""),
            "total_addresses": health.get("total_address_count", ""),
            "real_concentration": conc.get("real_concentration", ""),
            "top_n_long_notional": conc.get("top_n_long_notional", ""),
            "distinct_clusters": conc.get("distinct_clusters", ""),
            "same_cohort_delta": churn.get("same_cohort_delta", ""),
            "topn_jaccard": churn.get("topn_jaccard", ""),
            "g1": _boolstr(sig.get("g1")),
            "g2": _boolstr(sig.get("g2")),
            "g3": _boolstr(sig.get("g3")),
            "g4": _boolstr(sig.get("g4")),
            "g5": _boolstr(sig.get("g5")),
            "g6": _boolstr(sig.get("g6")),
            "gates_pass": _boolstr(sig.get("gates_pass")),
            "veto": _boolstr(anti.get("veto")),
            "veto_reason": anti.get("reason", ""),
            "detected": _boolstr(sig.get("detected")),
            "shadow_would_fire": _boolstr(sig.get("should_shadow_alert")),
            "throttle_count": health.get("throttle_count", 0),
            "new_address_rate": health.get("new_address_rate", ""),
            "registry_churn_rate": health.get("registry_churn_rate", ""),
            "insufficient_history": _boolstr(sig.get("blocked") == "insufficient_history"),
            "blocked_reason": sig.get("blocked") or "",
            "window_time_h": sig.get("window_time_h", ""),
            "snapshots_available": sig.get("snapshots_available", ""),
            "price_vs_baseline": sig.get("price_vs_baseline", ""),
            "price_trend_7v30": sig.get("price_trend_7v30", ""),
            "funding_8h_pct": sig.get("funding_8h_pct", ""),
        }
        path = self._log_path(coin)
        write_header = not os.path.exists(path)
        try:
            with open(path, "a", newline="") as fh:
                writer = csv.DictWriter(fh, fieldnames=_CSV_COLUMNS)
                if write_header:
                    writer.writeheader()
                writer.writerow(row)
        except Exception as e:
            print(f"[WatchlistManager] log_scan failed {coin}: {e}")

    # ── Paths ────────────────────────────────────────────────────────────────

    def _state_path(self, coin: str) -> str:
        return os.path.join(self._state_dir, f"{coin.replace('/', '_')}.json")

    def _log_path(self, coin: str) -> str:
        return os.path.join(self._log_dir, f"{coin.replace('/', '_')}_metrics.csv")


def _now_ts() -> str:
    import time
    return str(int(time.time() * 1000))


def _boolstr(v) -> str:
    if v is None:
        return "None"
    return "True" if v else "False"
