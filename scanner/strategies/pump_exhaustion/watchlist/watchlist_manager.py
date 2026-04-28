import csv
import json
import os
import threading
import time
from typing import Dict, List, Optional


DISCOVERY_LOG_SCHEMA = [
    "discovery_ts", "symbol", "pump_pct", "peak_age_h", "room_pct",
    "quote_vol_24h", "listing_age_days", "base_validity_flag",
    "anchor_order_valid", "exclusion_reason", "added_to_watchlist",
]

CASE_FIELDS = [
    # Identity
    "case_id", "symbol", "strategy_family", "case_state", "created_ts", "updated_ts",
    # Base
    "base_detection_method", "base_window_start_ts", "base_window_end_ts",
    "base_price_median", "base_range_pct", "base_trending_flag",
    "base_validity_flag", "base_invalid_reason",
    # Pump
    "peak_high", "peak_time", "peak_age_h_at_creation",
    "pump_pct", "pump_vol_ratio", "pump_speed_h", "pre_pump_low",
    "peak_vol",
    # Room
    "room_pct", "current_price_at_scan",
    "target_conservative", "target_extreme",
    "rr_conservative", "rr_extreme",
    # Breakdown
    "breakdown_level", "breakdown_level_source", "breakdown_level_confidence",
    "support_test_count_before_break", "break_distance_pct",
    "break_body_ratio", "break_volume_ratio",
    "immediate_followthrough_pct_1bar", "immediate_followthrough_pct_3bar",
    "false_break_reclaim_fast_flag", "breakdown_candle_time",
    # Retest
    "retest_depth_vs_break_pct", "max_reclaim_above_break_pct",
    "retest_duration_bars", "fail_strength", "fail_confirmation_bar_count",
    "signal_age_min", "taker_buy_ratio_at_retest",
    # Entry
    "entry_price", "stop_price", "pattern_type",
    "conservative_target_valid",
    # Anchors
    "p0_ts", "p0_price", "p1_ts", "p1_price",
    "p2_ts", "p2_price", "p3_ts", "p3_price",
    "p4_ts", "p4_price", "anchor_quality_flag",
    # Market context
    "oi_regime_label", "oi_change_from_peak_pct",
    "oi_change_1h_pct", "oi_change_4h_pct", "oi_context_note",
    "latest_funding_rate",
    "taker_sell_ratio_1h", "cvd_proxy_1h_trend",
    "taker_sell_ratio_5m", "cvd_proxy_5m_trend",
    "post_peak_avg_vol_12h",
    # Market regime (group F)
    "regime_label", "regime_score", "btc_vs_ema20_pct", "alts_declining_pct", "regime_note",
    # Scoring
    "score_total", "score_max", "confidence_label",
    "score_pump_context", "score_exhaustion",
    "score_breakdown_quality", "score_market_pressure", "score_delivery", "score_regime",
    # Exclusion
    "exclusion_reason", "semantic_clean_flag",
    # Validity flags
    "anchor_order_valid", "data_fetch_error",
    # Outcome
    "outcome_status",
    "future_1h_max_favor_pct", "future_1h_max_adverse_pct",
    "future_4h_max_favor_pct", "future_4h_max_adverse_pct",
    "reclaim_breakdown_flag", "entry_feasible_flag",
    "resolution_label", "resolution_reason_code",
]


class WatchlistManager:
    def __init__(self, cfg: Dict):
        pump_cfg = cfg.get("pump_exhaustion", {})
        wl_cfg = pump_cfg.get("watchlist", {})
        self._data_dir = wl_cfg.get("data_dir", "data/pump_exhaustion")
        os.makedirs(self._data_dir, exist_ok=True)

        self._wl_path = os.path.join(self._data_dir, "watchlist.json")
        self._lock = threading.Lock()
        self._cases: Dict[str, Dict] = {}
        self._load()

        self._active_csv = os.path.join(self._data_dir, "cases_active.csv")
        self._closed_csv = os.path.join(self._data_dir, "cases_closed.csv")
        self._excluded_csv = os.path.join(self._data_dir, "cases_excluded.csv")
        self._discovery_log = os.path.join(self._data_dir, "discovery_log.csv")
        self._csv_locks: Dict[str, threading.Lock] = {
            self._active_csv: threading.Lock(),
            self._closed_csv: threading.Lock(),
            self._excluded_csv: threading.Lock(),
            self._discovery_log: threading.Lock(),
        }

    def _load(self):
        if os.path.exists(self._wl_path):
            try:
                with open(self._wl_path, "r") as f:
                    self._cases = json.load(f)
                print(f"[WatchlistManager] Loaded {len(self._cases)} cases from {self._wl_path}")
            except Exception as e:
                print(f"[WatchlistManager] Load failed: {e}")
                self._cases = {}
        else:
            self._cases = {}

    def flush(self):
        with self._lock:
            try:
                with open(self._wl_path, "w") as f:
                    json.dump(self._cases, f, indent=2)
            except Exception as e:
                print(f"[WatchlistManager] flush failed: {e}")

    def add_case(self, case: Dict) -> bool:
        """Add a new DISCOVERED case. Returns True if added, False if skipped.

        If symbol already exists with state != EXCLUDED: skip entirely.
        Do NOT update peak fields on re-discovery.
        """
        symbol = case.get("symbol", "")
        with self._lock:
            for existing in self._cases.values():
                if existing.get("symbol") == symbol and existing.get("case_state") != "EXCLUDED":
                    return False
            case_id = f"{symbol}_pump_exhaustion_{int(time.time() * 1000)}"
            case["case_id"] = case_id
            case.setdefault("strategy_family", "pump_exhaustion_short")
            case.setdefault("case_state", "DISCOVERED")
            case.setdefault("outcome_status", "not_reached_yet")
            case.setdefault("p4_price", "not_reached_yet")
            case.setdefault("p4_ts", "not_reached_yet")
            case.setdefault("data_fetch_error", None)
            now = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
            case["created_ts"] = now
            case["updated_ts"] = now
            self._cases[case_id] = case
            return True

    def update_case(self, case_id: str, updates: Dict):
        with self._lock:
            if case_id in self._cases:
                self._cases[case_id].update(updates)
                self._cases[case_id]["updated_ts"] = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())

    def get_active_cases(self) -> List[Dict]:
        with self._lock:
            terminal = {"EXCLUDED", "WATCHLIST_ONLY_STRAIGHT_DUMP", "CLOSED_4H_READY"}
            return [c for c in self._cases.values() if c.get("case_state") not in terminal]

    def get_all_cases(self) -> List[Dict]:
        with self._lock:
            return list(self._cases.values())

    def get_case(self, case_id: str) -> Optional[Dict]:
        with self._lock:
            return self._cases.get(case_id)

    def remove_stale(self, max_peak_age_h: float = 72.0) -> int:
        """Move cases with peak_age_h > max to EXCLUDED. Returns count of cases marked stale."""
        now_ms = int(time.time() * 1000)
        count = 0
        with self._lock:
            for case in self._cases.values():
                state = case.get("case_state", "")
                if state in ("EXCLUDED", "CLOSED_4H_READY", "WATCHLIST_ONLY_STRAIGHT_DUMP"):
                    continue
                peak_time = case.get("peak_time")
                if peak_time:
                    try:
                        age_h = (now_ms - int(peak_time)) / 3_600_000
                    except (TypeError, ValueError):
                        continue
                    if age_h > max_peak_age_h:
                        reason = "retest_not_found" if state == "RETEST_WAITING" else "peak_too_old"
                        case["case_state"] = "EXCLUDED"
                        case["exclusion_reason"] = reason
                        count += 1
        return count

    def append_to_csv(self, filepath: str, row: Dict):
        lock = self._csv_locks.get(filepath, threading.Lock())
        with lock:
            exists = os.path.exists(filepath)
            schema = DISCOVERY_LOG_SCHEMA if "discovery_log" in filepath else CASE_FIELDS
            with open(filepath, "a", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=schema, extrasaction="ignore")
                if not exists or os.path.getsize(filepath) == 0:
                    writer.writeheader()
                writer.writerow({k: row.get(k, "") for k in schema})

    def write_active_csv(self):
        with self._lock:
            active = [c for c in self._cases.values()
                      if c.get("case_state") not in ("EXCLUDED", "CLOSED_4H_READY",
                                                       "WATCHLIST_ONLY_STRAIGHT_DUMP")]
        self._write_csv(self._active_csv, active)

    def write_excluded_csv(self):
        with self._lock:
            excluded = [c for c in self._cases.values()
                        if c.get("case_state") in ("EXCLUDED", "WATCHLIST_ONLY_STRAIGHT_DUMP")]
        self._write_csv(self._excluded_csv, excluded)

    def write_closed_csv(self):
        with self._lock:
            closed = [c for c in self._cases.values()
                      if c.get("case_state") == "CLOSED_4H_READY"]
        self._write_csv(self._closed_csv, closed)

    def _write_csv(self, filepath: str, rows: List[Dict]):
        lock = self._csv_locks.get(filepath, threading.Lock())
        with lock:
            with open(filepath, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=CASE_FIELDS, extrasaction="ignore")
                writer.writeheader()
                for row in rows:
                    writer.writerow({k: row.get(k, "") for k in CASE_FIELDS})

    def log_discovery(self, row: Dict):
        self.append_to_csv(self._discovery_log, row)

    def sync_csvs(self):
        """Write all CSVs from current in-memory state."""
        self.write_active_csv()
        self.write_excluded_csv()
        self.write_closed_csv()
