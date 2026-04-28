import time
import threading
from typing import Dict, List, Optional

from scanner.universe_filter import UniverseFilter
from scanner.strategies.pump_exhaustion.config import get_pump_cfg
from scanner.strategies.pump_exhaustion._client import _BinanceHttp
from scanner.strategies.pump_exhaustion.watchlist.watchlist_manager import WatchlistManager
from scanner.strategies.pump_exhaustion.detectors.base_detector import detect_base
from scanner.strategies.pump_exhaustion.detectors.pump_detector import detect_pump


class PumpDiscovery:
    def __init__(self, cfg: Dict, universe_filter: UniverseFilter, client,
                 wl_manager: Optional[WatchlistManager] = None):
        self._cfg = cfg
        self._uf = universe_filter
        pump_cfg = get_pump_cfg(cfg)
        self._interval_sec = pump_cfg.get("discovery", {}).get("interval_minutes", 60) * 60
        self._wl_manager = wl_manager if wl_manager is not None else WatchlistManager(cfg)
        self._http = _BinanceHttp(
            delay_sec=cfg.get("binance", {}).get("request_delay_sec", 0.2),
            timeout_sec=cfg.get("binance", {}).get("request_timeout_sec", 10),
        )
        self._stop_event = threading.Event()

    def stop(self):
        self._stop_event.set()

    def run_loop(self):
        print("[PumpDiscovery] Loop started.")
        self._safe_run()
        while not self._stop_event.is_set():
            self._stop_event.wait(timeout=self._interval_sec)
            if self._stop_event.is_set():
                break
            self._safe_run()
        print("[PumpDiscovery] Loop stopped.")

    def _safe_run(self, limit_symbols=None):
        try:
            self.run_once(limit_symbols=limit_symbols)
        except Exception as e:
            import traceback
            print(f"[PumpDiscovery] run_once error: {e}")
            traceback.print_exc()

    def run_once(self, limit_symbols: Optional[int] = None):
        pump_cfg = get_pump_cfg(self._cfg)
        disc_cfg = pump_cfg.get("discovery", {})

        print(f"[PumpDiscovery] Starting scan at {time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())}")

        eligible = self._uf.get_eligible_symbols()
        if not eligible:
            print("[PumpDiscovery] No eligible symbols, skipping.")
            return

        if limit_symbols is not None:
            eligible = eligible[:limit_symbols]

        max_watchlist = pump_cfg.get("watchlist", {}).get("max_symbols", 50)
        active_symbols = {c["symbol"] for c in self._wl_manager.get_active_cases()}

        added = 0
        processed = 0

        for sym in eligible:
            processed += 1

            if sym in active_symbols:
                continue

            bars_1h = self._http.klines(sym, "1h", disc_cfg.get("peak_window_candles", 200))
            if len(bars_1h) < 20:
                _log_discovery(self._wl_manager, sym, "insufficient_data", False)
                continue

            base = detect_base(bars_1h, {"discovery": disc_cfg})
            if not base.get("base_validity_flag"):
                reason = base.get("base_invalid_reason", "base_invalid")
                _log_discovery(self._wl_manager, sym, reason, False,
                               base_validity_flag=False)
                continue

            pump = detect_pump(bars_1h, base)

            peak_age_h = pump.get("peak_age_h", 9999.0)
            pump_pct = pump.get("pump_pct", 0.0)
            pump_vol_ratio = pump.get("pump_vol_ratio", 0.0)

            current_price = bars_1h[-1]["close"] if bars_1h else 0.0
            base_price_median = base.get("base_price_median", 0.0)
            peak_high = base.get("peak_high", 0.0)
            room_pct = (current_price - base_price_median) / base_price_median if base_price_median > 0 else 0.0

            p0_ts = base.get("base_window_end_ts")
            p1_ts = base.get("peak_time")
            anchor_order_valid = bool(p0_ts and p1_ts and p0_ts < p1_ts)

            gate_results = {
                "G_D1_base_valid": base.get("base_validity_flag", False),
                "G_D2_pump_pct": pump_pct >= disc_cfg.get("pump_pct_min", 0.35),
                "G_D3_pump_vol": pump_vol_ratio >= disc_cfg.get("pump_vol_ratio_min", 3.0),
                "G_D4_peak_age": peak_age_h <= disc_cfg.get("peak_age_max_hours", 72),
                "G_D5_room": room_pct >= disc_cfg.get("room_pct_min", 0.15),
                "G_D8_anchor_order": anchor_order_valid,
            }

            exclusion = None
            for gate, passed in gate_results.items():
                if not passed:
                    exclusion = _gate_to_reason(gate)
                    break

            _log_discovery(self._wl_manager, sym, exclusion, exclusion is None,
                           pump_pct=pump_pct, peak_age_h=peak_age_h,
                           room_pct=room_pct, anchor_order_valid=anchor_order_valid,
                           base_validity_flag=True)

            if exclusion:
                continue

            case = {
                "symbol": sym,
                "case_state": "DISCOVERED",
                "base_detection_method": base.get("base_detection_method"),
                "base_window_start_ts": base.get("base_window_start_ts"),
                "base_window_end_ts": base.get("base_window_end_ts"),
                "base_price_median": base_price_median,
                "base_range_pct": base.get("base_range_pct"),
                "base_trending_flag": base.get("base_trending_flag"),
                "base_validity_flag": True,
                "base_invalid_reason": None,
                "peak_high": peak_high,
                "peak_time": p1_ts,
                "peak_age_h_at_creation": peak_age_h,
                "pump_pct": pump_pct,
                "pump_vol_ratio": pump_vol_ratio,
                "pump_speed_h": pump.get("pump_speed_h"),
                "pre_pump_low": pump.get("pre_pump_low"),
                "peak_vol": bars_1h[base.get("peak_idx", 0)]["volume"] if base.get("peak_idx") is not None else None,
                "room_pct": room_pct,
                "current_price_at_scan": current_price,
                "target_conservative": base_price_median + 0.5 * (peak_high - base_price_median) if peak_high else None,
                "target_extreme": base_price_median,
                "p0_ts": p0_ts,
                "p0_price": base_price_median,
                "p1_ts": p1_ts,
                "p1_price": peak_high,
                "p2_ts": "not_reached_yet",
                "p2_price": "not_reached_yet",
                "p3_ts": "not_reached_yet",
                "p3_price": "not_reached_yet",
                "p4_ts": "not_reached_yet",
                "p4_price": "not_reached_yet",
                "anchor_quality_flag": "PENDING",
                "anchor_order_valid": anchor_order_valid,
                "data_fetch_error": None,
                "outcome_status": "not_reached_yet",
            }

            ok = self._wl_manager.add_case(case)
            if ok:
                added += 1
                print(f"[PumpDiscovery] Added {sym}: pump={pump_pct:.1%}, room={room_pct:.1%}, peak_age={peak_age_h:.1f}h")
                if len(self._wl_manager.get_active_cases()) >= max_watchlist:
                    print(f"[PumpDiscovery] Watchlist full ({max_watchlist}), stopping scan.")
                    break

        self._wl_manager.flush()
        self._wl_manager.sync_csvs()
        print(f"[PumpDiscovery] Done. Processed={processed}, Added={added}")


def _gate_to_reason(gate: str) -> str:
    mapping = {
        "G_D1_base_valid": "base_invalid",
        "G_D2_pump_pct": "insufficient_data",
        "G_D3_pump_vol": "insufficient_data",
        "G_D4_peak_age": "peak_too_old",
        "G_D5_room": "room_too_small",
        "G_D8_anchor_order": "invalid_anchor_order",
    }
    return mapping.get(gate, "insufficient_data")


def _log_discovery(wl_manager: WatchlistManager, symbol: str,
                   exclusion_reason, added: bool, **kwargs):
    row = {
        "discovery_ts": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
        "symbol": symbol,
        "pump_pct": kwargs.get("pump_pct", ""),
        "peak_age_h": kwargs.get("peak_age_h", ""),
        "room_pct": kwargs.get("room_pct", ""),
        "quote_vol_24h": "",
        "listing_age_days": "",
        "base_validity_flag": kwargs.get("base_validity_flag", ""),
        "anchor_order_valid": kwargs.get("anchor_order_valid", ""),
        "exclusion_reason": exclusion_reason or "",
        "added_to_watchlist": added,
    }
    wl_manager.log_discovery(row)
