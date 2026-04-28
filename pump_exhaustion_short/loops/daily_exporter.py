import time
import threading
from typing import Dict

from pump_exhaustion_short.watchlist.watchlist_manager import WatchlistManager


def run_daily_export_loop(wl_manager: WatchlistManager,
                          cfg: Dict,
                          stop_event: threading.Event):
    print("[Exporter] Daily export loop started.")
    while not stop_event.is_set():
        seconds_until_midnight = _seconds_until_utc_midnight()
        stop_event.wait(timeout=min(seconds_until_midnight, 300))
        if stop_event.is_set():
            break
        if _is_near_midnight():
            _safe_export(wl_manager)
    print("[Exporter] Loop stopped.")


def _safe_export(wl_manager):
    try:
        run_daily_export_once(wl_manager)
    except Exception as e:
        import traceback
        print(f"[Exporter] error: {e}")
        traceback.print_exc()


def run_daily_export_once(wl_manager: WatchlistManager):
    now = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
    all_cases = wl_manager.get_all_cases()

    counts = {
        "total": len(all_cases),
        "discovered": 0,
        "breakdown_confirmed": 0,
        "retest_waiting": 0,
        "failed_retest": 0,
        "outcome_pending": 0,
        "closed_1h": 0,
        "closed_4h": 0,
        "excluded": 0,
        "straight_dump": 0,
    }

    state_map = {
        "DISCOVERED": "discovered",
        "BREAKDOWN_CONFIRMED": "breakdown_confirmed",
        "RETEST_WAITING": "retest_waiting",
        "FAILED_RETEST_CONFIRMED": "failed_retest",
        "OUTCOME_PENDING": "outcome_pending",
        "CLOSED_1H_READY": "closed_1h",
        "CLOSED_4H_READY": "closed_4h",
        "EXCLUDED": "excluded",
        "WATCHLIST_ONLY_STRAIGHT_DUMP": "straight_dump",
    }

    for case in all_cases:
        state = case.get("case_state", "EXCLUDED")
        key = state_map.get(state, "excluded")
        counts[key] = counts.get(key, 0) + 1

    print(f"\n[Exporter] Daily snapshot at {now}")
    print(f"  Total cases: {counts['total']}")
    print(f"  Active: discovered={counts['discovered']}, "
          f"breakdown={counts['breakdown_confirmed']}, "
          f"retest_wait={counts['retest_waiting']}, "
          f"failed_retest={counts['failed_retest']}")
    print(f"  Outcome: pending={counts['outcome_pending']}, "
          f"1h_ready={counts['closed_1h']}, 4h_ready={counts['closed_4h']}")
    print(f"  Excluded={counts['excluded']}, straight_dump={counts['straight_dump']}")

    wl_manager.sync_csvs()
    print(f"[Exporter] CSVs synced.")


def _seconds_until_utc_midnight() -> float:
    now = time.gmtime()
    seconds_today = now.tm_hour * 3600 + now.tm_min * 60 + now.tm_sec
    return max(86400 - seconds_today, 60)


def _is_near_midnight() -> bool:
    now = time.gmtime()
    return now.tm_hour == 0 and now.tm_min < 5
