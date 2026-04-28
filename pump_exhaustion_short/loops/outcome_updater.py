import time
import threading
from typing import Dict, List, Optional

from pump_exhaustion_short.shared.r2_binance_client import R2BinanceClient
from pump_exhaustion_short.watchlist.watchlist_manager import WatchlistManager


def run_outcome_loop(client: R2BinanceClient,
                     wl_manager: WatchlistManager,
                     cfg: Dict,
                     stop_event: threading.Event):
    interval_sec = cfg["runtime"]["outcome_check_interval_minutes"] * 60
    print(f"[Outcome] Loop started. Interval: {interval_sec}s")
    while not stop_event.is_set():
        stop_event.wait(timeout=interval_sec)
        if stop_event.is_set():
            break
        _safe_run(client, wl_manager, cfg)
    print("[Outcome] Loop stopped.")


def _safe_run(client, wl_manager, cfg):
    try:
        run_outcome_once(client, wl_manager, cfg)
    except Exception as e:
        import traceback
        print(f"[Outcome] run_once error: {e}")
        traceback.print_exc()


def run_outcome_once(client: R2BinanceClient,
                     wl_manager: WatchlistManager,
                     cfg: Dict):
    now_ms = int(time.time() * 1000)
    cases = wl_manager.get_all_cases()
    updated = 0

    for case in cases:
        state = case.get("case_state", "")
        outcome_status = case.get("outcome_status", "")

        if state not in ("OUTCOME_PENDING", "CLOSED_1H_READY"):
            continue
        if outcome_status not in ("pending", "1h_partial"):
            continue

        entry_time = case.get("p3_ts")  # retest candle time = entry time
        if not entry_time:
            continue

        try:
            entry_time_ms = int(entry_time)
        except (ValueError, TypeError):
            continue

        elapsed_h = (now_ms - entry_time_ms) / 3_600_000

        # Fetch 5m klines from entry time
        bars = client.get_klines(case["symbol"], "5m", limit=200,
                                  start_ms=entry_time_ms)
        if not bars:
            continue

        entry_price = case.get("entry_price")
        breakdown_level = case.get("breakdown_level")
        if not entry_price:
            continue

        updates: Dict = {}

        # 1h outcome
        if elapsed_h >= 1.0 and outcome_status == "pending":
            bars_1h = [b for b in bars if b["open_time"] <= entry_time_ms + 3_600_000]
            if bars_1h:
                favor_1h = _max_favor(bars_1h, float(entry_price), side="short")
                adverse_1h = _max_adverse(bars_1h, float(entry_price), side="short")
                reclaim = _check_reclaim(bars_1h, breakdown_level) if breakdown_level else False
                entry_feasible = _check_entry_feasible(bars, float(entry_price))
                updates.update({
                    "future_1h_max_favor_pct": favor_1h,
                    "future_1h_max_adverse_pct": adverse_1h,
                    "reclaim_breakdown_flag": reclaim,
                    "entry_feasible_flag": entry_feasible,
                    "outcome_status": "1h_partial",
                    "case_state": "CLOSED_1H_READY",
                })
                updated += 1

        # 4h outcome — check both stored status and what was just set in this run
        effective_status = updates.get("outcome_status", outcome_status)
        if elapsed_h >= 4.0 and effective_status in ("1h_partial", "pending"):
            bars_4h = [b for b in bars if b["open_time"] <= entry_time_ms + 4 * 3_600_000]
            if bars_4h:
                favor_4h = _max_favor(bars_4h, float(entry_price), side="short")
                adverse_4h = _max_adverse(bars_4h, float(entry_price), side="short")
                reclaim_4h = _check_reclaim(bars_4h, breakdown_level) if breakdown_level else False
                resolution = _classify_resolution(
                    f1h=updates.get("future_1h_max_favor_pct") or case.get("future_1h_max_favor_pct") or 0,
                    a1h=updates.get("future_1h_max_adverse_pct") or case.get("future_1h_max_adverse_pct") or 0,
                    f4h=favor_4h,
                    a4h=adverse_4h,
                    reclaim_1h=updates.get("reclaim_breakdown_flag", False),
                    reclaim_4h=reclaim_4h,
                )
                p4_ts = bars_4h[-1]["open_time"] if bars_4h else "not_reached_yet"
                p4_price = bars_4h[-1]["close"] if bars_4h else "not_reached_yet"
                updates.update({
                    "future_4h_max_favor_pct": favor_4h,
                    "future_4h_max_adverse_pct": adverse_4h,
                    "reclaim_breakdown_flag": reclaim_4h,
                    "resolution_label": resolution["label"],
                    "resolution_reason_code": resolution["reason"],
                    "p4_ts": p4_ts,
                    "p4_price": p4_price,
                    "outcome_status": "4h_ready",
                    "case_state": "CLOSED_4H_READY",
                })
                updated += 1

        if updates:
            wl_manager.update_case(case["case_id"], updates)

    if updated:
        wl_manager.flush()
        wl_manager.sync_csvs()
        print(f"[Outcome] Updated {updated} cases.")


def _max_favor(bars: List[Dict], entry_price: float, side: str = "short") -> float:
    """Max favorable move as pct from entry."""
    if not bars or entry_price <= 0:
        return 0.0
    if side == "short":
        min_low = min(b["low"] for b in bars)
        return (entry_price - min_low) / entry_price
    else:
        max_high = max(b["high"] for b in bars)
        return (max_high - entry_price) / entry_price


def _max_adverse(bars: List[Dict], entry_price: float, side: str = "short") -> float:
    """Max adverse move as pct from entry."""
    if not bars or entry_price <= 0:
        return 0.0
    if side == "short":
        max_high = max(b["high"] for b in bars)
        return (max_high - entry_price) / entry_price
    else:
        min_low = min(b["low"] for b in bars)
        return (entry_price - min_low) / entry_price


def _check_reclaim(bars: List[Dict], breakdown_level) -> bool:
    try:
        level = float(breakdown_level)
    except (TypeError, ValueError):
        return False
    return any(b["close"] > level for b in bars)


def _check_entry_feasible(bars: List[Dict], entry_price: float) -> bool:
    """Entry feasible if first 2 candles have low <= entry_price."""
    first2 = bars[:2]
    return any(b["low"] <= entry_price for b in first2)


def _classify_resolution(f1h: float, a1h: float, f4h: float, a4h: float,
                          reclaim_1h: bool, reclaim_4h: bool) -> Dict:
    if reclaim_1h:
        return {"label": "fake_break_fast_reclaim", "reason": "price reclaimed breakdown in 1h"}
    if reclaim_4h:
        return {"label": "invalidated_above_breakdown", "reason": "price closed above breakdown in 4h"}
    if f4h >= 0.03 and a4h < 0.01:
        return {"label": "clean_short_followthrough", "reason": "strong continuation, low adverse"}
    if f4h >= 0.015 and f1h < 0.015:
        return {"label": "late_short_low_edge", "reason": "favor came late, edge questionable"}
    if 0 < f4h < 0.015 and a4h < 0.015:
        return {"label": "slow_bleed_not_clean", "reason": "slow move, no clean trend"}
    if f4h < 0.01 and a4h < 0.01:
        return {"label": "no_meaningful_move", "reason": "no significant directional move"}
    return {"label": "slow_bleed_not_clean", "reason": "mixed outcome"}
