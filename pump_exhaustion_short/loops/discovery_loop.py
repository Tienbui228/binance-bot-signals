import time
import threading
from typing import Dict, List, Optional

from pump_exhaustion_short.shared.r2_binance_client import R2BinanceClient
from pump_exhaustion_short.shared.market_cap_cache import MarketCapCache
from pump_exhaustion_short.watchlist.watchlist_manager import WatchlistManager
from pump_exhaustion_short.detectors.base_detector import detect_base
from pump_exhaustion_short.detectors.pump_detector import detect_pump


def run_discovery_loop(client: R2BinanceClient,
                       wl_manager: WatchlistManager,
                       cfg: Dict,
                       stop_event: threading.Event):
    interval_sec = cfg["runtime"]["discovery_interval_minutes"] * 60
    mc_cfg = cfg.get("market_cap_filter", {})
    mc_cache = MarketCapCache(mc_cfg) if mc_cfg.get("enabled", False) else None
    print(f"[Discovery] Loop started. Interval: {interval_sec}s")
    # Run immediately on start
    _safe_run(client, wl_manager, cfg, None, mc_cache)
    while not stop_event.is_set():
        stop_event.wait(timeout=interval_sec)
        if stop_event.is_set():
            break
        _safe_run(client, wl_manager, cfg, None, mc_cache)
    print("[Discovery] Loop stopped.")


def _safe_run(client, wl_manager, cfg, limit_symbols, mc_cache=None):
    try:
        run_discovery_once(client, wl_manager, cfg, limit_symbols, mc_cache)
    except Exception as e:
        import traceback
        print(f"[Discovery] run_once error: {e}")
        traceback.print_exc()


def run_discovery_once(client: R2BinanceClient,
                       wl_manager: WatchlistManager,
                       cfg: Dict,
                       limit_symbols: Optional[int] = None,
                       mc_cache: Optional[MarketCapCache] = None):
    disc_cfg = cfg["discovery"]
    now_ms = int(time.time() * 1000)

    print(f"[Discovery] Starting scan at {time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())}")

    # Step 1: Fetch all USDT perpetuals
    symbols_info = client.get_all_usdt_perpetuals()
    if not symbols_info:
        print("[Discovery] No symbols fetched, skipping.")
        return

    # Step 2: Fetch all 24h tickers
    tickers = client.get_all_tickers_24h()
    ticker_map: Dict[str, Dict] = {t["symbol"]: t for t in tickers}

    # Step 3: Filter by liquidity + listing age
    min_vol = disc_cfg["quote_vol_24h_min_usdt"]
    min_listing_days = disc_cfg["listing_age_min_days"]
    eligible = []
    for info in symbols_info:
        sym = info["symbol"]
        tick = ticker_map.get(sym, {})
        quote_vol = tick.get("quoteVolume", 0.0)
        if quote_vol < min_vol:
            continue
        listing_age_days = (now_ms - info["onboard_ts_ms"]) / 86_400_000 if info["onboard_ts_ms"] > 0 else 999
        if listing_age_days < min_listing_days:
            continue
        eligible.append({**info, "quote_vol_24h": quote_vol,
                          "listing_age_days": listing_age_days,
                          "last_price": tick.get("lastPrice", 0.0)})

    # Apply limit_symbols for testing
    if limit_symbols is not None:
        eligible = eligible[:limit_symbols]

    max_watchlist = cfg.get("watchlist", {}).get("max_symbols", 50)
    mc_cfg = cfg.get("market_cap_filter", {})
    max_cap = mc_cfg.get("max_market_cap_usd", 500_000_000)
    added = 0
    processed = 0

    # Refresh market cap cache once per discovery run (not per symbol)
    _mc = mc_cache
    if _mc is not None:
        _mc.refresh_if_stale()
        if not _mc.has_data():
            print("[Discovery] MarketCapCache has no data — skipping G_D0 this run")
            _mc = None

    # Skip symbols already active in watchlist
    active_symbols = {c["symbol"] for c in wl_manager.get_active_cases()}

    for info in eligible:
        sym = info["symbol"]
        processed += 1

        # Skip if already active (non-EXCLUDED)
        if sym in active_symbols:
            continue

        # G_D0: market cap gate — checked before fetching klines to save API calls
        if _mc is not None:
            cap_ok, mc_reason = _mc.is_eligible(sym, max_cap)
            if not cap_ok:
                _log_discovery(wl_manager, sym, info, mc_reason, False)
                continue

        # Fetch 1h klines
        bars_1h = client.get_klines(sym, "1h", disc_cfg["peak_window_candles"])
        if len(bars_1h) < 20:
            _log_discovery(wl_manager, sym, info, "insufficient_data", False)
            continue

        # Detect base
        base = detect_base(bars_1h, cfg)
        if not base.get("base_validity_flag"):
            reason = base.get("base_invalid_reason", "base_invalid")
            _log_discovery(wl_manager, sym, info, reason, False,
                           base_validity_flag=False)
            continue

        # Detect pump
        pump = detect_pump(bars_1h, base)

        peak_age_h = pump.get("peak_age_h", 9999.0)
        pump_pct = pump.get("pump_pct", 0.0)
        pump_vol_ratio = pump.get("pump_vol_ratio", 0.0)

        # Current price for room calc
        current_price = bars_1h[-1]["close"] if bars_1h else 0.0
        base_price_median = base.get("base_price_median", 0.0)
        peak_high = base.get("peak_high", 0.0)
        room_pct = (current_price - base_price_median) / base_price_median if base_price_median > 0 else 0.0

        # anchor_order_valid at discovery: p0_ts < p1_ts
        p0_ts = base.get("base_window_end_ts")
        p1_ts = base.get("peak_time")
        anchor_order_valid = bool(p0_ts and p1_ts and p0_ts < p1_ts)

        # --- Discovery gates G_D1-G_D8 ---
        gate_results = {
            "G_D1_base_valid": base.get("base_validity_flag", False),
            "G_D2_pump_pct": pump_pct >= disc_cfg["pump_pct_min"],
            "G_D3_pump_vol": pump_vol_ratio >= disc_cfg["pump_vol_ratio_min"],
            "G_D4_peak_age": peak_age_h <= disc_cfg["peak_age_max_hours"],
            "G_D5_room": room_pct >= disc_cfg["room_pct_min"],
            "G_D6_liquidity": info["quote_vol_24h"] >= disc_cfg["quote_vol_24h_min_usdt"],
            "G_D7_listing_age": info["listing_age_days"] >= disc_cfg["listing_age_min_days"],
            "G_D8_anchor_order": anchor_order_valid,
        }

        exclusion = None
        for gate, passed in gate_results.items():
            if not passed:
                exclusion = _gate_to_reason(gate)
                break

        _log_discovery(wl_manager, sym, info, exclusion, exclusion is None,
                       pump_pct=pump_pct, peak_age_h=peak_age_h,
                       room_pct=room_pct, anchor_order_valid=anchor_order_valid,
                       base_validity_flag=True)

        if exclusion:
            continue

        # Build case row
        case_id = f"{sym}_{int(time.time() * 1000)}"
        case = {
            "case_id": case_id,
            "symbol": sym,
            "case_state": "DISCOVERED",
            # Base
            "base_detection_method": base.get("base_detection_method"),
            "base_window_start_ts": base.get("base_window_start_ts"),
            "base_window_end_ts": base.get("base_window_end_ts"),
            "base_price_median": base_price_median,
            "base_range_pct": base.get("base_range_pct"),
            "base_trending_flag": base.get("base_trending_flag"),
            "base_validity_flag": True,
            "base_invalid_reason": None,
            # Pump
            "peak_high": peak_high,
            "peak_time": p1_ts,
            "peak_age_h_at_creation": peak_age_h,
            "pump_pct": pump_pct,
            "pump_vol_ratio": pump_vol_ratio,
            "pump_speed_h": pump.get("pump_speed_h"),
            "pre_pump_low": pump.get("pre_pump_low"),
            "peak_vol": bars_1h[base.get("peak_idx", 0)]["volume"] if base.get("peak_idx") is not None else None,
            # Room
            "room_pct": room_pct,
            "current_price_at_scan": current_price,
            "target_conservative": base_price_median + 0.5 * (peak_high - base_price_median) if peak_high else None,
            "target_extreme": base_price_median,
            # Anchors (at creation)
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
            # Validity
            "anchor_order_valid": anchor_order_valid,
            "data_fetch_error": None,
            # Outcome
            "outcome_status": "not_reached_yet",
        }

        ok = wl_manager.add_case(case)
        if ok:
            added += 1
            print(f"[Discovery] Added {sym}: pump={pump_pct:.1%}, room={room_pct:.1%}, peak_age={peak_age_h:.1f}h")
            if wl_manager.get_active_cases().__len__() >= max_watchlist:
                print(f"[Discovery] Watchlist full ({max_watchlist}), stopping scan.")
                break

    wl_manager.flush()
    wl_manager.sync_csvs()
    print(f"[Discovery] Done. Processed={processed}, Added={added}")


def _gate_to_reason(gate: str) -> str:
    mapping = {
        "G_D1_base_valid": "base_invalid",
        "G_D2_pump_pct": "insufficient_data",
        "G_D3_pump_vol": "insufficient_data",
        "G_D4_peak_age": "peak_too_old",
        "G_D5_room": "room_too_small",
        "G_D6_liquidity": "liquidity_too_low",
        "G_D7_listing_age": "liquidity_too_low",
        "G_D8_anchor_order": "invalid_anchor_order",
    }
    return mapping.get(gate, "insufficient_data")


def _log_discovery(wl_manager: WatchlistManager, symbol: str, info: Dict,
                   exclusion_reason, added: bool, **kwargs):
    row = {
        "discovery_ts": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
        "symbol": symbol,
        "pump_pct": kwargs.get("pump_pct", ""),
        "peak_age_h": kwargs.get("peak_age_h", ""),
        "room_pct": kwargs.get("room_pct", ""),
        "quote_vol_24h": info.get("quote_vol_24h", ""),
        "listing_age_days": info.get("listing_age_days", ""),
        "base_validity_flag": kwargs.get("base_validity_flag", ""),
        "anchor_order_valid": kwargs.get("anchor_order_valid", ""),
        "exclusion_reason": exclusion_reason or "",
        "added_to_watchlist": added,
    }
    wl_manager.log_discovery(row)
