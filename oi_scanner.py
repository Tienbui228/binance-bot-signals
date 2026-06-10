import sys
import os
import fcntl
import argparse
import time
import csv
import re
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import requests
import yaml

from scanner.dispatch.router import route_dispatch_v1
from scanner.regime.classifier import classify_regime
from regime.regime_normalizer import enrich_row_with_regime

BASE_FAPI = "https://fapi.binance.com"
BASE_BYBIT = "https://api.bybit.com"
HEADERS = {"User-Agent": "Mozilla/5.0"}

CODE_BUILD_ID = "geometry-to-warning-tag-2026-06-10"
CODE_BUILD_SOURCE = "cleanup-round4"
CODE_BUILD_NOTE = "Round 4: removed legacy_5m_retest + infer_legacy_strategy default. setup_id join fix (pending<->signal). Active strategies: long_accumulation_continuation (live), oi_range_breakout (ready, disabled)."

VALID_PENDING_STATUSES = {
    "PENDING",
    "CONFIRMED",
    "INVALIDATED",
    "EXPIRED_WAIT",
    "REJECTED_SCORE",
    "REJECTED_RULE",
    "SKIPPED_SEND",
}


@dataclass
class Signal:
    signal_id: str
    timestamp_ms: int
    symbol: str
    side: str
    score: float
    confidence: float
    reason: str

    breakout_level: float
    entry_low: float
    entry_high: float
    entry_ref: float
    stop: float
    tp1: float
    tp2: float

    price: float
    oi_jump_pct: float
    funding_pct: float
    vol_ratio: float
    retest_bars_waited: int

    setup_id: str = ""
    config_version: str = ""
    strategy: str = ""
    market_regime: str = "unknown"
    btc_4h_change_pct: float = 0.0
    btc_1h_change_pct: float = 0.0
    btc_24h_range_pct: float = 0.0
    btc_4h_range_pct: float = 0.0
    alt_market_breadth_pct: float = 0.0
    btc_price: float = 0.0
    btc_24h_change_pct: float = 0.0
    btc_regime: str = "unknown"
    risk_pct_real: float = 0.0
    sl_distance_pct: float = 0.0
    tp1_distance_pct: float = 0.0
    tp2_distance_pct: float = 0.0
    break_distance_pct: float = 0.0
    retest_depth_pct: float = 0.0
    score_oi: float = 0.0
    score_exhaustion: float = 0.0
    score_breakout: float = 0.0
    score_retest: float = 0.0
    reason_tags: str = ""
    stop_was_forced_min_risk: str = "no"
    manual_tradable: str = "yes"
    manual_trade_note: str = "good_for_manual"
    regime_label: str = "unclear_mixed"
    regime_fit_for_strategy: str = "MEDIUM"
    dispatch_action: str = "not_evaluated"
    dispatch_confidence_band: str = "not_evaluated"
    dispatch_reason: str = "not_evaluated"
    status: str = "OPEN"
    # oi_range_breakout specific fields (optional, default 0.0)
    range_high: float = 0.0
    range_low: float = 0.0
    range_height: float = 0.0
    range_width_pct: float = 0.0
    midpoint: float = 0.0
    atr_current: float = 0.0
    atr_avg: float = 0.0
    atr_ratio: float = 0.0
    oi_current: float = 0.0
    oi_prev: float = 0.0
    oi_delta_pct: float = 0.0
    vol_ema20: float = 0.0
    vol_24h_usdt: float = 0.0
    cross_exchange_confirmed: str = "N"   # "Y"/"N" — Binance + Bybit both confirmed
    bybit_vol_24h_usdt: float = 0.0
    oi_delta_abs_1h: float = 0.0          # OI absolute USDT delta 60min, informational only
    # long_accumulation_continuation specific fields
    setup_quality_band: str = "not_evaluated"
    accumulation_score: int = 0
    pos_now: float = 0.0
    acct_now: float = 0.0
    gap_now: float = 0.0
    retail_now: float = 0.0
    taker_now: float = 0.0
    pos_slope_30d: float = 0.0
    gap_slope_30d: float = 0.0
    retail_slope_30d: float = 0.0
    pos_trend_3v14: float = 0.0
    oi_trend_3v14: float = 0.0
    pos_min_30d: float = 0.0
    pos_recovery_from_min: float = 0.0
    retail_min_30d: float = 0.0
    retail_recovery: float = 0.0
    taker_7d_avg: float = 0.0
    market_cap_usd: float = 0.0


@dataclass
class PendingSetup:
    pending_id: str
    created_ts_ms: int
    signal_open_time: int
    symbol: str
    side: str
    score: float
    confidence: float
    reason: str
    breakout_level: float
    signal_price: float
    signal_high: float
    signal_low: float
    oi_jump_pct: float
    funding_pct: float
    vol_ratio: float
    setup_id: str = ""
    strategy: str = ""
    market_regime: str = "unknown"
    btc_4h_change_pct: float = 0.0
    btc_1h_change_pct: float = 0.0
    btc_24h_range_pct: float = 0.0
    btc_4h_range_pct: float = 0.0
    alt_market_breadth_pct: float = 0.0
    btc_price: float = 0.0
    btc_24h_change_pct: float = 0.0
    btc_regime: str = "unknown"
    score_oi: float = 0.0
    score_exhaustion: float = 0.0
    score_breakout: float = 0.0
    score_retest: float = 0.0
    reason_tags: str = ""
    status: str = "PENDING"
    close_reason: str = ""
    bars_waited: int = 0
    closed_ts_ms: int = 0
    regime_label: str = "unknown"
    regime_fit_for_strategy: str = "not_evaluated"
    setup_quality_band: str = "not_evaluated"
    delivery_band: str = "not_evaluated"
    veto_reason_code: str = "not_evaluated"
    dispatch_action: str = "not_evaluated"
    dispatch_confidence_band: str = "not_evaluated"
    dispatch_reason: str = "not_evaluated"
    entry_price: float = 0.0
    stop_loss: float = 0.0
    tp1: float = 0.0
    tp2: float = 0.0
    # ORB-specific fields (only populated for oi_range_breakout strategy)
    range_high: float = 0.0
    range_low: float = 0.0
    range_height: float = 0.0
    range_width_pct: float = 0.0
    midpoint: float = 0.0
    atr_current: float = 0.0
    atr_avg: float = 0.0
    atr_ratio: float = 0.0
    oi_current: float = 0.0
    oi_prev: float = 0.0
    oi_delta_pct: float = 0.0
    vol_ema20: float = 0.0
    cross_exchange_confirmed: str = "N"   # "Y"/"N"
    bybit_oi_delta_pct: float = 0.0
    bybit_vol_ok: str = "N"              # "Y"/"N"
    oi_delta_abs_1h: float = 0.0
    # long_accumulation_continuation specific fields
    accumulation_score: int = 0
    pos_now: float = 0.0
    acct_now: float = 0.0
    gap_now: float = 0.0
    retail_now: float = 0.0
    taker_now: float = 0.0
    pos_slope_30d: float = 0.0
    gap_slope_30d: float = 0.0
    retail_slope_30d: float = 0.0
    pos_trend_3v14: float = 0.0
    oi_trend_3v14: float = 0.0
    pos_min_30d: float = 0.0
    pos_recovery_from_min: float = 0.0
    retail_min_30d: float = 0.0
    retail_recovery: float = 0.0
    taker_7d_avg: float = 0.0
    # v2.0.1 gate fields
    price_vs_baseline: float = 0.0
    price_trend_7v30: float = 0.0
    funding_8h_pct: float = 0.0


_ORB_REGIME_MULT = {
    "trend_continuation_friendly":  1.20,
    "unclear_mixed":                1.00,
    "chop_fake_breakout_heavy":     0.75,
    "broad_weakness_sell_pressure": 0.55,
    "post_pump_exhaustion":         0.60,
}


def _orb_atr_bonus(atr_ratio: float, threshold: float) -> float:
    if atr_ratio < 0.7:
        return 0.25
    if atr_ratio < threshold:
        return 0.10
    return 0.00


import logging as _logging
_logger_bybit = _logging.getLogger("bybit")


class BinanceScanner:
    def __init__(self, config: Dict):
        self.cfg = config
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.sent_cache: Dict[str, float] = {}
        self.round_idx = 0
        self.current_market_snapshot: Dict[str, float] = {}
        self.current_regime = None
        self._orb_sent_today: set = set()   # symbols sent to Telegram today, reset daily
        self._orb_sent_today_date: str = ""  # "YYYY-MM-DD" of last reset
        self._acc_sent_today: set = set()   # acc_cont symbols sent today, reset daily
        self._acc_sent_today_date: str = ""  # "YYYY-MM-DD" of last reset

        self.project_dir = Path(__file__).resolve().parent
        storage_cfg = self.cfg.get("storage", {})
        self.data_dir = self.project_dir / str(storage_cfg.get("data_dir", "data"))
        self.pending_dir = self.data_dir / str(storage_cfg.get("pending_dir", "pending"))
        self.signals_dir = self.data_dir / str(storage_cfg.get("signals_dir", "signals"))
        self.results_dir = self.data_dir / str(storage_cfg.get("results_dir", "results"))
        self.include_legacy_flat_files = bool(storage_cfg.get("include_legacy_flat_files", False))

        self.signals_file = self.project_dir / "signals.csv"
        self.results_file = self.project_dir / "results.csv"
        self.pending_file = self.project_dir / "pending_setups.csv"
        self._top_marketcap_exclude: set = set()
        self._top_marketcap_fetched_ts: float = 0.0
        self._tt_cache: dict = {}
        self._tt_cache_ttl: int = 3600

        self.signal_fields = [
            "signal_id", "setup_id", "timestamp_ms", "symbol", "side", "score", "confidence",
            "reason", "breakout_level", "entry_low", "entry_high", "entry_ref",
            "stop", "tp1", "tp2", "price", "oi_jump_pct", "funding_pct",
            "vol_ratio", "retest_bars_waited", "config_version", "strategy", "market_regime",
            "btc_price", "btc_24h_change_pct", "btc_4h_change_pct", "btc_1h_change_pct",
            "btc_24h_range_pct", "btc_4h_range_pct", "alt_market_breadth_pct", "btc_regime",
            "risk_pct_real", "sl_distance_pct", "tp1_distance_pct", "tp2_distance_pct", "break_distance_pct",
            "retest_depth_pct", "score_oi", "score_exhaustion", "score_breakout", "score_retest",
            "reason_tags", "stop_was_forced_min_risk", "manual_tradable", "manual_trade_note",
            "regime_label", "regime_fit_for_strategy",
            "dispatch_action", "dispatch_confidence_band", "dispatch_reason", "status",
            # oi_range_breakout specific fields (were missing from signal_fields)
            "range_high", "range_low", "range_height", "range_width_pct", "midpoint",
            "atr_current", "atr_avg", "atr_ratio",
            "oi_current", "oi_prev", "oi_delta_pct", "vol_ema20", "vol_24h_usdt",
            "cross_exchange_confirmed", "bybit_vol_24h_usdt",
            "oi_delta_abs_1h",
            # long_accumulation_continuation specific fields
            "setup_quality_band", "accumulation_score",
            "pos_now", "acct_now", "gap_now", "retail_now", "taker_now",
            "pos_slope_30d", "gap_slope_30d", "retail_slope_30d",
            "pos_trend_3v14", "oi_trend_3v14",
            "pos_min_30d", "pos_recovery_from_min",
            "retail_min_30d", "retail_recovery", "taker_7d_avg",
            "market_cap_usd",
            # v2.0.1 gate fields
            "price_vs_baseline", "price_trend_7v30", "funding_8h_pct",
        ]
        self.result_fields = [
            "signal_id", "setup_id", "timestamp_ms", "symbol", "side", "entry_ref", "stop",
            "tp1", "tp2", "outcome", "r_multiple", "bars_checked",
            "close_time_ms", "close_reason", "config_version", "strategy", "market_regime", "btc_price",
            "btc_24h_change_pct", "btc_4h_change_pct", "btc_1h_change_pct", "btc_24h_range_pct",
            "btc_4h_range_pct", "alt_market_breadth_pct", "btc_regime", "risk_pct_real", "sl_distance_pct",
            "tp1_distance_pct", "tp2_distance_pct", "break_distance_pct",
            "retest_depth_pct", "score_oi", "score_exhaustion", "score_breakout", "score_retest",
            "reason_tags", "stop_was_forced_min_risk", "mfe_pct", "mae_pct", "manual_tradable", "manual_trade_note"
        ]
        self.pending_fields = [
            "pending_id", "setup_id", "created_ts_ms", "signal_open_time", "symbol", "side",
            "score", "confidence", "reason", "breakout_level", "signal_price",
            "signal_high", "signal_low", "oi_jump_pct", "funding_pct", "vol_ratio",
            "strategy", "market_regime", "btc_price", "btc_24h_change_pct", "btc_4h_change_pct",
            "btc_1h_change_pct", "btc_24h_range_pct", "btc_4h_range_pct", "alt_market_breadth_pct",
            "btc_regime", "score_oi", "score_exhaustion", "score_breakout", "score_retest", "reason_tags", "status",
            "close_reason", "bars_waited", "closed_ts_ms", "send_decision", "skip_reason",
            "is_confirmed", "confirmed_ts_ms", "is_sent_signal", "sent_ts_ms",
            "review_eligible", "review_exclusion_reason", "semantic_consistency", "semantic_issue",
            "close_anchor_time_ms", "close_capture_basis",
            "future_1h_max_favor_pct", "future_1h_max_adverse_pct",
            "future_2h_max_favor_pct", "future_2h_max_adverse_pct",
            "future_4h_max_favor_pct", "future_4h_max_adverse_pct",
            "reclaim_breakout_2h_YN", "reclaim_breakout_4h_YN",
            "outcome_1h_available", "outcome_2h_available", "outcome_1h_summary", "outcome_2h_summary",
            "post_close_outcome_notes", "outcome_conclusion_code",
            "entry_feasible_YN", "entry_feasible_window_minutes", "entry_slippage_pct", "entry_execution_note",
            "time_to_max_favor_minutes", "time_to_max_adverse_minutes",
            "close_trigger_detail", "confirm_fail_detail", "invalidation_detail",
            "regret_valid_YN", "regret_filter_reason",
            "regime_label", "regime_fit_for_strategy", "setup_quality_band",
            "delivery_band", "veto_reason_code", "dispatch_action", "dispatch_confidence_band", "dispatch_reason",
            # oi_range_breakout specific fields
            "entry_price", "stop_loss", "tp1", "tp2",
            "range_high", "range_low", "range_height", "range_width_pct", "midpoint",
            "atr_current", "atr_avg", "atr_ratio",
            "oi_current", "oi_prev", "oi_delta_pct", "oi_delta_abs_1h", "vol_ema20",
            "cross_exchange_confirmed", "bybit_oi_delta_pct", "bybit_vol_ok",
            # long_accumulation_continuation specific fields
            "accumulation_score",
            "pos_now", "acct_now", "gap_now", "retail_now", "taker_now",
            "pos_slope_30d", "gap_slope_30d", "retail_slope_30d",
            "pos_trend_3v14", "oi_trend_3v14",
            "pos_min_30d", "pos_recovery_from_min",
            "retail_min_30d", "retail_recovery", "taker_7d_avg",
            # v2.0.1 gate fields
            "price_vs_baseline", "price_trend_7v30", "funding_8h_pct",
        ]
        self.table_specs = {
            "pending": {"logical": self.pending_file, "dir": self.pending_dir, "fieldnames": self.pending_fields, "ts_col": "created_ts_ms", "granularity": "day"},
            "signals": {"logical": self.signals_file, "dir": self.signals_dir, "fieldnames": self.signal_fields, "ts_col": "timestamp_ms", "granularity": "month"},
            "results": {"logical": self.results_file, "dir": self.results_dir, "fieldnames": self.result_fields, "ts_col": "close_time_ms", "granularity": "month"},
        }

        self._ensure_storage_layout()

    def _normalize_regime_label_value(self, regime_label: str = "", market_regime: str = "", btc_regime: str = "") -> str:
        raw = str(regime_label or market_regime or btc_regime or "").strip()
        if not raw:
            return "unclear_mixed"
        lowered = raw.lower()
        if lowered in {"trend_continuation_friendly", "broad_weakness_sell_pressure", "unclear_mixed"}:
            return lowered
        if lowered in {"bullish", "bull", "up", "uptrend", "trend", "continuation", "range_bullish"}:
            return "trend_continuation_friendly"
        if lowered in {"bearish", "bear", "down", "downtrend", "sell_pressure"}:
            return "broad_weakness_sell_pressure"
        if lowered in {"neutral", "range", "mixed", "sideways", "sideway", "unknown"}:
            return "unclear_mixed"
        return "unclear_mixed"

    def _derive_regime_fit_for_strategy(self, strategy: str = "", side: str = "", regime_label: str = "unclear_mixed") -> str:
        label = self._normalize_regime_label_value(regime_label)
        strategy_l = str(strategy or "").lower()
        side_u = str(side or "").upper()
        is_short = side_u == "SHORT"
        if label == "trend_continuation_friendly":
            return "LOW" if is_short else "HIGH"
        if label == "broad_weakness_sell_pressure":
            return "HIGH" if is_short else "LOW"
        return "MEDIUM"

    def _apply_regime_trace_defaults(self, row: Dict, strategy_hint: str = "", side_hint: str = "") -> Dict:
        # Sprint 3A.2: delegate to canonical normalizer.
        # enrich_row_with_regime() checks fit explicitly against
        # {HIGH, MEDIUM, LOW} — correctly handles the truthy
        # 'not_evaluated' string that caused the original bug.
        # Covers all 3 write paths: save_pending, close_pending,
        # sync_pending_send_decision.
        return enrich_row_with_regime(
            row,
            strategy_family=strategy_hint or (row or {}).get("strategy", ""),
            side=side_hint or (row or {}).get("side", ""),
        )

    def _default_value_for_field(self, fieldname: str, row: Optional[Dict] = None) -> str:
        row = row or {}
        if fieldname == "regime_label":
            return self._normalize_regime_label_value(
                row.get("regime_label", ""),
                row.get("market_regime", ""),
                row.get("btc_regime", ""),
            )
        if fieldname == "regime_fit_for_strategy":
            return self._derive_regime_fit_for_strategy(
                row.get("strategy", ""),
                row.get("side", ""),
                row.get("regime_label", row.get("market_regime", row.get("btc_regime", "unclear_mixed"))),
            )
        if fieldname in {
            "setup_quality_band",
            "delivery_band",
            "veto_reason_code",
            "dispatch_action",
            "dispatch_confidence_band",
            "dispatch_reason",
        }:
            return "not_evaluated"
        return ""

    def _normalize_row_for_fields(self, row: Dict, fieldnames: List[str]) -> Dict:
        out = {}
        for k in fieldnames:
            v = row.get(k, "") if row is not None else ""
            if v is None or v == "":
                out[k] = self._default_value_for_field(k, row)
            else:
                out[k] = v
        return out

    def _needs_row_migration(self, rows: List[Dict], fieldnames: List[str]) -> bool:
        for row in rows:
            if None in row:
                return True
            for k in fieldnames:
                if row.get(k, "__MISSING__") is None:
                    return True
        return False

    def _ensure_header(self, path: Path, fieldnames: List[str]):
        if not path.exists():
            with open(path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
            return
        try:
            with open(path, "r", newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                existing = reader.fieldnames or []
                rows = list(reader)
        except Exception:
            existing, rows = [], []
        needs_migration = existing != fieldnames or self._needs_row_migration(rows, fieldnames)
        if not needs_migration:
            return
        migrated = []
        for row in rows:
            migrated.append(self._normalize_row_for_fields(row, fieldnames))
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(migrated)
        print(f"[schema migrate] {path.name} -> columns={len(fieldnames)}")

    def get(self, path: str, params: Optional[Dict] = None):
        url = BASE_FAPI + path
        r = self.session.get(url, params=params, timeout=20)
        r.raise_for_status()
        return r.json()

    def load_symbols(self) -> List[str]:
        info = self.get("/fapi/v1/exchangeInfo")
        quote = self.cfg["scanner"]["quote_asset"]
        exclude = set(self.cfg["scanner"].get("exclude_symbols", []))
        symbols = []
        for s in info["symbols"]:
            if s.get("contractType") != "PERPETUAL":
                continue
            if s.get("quoteAsset") != quote:
                continue
            if s.get("status") != "TRADING":
                continue
            symbol = s["symbol"]
            if symbol in exclude:
                continue
            symbols.append(symbol)
        return symbols

    def load_24h_tickers(self) -> Dict[str, Dict]:
        data = self.get("/fapi/v1/ticker/24hr")
        return {x["symbol"]: x for x in data}

    @staticmethod
    def _fetch_top_marketcap_symbols(n: int = 100) -> set:
        try:
            resp = requests.get(
                "https://api.coingecko.com/api/v3/coins/markets",
                params={"vs_currency": "usd", "order": "market_cap_desc", "per_page": n, "page": 1},
                timeout=15,
            )
            resp.raise_for_status()
            symbols = set()
            for coin in resp.json():
                sym = coin.get("symbol", "").upper()
                if sym:
                    symbols.add(f"{sym}USDT")
            print(f"[marketcap_filter] Top {n} market cap loaded — {len(symbols)} symbols excluded from scan")
            return symbols
        except Exception as e:
            print(f"[marketcap_filter] CoinGecko fetch failed: {e} — no market cap exclusion applied")
            return set()

    def filter_symbols(self, symbols: List[str], tickers: Dict[str, Dict]) -> List[str]:
        min_qv = float(self.cfg["scanner"]["min_quote_volume_usdt_24h"])
        max_qv = float(self.cfg["scanner"].get("max_quote_volume_usdt_24h", float("inf")))
        min_price = float(self.cfg["scanner"]["min_price"])
        top_mc_exclude = self._top_marketcap_exclude
        kept = []
        for s in symbols:
            if s in top_mc_exclude:
                continue
            t = tickers.get(s)
            if not t:
                continue
            qv = float(t.get("quoteVolume", 0))
            price = float(t.get("lastPrice", 0))
            if min_qv <= qv <= max_qv and price >= min_price:
                kept.append(s)
        kept.sort(key=lambda sym: float(tickers[sym]["quoteVolume"]), reverse=True)
        return kept[: int(self.cfg["scanner"]["max_symbols"])]

    def klines(self, symbol: str, interval: str, limit: int = 50):
        data = self.get("/fapi/v1/klines", {"symbol": symbol, "interval": interval, "limit": limit})
        out = []
        for x in data:
            out.append({
                "open_time": int(x[0]),
                "open": float(x[1]),
                "high": float(x[2]),
                "low": float(x[3]),
                "close": float(x[4]),
                "volume": float(x[5]),
                "close_time": int(x[6]),
                "quote_volume": float(x[7]),
            })
        return out

    def fetch_klines(self, symbol: str, interval: str, limit: int = 50):
        return self.klines(symbol, interval, limit)

    def price_change_pct(self, bars: List[Dict], lookback_bars: int = 1) -> float:
        if not bars or len(bars) <= lookback_bars:
            return 0.0
        prev = float(bars[-(lookback_bars + 1)].get("close", 0.0) or 0.0)
        curr = float(bars[-1].get("close", 0.0) or 0.0)
        if prev <= 0:
            return 0.0
        return (curr - prev) / prev * 100.0

    def range_pct(self, bar: Dict) -> float:
        high = float(bar.get("high", 0.0) or 0.0)
        low = float(bar.get("low", 0.0) or 0.0)
        close = float(bar.get("close", 0.0) or 0.0)
        base = close if close > 0 else high if high > 0 else 0.0
        if base <= 0:
            return 0.0
        return (high - low) / base * 100.0

    def oi_hist(self, symbol: str, period: str = "5m", limit: int = 10):
        data = self.get("/futures/data/openInterestHist", {
            "symbol": symbol,
            "period": period,
            "limit": limit,
        })
        out = []
        for x in data:
            total = float(x.get("sumOpenInterestValue") or 0.0)
            out.append({"ts": int(x["timestamp"]), "oi_value": total})
        return out

    # ── Bybit V5 client methods ─────────────────────────────────────────────────

    def bybit_get(self, path: str, params: dict | None = None) -> dict:
        r = self.session.get(BASE_BYBIT + path, params=params, timeout=15)
        r.raise_for_status()
        d = r.json()
        if d.get("retCode", -1) != 0:
            raise ValueError(f"retCode={d.get('retCode')} {d.get('retMsg', '')}")
        return d

    def bybit_oi_hist(self, symbol: str, interval_time: str = "5min", limit: int = 13) -> list:
        """
        Fetch Bybit OI history. Returns [{ts, oi_coins}] oldest-first.
        oi_coins = open interest in BASE ASSET (e.g. BTC for BTCUSDT).
        Multiply by current price to convert to USDT before combining with Binance.
        Returns [] on any error.
        """
        try:
            d = self.bybit_get("/v5/market/open-interest", {
                "category": "linear",
                "symbol": symbol,
                "intervalTime": interval_time,
                "limit": limit,
            })
            raw = d["result"]["list"]   # newest-first
            out = []
            for x in raw:
                oi_coins = float(x.get("openInterest") or 0.0)
                out.append({"ts": int(x["timestamp"]), "oi_coins": oi_coins})
            out.reverse()               # convert to oldest-first
            return out
        except Exception as e:
            _logger_bybit.debug(f"[bybit_oi_hist] {symbol} — {e}")
            return []

    def bybit_klines_1h(self, symbol: str, limit: int = 22) -> list:
        """
        Fetch Bybit 1h klines. Returns [{open_time, open, high, low, close, volume, quote_volume}]
        oldest-first. volume = base asset (same denomination as Binance klines["volume"]).
        Returns [] on any error.
        """
        try:
            d = self.bybit_get("/v5/market/kline", {
                "category": "linear",
                "symbol": symbol,
                "interval": "60",
                "limit": limit,
            })
            raw = d["result"]["list"]   # newest-first: [startTime, open, high, low, close, volume, turnover]
            out = []
            for x in raw:
                out.append({
                    "open_time":    int(x[0]),
                    "open":         float(x[1]),
                    "high":         float(x[2]),
                    "low":          float(x[3]),
                    "close":        float(x[4]),
                    "volume":       float(x[5]),    # base asset
                    "quote_volume": float(x[6]),    # USDT turnover
                })
            out.reverse()               # oldest-first
            return out
        except Exception as e:
            _logger_bybit.debug(f"[bybit_klines_1h] {symbol} — {e}")
            return []

    def bybit_ticker_24h(self, symbol: str) -> float:
        """24h USDT turnover for symbol. Returns 0.0 on error."""
        try:
            d = self.bybit_get("/v5/market/tickers", {
                "category": "linear",
                "symbol": symbol,
            })
            return float(d["result"]["list"][0].get("turnover24h") or 0.0)
        except Exception as e:
            _logger_bybit.debug(f"[bybit_ticker_24h] {symbol} — {e}")
            return 0.0

    def bybit_funding(self, symbol: str) -> float:
        """Current funding rate from Bybit for symbol, in %. Returns 0.0 on error."""
        try:
            d = self.bybit_get("/v5/market/tickers", {
                "category": "linear",
                "symbol": symbol,
            })
            return float(d["result"]["list"][0].get("fundingRate", 0.0)) * 100.0
        except Exception as e:
            _logger_bybit.debug(f"[bybit_funding] {symbol} — {e}")
            return 0.0

    # ── Top-trader / taker sentiment (long_accumulation_continuation) ──────────

    def _tt_fetch_binance(self, endpoint: str, symbol: str, period: str, limit: int, value_key: str) -> list | None:
        """Fetch and cache a top-trader/taker series from Binance futures data endpoint.

        Returns [{ts: int, <value_key>: float}] sorted ascending, or None on error.
        """
        key = (symbol, endpoint, period, limit)
        now = time.time()
        cached = self._tt_cache.get(key)
        if cached and now < cached["expires_at"]:
            return cached["data"]
        try:
            raw = self.get(endpoint, {"symbol": symbol, "period": period, "limit": limit})
            if not raw:
                return None
            rows = []
            for item in raw:
                ts = item.get("timestamp")
                val = item.get(value_key)
                if ts is None or val is None:
                    continue
                rows.append({"ts": int(ts), value_key: float(val)})
            rows.sort(key=lambda x: x["ts"])
            self._tt_cache[key] = {"data": rows, "expires_at": now + self._tt_cache_ttl}
            return rows
        except Exception as e:
            print(f"[top_trader_fetch] {endpoint} {symbol}: {e}")
            return None

    def top_long_short_position_ratio(self, symbol: str, period: str = "1d", limit: int = 30) -> list | None:
        return self._tt_fetch_binance(
            "/futures/data/topLongShortPositionRatio", symbol, period, limit, "longAccount"
        )

    def top_long_short_account_ratio(self, symbol: str, period: str = "1d", limit: int = 30) -> list | None:
        return self._tt_fetch_binance(
            "/futures/data/topLongShortAccountRatio", symbol, period, limit, "longAccount"
        )

    def global_long_short_account_ratio(self, symbol: str, period: str = "1d", limit: int = 30) -> list | None:
        return self._tt_fetch_binance(
            "/futures/data/globalLongShortAccountRatio", symbol, period, limit, "longAccount"
        )

    def taker_long_short_ratio(self, symbol: str, period: str = "1d", limit: int = 30) -> list | None:
        return self._tt_fetch_binance(
            "/futures/data/takerlongshortRatio", symbol, period, limit, "buySellRatio"
        )

    @staticmethod
    def _combine_oi_histories(binance_hist: list, bybit_hist: list, current_price: float) -> tuple:
        """
        Combine Binance (USDT) + Bybit (base coins) OI histories into a single USDT series.
        bybit_hist items have key 'oi_coins'; Bybit coins are converted via current_price.
        Both lists must be oldest-first [{ts, oi_value/oi_coins}].
        Alignment: each Binance bar matched to nearest Bybit bar within ±90s.
        Returns (combined_list, bybit_aligned_coins) where combined_list has same
        format as binance_hist [{ts, oi_value}] and bybit_aligned_coins is [float].
        """
        if not bybit_hist or current_price <= 0:
            return binance_hist, [0.0] * len(binance_hist)

        TOLERANCE_MS = 90_000
        bybit_lookup = {x["ts"]: x["oi_coins"] for x in bybit_hist}
        bybit_ts_sorted = sorted(bybit_lookup.keys())

        combined = []
        bybit_aligned = []

        for bar in binance_hist:
            b_ts = bar["ts"]
            b_oi = bar["oi_value"]
            by_coins = 0.0
            for bt in bybit_ts_sorted:
                if abs(bt - b_ts) <= TOLERANCE_MS:
                    by_coins = bybit_lookup[bt]
                    break
            bybit_aligned.append(by_coins)
            combined.append({"ts": b_ts, "oi_value": b_oi + by_coins * current_price})

        return combined, bybit_aligned

    @staticmethod
    def _combine_klines_volume(binance_klines: list, bybit_klines: list) -> list:
        """
        Merge Bybit volume into Binance klines by matching open_time (±5 min tolerance).
        Price/OHLC always from Binance. Only base-asset volume is summed.
        Returns new list (binance_klines unchanged if bybit_klines is empty).
        """
        if not bybit_klines:
            return binance_klines

        TOLERANCE_MS = 300_000
        bybit_lookup = {x["open_time"]: x["volume"] for x in bybit_klines}
        bybit_ts_sorted = sorted(bybit_lookup.keys())

        result = []
        for bar in binance_klines:
            b_ts = bar["open_time"]
            by_vol = 0.0
            for bt in bybit_ts_sorted:
                if abs(bt - b_ts) <= TOLERANCE_MS:
                    by_vol = bybit_lookup[bt]
                    break
            combined_bar = dict(bar)
            combined_bar["volume"] = bar["volume"] + by_vol
            result.append(combined_bar)

        return result

    # ── End Bybit methods ───────────────────────────────────────────────────────

    def funding(self, symbol: str) -> float:
        data = self.get("/fapi/v1/premiumIndex", {"symbol": symbol})
        return float(data.get("lastFundingRate", 0.0)) * 100.0

    def avg(self, vals: List[float]) -> float:
        return sum(vals) / max(len(vals), 1)

    def calc_oi_change_pct(self, symbol: str, period: str = "15m", limit: int = 3) -> Optional[float]:
        hist = self.oi_hist(symbol, period=period, limit=limit)
        if len(hist) < 2:
            return None
        prev = hist[-2]["oi_value"]
        curr = hist[-1]["oi_value"]
        if prev <= 0:
            return None
        return (curr - prev) / prev * 100.0

    def volume_ratio(self, bars_5m_closed: List[Dict]) -> float:
        if len(bars_5m_closed) < 8:
            return 0.0
        recent = bars_5m_closed[-1]["quote_volume"]
        base = self.avg([b["quote_volume"] for b in bars_5m_closed[-7:-1]])
        if base <= 0:
            return 0.0
        return recent / base

    def volume_ratio_generic(self, bars_closed: List[Dict], recent_bars: int = 1, base_bars: int = 6) -> float:
        needed = recent_bars + base_bars
        if len(bars_closed) < needed + 1:
            return 0.0
        recent = self.avg([b["quote_volume"] for b in bars_closed[-recent_bars:]])
        base = self.avg([b["quote_volume"] for b in bars_closed[-(recent_bars + base_bars):-recent_bars]])
        if base <= 0:
            return 0.0
        return recent / base

    def wick_ratio(self, bar: Dict) -> float:
        high, low, op, cl = bar["high"], bar["low"], bar["open"], bar["close"]
        total = max(high - low, 1e-12)
        body_high = max(op, cl)
        body_low = min(op, cl)
        upper = high - body_high
        lower = body_low - low
        return max(upper, lower) / total

    def upper_wick_ratio(self, bar: Dict) -> float:
        total = max(bar["high"] - bar["low"], 1e-12)
        body_high = max(bar["open"], bar["close"])
        return max(bar["high"] - body_high, 0.0) / total

    def lower_wick_ratio(self, bar: Dict) -> float:
        total = max(bar["high"] - bar["low"], 1e-12)
        body_low = min(bar["open"], bar["close"])
        return max(body_low - bar["low"], 0.0) / total

    def candle_body_ratio(self, bar: Dict) -> float:
        total = max(bar["high"] - bar["low"], 1e-12)
        body = abs(bar["close"] - bar["open"])
        return body / total

    def _ensure_storage_layout(self):
        for key, spec in self.table_specs.items():
            spec["dir"].mkdir(parents=True, exist_ok=True)
            fieldnames = spec.get("fieldnames")
            if fieldnames:
                for fp in spec["dir"].glob("*.csv"):
                    if fp.name == "_schema.csv":
                        continue
                    self._ensure_header(fp, fieldnames)
                if self.include_legacy_flat_files and spec["logical"].exists():
                    self._ensure_header(spec["logical"], fieldnames)
        self._cleanup_old_data(days=7)

    def _cleanup_old_data(self, days: int = 7):
        from datetime import timedelta, date as date_cls
        cutoff = datetime.now(timezone.utc).date() - timedelta(days=days)
        cutoff_month = cutoff.strftime("%Y-%m")
        deleted = 0
        for fp in self.pending_dir.glob("pending_*.csv"):
            try:
                file_date = date_cls.fromisoformat(fp.stem.replace("pending_", ""))
                if file_date < cutoff:
                    fp.unlink()
                    deleted += 1
            except Exception:
                continue
        for fp in self.signals_dir.glob("signals_*.csv"):
            try:
                if fp.stem.replace("signals_", "") < cutoff_month:
                    fp.unlink()
                    deleted += 1
            except Exception:
                continue
        for fp in self.results_dir.glob("results_*.csv"):
            try:
                if fp.stem.replace("results_", "") < cutoff_month:
                    fp.unlink()
                    deleted += 1
            except Exception:
                continue
        if deleted:
            print(f"[cleanup] deleted {deleted} data file(s) older than {days} days")

    def _ensure_table_seed(self, table_key: str):
        spec = self.table_specs[table_key]
        if not spec.get("fieldnames"):
            return
        seed_name = "_schema.csv"
        seed = spec["dir"] / seed_name
        if not seed.exists():
            with open(seed, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=spec["fieldnames"])
                writer.writeheader()

    def _table_key_for_path(self, path: Path) -> Optional[str]:
        for key, spec in self.table_specs.items():
            if path == spec["logical"]:
                return key
        return None

    def _partition_key(self, ts_ms: int, granularity: str) -> str:
        dt = datetime.fromtimestamp(max(float(ts_ms), 0.0) / 1000.0, tz=timezone.utc)
        if granularity == "day":
            return dt.strftime("%Y-%m-%d")
        return dt.strftime("%Y-%m")

    def _table_partition_file(self, table_key: str, row: Dict) -> Path:
        spec = self.table_specs[table_key]
        ts_col = spec.get("ts_col")
        if ts_col:
            raw_ts = row.get(ts_col)
            try:
                if raw_ts in (None, "", "nan"):
                    ts_ms = int(time.time() * 1000)
                else:
                    ts_ms = int(float(raw_ts))
            except Exception:
                ts_ms = int(time.time() * 1000)
        else:
            ts_ms = int(time.time() * 1000)
        part = self._partition_key(ts_ms, spec['granularity'])
        return spec["dir"] / f"{table_key}_{part}.csv"

    def _iter_table_files(self, table_key: str) -> List[Path]:
        spec = self.table_specs[table_key]
        files = sorted(p for p in spec["dir"].glob("*.csv") if p.name != "_schema.csv")
        if self.include_legacy_flat_files and spec["logical"].exists():
            files = [spec["logical"]] + files
        return files

    def _mark_pending_confirmed_fields(self, row: Dict, confirmed_ts_ms: Optional[int] = None, note: str = "signal confirmed") -> Dict:
        row = dict(row)
        fallback_ts = row.get("confirmed_ts_ms") or row.get("closed_ts_ms") or int(time.time() * 1000)
        ts_ms = int(float(confirmed_ts_ms or fallback_ts))
        row["status"] = "CONFIRMED"
        row["close_reason"] = note or row.get("close_reason", "signal confirmed")
        row["is_confirmed"] = "Y"
        row["confirmed_ts_ms"] = str(ts_ms)
        row["close_capture_basis"] = "not_due_yet"
        row["close_anchor_time_ms"] = ""
        if not str(row.get("send_decision", "")).strip():
            row["send_decision"] = "UNDECIDED"
        if not str(row.get("close_trigger_detail", "")).strip():
            row["close_trigger_detail"] = "signal_confirmed"
        return self._normalize_row_for_fields(row, self.pending_fields)

    def _sync_confirmed_pending_row(self, setup_id: str, confirmed_ts_ms: Optional[int] = None, note: str = "signal confirmed"):
        rows = self.read_csv(self.pending_file)
        changed = False
        synced_row = None
        for idx, row in enumerate(rows):
            row_setup = row.get("setup_id") or row.get("pending_id", "")
            if row_setup == setup_id:
                updated = self._mark_pending_confirmed_fields(row, confirmed_ts_ms=confirmed_ts_ms, note=note)
                rows[idx] = updated
                synced_row = dict(updated)
                changed = True
                break
        if changed:
            self.write_csv(self.pending_file, rows, fieldnames=self.pending_fields)
            return synced_row
        return None

    def read_csv(self, path: Path) -> List[Dict]:
        table_key = self._table_key_for_path(path)
        if table_key:
            fieldnames = self.table_specs[table_key].get("fieldnames")
            rows: List[Dict] = []
            for fp in self._iter_table_files(table_key):
                if not fp.exists():
                    continue
                try:
                    with open(fp, "r", newline="", encoding="utf-8") as f:
                        file_rows = list(csv.DictReader(f))
                    if fieldnames:
                        file_rows = [self._normalize_row_for_fields(r, fieldnames) for r in file_rows]
                    rows.extend(file_rows)
                except Exception:
                    continue
            return rows
        if not path.exists():
            return []
        with open(path, "r", newline="", encoding="utf-8") as f:
            return list(csv.DictReader(f))

    def write_csv(self, path: Path, rows: List[Dict], fieldnames: List[str]):
        table_key = self._table_key_for_path(path)
        if table_key:
            spec = self.table_specs[table_key]
            target_dir = spec["dir"]
            target_dir.mkdir(parents=True, exist_ok=True)
            for old_fp in target_dir.glob("*.csv"):
                if old_fp.name != "_schema.csv":
                    old_fp.unlink()
            grouped: Dict[Path, List[Dict]] = {}
            for row in rows:
                fp = self._table_partition_file(table_key, row)
                grouped.setdefault(fp, []).append(self._normalize_row_for_fields(row, fieldnames))
            for fp, grp in grouped.items():
                with open(fp, "w", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(grp)
            self._ensure_table_seed(table_key)
            return
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    @staticmethod
    def _safe_orb_float(val, default: float = 0.0) -> float:
        try:
            return float(val) if val not in (None, "", "not_evaluated") else default
        except (ValueError, TypeError):
            return default

    @staticmethod
    def _safe_acc_int(val, default: int = 0) -> int:
        try:
            return int(float(val)) if val not in (None, "", "not_evaluated") else default
        except (ValueError, TypeError):
            return default

    def _safe_pending_status(self, row: Dict) -> str:
        status = str(row.get("status", "") or "").strip().upper()
        if status in VALID_PENDING_STATUSES:
            return status
        return "UNKNOWN_SCHEMA"

    def _safe_pending_reason(self, row: Dict) -> str:
        reason = str(row.get("close_reason", "") or "").strip()
        status = self._safe_pending_status(row)
        if reason:
            return reason
        return "active" if status == "PENDING" else "unknown"

    def append_csv(self, path: Path, row: Dict, fieldnames: List[str]):
        table_key = self._table_key_for_path(path)
        if table_key:
            fp = self._table_partition_file(table_key, row)
            self._ensure_header(fp, fieldnames)
            with open(fp, "a", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writerow(self._normalize_row_for_fields(row, fieldnames))
            return
        with open(path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writerow(row)

    def _local_day_from_ms(self, ts_ms: int) -> str:
        return datetime.fromtimestamp(max(int(ts_ms), 0) / 1000.0, tz=timezone.utc).astimezone().strftime("%Y-%m-%d")

    def _find_pending_row(self, pending_id: str) -> Optional[Dict]:
        rows = self.read_csv(self.pending_file)
        for row in rows:
            if row.get("pending_id") == pending_id:
                return row
        return None

    def _find_pending_by_setup(self, setup_id: str) -> Optional[Dict]:
        rows = self.read_csv(self.pending_file)
        for row in rows:
            row_setup = row.get("setup_id") or row.get("pending_id", "")
            if row_setup == setup_id:
                return row
        return None

    def already_open_signal(self, symbol: str, side: str) -> bool:
        rows = self.read_csv(self.signals_file)
        for row in rows:
            if row.get("symbol") == symbol and row.get("side") == side and row.get("status") == "OPEN":
                return True
        return False

    def already_pending_setup(self, symbol: str, side: str) -> bool:
        rows = self.read_csv(self.pending_file)
        for row in rows:
            if row.get("symbol") == symbol and row.get("side") == side and row.get("status") == "PENDING":
                return True
        return False

    def save_signal(self, s: Signal):
        if not s.setup_id:
            s.setup_id = s.signal_id
        if not getattr(s, "market_regime", ""):
            s.market_regime = getattr(s, "btc_regime", "unknown")
        s.regime_label = self._normalize_regime_label_value(
            getattr(s, "regime_label", ""),
            getattr(s, "market_regime", ""),
            getattr(s, "btc_regime", ""),
        )
        if not getattr(s, "regime_fit_for_strategy", ""):
            s.regime_fit_for_strategy = self._derive_regime_fit_for_strategy(
                getattr(s, "strategy", ""),
                getattr(s, "side", ""),
                s.regime_label,
            )
        if not getattr(s, "dispatch_action", ""):
            s.dispatch_action = "not_evaluated"
        if not getattr(s, "dispatch_confidence_band", ""):
            s.dispatch_confidence_band = "not_evaluated"
        if not getattr(s, "dispatch_reason", ""):
            s.dispatch_reason = "not_evaluated"
        row = self._normalize_row_for_fields(asdict(s), self.signal_fields)
        self.append_csv(self.signals_file, row, fieldnames=self.signal_fields)
        self._sync_confirmed_pending_row(
            s.setup_id,
            confirmed_ts_ms=int(s.timestamp_ms),
            note=s.reason or "signal confirmed",
        )
        self.sync_pending_send_decision(s.setup_id, "SENT", sent_ts_ms=int(s.timestamp_ms))

    def save_pending(self, p: PendingSetup):
        if not p.setup_id:
            p.setup_id = p.pending_id
        if not getattr(p, "market_regime", ""):
            p.market_regime = getattr(p, "btc_regime", "unknown")
        pending_row = asdict(p)
        pending_row.setdefault("is_confirmed", "N")
        pending_row.setdefault("confirmed_ts_ms", "")
        pending_row.setdefault("is_sent_signal", "N")
        pending_row.setdefault("sent_ts_ms", "")
        pending_row.setdefault("semantic_consistency", "Y")
        pending_row.setdefault("semantic_issue", "")
        pending_row.setdefault("review_eligible", "Y")
        pending_row.setdefault("review_exclusion_reason", "")
        pending_row.setdefault("close_anchor_time_ms", "")
        pending_row.setdefault("close_capture_basis", "")
        pending_row.setdefault("close_trigger_detail", "")
        pending_row.setdefault("confirm_fail_detail", "")
        pending_row.setdefault("invalidation_detail", "")
        pending_row = self._apply_regime_trace_defaults(pending_row, strategy_hint=p.strategy, side_hint=p.side)
        # Sprint 3A.2 audit log — remove or downgrade after patch is confirmed.
        print(
            f"[RegimeAttach] {p.symbol} {p.side} | strategy={p.strategy} "
            f"| regime_label={pending_row.get('regime_label')} "
            f"| fit={pending_row.get('regime_fit_for_strategy')}"
        )
        pending_row["setup_quality_band"] = pending_row.get("setup_quality_band") or "not_evaluated"
        pending_row["delivery_band"] = pending_row.get("delivery_band") or "not_evaluated"
        pending_row["veto_reason_code"] = pending_row.get("veto_reason_code") or "not_evaluated"
        pending_row["dispatch_action"] = pending_row.get("dispatch_action") or "not_evaluated"
        pending_row["dispatch_confidence_band"] = pending_row.get("dispatch_confidence_band") or "not_evaluated"
        pending_row["dispatch_reason"] = pending_row.get("dispatch_reason") or "not_evaluated"
        self.append_csv(self.pending_file, self._normalize_row_for_fields(pending_row, self.pending_fields), fieldnames=self.pending_fields)

    def sync_pending_send_decision(self, setup_id: str, send_decision: str, skip_reason: str = "", sent_ts_ms: Optional[int] = None):
        rows = self.read_csv(self.pending_file)
        changed = False
        for idx, row in enumerate(rows):
            row_setup = row.get("setup_id") or row.get("pending_id", "")
            if row_setup == setup_id:
                if str(send_decision or "").upper() == "SENT" and (
                    str(row.get("status", "")).upper() != "CONFIRMED"
                    or str(row.get("is_confirmed", "N")).upper() != "Y"
                    or not str(row.get("confirmed_ts_ms", "") or "").strip()
                ):
                    row = self._mark_pending_confirmed_fields(
                        row,
                        confirmed_ts_ms=int(sent_ts_ms or time.time() * 1000),
                        note="signal confirmed",
                    )
                row["send_decision"] = send_decision
                row["skip_reason"] = skip_reason
                row = self._apply_regime_trace_defaults(row)
                row["dispatch_action"] = row.get("dispatch_action") or "not_evaluated"
                row["dispatch_confidence_band"] = row.get("dispatch_confidence_band") or "not_evaluated"
                row["dispatch_reason"] = row.get("dispatch_reason") or "not_evaluated"
                if str(send_decision or "").upper() == "SENT":
                    row["is_sent_signal"] = "Y"
                    row["sent_ts_ms"] = str(int(sent_ts_ms or time.time() * 1000))
                rows[idx] = self._normalize_row_for_fields(row, self.pending_fields)
                changed = True
                break
        if changed:
            self.write_csv(self.pending_file, rows, fieldnames=self.pending_fields)
        return changed

    def format_close_message(self, signal_row: Dict, outcome: str, r_multiple: float, close_reason: str, mfe_pct: float = 0.0, mae_pct: float = 0.0) -> str:
        side    = signal_row.get("side", "UNKNOWN")
        symbol  = signal_row.get("symbol", "")
        strategy = signal_row.get("strategy", "unknown")

        outcome_icon = {
            "WIN_TP2":   "🎯🎯",
            "WIN_TP1":   "🎯",
            "LOSS_STOP": "❌",
            "EXPIRED":   "⌛",
        }.get(outcome, "🔔")

        outcome_label = {
            "WIN_TP2":   "TP2 Hit",
            "WIN_TP1":   "TP1 Hit",
            "LOSS_STOP": "Stop Hit",
            "EXPIRED":   "Expired",
        }.get(outcome, outcome)

        side_tag = "LONG" if side == "LONG" else "SHORT"

        try:
            entry = float(signal_row.get("entry_ref") or 0)
            stop  = float(signal_row.get("stop") or 0)
            tp1   = float(signal_row.get("tp1") or 0)
            tp2   = float(signal_row.get("tp2") or 0)
            sl_pct  = float(signal_row.get("sl_distance_pct") or 0)
            tp1_pct = float(signal_row.get("tp1_distance_pct") or 0)
            tp2_pct = float(signal_row.get("tp2_distance_pct") or 0)
            levels_line = (
                f"Entry: {entry:.6g}\n"
                f"Stop: {stop:.6g} ({sl_pct:.2f}%)\n"
                f"TP1: {tp1:.6g} ({tp1_pct:.2f}%)\n"
                f"TP2: {tp2:.6g} ({tp2_pct:.2f}%)\n"
            )
        except Exception:
            levels_line = ""

        mfe_mae_line = f"MFE: {mfe_pct:.2f}% | MAE: {mae_pct:.2f}%\n" if (mfe_pct or mae_pct) else ""

        return (
            f"{outcome_icon} {outcome_label} | {side_tag} #{symbol} | R={r_multiple:+.2f}\n\n"
            f"{levels_line}"
            f"\n{mfe_mae_line}"
            f"Strategy: {strategy}\n"
            f"#{symbol} #BINANCE"
        )

    def close_pending(self, pending_id: str, status: str, close_reason: str, bars_waited: int = 0):
        rows = self.read_csv(self.pending_file)
        changed = False
        closed_row = None
        for idx, row in enumerate(rows):
            if row.get("pending_id") == pending_id and self._safe_pending_status(row) == "PENDING":
                row["status"] = status
                row["close_reason"] = close_reason
                row["bars_waited"] = bars_waited
                row["closed_ts_ms"] = int(time.time() * 1000)
                row["close_trigger_detail"] = close_reason
                row = self._apply_regime_trace_defaults(row)
                # Sprint 3A.2 audit log — remove or downgrade after patch is confirmed.
                print(
                    f"[RegimeClose] {row.get('symbol')} {row.get('side')} | status={status} "
                    f"| regime_label={row.get('regime_label')} "
                    f"| fit={row.get('regime_fit_for_strategy')}"
                )
                row["dispatch_action"] = row.get("dispatch_action") or "not_evaluated"
                row["dispatch_confidence_band"] = row.get("dispatch_confidence_band") or "not_evaluated"
                row["dispatch_reason"] = row.get("dispatch_reason") or "not_evaluated"
                if status == "CONFIRMED":
                    row = self._mark_pending_confirmed_fields(
                        row,
                        confirmed_ts_ms=int(row["closed_ts_ms"]),
                        note=close_reason or "signal confirmed",
                    )
                else:
                    row["close_anchor_time_ms"] = str(row["closed_ts_ms"])
                    row["close_capture_basis"] = "true_close"
                    if status == "EXPIRED_WAIT":
                        row["confirm_fail_detail"] = close_reason or "timeout_no_followthrough"
                    elif status == "INVALIDATED":
                        row["invalidation_detail"] = close_reason or "invalidated"
                if status == "CONFIRMED" and not str(row.get("send_decision", "")).strip():
                    row["send_decision"] = "UNDECIDED"
                # Phase 5A: persist explicit dispatch trace for non-CONFIRMED terminal cases.
                # CONFIRMED rows get dispatch trace from _update_pending_dispatch_trace()
                # in scan_once() after routing. All other terminal statuses are closed
                # inside process_pending_setups() before dispatch runs — mark explicitly.
                if status != "CONFIRMED":
                    if (row.get("dispatch_action") or "not_evaluated") == "not_evaluated":
                        row["dispatch_action"] = "not_routed"
                        row["dispatch_confidence_band"] = "none"
                        row["dispatch_reason"] = "closed_before_dispatch"
                rows[idx] = self._normalize_row_for_fields(row, self.pending_fields)
                changed = True
                closed_row = dict(rows[idx])
                break
        if changed:
            self.write_csv(self.pending_file, rows, fieldnames=self.pending_fields)
        return changed

    def close_signal(self, signal_row: Dict, outcome: str, r_multiple: float, bars_checked: int, close_reason: str, mfe_pct: float = 0.0, mae_pct: float = 0.0):
        results = self.read_csv(self.results_file)
        sig_id = signal_row.get("signal_id", "")
        sym = signal_row.get("symbol", "")
        ts = signal_row.get("timestamp_ms", "")
        already_closed = (
            any(r.get("signal_id") == sig_id for r in results) if sig_id
            else any(r.get("symbol") == sym and r.get("timestamp_ms") == ts for r in results)
        )
        if already_closed:
            print(f"[close_signal] skip duplicate: {sig_id or sym+':'+str(ts)} already in results")
            return
        result_row = {
            "signal_id": signal_row.get("signal_id", ""),
            "setup_id": signal_row.get("setup_id", signal_row.get("signal_id", "")),
            "timestamp_ms": signal_row.get("timestamp_ms", ""),
            "symbol": signal_row.get("symbol", ""),
            "side": signal_row.get("side", ""),
            "entry_ref": signal_row.get("entry_ref", ""),
            "stop": signal_row.get("stop", ""),
            "tp1": signal_row.get("tp1", ""),
            "tp2": signal_row.get("tp2", ""),
            "outcome": outcome,
            "r_multiple": f"{r_multiple:.4f}",
            "bars_checked": bars_checked,
            "close_time_ms": int(time.time() * 1000),
            "close_reason": close_reason,
            "config_version": signal_row.get("config_version", ""),
            "strategy": signal_row.get("strategy", signal_row.get("side", "")),
            "market_regime": signal_row.get("market_regime", signal_row.get("btc_regime", "unknown")),
            "btc_price": signal_row.get("btc_price", ""),
            "btc_24h_change_pct": signal_row.get("btc_24h_change_pct", ""),
            "btc_4h_change_pct": signal_row.get("btc_4h_change_pct", ""),
            "btc_1h_change_pct": signal_row.get("btc_1h_change_pct", ""),
            "btc_24h_range_pct": signal_row.get("btc_24h_range_pct", ""),
            "btc_4h_range_pct": signal_row.get("btc_4h_range_pct", ""),
            "alt_market_breadth_pct": signal_row.get("alt_market_breadth_pct", ""),
            "btc_regime": signal_row.get("btc_regime", ""),
            "risk_pct_real": signal_row.get("risk_pct_real", ""),
            "sl_distance_pct": signal_row.get("sl_distance_pct", ""),
            "tp1_distance_pct": signal_row.get("tp1_distance_pct", ""),
            "tp2_distance_pct": signal_row.get("tp2_distance_pct", ""),
            "break_distance_pct": signal_row.get("break_distance_pct", ""),
            "retest_depth_pct": signal_row.get("retest_depth_pct", ""),
            "score_oi": signal_row.get("score_oi", ""),
            "score_exhaustion": signal_row.get("score_exhaustion", ""),
            "score_breakout": signal_row.get("score_breakout", ""),
            "score_retest": signal_row.get("score_retest", ""),
            "reason_tags": signal_row.get("reason_tags", ""),
            "stop_was_forced_min_risk": signal_row.get("stop_was_forced_min_risk", ""),
            "mfe_pct": f"{mfe_pct:.4f}",
            "mae_pct": f"{mae_pct:.4f}",
            "manual_tradable": signal_row.get("manual_tradable", ""),
            "manual_trade_note": signal_row.get("manual_trade_note", ""),
        }
        results.append(self._normalize_row_for_fields(result_row, self.result_fields))
        self.write_csv(self.results_file, results, fieldnames=self.result_fields)

        signals = self.read_csv(self.signals_file)
        for row in signals:
            if row.get("signal_id") == signal_row.get("signal_id"):
                row["status"] = outcome
        self.write_csv(self.signals_file, signals, fieldnames=self.signal_fields)

        print(f"[close] {signal_row.get('symbol','')} {signal_row.get('side','')} | {outcome} | r={r_multiple:.2f} | {close_reason}")
        if self.cfg.get("telegram", {}).get("send_close_notifications", True):
            try:
                self.telegram_send(self.format_close_message(signal_row, outcome, r_multiple, close_reason, mfe_pct=mfe_pct, mae_pct=mae_pct))
            except Exception as e:
                print(f"[telegram close error] {e}")

    def _update_pending_dispatch_trace(self, setup_id: str, dispatch_action: str, dispatch_confidence_band: str, dispatch_reason: str, send_decision: Optional[str] = None, skip_reason: str = "") -> bool:
        rows = self.read_csv(self.pending_file)
        changed = False
        for idx, row in enumerate(rows):
            row_setup = row.get("setup_id") or row.get("pending_id", "")
            if row_setup != setup_id:
                continue
            row["dispatch_action"] = dispatch_action
            row["dispatch_confidence_band"] = dispatch_confidence_band
            row["dispatch_reason"] = dispatch_reason
            if send_decision is not None:
                row["send_decision"] = send_decision
                row["skip_reason"] = skip_reason
            rows[idx] = self._normalize_row_for_fields(row, self.pending_fields)
            changed = True
            break
        if changed:
            self.write_csv(self.pending_file, rows, fieldnames=self.pending_fields)
        return changed

    def format_watchlist_signal(self, s: Signal) -> str:
        return (
            f"[WATCHLIST] #{s.symbol} | ${s.price:.6g} | Score {s.score/10:.1f}/10\n\n"
            f"Entry ref: {s.entry_ref:.6g}\n"
            f"Stop: {s.stop:.6g} ({s.sl_distance_pct:.2f}%)\n"
            f"TP1: {s.tp1:.6g} ({s.tp1_distance_pct:.2f}%)\n"
            f"TP2: {s.tp2:.6g} ({s.tp2_distance_pct:.2f}%)\n\n"
            f"Dispatch: {s.dispatch_action} [{s.dispatch_confidence_band}]\n"
            f"Reason: {s.reason}\n"
            f"Dispatch note: {s.dispatch_reason}\n\n"
            f"#WATCHLIST #{s.symbol} #BINANCE"
        )

    def format_signal(self, s: Signal) -> str:
        side_icon = "[LONG]" if s.side == "LONG" else "[SHORT]"
        side_tag = "#LONG" if s.side == "LONG" else "#SHORT"

        dispatch_line = f"Dispatch: {s.dispatch_action} [{s.dispatch_confidence_band}]\n" if getattr(s, "dispatch_action", "") not in ("", "not_evaluated") else ""
        dispatch_reason_line = f"Dispatch note: {s.dispatch_reason}\n" if getattr(s, "dispatch_reason", "") not in ("", "not_evaluated") else ""

        if getattr(s, "strategy", "") == "long_accumulation_continuation":
            _tags = (s.reason_tags or "").split(";")
            participation = next(
                (t.split("=", 1)[1] for t in _tags if t.startswith("participation=")), "n/a"
            )
            _oi_str = next(
                (t.split("=", 1)[1] for t in _tags if t.startswith("oi_vs_baseline=")),
                f"{s.oi_jump_pct:.2f}%"
            )
            _mc = getattr(s, "market_cap_usd", 0) or 0
            _mc_str = f"${_mc / 1_000_000:.0f}M" if _mc > 0 else "N/A"
            _tp1c = next((t.split("=", 1)[1] for t in _tags if t.startswith("geo_tp1_too_close=")), None)
            _nou  = next((t.split("=", 1)[1] for t in _tags if t.startswith("geo_no_upside=")), None)
            _rkt  = next((t.split("=", 1)[1] for t in _tags if t.startswith("geo_risk_tight=")), None)
            _geo_lines = []
            if _tp1c is not None:
                _geo_lines.append(f"⚠️ R:R thấp — TP1 cách entry ~{_tp1c}, cân nhắc trước khi vào.")
            if _nou is not None:
                _geo_lines.append("⚠️ Giá đã vượt vùng kháng cự gần — entry có thể MUỘN, R:R xấu.")
            if _rkt is not None:
                _geo_lines.append(f"⚠️ SL rất sát (~{_rkt}) — dễ bị quét, cân nhắc.")
            _geo_warn_block = ("\n".join(_geo_lines) + "\n\n") if _geo_lines else ""
            return (
                f"{side_icon} #{s.symbol} | ${s.price:.6g} | Score {s.score/10:.1f}/10\n\n"
                f"Entry: {s.entry_ref:.6g}\n"
                f"Stop: {s.stop:.6g} ({s.sl_distance_pct:.2f}%)\n"
                f"TP1: {s.tp1:.6g} ({s.tp1_distance_pct:.2f}%)\n"
                f"TP2: {s.tp2:.6g} ({s.tp2_distance_pct:.2f}%)\n\n"
                f"Volume 1h: {s.vol_ratio:.2f}x EMA20\n"
                f"Volume 24h: ${getattr(s, 'vol_24h_usdt', 0)/1_000_000:.1f}M\n"
                f"Market Cap: {_mc_str}\n"
                f"Funding: {s.funding_pct:+.4f}% (B+B)\n"
                f"OI Delta 1h: +{_oi_str}\n"
                f"Participation: {participation}\n\n"
                f"{_geo_warn_block}"
                f"BTC 24h: {s.btc_24h_change_pct:+.2f}% ({s.btc_regime})\n"
                f"Reason: {s.reason}\n\n"
                f"{side_tag} #{s.symbol} #BINANCE"
            )

        if getattr(s, "strategy", "") == "oi_range_breakout":
            _orb_side = "📈 LONG" if s.side == "LONG" else "📉 SHORT"
            _orb_icon = "📈" if s.side == "LONG" else "📉"
            return (
                f"{_orb_side} #{s.symbol} | ${s.price:.6g} | Score {s.score/10:.1f}/10\n\n"
                f"Entry: {s.entry_ref:.6g}\n"
                f"Stop: {s.stop:.6g} ({s.sl_distance_pct:.2f}%)\n"
                f"TP1: {s.tp1:.6g} ({s.tp1_distance_pct:.2f}%)\n"
                f"TP2: {s.tp2:.6g} ({s.tp2_distance_pct:.2f}%)\n\n"
                f"OI Delta: {s.oi_delta_pct:+.2f}% (${getattr(s, 'oi_delta_abs_1h', 0)/1_000_000:.2f}M)\n"
                f"Volume 1h: {s.vol_ratio:.2f}x EMA20\n"
                f"Volume 24h: ${s.vol_24h_usdt/1_000_000:.1f}M\n"
                f"Range: {s.range_height:.6g} ({s.range_width_pct:.2f}%)\n"
                f"ATR Ratio: {s.atr_ratio:.2f}\n"
                f"Funding: {s.funding_pct:+.4f}% (B+B)\n\n"
                f"BTC 24h: {s.btc_24h_change_pct:+.2f}% ({s.btc_regime})\n"
                f"Reason: {s.reason}\n\n"
                f"{_orb_icon} #{s.symbol} #BINANCE"
            )

        metrics = (
            f"OI(5m): {s.oi_jump_pct:+.2f}%\n"
            f"Vol: {s.vol_ratio:.2f}x\n"
            f"Funding: {s.funding_pct:+.4f}%\n"
            f"Retest waited: {s.retest_bars_waited} bars\n"
        )
        return (
            f"{side_icon} #{s.symbol} | ${s.price:.6g} | Score {s.score/10:.1f}/10\n\n"
            f"Entry: {s.entry_ref:.6g}\n"
            f"Stop: {s.stop:.6g} ({s.sl_distance_pct:.2f}%)\n"
            f"TP1: {s.tp1:.6g} ({s.tp1_distance_pct:.2f}%)\n"
            f"TP2: {s.tp2:.6g} ({s.tp2_distance_pct:.2f}%)\n\n"
            f"{metrics}"
            f"BTC 24h: {s.btc_24h_change_pct:+.2f}% ({s.btc_regime})\n"
            f"Manual: {s.manual_tradable} [{s.manual_trade_note}]\n"
            f"{dispatch_line}"
            f"{dispatch_reason_line}\n"
            f"Reason: {s.reason}\n\n"
            f"{side_tag} #{s.symbol} #BINANCE"
        )

    def telegram_send(self, text: str):
        token = self.cfg["telegram"]["bot_token"]
        chat_id = self.cfg["telegram"]["chat_id"]
        if not token or not chat_id or "YOUR_" in str(token) or "YOUR_" in str(chat_id) or "CUA_BAN" in str(token) or "CUA_BAN" in str(chat_id):
            print("[telegram skipped] Please set bot_token and chat_id in config.yaml")
            print(text)
            return
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {"chat_id": chat_id, "text": text}
        r = requests.post(url, json=payload, timeout=20)
        r.raise_for_status()

    def should_send(self, s: Signal) -> bool:
        key = f"{s.symbol}:{s.side}"
        now = time.time()
        ttl = 60 * 45
        last = self.sent_cache.get(key, 0)
        if now - last < ttl:
            return False
        self.sent_cache[key] = now
        return True

    def _pct_to_fraction(self, x: float) -> float:
        return x / 100.0

    def get_btc_context(self) -> Dict[str, float]:
        try:
            ticker = self.get("/fapi/v1/ticker/24hr", {"symbol": "BTCUSDT"})
            btc_price = float(ticker.get("lastPrice", 0.0))
            btc_24h_change_pct = float(ticker.get("priceChangePercent", 0.0))
            high_24h = float(ticker.get("highPrice", 0.0))
            low_24h = float(ticker.get("lowPrice", 0.0))
        except Exception:
            btc_price = 0.0
            btc_24h_change_pct = 0.0
            high_24h = 0.0
            low_24h = 0.0

        kl_4h = self.fetch_klines("BTCUSDT", "4h", 2)
        kl_1h = self.fetch_klines("BTCUSDT", "1h", 2)
        btc_4h_change_pct = self.price_change_pct(kl_4h, 1) if kl_4h else 0.0
        btc_1h_change_pct = self.price_change_pct(kl_1h, 1) if kl_1h else 0.0
        btc_24h_range_pct = ((high_24h - low_24h) / max(btc_price, 1e-12) * 100.0) if btc_price > 0 else 0.0
        btc_4h_range_pct = self.range_pct(kl_4h[-1]) if kl_4h else 0.0

        regime_cfg = self.cfg.get("btc_sentiment", {})
        bullish = float(regime_cfg.get("bullish_threshold_pct", 1.0))
        bearish = float(regime_cfg.get("bearish_threshold_pct", -1.0))

        if btc_24h_change_pct >= bullish:
            btc_regime = "bullish"
        elif btc_24h_change_pct <= bearish:
            btc_regime = "bearish"
        else:
            btc_regime = "neutral"

        return {
            "btc_price": btc_price,
            "btc_24h_change_pct": btc_24h_change_pct,
            "btc_4h_change_pct": btc_4h_change_pct,
            "btc_1h_change_pct": btc_1h_change_pct,
            "btc_24h_range_pct": btc_24h_range_pct,
            "btc_4h_range_pct": btc_4h_range_pct,
            "btc_regime": btc_regime,
            "market_regime": btc_regime.upper() if btc_regime != "neutral" else "RANGE",
            "alt_market_breadth_pct": 0.0,
        }

    def classify_market_regime(self, btc_ctx: Dict[str, float], alt_market_breadth_pct: float) -> str:
        btc_24h = float(btc_ctx.get("btc_24h_change_pct", 0.0) or 0.0)
        btc_4h = float(btc_ctx.get("btc_4h_change_pct", 0.0) or 0.0)
        btc_1h = float(btc_ctx.get("btc_1h_change_pct", 0.0) or 0.0)
        range_24h = float(btc_ctx.get("btc_24h_range_pct", 0.0) or 0.0)
        range_4h = float(btc_ctx.get("btc_4h_range_pct", 0.0) or 0.0)
        breadth = float(alt_market_breadth_pct or 0.0)

        if breadth >= 65.0 and btc_24h > -1.0:
            return "ALT_MOMO"
        if btc_24h >= 1.0 and btc_4h >= 0.3 and btc_1h >= -0.3:
            return "UPTREND"
        if btc_24h <= -1.0 and btc_4h <= -0.3 and btc_1h <= 0.3:
            return "DOWNTREND"
        if range_24h >= 5.0 or range_4h >= 2.0:
            return "VOLATILE_CHOP"
        return "RANGE"

    def build_market_snapshot(self, symbols: List[str], tickers: Dict[str, Dict]) -> None:
        btc_ctx = self.get_btc_context()
        movers = []
        positive = 0
        total = 0
        for symbol in symbols:
            t = tickers.get(symbol) or {}
            try:
                pct = float(t.get("priceChangePercent", 0.0) or 0.0)
            except Exception:
                pct = 0.0
            movers.append((symbol, pct))
            total += 1
            if pct > 0:
                positive += 1
        alt_market_breadth_pct = (positive / max(total, 1)) * 100.0
        market_regime = self.classify_market_regime(btc_ctx, alt_market_breadth_pct)
        movers.sort(key=lambda x: abs(x[1]), reverse=True)
        top_n = int(self.cfg.get("observability", {}).get("market_review_top_n", 20))
        self.current_market_snapshot = {
            **btc_ctx,
            "alt_market_breadth_pct": alt_market_breadth_pct,
            "market_regime": market_regime,
            "top_movers": movers[:top_n],
        }

    def evaluate_manual_tradable(self, side: str, entry_ref: float, stop: float, tp1: float) -> Dict[str, str]:
        manual_cfg = self.cfg.get("manual_review", {})
        min_sl_distance_pct = float(manual_cfg.get("min_sl_distance_pct", 2.0))
        min_tp1_distance_pct = float(manual_cfg.get("min_tp1_distance_pct", 2.5))
        min_risk_reward_for_manual = float(manual_cfg.get("min_risk_reward_for_manual", 1.0))

        sl_distance_pct = abs(entry_ref - stop) / max(entry_ref, 1e-12) * 100.0
        tp1_distance_pct = abs(tp1 - entry_ref) / max(entry_ref, 1e-12) * 100.0
        rr_tp1 = tp1_distance_pct / max(sl_distance_pct, 1e-12)

        notes = []
        if sl_distance_pct < min_sl_distance_pct:
            notes.append("sl_too_tight")
        if tp1_distance_pct < min_tp1_distance_pct:
            notes.append("tp1_too_small")
        if rr_tp1 < min_risk_reward_for_manual:
            notes.append("rr_tp1_too_low")

        if notes:
            return {
                "manual_tradable": "no",
                "manual_trade_note": ",".join(notes) + ";better_for_auto",
            }

        return {
            "manual_tradable": "yes",
            "manual_trade_note": "good_for_manual",
        }

    def _already_pending_for_strategy(self, symbol: str, side: str, strategy: str) -> bool:
        rows = self.read_csv(self.pending_file)
        for row in rows:
            if (row.get("symbol") == symbol
                    and row.get("side") == side
                    and row.get("strategy") == strategy
                    and row.get("status") == "PENDING"):
                return True
        return False

    def _orb_already_signaled_today(self, symbol: str) -> bool:
        today_start_ms = int(datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)
        try:
            rows = self.read_csv(self.pending_file)
        except Exception:
            return False
        for row in rows:
            if row.get("symbol") != symbol:
                continue
            if row.get("strategy") != "oi_range_breakout":
                continue
            try:
                ts = int(float(row.get("created_ts_ms") or 0))
            except (ValueError, TypeError):
                continue
            if ts >= today_start_ms:
                return True
        return False

    def _acc_already_signaled_today(self, symbol: str) -> bool:
        if symbol in self._acc_sent_today:
            return True
        today_start_ms = int(datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)
        try:
            for row in self.read_csv(self.signals_file):
                if row.get("symbol") != symbol:
                    continue
                if row.get("strategy") != "long_accumulation_continuation":
                    continue
                try:
                    ts = int(float(row.get("timestamp_ms") or 0))
                except (ValueError, TypeError):
                    continue
                if ts >= today_start_ms:
                    return True
        except Exception:
            pass
        return False

    def build_pending_setups_for_symbol(
        self,
        symbol: str,
        oi_1h_history: list = None,
        bybit_oi_ok: bool = False,
        bybit_aligned_coins: list = None,
    ) -> List[PendingSetup]:
        setups: List[PendingSetup] = []
        strategy_cfg = self.cfg.get("strategy", {})

        if strategy_cfg.get("oi_range_breakout", {}).get("enabled", False):
            orb_setups = self.build_pending_oi_range_breakout_setup(
                symbol, oi_1h_history,
                bybit_oi_ok=bybit_oi_ok,
                bybit_aligned_coins=bybit_aligned_coins or [],
            )
            setups.extend(orb_setups)

        if strategy_cfg.get("long_accumulation_continuation", {}).get("enabled", False):
            acc_setups = self.build_pending_long_accumulation_continuation_setup(
                symbol, oi_1h_history
            )
            setups.extend(acc_setups)

        return setups

    def process_pending_setups(self) -> List[Signal]:
        retest_cfg = self.cfg["retest"]
        risk_cfg = self.cfg["risk"]
        rows = self.read_csv(self.pending_file)
        confirmed: List[Signal] = []
        orb_confirmed_symbols: set = set()
        acc_cont_confirmed_symbols: set = set()

        strategy_cfg = self.cfg.get("strategy", {})

        for row in rows:
            if row.get("status") != "PENDING":
                continue

            symbol = row["symbol"]
            side = row["side"]
            signal_open_time = int(row["signal_open_time"])
            breakout_level = float(row["breakout_level"])
            score = float(row["score"])
            confidence = float(row["confidence"])
            reason = row["reason"]
            oi_jump_pct = float(row.get("oi_jump_pct") or 0.0)
            funding_pct = float(row.get("funding_pct") or 0.0)
            vol_ratio = float(row.get("vol_ratio") or 0.0)
            price = float(row["signal_price"])
            strategy = (row.get("strategy") or "").strip()
            if not strategy:
                continue

            # Skip strategies that are disabled in config
            if strategy == "long_accumulation_continuation":
                if not strategy_cfg.get(strategy, {}).get("enabled", True):
                    continue
            score_oi = float(row.get("score_oi") or 0.0)
            score_exhaustion = float(row.get("score_exhaustion") or 0.0)
            score_breakout = float(row.get("score_breakout") or 0.0)
            score_retest = float(row.get("score_retest") or 0.0)
            reason_tags = row.get("reason_tags", "") or ""

            # ── No-retest path: oi_range_breakout (immediate confirm at live price >= entry) ─
            if strategy == "oi_range_breakout":
                if symbol in orb_confirmed_symbols:
                    self.close_pending(row.get("pending_id", ""), "REJECTED_RULE", "orb_duplicate_dedup")
                    print(f"[orb dedup] {symbol} — duplicate PENDING in same scan, expired")
                    continue
                orb_signals = self._process_oi_range_breakout_pending(row, risk_cfg)
                if orb_signals:
                    orb_confirmed_symbols.add(symbol)
                confirmed.extend(orb_signals)
                continue

            # ── No-retest path: long_accumulation_continuation ───────────────────
            if strategy == "long_accumulation_continuation":
                if symbol in acc_cont_confirmed_symbols:
                    self.close_pending(row.get("pending_id", ""), "REJECTED_RULE", "acc_cont_duplicate_dedup")
                    continue
                acc_signals = self._process_acc_cont_pending(row, risk_cfg)
                if acc_signals:
                    acc_cont_confirmed_symbols.add(symbol)
                confirmed.extend(acc_signals)
                continue

        return confirmed

    def _process_acc_cont_pending(self, row: dict, risk_cfg: dict) -> list:
        """No-retest confirmation path for long_accumulation_continuation.

        Immediately confirms the pending setup using data stored at detection time.
        Expiry check prevents stale setups from firing.
        """
        pending_id   = row.get("pending_id", "")
        symbol       = row.get("symbol", "")
        created_ms   = int(row.get("created_ts_ms") or 0)
        signal_price = float(row.get("signal_price") or 0)
        signal_low   = float(row.get("signal_low") or 0)
        breakout_lvl = float(row.get("breakout_level") or 0)

        acc_cfg          = self.cfg.get("long_accumulation_continuation", {})
        stop_buffer_pct  = float(acc_cfg.get("stop_buffer_pct", 0.3))
        tp2_ext_pct      = float(acc_cfg.get("tp2_extension_pct", 1.0))
        min_tp1_dist     = float(acc_cfg.get("min_tp1_distance_pct", 3.0)) / 100.0
        min_risk_pct     = float(acc_cfg.get("min_risk_pct", 0.5)) / 100.0
        max_risk_pct     = float(acc_cfg.get("max_risk_pct", 35.0)) / 100.0
        max_pending_bars = int(acc_cfg.get("max_pending_bars_5m", 3))

        now_ms    = int(time.time() * 1000)
        expiry_ms = max_pending_bars * 5 * 60 * 1000

        if (now_ms - created_ms) > expiry_ms:
            self.close_pending(pending_id, "EXPIRED_WAIT", "acc_cont_expired", max_pending_bars)
            return []

        # Chart-based S/R levels stored at detection time
        swing_high = float(row.get("range_high") or 0)   # 48-bar resistance (TP anchor)
        base       = signal_low if signal_low > 0 else breakout_lvl
        stop       = base * (1.0 - stop_buffer_pct / 100.0)
        risk       = signal_price - stop

        if risk <= 0 or signal_price <= 0 or swing_high <= 0:
            self.close_pending(pending_id, "REJECTED_RULE", "acc_cont_bad_risk")
            return []
        # Discretionary geometry checks — tag and SEND rather than hard-block
        # (R:R is a human decision on low-cap manual-trade bot; only safety/dedup below are hard blocks)
        _geo_tags: list = []

        if swing_high <= signal_price:
            _nu_pct = (swing_high - signal_price) / signal_price * 100.0
            _geo_tags.append(f"geo_no_upside={_nu_pct:.2f}%")

        tp1 = swing_high
        tp2 = swing_high * (1.0 + tp2_ext_pct / 100.0)

        # Compute distance metrics here so all geometry checks can reference exact numbers
        sl_distance_pct    = abs(signal_price - stop) / max(signal_price, 1e-12) * 100.0
        tp1_distance_pct   = abs(tp1 - signal_price) / max(signal_price, 1e-12) * 100.0
        tp2_distance_pct   = abs(tp2 - signal_price) / max(signal_price, 1e-12) * 100.0
        break_distance_pct = abs(signal_price - breakout_lvl) / max(breakout_lvl, 1e-12) * 100.0
        risk_pct_real      = sl_distance_pct

        _tp1_dist_frac = (tp1 - signal_price) / signal_price
        if _tp1_dist_frac < min_tp1_dist and swing_high > signal_price:
            _geo_tags.append(f"geo_tp1_too_close={tp1_distance_pct:.2f}%")

        risk_pct = risk / signal_price
        if risk_pct < min_risk_pct:
            _geo_tags.append(f"geo_risk_tight={sl_distance_pct:.2f}%")
        if risk_pct > max_risk_pct:                                     # catastrophic-stop guard — hard block
            self.close_pending(pending_id, "REJECTED_RULE", "acc_cont_risk_too_wide")
            return []

        if self.already_open_signal(symbol, "LONG"):                    # dedup vs status==OPEN — hard block
            self.close_pending(pending_id, "REJECTED_RULE", "acc_cont_open_collision")
            return []

        _base_tags = row.get("reason_tags", "") or ""
        reason_tags_out = (_base_tags.rstrip(";") + ";" + ";".join(_geo_tags)).lstrip(";") if _geo_tags else _base_tags

        btc_ctx      = self.get_btc_context()
        manual_eval  = self.evaluate_manual_tradable("LONG", signal_price, stop, tp1)
        signal_open_time = int(row.get("signal_open_time") or 0)
        signal_id    = f"{symbol}-LONG-acc_cont-{signal_open_time}"

        # Fetch funding and vol_24h live at confirmation time
        try:
            _binance_fr = self.funding(symbol)
        except Exception:
            _binance_fr = 0.0
        try:
            _bybit_fr = self.bybit_funding(symbol) if self.cfg.get("bybit", {}).get("enabled", True) else 0.0
        except Exception:
            _bybit_fr = 0.0
        _funding_pct = _binance_fr + _bybit_fr

        try:
            _ticker = self.get("/fapi/v1/ticker/24hr", {"symbol": symbol})
            _vol_24h_usdt = float(_ticker.get("quoteVolume", 0))
        except Exception:
            _vol_24h_usdt = 0.0

        try:
            from scanner.strategies.oi_range_breakout import fetch_market_cap as _fetch_mc
            _market_cap = _fetch_mc(symbol) or 0.0
        except Exception:
            _market_cap = 0.0

        signal = Signal(
            signal_id=signal_id,
            setup_id=row.get("setup_id") or row.get("pending_id", ""),
            timestamp_ms=signal_open_time,
            symbol=symbol,
            side="LONG",
            score=float(row.get("score") or 0),
            confidence=float(row.get("confidence") or 0),
            reason=row.get("reason", ""),
            breakout_level=breakout_lvl,
            entry_low=signal_price,
            entry_high=signal_price,
            entry_ref=signal_price,
            stop=stop,
            tp1=tp1,
            tp2=tp2,
            price=signal_price,
            oi_jump_pct=float(row.get("oi_jump_pct") or 0),
            funding_pct=_funding_pct,
            vol_ratio=float(row.get("vol_ratio") or 0),
            vol_24h_usdt=_vol_24h_usdt,
            retest_bars_waited=0,
            config_version=str(self.cfg.get("config_version", "")),
            strategy="long_accumulation_continuation",
            market_regime=btc_ctx["market_regime"],
            btc_price=btc_ctx["btc_price"],
            btc_24h_change_pct=btc_ctx["btc_24h_change_pct"],
            btc_4h_change_pct=btc_ctx["btc_4h_change_pct"],
            btc_1h_change_pct=btc_ctx["btc_1h_change_pct"],
            btc_24h_range_pct=btc_ctx["btc_24h_range_pct"],
            btc_4h_range_pct=btc_ctx["btc_4h_range_pct"],
            alt_market_breadth_pct=btc_ctx["alt_market_breadth_pct"],
            btc_regime=btc_ctx["btc_regime"],
            risk_pct_real=risk_pct_real,
            sl_distance_pct=sl_distance_pct,
            tp1_distance_pct=tp1_distance_pct,
            tp2_distance_pct=tp2_distance_pct,
            break_distance_pct=break_distance_pct,
            retest_depth_pct=0.0,
            score_oi=float(row.get("score_oi") or 0),
            score_exhaustion=0.0,
            score_breakout=float(row.get("score_breakout") or 0),
            score_retest=0.0,
            reason_tags=reason_tags_out,
            stop_was_forced_min_risk="no",
            manual_tradable=manual_eval["manual_tradable"],
            manual_trade_note=manual_eval["manual_trade_note"],
            regime_label=self._normalize_regime_label_value(
                row.get("regime_label", ""), row.get("market_regime", ""), row.get("btc_regime", "")
            ),
            regime_fit_for_strategy=self._derive_regime_fit_for_strategy(
                "long_accumulation_continuation", "LONG",
                row.get("regime_label") or row.get("market_regime") or row.get("btc_regime") or "unclear_mixed"
            ),
            dispatch_action=row.get("dispatch_action") or "not_evaluated",
            dispatch_confidence_band="not_evaluated",
            dispatch_reason="not_evaluated",
            status="OPEN",
            # Acc-specific fields copied from pending row
            setup_quality_band=row.get("setup_quality_band") or "not_evaluated",
            accumulation_score=self._safe_acc_int(row.get("accumulation_score")),
            pos_now=self._safe_orb_float(row.get("pos_now")),
            acct_now=self._safe_orb_float(row.get("acct_now")),
            gap_now=self._safe_orb_float(row.get("gap_now")),
            retail_now=self._safe_orb_float(row.get("retail_now")),
            taker_now=self._safe_orb_float(row.get("taker_now")),
            pos_slope_30d=self._safe_orb_float(row.get("pos_slope_30d")),
            gap_slope_30d=self._safe_orb_float(row.get("gap_slope_30d")),
            retail_slope_30d=self._safe_orb_float(row.get("retail_slope_30d")),
            pos_trend_3v14=self._safe_orb_float(row.get("pos_trend_3v14")),
            oi_trend_3v14=self._safe_orb_float(row.get("oi_trend_3v14")),
            pos_min_30d=self._safe_orb_float(row.get("pos_min_30d")),
            pos_recovery_from_min=self._safe_orb_float(row.get("pos_recovery_from_min")),
            retail_min_30d=self._safe_orb_float(row.get("retail_min_30d")),
            retail_recovery=self._safe_orb_float(row.get("retail_recovery")),
            taker_7d_avg=self._safe_orb_float(row.get("taker_7d_avg")),
            market_cap_usd=_market_cap,
        )
        self.close_pending(pending_id, "CONFIRMED", "acc_cont_immediate_confirm", bars_waited=0)
        return [signal]

    def build_pending_oi_range_breakout_setup(
        self,
        symbol: str,
        oi_1h_history: list | None,
        bybit_oi_ok: bool = False,
        bybit_aligned_coins: list = None,
    ) -> list:
        """Build pending setups for oi_range_breakout strategy."""
        if not oi_1h_history:
            return []

        if self._orb_already_signaled_today(symbol):
            return []

        try:
            orb_cfg = self.cfg.get("oi_range_breakout", {})

            # Fetch Binance 1h klines (price authority)
            klines_1h_binance = self.klines(symbol, "1h", limit=22)
            if not klines_1h_binance or len(klines_1h_binance) < 22:
                return []

            # Optionally fetch Bybit 1h klines and combine volume
            klines_1h = klines_1h_binance
            _bybit_vol_ok = False
            if bybit_oi_ok and self.cfg.get("bybit", {}).get("enabled", True):
                bybit_kl = self.bybit_klines_1h(symbol)
                if len(bybit_kl) >= 22:
                    klines_1h = self._combine_klines_volume(klines_1h_binance, bybit_kl)
                    _bybit_vol_ok = True

            # Compute per-exchange OI deltas; inject MAX as override for gate
            _bybit_oi_delta_pct = 0.0
            _aligned = bybit_aligned_coins or []
            if bybit_oi_ok and len(_aligned) >= 13 and _aligned[-13] > 0:
                _bybit_oi_delta_pct = (_aligned[-1] - _aligned[-13]) / _aligned[-13] * 100.0

            _binance_oi_delta_pct = 0.0
            if len(oi_1h_history) >= 13 and float(oi_1h_history[-13].get("oi_value", 0)) > 0:
                _binance_oi_delta_pct = (
                    float(oi_1h_history[-1]["oi_value"]) - float(oi_1h_history[-13]["oi_value"])
                ) / float(oi_1h_history[-13]["oi_value"]) * 100.0

            _max_oi_delta = max(_binance_oi_delta_pct, _bybit_oi_delta_pct) if bybit_oi_ok else _binance_oi_delta_pct

            # Bybit OI abs delta in USDT: coins × last-closed 1h price
            _bybit_abs_usdt_delta = 0.0
            if bybit_oi_ok and len(_aligned) >= 13:
                _price_1h = float(klines_1h_binance[-2].get("close", 0) or 0)
                if _price_1h > 0:
                    _bybit_abs_usdt_delta = (_aligned[-1] - _aligned[-13]) * _price_1h

            # Smuggle cross-exchange metadata into config dict so strategy can use it
            orb_cfg_ext = dict(orb_cfg)
            orb_cfg_ext["_bybit_oi_ok"] = bybit_oi_ok
            orb_cfg_ext["_bybit_vol_ok"] = _bybit_vol_ok
            orb_cfg_ext["_bybit_oi_delta_pct"] = _bybit_oi_delta_pct
            orb_cfg_ext["_override_oi_delta_pct"] = _max_oi_delta
            orb_cfg_ext["_binance_oi_delta_pct"] = _binance_oi_delta_pct
            orb_cfg_ext["_bybit_abs_usdt_delta"] = _bybit_abs_usdt_delta

            from scanner.strategies.oi_range_breakout import detect_oi_range_breakout

            signal_dict = detect_oi_range_breakout(symbol, klines_1h, oi_1h_history, orb_cfg_ext)

            if not signal_dict:
                return []

            setup = self._wrap_oi_range_breakout_signal(signal_dict)
            return [setup] if setup else []

        except Exception as e:
            print(f"[oi_range_breakout] {symbol} — {e}")
            return []

    def build_pending_long_accumulation_continuation_setup(
        self,
        symbol: str,
        oi_1h_history: list | None,
    ) -> list:
        """Build pending setups for long_accumulation_continuation strategy.

        Fetches 4 top-trader/taker series (cached), computes accumulation features,
        applies hard gates, and returns [PendingSetup] if setup_detected or [] otherwise.
        """
        if self._acc_already_signaled_today(symbol):
            return []

        acc_cfg = self.cfg.get("long_accumulation_continuation", {})
        period  = str(acc_cfg.get("history_period", "1d"))
        limit   = int(acc_cfg.get("history_limit", 30))

        try:
            # Fetch 4 top-trader series (each individually cached per symbol+endpoint+TTL)
            raw_pos    = self.top_long_short_position_ratio(symbol, period, limit)
            raw_acct   = self.top_long_short_account_ratio(symbol, period, limit)
            raw_retail = self.global_long_short_account_ratio(symbol, period, limit)
            raw_taker  = self.taker_long_short_ratio(symbol, period, limit)

            series_pos    = [r["longAccount"]    for r in raw_pos]    if raw_pos    else None
            series_acct   = [r["longAccount"]    for r in raw_acct]   if raw_acct   else None
            series_retail = [r["longAccount"]    for r in raw_retail] if raw_retail else None
            series_taker  = [r["buySellRatio"]   for r in raw_taker]  if raw_taker  else None

            # OI daily series for oi_trend_3v14 — separate from 5m Gate2 source
            # Cached (TTL=1h via _tt_cache); fail-open: None → oi_trend_3v14=0.0 in features
            series_oi_1d: list | None = None
            try:
                _oi_1d_rows = self._tt_fetch_binance(
                    "/futures/data/openInterestHist", symbol, "1d", 16, "sumOpenInterestValue"
                )
                if _oi_1d_rows and len(_oi_1d_rows) >= 2:
                    series_oi_1d = [float(r["sumOpenInterestValue"]) for r in _oi_1d_rows[:-1]]
            except Exception:
                pass

            # 1h OI delta — same formula as ORB (bar[-13] to bar[-1] at 5m = 60 min)
            oi_delta_1h_pct = 0.0
            if oi_1h_history and len(oi_1h_history) >= 13:
                _prev = float(oi_1h_history[-13].get("oi_value", 0))
                _cur  = float(oi_1h_history[-1].get("oi_value", 0))
                if _prev > 0:
                    oi_delta_1h_pct = (_cur - _prev) / _prev * 100.0

            # Current price from last closed 5m bar
            kline = self.klines(symbol, "5m", limit=2)
            if not kline or len(kline) < 2:
                return []
            current_price = float(kline[-2].get("close", 0) or 0)
            if current_price <= 0:
                return []
            current_low = float(kline[-2].get("low", current_price) or current_price)

            # Fetch 1h klines — used for vol MA20 AND chart-based SL/TP structure
            vol_ratio       = 0.0
            recent_low_1h   = current_low   # fallback to 5m bar low
            swing_high_1h   = current_price  # fallback
            range_height_1h = 0.0
            try:
                _sl_lookback = int(acc_cfg.get("range_lookback_bars_1h", 14))  # SL: tight accumulation zone
                _tp_lookback = int(acc_cfg.get("tp_lookback_bars_1h", 48))     # TP: swing high / resistance
                klines_1h = self.klines(symbol, "1h", limit=max(22, _tp_lookback + 2))
                if klines_1h and len(klines_1h) >= 2:
                    closed_1h = klines_1h[:-1]  # exclude currently forming bar
                    # Vol MA20
                    vols = [float(b.get("volume", 0)) for b in closed_1h]
                    if len(vols) >= 20:
                        ma20 = sum(vols[-20:]) / 20
                        if ma20 > 0:
                            vol_ratio = vols[-1] / ma20
                    # SL anchor: bottom of current accumulation zone (short lookback)
                    n_sl = min(_sl_lookback, len(closed_1h))
                    recent_low_1h = min(float(b["low"]) for b in closed_1h[-n_sl:])
                    # TP anchor: swing high / resistance above (longer lookback)
                    n_tp = min(_tp_lookback, len(closed_1h))
                    swing_high_1h = max(float(b["high"]) for b in closed_1h[-n_tp:])
                    range_height_1h = swing_high_1h - recent_low_1h
            except Exception:
                pass

            # Fetch daily close for Gate T price trend (v2.0.1)
            # klines() returns list of dicts with string keys; limit=31 + [:-1] = 30 complete bars
            series_close: list = []
            try:
                klines_1d_price = self.klines(symbol, "1d", limit=31)
                if klines_1d_price and len(klines_1d_price) >= 2:
                    series_close = [float(k["close"]) for k in klines_1d_price[:-1] if k.get("close")]
            except Exception:
                pass

            # Fetch Binance funding for Gate F (v2.0.1)
            # funding() returns percent (lastFundingRate × 100). BSB 0.035% → 0.035.
            funding_8h_pct = 0.0
            try:
                funding_8h_pct = self.funding(symbol)
            except Exception:
                pass

            # Regime label from current_regime (set by scan_once before detection)
            regime_label = "unclear_mixed"
            if self.current_regime is not None:
                regime_label = getattr(self.current_regime, "regime_label", "unclear_mixed") or "unclear_mixed"

            from scanner.strategies.long_accumulation_continuation import detect_long_accumulation_continuation

            result = detect_long_accumulation_continuation(
                symbol=symbol,
                series_pos=series_pos,
                series_acct=series_acct,
                series_retail=series_retail,
                series_taker=series_taker,
                series_oi=series_oi_1d,
                oi_delta_1h_pct=oi_delta_1h_pct,
                regime_label=regime_label,
                config=acc_cfg,
                series_close=series_close,
                funding_8h_pct=funding_8h_pct,
            )

            if not result.get("setup_detected"):
                return []

            now_ms       = int(time.time() * 1000)
            pending_id   = f"ACC-{symbol}-{now_ms}"
            quality_band = result.get("setup_quality_band", "WEAK")
            acc_score    = int(result.get("accumulation_score", 0))
            reason_tags  = ";".join(result.get("setup_reason_tags", []))
            gap          = result.get("gap_now", 0.0)
            pos          = result.get("pos_now", 0.0)

            return [PendingSetup(
                pending_id=pending_id,
                symbol=symbol,
                side="LONG",
                strategy="long_accumulation_continuation",
                created_ts_ms=now_ms,
                signal_open_time=now_ms,
                score=float(acc_score),
                confidence=float(acc_score) / 100.0,
                reason=f"Acc:{quality_band} gap={gap:.2f} pos={pos:.2f} OI={oi_delta_1h_pct:+.1f}%",
                reason_tags=reason_tags,
                breakout_level=current_price,
                signal_price=current_price,
                signal_high=current_price,
                signal_low=recent_low_1h,
                range_high=swing_high_1h,
                range_height=range_height_1h,
                oi_jump_pct=oi_delta_1h_pct,
                funding_pct=0.0,
                vol_ratio=vol_ratio,
                score_oi=0.0,
                score_breakout=float(acc_score),
                regime_label=regime_label,
                regime_fit_for_strategy=self._derive_regime_fit_for_strategy(
                    "long_accumulation_continuation", "LONG", regime_label
                ),
                setup_quality_band=quality_band,
                accumulation_score=acc_score,
                pos_now=result.get("pos_now", 0.0),
                acct_now=result.get("acct_now", 0.0),
                gap_now=result.get("gap_now", 0.0),
                retail_now=result.get("retail_now", 0.0),
                taker_now=result.get("taker_now", 0.0),
                pos_slope_30d=result.get("pos_slope_30d", 0.0),
                gap_slope_30d=result.get("gap_slope_30d", 0.0),
                retail_slope_30d=result.get("retail_slope_30d", 0.0),
                pos_trend_3v14=result.get("pos_trend_3v14", 0.0),
                oi_trend_3v14=result.get("oi_trend_3v14", 0.0),
                pos_min_30d=result.get("pos_min_30d", 0.0),
                pos_recovery_from_min=result.get("pos_recovery_from_min", 0.0),
                retail_min_30d=result.get("retail_min_30d", 0.0),
                retail_recovery=result.get("retail_recovery", 0.0),
                taker_7d_avg=result.get("taker_7d_avg", 0.0),
                price_vs_baseline=result.get("price_vs_baseline", 0.0),
                price_trend_7v30=result.get("price_trend_7v30", 0.0),
                funding_8h_pct=result.get("funding_8h_pct", 0.0),
            )]

        except Exception as e:
            print(f"[acc_cont build] {symbol} — {e}")
            return []

    def _wrap_oi_range_breakout_signal(self, signal_dict: dict):
        """Convert oi_range_breakout signal dict → PendingSetup."""
        symbol = signal_dict.get("symbol", "")
        entry = signal_dict.get("entry_price", 0)
        sl = signal_dict.get("sl_price", 0)
        tp1 = signal_dict.get("tp1_price", 0)
        tp2 = signal_dict.get("tp2_price", 0)

        if not all([entry, sl, tp1, tp2]) or entry <= 0 or sl >= entry:
            return None

        now_ms = int(time.time() * 1000)
        pending_id = f"ORB-{symbol}-{now_ms}"

        _xex = "Y" if signal_dict.get("cross_exchange_confirmed") else "N"
        reason_tags = (
            f"oi_delta_pct={signal_dict.get('oi_delta_pct', 0):.2f}|"
            f"vol_ratio={signal_dict.get('vol_ratio', 0):.2f}|"
            f"range_width={signal_dict.get('range_width_pct', 0):.2f}%|"
            f"atr_ratio={signal_dict.get('atr_ratio', 0):.2f}|"
            f"score={signal_dict.get('score', 0)}|"
            f"band={signal_dict.get('setup_quality_band', 'WEAK')}|"
            f"xex={_xex}"
        )

        return PendingSetup(
            pending_id=pending_id,
            symbol=symbol,
            side="LONG",
            strategy="oi_range_breakout",
            entry_price=entry,
            stop_loss=sl,
            tp1=tp1,
            tp2=tp2,
            created_ts_ms=now_ms,
            signal_open_time=signal_dict.get("trigger_bar_open_ts", now_ms),
            score=float(signal_dict.get("score", 0)),
            confidence=float(signal_dict.get("score", 0)) / 100.0,
            reason=f"OI +{signal_dict.get('oi_delta_pct', 0):.1f}% | Vol {signal_dict.get('vol_ratio', 0):.2f}x | Range {signal_dict.get('range_width_pct', 0):.2f}%",
            reason_tags=reason_tags,
            score_oi=0.0,
            score_breakout=float(signal_dict.get("score", 0)),
            score_retest=0.0,
            oi_jump_pct=float(signal_dict.get("oi_delta_pct", 0)),
            funding_pct=0.0,
            vol_ratio=float(signal_dict.get("vol_ratio", 0)),
            breakout_level=float(signal_dict.get("entry_price", 0)),
            signal_price=float(signal_dict.get("entry_price", 0)),
            signal_high=float(signal_dict.get("entry_price", 0)),
            signal_low=float(signal_dict.get("entry_price", 0)),
            regime_label="",
            market_regime="",
            range_high=self._safe_orb_float(signal_dict.get("range_high")),
            range_low=self._safe_orb_float(signal_dict.get("range_low")),
            range_height=self._safe_orb_float(signal_dict.get("range_height")),
            range_width_pct=self._safe_orb_float(signal_dict.get("range_width_pct")),
            midpoint=self._safe_orb_float(signal_dict.get("midpoint")),
            atr_current=self._safe_orb_float(signal_dict.get("atr_current")),
            atr_avg=self._safe_orb_float(signal_dict.get("atr_avg")),
            atr_ratio=self._safe_orb_float(signal_dict.get("atr_ratio")),
            oi_current=self._safe_orb_float(signal_dict.get("oi_current")),
            oi_prev=self._safe_orb_float(signal_dict.get("oi_prev")),
            oi_delta_pct=self._safe_orb_float(signal_dict.get("oi_delta_pct")),
            oi_delta_abs_1h=self._safe_orb_float(signal_dict.get("oi_delta_abs_1h", 0.0)),
            vol_ema20=self._safe_orb_float(signal_dict.get("vol_ema20")),
            cross_exchange_confirmed=_xex,
            bybit_oi_delta_pct=self._safe_orb_float(signal_dict.get("bybit_oi_delta_pct", 0.0)),
            bybit_vol_ok="Y" if signal_dict.get("bybit_vol_ok") else "N",
        )

    def _process_oi_range_breakout_pending(self, row: dict, risk_cfg: dict) -> list:
        """Immediate confirmation for oi_range_breakout pending.

        Confirms immediately when live price >= entry_price (no retest waiting).
        """
        pending_id = row.get("pending_id", "")
        symbol = row.get("symbol", "")
        signal_price = self._safe_orb_float(row.get("entry_price"))
        sl = self._safe_orb_float(row.get("stop_loss"))
        tp1 = self._safe_orb_float(row.get("tp1"))
        tp2 = self._safe_orb_float(row.get("tp2"))

        if not all([signal_price, sl, tp1, tp2]):
            self.close_pending(pending_id, "REJECTED_RULE", "orb_missing_levels")
            return []

        # Standard confirmation
        btc_ctx = self.get_btc_context()
        manual_eval = self.evaluate_manual_tradable("LONG", signal_price, sl, tp1)
        sl_distance_pct = abs(signal_price - sl) / max(signal_price, 1e-12) * 100.0
        tp1_distance_pct = abs(tp1 - signal_price) / max(signal_price, 1e-12) * 100.0
        tp2_distance_pct = abs(tp2 - signal_price) / max(signal_price, 1e-12) * 100.0

        try:
            _ticker = self.get("/fapi/v1/ticker/24hr", {"symbol": symbol})
            _binance_vol_24h = float(_ticker.get("quoteVolume", 0))
        except Exception:
            _binance_vol_24h = 0.0

        _bybit_vol_24h = 0.0
        if self.cfg.get("bybit", {}).get("enabled", True):
            _bybit_vol_24h = self.bybit_ticker_24h(symbol)

        _vol_24h_usdt = _binance_vol_24h + _bybit_vol_24h

        _binance_fr = 0.0
        try:
            _binance_fr = self.funding(symbol)
        except Exception:
            pass
        _bybit_fr = 0.0
        if self.cfg.get("bybit", {}).get("enabled", True):
            _bybit_fr = self.bybit_funding(symbol)
        _funding_pct = _binance_fr + _bybit_fr

        signal = Signal(
            signal_id=f"{symbol}-LONG-oi_range_breakout-{int(time.time() * 1000)}",
            timestamp_ms=int(row.get("signal_open_time") or 0),
            symbol=symbol,
            side="LONG",
            score=self._safe_orb_float(row.get("score")),
            confidence=self._safe_orb_float(row.get("confidence")),
            reason=row.get("reason", ""),
            entry_ref=signal_price,
            stop=sl,
            tp1=tp1,
            tp2=tp2,
            price=signal_price,
            breakout_level=float(row.get("breakout_level", 0)),
            entry_low=signal_price,
            entry_high=signal_price,
            oi_jump_pct=float(row.get("oi_jump_pct", 0)),
            funding_pct=_funding_pct,
            vol_ratio=float(row.get("vol_ratio", 0)),
            retest_bars_waited=0,
            config_version=str(self.cfg.get("config_version", "")),
            strategy="oi_range_breakout",
            market_regime=btc_ctx["market_regime"],
            btc_price=btc_ctx["btc_price"],
            btc_24h_change_pct=btc_ctx["btc_24h_change_pct"],
            btc_4h_change_pct=btc_ctx["btc_4h_change_pct"],
            btc_1h_change_pct=btc_ctx["btc_1h_change_pct"],
            btc_24h_range_pct=btc_ctx["btc_24h_range_pct"],
            btc_4h_range_pct=btc_ctx["btc_4h_range_pct"],
            alt_market_breadth_pct=btc_ctx["alt_market_breadth_pct"],
            btc_regime=btc_ctx["btc_regime"],
            risk_pct_real=sl_distance_pct,
            sl_distance_pct=sl_distance_pct,
            tp1_distance_pct=tp1_distance_pct,
            tp2_distance_pct=tp2_distance_pct,
            break_distance_pct=0.0,
            retest_depth_pct=0.0,
            score_oi=float(row.get("score_oi", 0)),
            score_exhaustion=0.0,
            score_breakout=float(row.get("score_breakout", 0)),
            score_retest=0.0,
            reason_tags=row.get("reason_tags", ""),
            stop_was_forced_min_risk="no",
            manual_tradable=manual_eval["manual_tradable"],
            manual_trade_note=manual_eval["manual_trade_note"],
            regime_label=row.get("regime_label", ""),
            regime_fit_for_strategy=row.get("regime_fit_for_strategy", "MEDIUM"),
            # oi_range_breakout specific fields
            range_high=self._safe_orb_float(row.get("range_high")),
            range_low=self._safe_orb_float(row.get("range_low")),
            range_height=self._safe_orb_float(row.get("range_height")),
            midpoint=self._safe_orb_float(row.get("midpoint")),
            atr_current=self._safe_orb_float(row.get("atr_current")),
            atr_avg=self._safe_orb_float(row.get("atr_avg")),
            atr_ratio=self._safe_orb_float(row.get("atr_ratio")),
            oi_current=self._safe_orb_float(row.get("oi_current")),
            oi_prev=self._safe_orb_float(row.get("oi_prev")),
            oi_delta_pct=self._safe_orb_float(row.get("oi_delta_pct")),
            vol_ema20=self._safe_orb_float(row.get("vol_ema20")),
            range_width_pct=self._safe_orb_float(row.get("range_width_pct")),
            vol_24h_usdt=_vol_24h_usdt,
            cross_exchange_confirmed=str(row.get("cross_exchange_confirmed", "N")),
            bybit_vol_24h_usdt=_bybit_vol_24h,
            oi_delta_abs_1h=self._safe_orb_float(row.get("oi_delta_abs_1h", 0.0)),
        )

        self.close_pending(row.get("pending_id", ""), "CONFIRMED", "orb_immediate_confirm", bars_waited=0)
        return [signal]

    def evaluate_open_signals(self):
        tracking_cfg = self.cfg["tracking"]
        max_bars_after_entry = int(tracking_cfg["max_bars_after_entry"])

        signals = self.read_csv(self.signals_file)
        open_rows = [r for r in signals if r.get("status") == "OPEN"]

        for row in open_rows:
            try:
                symbol = row["symbol"]
                side = row["side"]
                signal_ts = int(row["timestamp_ms"])
                entry_ref = float(row["entry_ref"])
                stop = float(row["stop"])
                tp1 = float(row["tp1"])
                tp2 = float(row["tp2"])
                strategy = (row.get("strategy") or "").strip()
                if not strategy:
                    continue

                # Silently expire OPEN signals of disabled strategies — no Telegram
                _strategy_cfg = self.cfg.get("strategy", {})
                if strategy == "long_accumulation_continuation":
                    if not _strategy_cfg.get(strategy, {}).get("enabled", True):
                        _sigs = self.read_csv(self.signals_file)
                        for _s in _sigs:
                            if _s.get("signal_id") == row.get("signal_id"):
                                _s["status"] = "EXPIRED_DISABLED"
                        self.write_csv(self.signals_file, _sigs, fieldnames=self.signal_fields)
                        print(f"[eval skip] {symbol} {strategy} disabled — expired {row.get('signal_id')}")
                        continue

                if strategy == "long_accumulation_continuation":
                    interval = self.cfg["scanner"]["interval_1h"]
                    _max_bars = int(tracking_cfg.get("acc_cont_max_bars_1h", 168))
                else:
                    interval = self.cfg["scanner"]["interval_5m"]
                    _max_bars = max_bars_after_entry
                bars = self.klines(symbol, interval, limit=_max_bars + 40)
                closed_bars = bars[:-1]
                future_bars = [b for b in closed_bars if b["open_time"] > signal_ts][:_max_bars]

                if not future_bars:
                    continue

                outcome = None
                r_multiple = 0.0
                bars_checked = 0
                close_reason = ""

                risk = abs(entry_ref - stop)
                if risk <= 1e-12:
                    continue

                mfe_pct = 0.0
                mae_pct = 0.0
                for idx, bar in enumerate(future_bars, start=1):
                    bars_checked = idx
                    if side == "LONG":
                        mfe_pct = max(mfe_pct, (float(bar["high"]) - entry_ref) / max(entry_ref, 1e-12) * 100.0)
                        mae_pct = max(mae_pct, (entry_ref - float(bar["low"])) / max(entry_ref, 1e-12) * 100.0)
                    else:
                        mfe_pct = max(mfe_pct, (entry_ref - float(bar["low"])) / max(entry_ref, 1e-12) * 100.0)
                        mae_pct = max(mae_pct, (float(bar["high"]) - entry_ref) / max(entry_ref, 1e-12) * 100.0)

                    if side == "LONG":
                        if bar["low"] <= stop:
                            outcome = "LOSS_STOP"
                            r_multiple = -1.0
                            close_reason = "stop hit before tp"
                            break
                        if bar["high"] >= tp2:
                            outcome = "WIN_TP2"
                            r_multiple = 2.0
                            close_reason = "tp2 hit"
                            break
                        if bar["high"] >= tp1:
                            outcome = "WIN_TP1"
                            r_multiple = 1.0
                            close_reason = "tp1 hit"
                            break
                    else:
                        if bar["high"] >= stop:
                            outcome = "LOSS_STOP"
                            r_multiple = -1.0
                            close_reason = "stop hit before tp"
                            break
                        if bar["low"] <= tp2:
                            outcome = "WIN_TP2"
                            r_multiple = 2.0
                            close_reason = "tp2 hit"
                            break
                        if bar["low"] <= tp1:
                            outcome = "WIN_TP1"
                            r_multiple = 1.0
                            close_reason = "tp1 hit"
                            break

                if outcome is None and len(future_bars) >= _max_bars:
                    outcome = "EXPIRED"
                    last_close = future_bars[-1]["close"]
                    if side == "LONG":
                        r_multiple = (last_close - entry_ref) / risk
                    else:
                        r_multiple = (entry_ref - last_close) / risk
                    close_reason = "max bars reached"

                if outcome is not None:
                    self.close_signal(row, outcome, r_multiple, bars_checked, close_reason, mfe_pct=mfe_pct, mae_pct=mae_pct)

            except Exception as e:
                print(f"[eval warn] {row.get('signal_id')}: {e}")

    def print_stats(self):
        rows = self.read_csv(self.results_file)
        pending_rows = self.read_csv(self.pending_file)
        pending_active = [r for r in pending_rows if r.get("status") == "PENDING"]

        if not rows:
            print(f"Stats: no closed signals yet. pending={len(pending_active)}")
            return

        total = len(rows)
        win_tp1 = sum(1 for r in rows if r.get("outcome") == "WIN_TP1")
        win_tp2 = sum(1 for r in rows if r.get("outcome") == "WIN_TP2")
        stop = sum(1 for r in rows if r.get("outcome") == "LOSS_STOP")
        expired = sum(1 for r in rows if r.get("outcome") == "EXPIRED")

        r_values = []
        for r in rows:
            try:
                r_values.append(float(r.get("r_multiple", 0)))
            except Exception:
                pass
        expectancy = sum(r_values) / len(r_values) if r_values else 0.0

        print(
            f"Stats | total={total} | pending={len(pending_active)} | TP1={win_tp1} ({win_tp1/total:.1%}) "
            f"| TP2={win_tp2} ({win_tp2/total:.1%}) | STOP={stop} ({stop/total:.1%}) "
            f"| EXPIRED={expired} ({expired/total:.1%}) | avgR={expectancy:.3f}"
        )

        self.print_breakdown(rows, label="side", key_fn=lambda r: r.get("side", "UNKNOWN"))
        self.print_breakdown(rows, label="strategy", key_fn=lambda r: r.get("strategy", "unknown"))
        self.print_breakdown(rows, label="btc_regime", key_fn=lambda r: r.get("btc_regime", "unknown"))
        self.print_strategy_pipeline_summary()
        self.print_pending_reason_breakdown()
        self.print_pending_age_summary()
        self.print_score_component_summary()
        self.print_result_breakdown_by_score_bucket()
        self.print_outcome_breakdown_by_strategy_side()
        self.print_manual_trading_diagnostics()
        manual_yes = sum(1 for r in rows if r.get("manual_tradable") == "yes")
        print(f"Manual summary | tradable_yes={manual_yes}/{total} ({manual_yes/max(total,1):.1%})")

    def print_breakdown(self, rows: List[Dict], label: str, key_fn):
        groups: Dict[str, List[Dict]] = {}
        for row in rows:
            key = key_fn(row)
            groups.setdefault(key, []).append(row)

        for key in sorted(groups.keys()):
            grp = groups[key]
            total = len(grp)
            tp1 = sum(1 for r in grp if r.get("outcome") == "WIN_TP1")
            tp2 = sum(1 for r in grp if r.get("outcome") == "WIN_TP2")
            stop = sum(1 for r in grp if r.get("outcome") == "LOSS_STOP")
            expired = sum(1 for r in grp if r.get("outcome") == "EXPIRED")
            r_vals = []
            for r in grp:
                try:
                    r_vals.append(float(r.get("r_multiple", 0)))
                except Exception:
                    pass
            avg_r = sum(r_vals) / len(r_vals) if r_vals else 0.0
            manual_yes = sum(1 for r in grp if r.get("manual_tradable") == "yes")
            print(
                f"Breakdown[{label}={key}] | total={total} | TP1={tp1} | TP2={tp2} | STOP={stop} "
                f"| EXPIRED={expired} | avgR={avg_r:.3f} | manual_yes={manual_yes}/{total}"
            )

    def _to_float(self, value, default: float = 0.0) -> float:
        try:
            if value in (None, ""):
                return default
            return float(value)
        except Exception:
            return default

    def _to_int(self, value, default: int = 0) -> int:
        try:
            if value in (None, ""):
                return default
            return int(float(value))
        except Exception:
            return default

    def _observability_cfg(self) -> Dict:
        return self.cfg.get("observability", {})

    def _signal_score_map(self) -> Dict[str, float]:
        out = {}
        for row in self.read_csv(self.signals_file):
            sid = row.get("signal_id", "")
            if sid:
                out[sid] = self._to_float(row.get("score"), 0.0)
        return out

    def print_strategy_pipeline_summary(self):
        cfg = self._observability_cfg()
        if not cfg.get("enabled", True):
            return

        pending_rows = self.read_csv(self.pending_file)
        signal_rows = self.read_csv(self.signals_file)
        result_rows = self.read_csv(self.results_file)

        groups: Dict[str, Dict[str, int]] = {}

        def ensure(strategy: str, side: str):
            key = f"{strategy}|{side}"
            groups.setdefault(key, {
                "detected": 0,
                "pending": 0,
                "confirmed": 0,
                "invalidated": 0,
                "expired": 0,
                "signals": 0,
                "open": 0,
                "closed": 0,
            })
            return groups[key]

        for row in pending_rows:
            strategy = row.get("strategy", "unknown") or "unknown"
            side = row.get("side", "UNKNOWN") or "UNKNOWN"
            status = (row.get("status", "") or "").upper()
            g = ensure(strategy, side)
            g["detected"] += 1
            if status == "PENDING":
                g["pending"] += 1
            elif status == "CONFIRMED":
                g["confirmed"] += 1
            elif status == "INVALIDATED":
                g["invalidated"] += 1
            elif status == "EXPIRED_WAIT":
                g["expired"] += 1

        for row in signal_rows:
            strategy = row.get("strategy", "unknown") or "unknown"
            side = row.get("side", "UNKNOWN") or "UNKNOWN"
            status = (row.get("status", "") or "").upper()
            g = ensure(strategy, side)
            g["signals"] += 1
            if status == "OPEN":
                g["open"] += 1

        for row in result_rows:
            strategy = row.get("strategy", "unknown") or "unknown"
            side = row.get("side", "UNKNOWN") or "UNKNOWN"
            g = ensure(strategy, side)
            g["closed"] += 1

        for key in sorted(groups.keys()):
            strategy, side = key.split("|", 1)
            g = groups[key]
            print(
                f"Pipeline[{strategy}][{side}] | detected={g['detected']} | pending={g['pending']} "
                f"| confirmed={g['confirmed']} | invalidated={g['invalidated']} | expired={g['expired']} "
                f"| signals={g['signals']} | open={g['open']} | closed={g['closed']}"
            )

    def print_pending_reason_breakdown(self):
        cfg = self._observability_cfg()
        if not cfg.get("enabled", True):
            return

        groups: Dict[str, int] = {}
        bad_schema_rows = 0

        try:
            with open(self.pending_file, "r", newline="", encoding="utf-8") as f:
                reader = csv.reader(f)
                header = next(reader, None) or []
                if not header:
                    return

                idx = {name: i for i, name in enumerate(header)}

                def cell(row_vals, name, default=""):
                    i = idx.get(name)
                    if i is None:
                        return default
                    return row_vals[i].strip() if i < len(row_vals) else default

                for row_vals in reader:
                    if not row_vals:
                        continue
                    # tolerate short/long rows by padding/truncating to header width
                    if len(row_vals) < len(header):
                        row_vals = row_vals + [""] * (len(header) - len(row_vals))
                    elif len(row_vals) > len(header):
                        row_vals = row_vals[:len(header)]

                    strategy = cell(row_vals, "strategy", "unknown") or "unknown"
                    side = (cell(row_vals, "side", "UNKNOWN") or "UNKNOWN").upper()
                    status = (cell(row_vals, "status", "") or "").upper()
                    reason = cell(row_vals, "close_reason", "")

                    if status not in VALID_PENDING_STATUSES:
                        bad_schema_rows += 1
                        status = "UNKNOWN_SCHEMA"

                    if not reason:
                        reason = "active" if status == "PENDING" else "unknown"

                    key = f"{strategy}|{side}|{status}|{reason}"
                    groups[key] = groups.get(key, 0) + 1
        except FileNotFoundError:
            return

        for key in sorted(groups.keys()):
            strategy, side, status, reason = key.split("|", 3)
            print(f"PendingReason[{strategy}][{side}] | status={status} | reason={reason} | total={groups[key]}")
        if bad_schema_rows:
            print(f"PendingReason[SCHEMA] | bad_rows={bad_schema_rows}")

    def print_pending_age_summary(self):
        cfg = self._observability_cfg()
        if not cfg.get("enabled", True):
            return

        rows = [r for r in self.read_csv(self.pending_file) if self._safe_pending_status(r) == "PENDING"]
        if not rows:
            print("PendingAge | active=0")
            return

        waited = [self._to_int(r.get("bars_waited"), 0) for r in rows]
        avg_waited = sum(waited) / max(len(waited), 1)
        oldest = min(rows, key=lambda r: self._to_int(r.get("created_ts_ms"), 0))
        print(
            f"PendingAge | active={len(rows)} | avg_bars_waited={avg_waited:.2f} "
            f"| oldest={oldest.get('symbol','')} {oldest.get('side','')} {oldest.get('strategy','')} "
            f"| oldest_bars_waited={self._to_int(oldest.get('bars_waited'), 0)}"
        )

    def print_result_breakdown_by_score_bucket(self):
        cfg = self._observability_cfg()
        if not cfg.get("enabled", True):
            return

        score_map = self._signal_score_map()
        rows = self.read_csv(self.results_file)
        if not rows:
            return

        buckets = {
            "70-79": [],
            "80-89": [],
            "90+": [],
            "<70": [],
        }
        for row in rows:
            score = score_map.get(row.get("signal_id", ""), 0.0)
            if score >= 90:
                buckets["90+"].append((row, score))
            elif score >= 80:
                buckets["80-89"].append((row, score))
            elif score >= 70:
                buckets["70-79"].append((row, score))
            else:
                buckets["<70"].append((row, score))

        for label in ["<70", "70-79", "80-89", "90+"]:
            grp = buckets[label]
            if not grp:
                continue
            total = len(grp)
            tp1 = sum(1 for r, _ in grp if r.get("outcome") == "WIN_TP1")
            tp2 = sum(1 for r, _ in grp if r.get("outcome") == "WIN_TP2")
            stop = sum(1 for r, _ in grp if r.get("outcome") == "LOSS_STOP")
            expired = sum(1 for r, _ in grp if r.get("outcome") == "EXPIRED")
            avg_r = sum(self._to_float(r.get("r_multiple"), 0.0) for r, _ in grp) / max(total, 1)
            print(
                f"ScoreBucket[{label}] | total={total} | TP1={tp1} | TP2={tp2} | STOP={stop} | EXPIRED={expired} | avgR={avg_r:.3f}"
            )

    def _infer_strategy_from_row(self, row: Dict) -> str:
        strategy = (row.get("strategy", "") or "").strip()
        if strategy:
            return strategy
        reason = (row.get("reason", "") or row.get("manual_trade_note", "") or "").lower()
        side = (row.get("side", "") or "").upper()
        if "exhaustion" in reason or "retest fail" in reason or side == "SHORT":
            return "short_exhaustion_retest" if side == "SHORT" else "unknown"
        return "unknown"

    def _row_has_score_parts(self, row: Dict) -> bool:
        return any(str(row.get(k, "")).strip() for k in ("score_oi", "score_exhaustion", "score_breakout", "score_retest"))

    def _results_with_signal_context(self) -> List[Dict]:
        results = self.read_csv(self.results_file)
        if not results:
            return []
        signal_lookup = {r.get("signal_id", ""): r for r in self.read_csv(self.signals_file)}
        merged: List[Dict] = []
        for row in results:
            sig = signal_lookup.get(row.get("signal_id", ""), {})
            merged_row = dict(row)
            for key in [
                "score", "strategy", "side", "symbol", "btc_regime", "manual_tradable", "manual_trade_note",
                "score_oi", "score_exhaustion", "score_breakout", "score_retest", "reason_tags",
                "sl_distance_pct", "tp1_distance_pct", "tp2_distance_pct", "stop_was_forced_min_risk"
            ]:
                if (not str(merged_row.get(key, "")).strip()) and key in sig:
                    merged_row[key] = sig.get(key, "")
            merged_row["strategy"] = self._infer_strategy_from_row(merged_row)
            merged_row["has_score_parts"] = "yes" if self._row_has_score_parts(merged_row) else "no"
            merged.append(merged_row)
        return merged

    def print_score_component_summary(self):
        cfg = self._observability_cfg()
        if not cfg.get("enabled", True):
            return
        rows = self._results_with_signal_context()
        if not rows:
            return
        groups: Dict[str, List[Dict]] = {}
        for row in rows:
            strategy = row.get('strategy', 'unknown') or 'unknown'
            outcome = row.get('outcome', 'UNKNOWN') or 'UNKNOWN'
            key = f"{strategy}|{outcome}"
            groups.setdefault(key, []).append(row)
        for key in sorted(groups.keys()):
            strategy, outcome = key.split("|", 1)
            grp = groups[key]
            scored_grp = [r for r in grp if str(r.get("has_score_parts", "")).lower() == "yes"]
            missing_parts = len(grp) - len(scored_grp)
            if not scored_grp:
                print(
                    f"ScoreParts[{strategy}][{outcome}] | total={len(grp)} | scored_rows=0/{len(grp)} | "
                    f"missing_score_parts={missing_parts}"
                )
                continue
            oi_vals = [self._to_float(r.get("score_oi"), 0.0) for r in scored_grp]
            exh_vals = [self._to_float(r.get("score_exhaustion"), 0.0) for r in scored_grp]
            bo_vals = [self._to_float(r.get("score_breakout"), 0.0) for r in scored_grp]
            rt_vals = [self._to_float(r.get("score_retest"), 0.0) for r in scored_grp]
            total_score_vals = [self._to_float(r.get("score"), 0.0) for r in scored_grp if str(r.get("score", "")).strip()]
            avg_oi = sum(oi_vals) / max(len(oi_vals), 1)
            avg_exh = sum(exh_vals) / max(len(exh_vals), 1)
            avg_bo = sum(bo_vals) / max(len(bo_vals), 1)
            avg_rt = sum(rt_vals) / max(len(rt_vals), 1)
            pieces = [
                f"ScoreParts[{strategy}][{outcome}]",
                f"total={len(grp)}",
                f"scored_rows={len(scored_grp)}/{len(grp)}",
                f"missing_score_parts={missing_parts}",
            ]
            if total_score_vals:
                avg_total = sum(total_score_vals) / len(total_score_vals)
                pieces.append(f"avgTotal={avg_total:.1f}")
            else:
                pieces.append("avgTotal=n/a")
            pieces.extend([
                f"oi={avg_oi:.1f}",
                f"exhaustion={avg_exh:.1f}",
                f"breakout={avg_bo:.1f}",
                f"retest={avg_rt:.1f}",
            ])
            print(" | ".join(pieces))

    def print_manual_trading_diagnostics(self):
        cfg = self._observability_cfg()
        if not cfg.get("enabled", True):
            return
        rows = self._results_with_signal_context()
        if not rows:
            return
        total = len(rows)
        forced = [r for r in rows if (r.get("stop_was_forced_min_risk", "") or "").lower() == "yes"]
        avg_sl = sum(self._to_float(r.get("sl_distance_pct"), 0.0) for r in rows) / max(total, 1)
        avg_tp1 = sum(self._to_float(r.get("tp1_distance_pct"), 0.0) for r in rows) / max(total, 1)
        avg_tp2 = sum(self._to_float(r.get("tp2_distance_pct"), 0.0) for r in rows) / max(total, 1)
        avg_mfe = sum(self._to_float(r.get("mfe_pct"), 0.0) for r in rows) / max(total, 1)
        avg_mae = sum(self._to_float(r.get("mae_pct"), 0.0) for r in rows) / max(total, 1)

        mr = self.cfg.get("manual_review", {})
        min_sl = float(mr.get("min_sl_distance_pct", 2.0))
        min_tp1 = float(mr.get("min_tp1_distance_pct", 2.5))
        min_rr = float(mr.get("min_risk_reward_for_manual", 1.0))
        sl_too_small = [r for r in rows if self._to_float(r.get("sl_distance_pct"), 0.0) < min_sl]
        tp1_too_small = [r for r in rows if self._to_float(r.get("tp1_distance_pct"), 0.0) < min_tp1]
        rr_too_low = [r for r in rows if self._to_float(r.get("tp1_distance_pct"), 0.0) / max(self._to_float(r.get("sl_distance_pct"), 0.0), 1e-12) < min_rr]
        manual_yes = [r for r in rows if (r.get("manual_tradable", "") or "").lower() == "yes"]
        manual_no = [r for r in rows if (r.get("manual_tradable", "") or "").lower() == "no"]

        print(
            f"ManualDiag | total={total} | forced_stop={len(forced)} ({len(forced)/max(total,1):.1%}) | "
            f"avgSL={avg_sl:.2f}% | avgTP1={avg_tp1:.2f}% | avgTP2={avg_tp2:.2f}% | avgMFE={avg_mfe:.2f}% | avgMAE={avg_mae:.2f}%"
        )
        print(
            f"ManualDiag[thresholds] | minSL={min_sl:.2f}% | minTP1={min_tp1:.2f}% | minRR={min_rr:.2f} | "
            f"sl_too_small={len(sl_too_small)} ({len(sl_too_small)/max(total,1):.1%}) | "
            f"tp1_too_small={len(tp1_too_small)} ({len(tp1_too_small)/max(total,1):.1%}) | "
            f"rr_too_low={len(rr_too_low)} ({len(rr_too_low)/max(total,1):.1%}) | "
            f"manual_yes={len(manual_yes)} ({len(manual_yes)/max(total,1):.1%}) | manual_no={len(manual_no)} ({len(manual_no)/max(total,1):.1%})"
        )

        bucket_defs = [
            ("sl<2", lambda r: self._to_float(r.get("sl_distance_pct"), 0.0) < 2.0),
            ("sl_2_3", lambda r: 2.0 <= self._to_float(r.get("sl_distance_pct"), 0.0) < 3.0),
            ("sl>=3", lambda r: self._to_float(r.get("sl_distance_pct"), 0.0) >= 3.0),
            ("tp1<2.5", lambda r: self._to_float(r.get("tp1_distance_pct"), 0.0) < 2.5),
            ("tp1_2.5_5", lambda r: 2.5 <= self._to_float(r.get("tp1_distance_pct"), 0.0) < 5.0),
            ("tp1>=5", lambda r: self._to_float(r.get("tp1_distance_pct"), 0.0) >= 5.0),
        ]
        for label, fn in bucket_defs:
            grp = [r for r in rows if fn(r)]
            if not grp:
                continue
            avg_r = sum(self._to_float(r.get("r_multiple"), 0.0) for r in grp) / max(len(grp), 1)
            wins = sum(1 for r in grp if r.get("outcome") in ("WIN_TP1", "WIN_TP2"))
            print(f"ManualDiag[{label}] | total={len(grp)} | win={wins} ({wins/max(len(grp),1):.1%}) | avgR={avg_r:.3f}")

        groups: Dict[str, List[Dict]] = {}
        for row in rows:
            strategy = row.get('strategy', 'unknown') or 'unknown'
            side = row.get('side', 'UNKNOWN') or 'UNKNOWN'
            groups.setdefault(f"{strategy}|{side}", []).append(row)
        for key in sorted(groups.keys()):
            strategy, side = key.split("|", 1)
            grp = groups[key]
            avg_sl_g = sum(self._to_float(r.get("sl_distance_pct"), 0.0) for r in grp) / max(len(grp), 1)
            avg_tp1_g = sum(self._to_float(r.get("tp1_distance_pct"), 0.0) for r in grp) / max(len(grp), 1)
            avg_tp2_g = sum(self._to_float(r.get("tp2_distance_pct"), 0.0) for r in grp) / max(len(grp), 1)
            avg_r_g = sum(self._to_float(r.get("r_multiple"), 0.0) for r in grp) / max(len(grp), 1)
            manual_yes_g = sum(1 for r in grp if (r.get("manual_tradable", "") or "").lower() == "yes")
            print(
                f"ManualDiag[{strategy}][{side}] | total={len(grp)} | avgSL={avg_sl_g:.2f}% | avgTP1={avg_tp1_g:.2f}% | "
                f"avgTP2={avg_tp2_g:.2f}% | avgR={avg_r_g:.3f} | manual_yes={manual_yes_g}/{len(grp)}"
            )

    def print_outcome_breakdown_by_strategy_side(self):
        cfg = self._observability_cfg()
        if not cfg.get("enabled", True):
            return
        rows = self.read_csv(self.results_file)
        if not rows:
            return
        groups: Dict[str, List[Dict]] = {}
        for row in rows:
            key = f"{row.get('strategy','unknown')}|{row.get('side','UNKNOWN')}"
            groups.setdefault(key, []).append(row)
        for key in sorted(groups.keys()):
            strategy, side = key.split("|", 1)
            grp = groups[key]
            total = len(grp)
            avg_r = sum(self._to_float(r.get("r_multiple"), 0.0) for r in grp) / max(total, 1)
            avg_mfe = sum(self._to_float(r.get("mfe_pct"), 0.0) for r in grp) / max(total, 1)
            avg_mae = sum(self._to_float(r.get("mae_pct"), 0.0) for r in grp) / max(total, 1)
            print(f"Outcome[{strategy}][{side}] | total={total} | avgR={avg_r:.3f} | avgMFE={avg_mfe:.2f}% | avgMAE={avg_mae:.2f}%")

    def _write_runtime_build_marker(self):
        try:
            marker = Path(self.cfg.get("runtime", {}).get("code_version_file", "RUNNING_CODE_VERSION.txt"))
            if not marker.is_absolute():
                marker = Path(__file__).resolve().parent / marker
            marker.write_text(
                "\n".join([
                    f"code_build_id={CODE_BUILD_ID}",
                    f"code_build_source={CODE_BUILD_SOURCE}",
                    f"code_build_note={CODE_BUILD_NOTE}",
                    f"config_version={self.cfg.get('config_version', '')}",
                    f"written_at_utc={datetime.now(timezone.utc).isoformat()}",
                ]) + "\n",
                encoding="utf-8",
            )
        except Exception as e:
            print(f"[startup warn] failed to write runtime build marker: {e}")

    def startup_print(self, config_path: str):
        print("=" * 72)
        print("BINANCE OI RETEST BOT STARTING")
        print(f"[startup] config_path={config_path}")
        print(f"[startup] config_version={self.cfg.get('config_version', '')}")
        print(f"[startup] code_build_id={CODE_BUILD_ID}")
        print(f"[startup] code_build_source={CODE_BUILD_SOURCE}")
        print(f"[startup] code_build_note={CODE_BUILD_NOTE}")
        print(f"[startup] telegram.chat_id={self.cfg['telegram']['chat_id']}")
        print(f"[startup] scanner.loop_seconds={self.cfg['scanner']['loop_seconds']}")
        print(f"[startup] scanner.top_n={self.cfg['scanner']['top_n']}")
        print(f"[startup] retest.retest_max_bars={self.cfg['retest']['retest_max_bars']}")
        print(f"[startup] retest.stop_buffer_pct={self.cfg['retest']['stop_buffer_pct']}")
        print(f"[startup] retest.max_deep_retest_pct={self.cfg['retest'].get('max_deep_retest_pct')}")
        print(f"[startup] risk.min_risk_pct={self.cfg['risk']['min_risk_pct']}")
        print(f"[startup] risk.tp1_r_multiple={self.cfg['risk']['tp1_r_multiple']}")
        print(f"[startup] risk.tp2_r_multiple={self.cfg['risk']['tp2_r_multiple']}")
        print(f"[startup] tracking.max_bars_after_entry={self.cfg['tracking']['max_bars_after_entry']}")
        print(f"[startup] strategy.long_accumulation_continuation.enabled={self.cfg.get('strategy', {}).get('long_accumulation_continuation', {}).get('enabled', False)}")
        print(f"[startup] btc_sentiment.bullish_threshold_pct={self.cfg.get('btc_sentiment', {}).get('bullish_threshold_pct', 1.0)}")
        print(f"[startup] btc_sentiment.bearish_threshold_pct={self.cfg.get('btc_sentiment', {}).get('bearish_threshold_pct', -1.0)}")
        print(f"[startup] observability.enabled={self.cfg.get('observability', {}).get('enabled', True)}")
        print(f"[startup] storage.data_dir={self.data_dir}")
        print(f"[startup] storage.pending_dir={self.pending_dir}")
        print(f"[startup] storage.signals_dir={self.signals_dir}")
        print(f"[startup] storage.results_dir={self.results_dir}")
        print(f"[startup] observability.score_component_summary={self.cfg.get('observability', {}).get('score_component_summary', True)}")
        print("=" * 72)

    def scan_once(self):
        self.round_idx += 1
        now_ms = int(time.time() * 1000)
        print(f"[round start] idx={self.round_idx} time_ms={now_ms}")

        # Reset daily ORB sent-today set at UTC midnight, repopulate from signals CSV
        _today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if _today_str != self._orb_sent_today_date:
            self._orb_sent_today.clear()
            self._orb_sent_today_date = _today_str
            _today_start_ms = int(datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)
            try:
                for _row in self.read_csv(self.signals_file):
                    if _row.get("strategy") == "oi_range_breakout":
                        try:
                            _ts = int(float(_row.get("timestamp_ms") or 0))
                            if _ts >= _today_start_ms:
                                self._orb_sent_today.add(_row.get("symbol", ""))
                        except (ValueError, TypeError):
                            pass
            except Exception:
                pass
            print(f"[orb dedup] daily sent-set reset for {_today_str}, loaded={len(self._orb_sent_today)} from signals")

        # Reset daily acc_cont sent-today set at UTC midnight, repopulate from signals CSV
        if _today_str != self._acc_sent_today_date:
            self._acc_sent_today.clear()
            self._acc_sent_today_date = _today_str
            _today_start_ms = int(datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)
            try:
                for _row in self.read_csv(self.signals_file):
                    if _row.get("strategy") == "long_accumulation_continuation":
                        try:
                            _ts = int(float(_row.get("timestamp_ms") or 0))
                            if _ts >= _today_start_ms:
                                self._acc_sent_today.add(_row.get("symbol", ""))
                        except (ValueError, TypeError):
                            pass
            except Exception:
                pass
            print(f"[acc dedup] daily sent-set reset for {_today_str}, loaded={len(self._acc_sent_today)} from signals")

        # Refresh top-N market cap exclusion list once per day
        n_exclude = int(self.cfg.get("scanner", {}).get("top_marketcap_exclude_n", 100))
        if n_exclude > 0 and (time.time() - self._top_marketcap_fetched_ts) > 86400:
            self._top_marketcap_exclude = self._fetch_top_marketcap_symbols(n_exclude)
            self._top_marketcap_fetched_ts = time.time()

        tickers = self.load_24h_tickers()
        symbols = self.filter_symbols(self.load_symbols(), tickers)
        self.build_market_snapshot(symbols, tickers)
        regime = classify_regime(self, now_ms=now_ms)
        self.current_regime = regime
        print(
            f"Regime | label={regime.regime_label} | conf={regime.regime_confidence} | "
            f"long_fit={regime.regime_fit_long_breakout} | "
            f"note={regime.regime_note}"
        )

        self.evaluate_open_signals()
        confirmed = self.process_pending_setups()
        sent_count = 0
        skipped_top_n = 0

        if confirmed:
            confirmed.sort(key=lambda x: x.score, reverse=True)
            # Dispatch-level dedup: drop duplicate same-symbol signals per strategy.
            _seen_orb: set = set()
            _seen_acc: set = set()
            _deduped: list = []
            for _s in confirmed:
                _strat = getattr(_s, "strategy", "")
                if _strat == "oi_range_breakout":
                    if _s.symbol in _seen_orb:
                        print(f"[dispatch dedup] {_s.symbol} ORB — duplicate dropped")
                        continue
                    _seen_orb.add(_s.symbol)
                elif _strat == "long_accumulation_continuation":
                    if _s.symbol in _seen_acc:
                        print(f"[dispatch dedup] {_s.symbol} acc_cont — duplicate dropped")
                        continue
                    _seen_acc.add(_s.symbol)
                _deduped.append(_s)
            confirmed = _deduped

            top_n = int(self.cfg["scanner"]["top_n"])
            dispatch_floor_score = float(self.cfg.get("dispatch", {}).get("dispatch_floor_score", 70.0))
            send_watchlist = bool(self.cfg.get("telegram", {}).get("send_watchlist_signals", False))
            main_count = 0
            watchlist_count = 0
            no_send_count = 0
            for rank, s in enumerate(confirmed):
                dispatch = route_dispatch_v1(s, rank=rank, top_n=top_n, dispatch_floor_score=dispatch_floor_score)
                s.dispatch_action = dispatch.dispatch_action
                s.dispatch_confidence_band = dispatch.dispatch_confidence_band
                s.dispatch_reason = dispatch.dispatch_reason
                if getattr(s, "strategy", "") == "oi_range_breakout":
                    _orb_cfg = self.cfg.get("oi_range_breakout", {})
                    _atr_threshold = float(_orb_cfg.get("atr_bonus_threshold", 1.0))
                    _regime = getattr(s, "regime_label", "unclear_mixed") or "unclear_mixed"
                    _base_mult = _ORB_REGIME_MULT.get(_regime, 1.00)
                    _atr_bonus = _orb_atr_bonus(float(getattr(s, "atr_ratio", 0.0)), _atr_threshold)
                    _final_mult = _base_mult + _atr_bonus
                    _raw = float(s.score)
                    s.score = min(int(_raw * _final_mult), 100)
                    print(
                        f"[orb score adj] {s.symbol} "
                        f"raw={_raw:.0f} × {_final_mult:.2f} "
                        f"(regime={_base_mult} + atr={_atr_bonus}) "
                        f"label={_regime} atr_ratio={getattr(s, 'atr_ratio', 0):.2f} "
                        f"→ adj={s.score}"
                    )
                    _ORB_MIN_SCORE = 35
                    if s.score < _ORB_MIN_SCORE:
                        print(
                            f"[orb score gate] {s.symbol} "
                            f"score={s.score} < min={_ORB_MIN_SCORE} → NO_SEND"
                        )
                        s.dispatch_action = "NO_SEND"
                        s.dispatch_confidence_band = "LOW"
                        s.dispatch_reason = f"{s.dispatch_reason}|orb_score_below_min_{_ORB_MIN_SCORE}"
                        self._update_pending_dispatch_trace(
                            s.setup_id,
                            dispatch_action="NO_SEND",
                            dispatch_confidence_band="LOW",
                            dispatch_reason=s.dispatch_reason,
                            send_decision="NO_SEND",
                            skip_reason=f"orb_score_below_min_{_ORB_MIN_SCORE}",
                        )
                        no_send_count += 1
                        continue

                if s.dispatch_action == "MAIN_SIGNAL":
                    # Belt-and-suspenders: daily dedup per strategy
                    if getattr(s, "strategy", "") == "oi_range_breakout":
                        if s.symbol in self._orb_sent_today:
                            print(f"[orb dedup] {s.symbol} — already sent today, skipped")
                            no_send_count += 1
                            continue
                        self._orb_sent_today.add(s.symbol)
                    elif getattr(s, "strategy", "") == "long_accumulation_continuation":
                        if s.symbol in self._acc_sent_today:
                            print(f"[acc dedup] {s.symbol} — already sent today, skipped")
                            no_send_count += 1
                            continue
                        self._acc_sent_today.add(s.symbol)
                    self.save_signal(s)
                    self._update_pending_dispatch_trace(
                        s.setup_id,
                        dispatch_action=dispatch.dispatch_action,
                        dispatch_confidence_band=dispatch.dispatch_confidence_band,
                        dispatch_reason=dispatch.dispatch_reason,
                        send_decision="SENT",
                        skip_reason="",
                    )
                    msg = self.format_signal(s)
                    print("\n" + msg + "\n")
                    if self.should_send(s):
                        try:
                            self.telegram_send(msg)
                            sent_count += 1
                        except Exception as e:
                            print(f"[telegram error] {e}")
                    main_count += 1
                    continue

                if s.dispatch_action == "WATCHLIST":
                    skipped_top_n += 1
                    self._update_pending_dispatch_trace(
                        s.setup_id,
                        dispatch_action=dispatch.dispatch_action,
                        dispatch_confidence_band=dispatch.dispatch_confidence_band,
                        dispatch_reason=dispatch.dispatch_reason,
                        send_decision="WATCHLIST",
                        skip_reason="dispatch_watchlist",
                    )
                    msg = self.format_watchlist_signal(s)
                    print("\n" + msg + "\n")
                    if send_watchlist and self.should_send(s):
                        try:
                            self.telegram_send(msg)
                        except Exception as e:
                            print(f"[telegram watchlist error] {e}")
                    watchlist_count += 1
                    continue

                self._update_pending_dispatch_trace(
                    s.setup_id,
                    dispatch_action=dispatch.dispatch_action,
                    dispatch_confidence_band=dispatch.dispatch_confidence_band,
                    dispatch_reason=dispatch.dispatch_reason,
                    send_decision="NO_SEND",
                    skip_reason="dispatch_no_send",
                )
                print(f"[confirm no_send] {s.symbol} {s.side} score={s.score:.2f} reason={dispatch.dispatch_reason}")
                no_send_count += 1

            print(
                f"DispatchSummary | main_signal={main_count} | watchlist={watchlist_count} | no_send={no_send_count} | sent={sent_count}"
            )

        self.print_stats()

        new_pending = 0

        for sym in symbols:
            try:
                # Fetch 5m OI history (13 bars = 65 min span) for oi_range_breakout and long_accumulation_continuation
                oi_1h_history = None
                _bybit_oi_ok = False
                _bybit_aligned_coins: list = []
                _need_oi_hist = (
                    self.cfg.get("strategy", {}).get("oi_range_breakout", {}).get("enabled", False)
                    or self.cfg.get("strategy", {}).get("long_accumulation_continuation", {}).get("enabled", False)
                )
                if _need_oi_hist:
                    try:
                        oi_binance = self.oi_hist(sym, "5m", 13)
                    except Exception as e:
                        print(f"[scan_once] {sym} — failed to fetch Binance OI: {e}")
                        oi_binance = None

                    if oi_binance:
                        if self.cfg.get("bybit", {}).get("enabled", True):
                            _bybit_raw = self.bybit_oi_hist(sym)
                            if len(_bybit_raw) >= 13:
                                # current price from tickers dict (already loaded this round)
                                _cur_price = float(tickers.get(sym, {}).get("lastPrice", 0) or 0)
                                if _cur_price > 0:
                                    # Use combine only for timestamp alignment; oi_1h_history stays Binance-only
                                    _, _bybit_aligned_coins = self._combine_oi_histories(
                                        oi_binance, _bybit_raw, _cur_price
                                    )
                                    oi_1h_history = oi_binance
                                    _bybit_oi_ok = True
                                    print(f"[bybit OI] {sym} — MAX formula ready (bybit aligned)")
                                else:
                                    oi_1h_history = oi_binance
                            else:
                                oi_1h_history = oi_binance
                        else:
                            oi_1h_history = oi_binance

                setups = self.build_pending_setups_for_symbol(
                    sym, oi_1h_history,
                    bybit_oi_ok=_bybit_oi_ok,
                    bybit_aligned_coins=_bybit_aligned_coins,
                )
                for p in setups:
                    p.regime_label = regime.regime_label
                    if p.side == "LONG":
                        p.regime_fit_for_strategy = regime.regime_fit_long_breakout
                    else:
                        p.regime_fit_for_strategy = "MEDIUM"
                    # Data plumbing fix: pass round-level breadth into PendingSetup.
                    # get_btc_context() always returns 0.0 for breadth; the real value
                    # is computed in build_market_snapshot() and stored in current_market_snapshot.
                    p.alt_market_breadth_pct = float(
                        self.current_market_snapshot.get("alt_market_breadth_pct", 0.0) or 0.0
                    )
                    self.save_pending(p)
                    new_pending += 1
                    print(
                        f"[pending] {p.symbol} {p.side} | strategy={p.strategy} | breakout={p.breakout_level:.6g} "
                        f"| signal_price={p.signal_price:.6g} | oi={p.oi_jump_pct:.2f}% "
                        f"| vol={p.vol_ratio:.2f}x | btc24h={p.btc_24h_change_pct:+.2f}% ({p.btc_regime})"
                    )
            except Exception as e:
                print(f"[warn] {sym}: {e}")

        if not confirmed and new_pending == 0:
            print("No valid setups this round.")

        pending_active = sum(1 for r in self.read_csv(self.pending_file) if r.get("status") == "PENDING")
        open_signals = sum(1 for r in self.read_csv(self.signals_file) if r.get("status") == "OPEN")
        print(
            f"[round summary] idx={self.round_idx} | scanned={len(symbols)} | confirmed={len(confirmed)} "
            f"| sent={sent_count} | skipped_top_n={skipped_top_n} | new_pending={new_pending} "
            f"| pending_now={pending_active} | open_now={open_signals}"
        )


    def run_simulation_case(self, case: str):
        case = case.strip().lower()
        valid = {"short_tp1", "short_tp2", "short_sl", "long_tp1", "long_tp2", "long_sl"}
        if case not in valid:
            raise ValueError(f"unsupported simulate case: {case}")

        now_ms = int(time.time() * 1000)
        side = "LONG" if case.startswith("long_") else "SHORT"
        symbol = "SIMLONGUSDT" if side == "LONG" else "SIMSHORTUSDT"
        pending_id = f"SIMCASE-{symbol}-{side}-{now_ms}"
        setup_id = pending_id
        outcome_map = {
            "short_tp1": ("WIN_TP1", 1.0, "simulated_short_tp1"),
            "short_tp2": ("WIN_TP2", 2.0, "simulated_short_tp2"),
            "short_sl": ("LOSS_SL", -1.0, "simulated_short_sl"),
            "long_tp1": ("WIN_TP1", 1.0, "simulated_long_tp1"),
            "long_tp2": ("WIN_TP2", 2.0, "simulated_long_tp2"),
            "long_sl": ("LOSS_SL", -1.0, "simulated_long_sl"),
        }
        outcome, r_multiple, close_reason = outcome_map[case]
        price = 100.0 if side == "LONG" else 50.0
        sl_pct = 0.03
        if side == "LONG":
            stop = price * (1 - sl_pct)
            tp1 = price * (1 + sl_pct)
            tp2 = price * (1 + 2 * sl_pct)
            breakout_level = price * 0.99
            strategy = "long_breakout_retest"
        else:
            stop = price * (1 + sl_pct)
            tp1 = price * (1 - sl_pct)
            tp2 = price * (1 - 2 * sl_pct)
            breakout_level = price * 1.01
            strategy = "short_exhaustion_retest"

        pending = PendingSetup(
            pending_id=pending_id,
            created_ts_ms=now_ms,
            signal_open_time=now_ms,
            symbol=symbol,
            side=side,
            score=88.0,
            confidence=0.91,
            reason=f"SIMULATED_{case.upper()}",
            breakout_level=breakout_level,
            signal_price=price,
            signal_high=max(price, breakout_level),
            signal_low=min(price, breakout_level),
            oi_jump_pct=1.2,
            funding_pct=0.01,
            vol_ratio=1.9,
            setup_id=setup_id,
            strategy=strategy,
            market_regime="simulated",
            btc_price=85000.0,
            btc_24h_change_pct=1.2,
            btc_4h_change_pct=0.4,
            btc_1h_change_pct=0.1,
            btc_24h_range_pct=3.0,
            btc_4h_range_pct=1.1,
            alt_market_breadth_pct=56.0,
            btc_regime="neutral",
            score_oi=1.5,
            score_exhaustion=1.2 if side == "SHORT" else 0.0,
            score_breakout=2.4,
            score_retest=4.5,
            reason_tags=f"simulated;case:{case}",
            status="PENDING",
            regime_label="unclear_mixed",
            regime_fit_for_strategy="MEDIUM",
        )
        self.save_pending(pending)
        self.close_pending(pending_id, "CONFIRMED", f"simulated_confirmed_{case}", 1)

        signal_id = f"SIMCASE-SIG-{symbol}-{side}-{now_ms}"
        signal = Signal(
            signal_id=signal_id,
            timestamp_ms=now_ms,
            symbol=symbol,
            side=side,
            score=88.0,
            confidence=0.91,
            reason=f"SIMULATED_{case.upper()} | full lifecycle simulation",
            breakout_level=breakout_level,
            entry_low=price,
            entry_high=price,
            entry_ref=price,
            stop=stop,
            tp1=tp1,
            tp2=tp2,
            price=price,
            oi_jump_pct=1.2,
            funding_pct=0.01,
            vol_ratio=1.9,
            retest_bars_waited=1,
            setup_id=setup_id,
            config_version="SIMULATED_SIGNAL_CASE_V4",
            strategy=strategy,
            market_regime="simulated",
            btc_price=85000.0,
            btc_24h_change_pct=1.2,
            btc_4h_change_pct=0.4,
            btc_1h_change_pct=0.1,
            btc_24h_range_pct=3.0,
            btc_4h_range_pct=1.1,
            alt_market_breadth_pct=56.0,
            btc_regime="neutral",
            risk_pct_real=0.8,
            sl_distance_pct=3.0,
            tp1_distance_pct=3.0,
            tp2_distance_pct=6.0,
            break_distance_pct=0.5,
            retest_depth_pct=0.2,
            score_oi=1.5,
            score_exhaustion=1.2 if side == "SHORT" else 0.0,
            score_breakout=2.4,
            score_retest=4.5,
            reason_tags=f"simulated;case:{case}",
            stop_was_forced_min_risk="no",
            manual_tradable="yes",
            manual_trade_note="simulated_runtime_case",
            status="OPEN",
        )
        self.save_signal(signal)
        signal_row = self._normalize_row_for_fields(asdict(signal), self.signal_fields)
        self.close_signal(
            signal_row,
            outcome,
            r_multiple,
            3,
            close_reason,
            mfe_pct=6.4 if r_multiple > 0 else 1.1,
            mae_pct=0.7 if r_multiple > 0 else 3.2,
        )
        print("SIMULATION_OK")
        print(f"pending_id={pending_id}")
        print(f"setup_id={setup_id}")
        print(f"signal_id={signal_id}")
        print(f"outcome={outcome}")

    def run_forever(self):
        import traceback
        import threading
        sec = int(self.cfg["scanner"]["loop_seconds"])

        while True:
            try:
                self.scan_once()
            except Exception as e:
                print(f"[fatal loop warn] {e}")
                traceback.print_exc()
            time.sleep(sec)


def load_config(path: str) -> Dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


_LOCK_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "oi_scanner.lock")
_lock_fd = None


def _acquire_single_instance_lock():
    global _lock_fd
    _lock_fd = open(_LOCK_PATH, "w")
    try:
        fcntl.flock(_lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        existing = ""
        try:
            with open(_LOCK_PATH) as _f:
                existing = _f.read().strip()
        except Exception:
            pass
        print(f"[startup] ERROR: another oi_scanner instance is already running "
              f"(lock held; pid in lockfile={existing}). Refusing to start a second "
              f"writer to avoid CSV corruption. Lock: {_LOCK_PATH}")
        raise SystemExit(1)
    _lock_fd.truncate(0)
    _lock_fd.write(str(os.getpid()))
    _lock_fd.flush()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("config_path", nargs="?", default="config.yaml")
    parser.add_argument("--simulate-case", dest="simulate_case", default="")
    args = parser.parse_args()
    _acquire_single_instance_lock()

    cfg = load_config(args.config_path)
    scanner = BinanceScanner(cfg)
    scanner._write_runtime_build_marker()
    scanner.startup_print(args.config_path)

    if args.simulate_case:
        scanner.run_simulation_case(args.simulate_case)
        return

    scanner.run_forever()


if __name__ == "__main__":
    main()
