import sys
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
import shutil
import subprocess

from scanner.strategies.long_breakout_retest import build_pending_long_setup as strategy_build_pending_long_setup
from scanner.strategies.short_exhaustion_retest import build_pending_short_exhaustion_setup as strategy_build_pending_short_exhaustion_setup
from scanner.dispatch.router import route_dispatch_v1
from scanner.regime.classifier import classify_regime
from scanner import lifecycle as lifecycle_mod
from scanner import review_service as review_svc
from regime.regime_normalizer import enrich_row_with_regime

try:
    from review_capture_runtime import CaseReviewRuntime
except Exception:
    CaseReviewRuntime = None

BASE_FAPI = "https://fapi.binance.com"
HEADERS = {"User-Agent": "Mozilla/5.0"}

CODE_BUILD_ID = "dispatch-trace-fix-2026-04-05"
CODE_BUILD_SOURCE = "version-marker-fix-on-live-file"
CODE_BUILD_NOTE = "Adds trustworthy runtime build marker and startup build logs so live code version is explicit."

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
    strategy: str = "legacy_5m_retest"
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
    strategy: str = "legacy_5m_retest"
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



class BinanceScanner:
    def __init__(self, config: Dict):
        self.cfg = config
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.sent_cache: Dict[str, float] = {}
        self.round_idx = 0
        self.current_market_snapshot: Dict[str, float] = {}
        self.current_regime = None

        self.project_dir = Path(__file__).resolve().parent
        storage_cfg = self.cfg.get("storage", {})
        self.data_dir = self.project_dir / str(storage_cfg.get("data_dir", "data"))
        self.pending_dir = self.data_dir / str(storage_cfg.get("pending_dir", "pending"))
        self.signals_dir = self.data_dir / str(storage_cfg.get("signals_dir", "signals"))
        self.results_dir = self.data_dir / str(storage_cfg.get("results_dir", "results"))
        self.review_candidates_dir = self.data_dir / str(storage_cfg.get("review_candidates_dir", "review_candidates"))
        self.market_review_dir = self.data_dir / str(storage_cfg.get("market_review_dir", "market_review"))
        self.include_legacy_flat_files = bool(storage_cfg.get("include_legacy_flat_files", False))

        self.signals_file = self.project_dir / "signals.csv"
        self.results_file = self.project_dir / "results.csv"
        self.pending_file = self.project_dir / "pending_setups.csv"
        self.review_candidates_file = self.project_dir / str(self.cfg.get("observability", {}).get("review_candidates_file", "review_candidates.csv"))
        self.market_review_file = self.project_dir / str(storage_cfg.get("market_review_index_file", "market_opportunity_review.csv"))
        self.snapshots_dir = self.project_dir / str(self.cfg.get("review_snapshots", {}).get("output_dir", "review_snapshots"))
        self.snapshot_index_file = self.project_dir / str(self.cfg.get("review_snapshots", {}).get("index_file", "review_snapshots.csv"))

        review_cfg = self.cfg.get("review_case_system", {})
        self.review_case_system_enabled = bool(review_cfg.get("enabled", False))
        self.review_case_workspace = self.project_dir / str(review_cfg.get("workspace_dir", "review_workspace"))
        self.review_case_timezone = str(review_cfg.get("timezone", "Asia/Ho_Chi_Minh"))
        self.review_case_fallback_close_hours = int(review_cfg.get("fallback_close_hours", 4))
        self.review_daily_exports_dir = self.project_dir / str(review_cfg.get("daily_exports_dir", "review_exports"))
        self.review_builder_script = self.project_dir / str(review_cfg.get("builder_script", "build_daily_review_pack.py"))
        self.review_runtime = None
        if self.review_case_system_enabled and CaseReviewRuntime is not None:
            try:
                self.review_runtime = CaseReviewRuntime(
                    self.review_case_workspace,
                    tz_name=self.review_case_timezone,
                    fallback_close_hours=self.review_case_fallback_close_hours,
                )
            except Exception as e:
                print(f"[review_case warn] runtime init failed: {e}")
                self.review_runtime = None

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
            "dispatch_action", "dispatch_confidence_band", "dispatch_reason", "status"
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
            "delivery_band", "veto_reason_code", "dispatch_action", "dispatch_confidence_band", "dispatch_reason"
        ]
        self.snapshot_fields = [
            "snapshot_ts_ms", "stage", "symbol", "side", "strategy", "signal_id", "pending_id",
            "setup_id", "outcome", "image_path", "context_interval", "entry_interval", "breakout_level",
            "entry_ref", "stop", "tp1", "tp2", "note", "image_available"
        ]
        self.market_review_fields = [
            "review_ts_ms", "review_date", "symbol", "side_hint", "strategy_hint", "market_regime",
            "opportunity_type", "bot_status", "miss_stage", "miss_type", "reason_code", "reason_text",
            "setup_id", "signal_id", "manual_tradable", "improvement_candidate", "note"
        ]

        self.table_specs = {
            "pending": {"logical": self.pending_file, "dir": self.pending_dir, "fieldnames": self.pending_fields, "ts_col": "created_ts_ms", "granularity": "day"},
            "signals": {"logical": self.signals_file, "dir": self.signals_dir, "fieldnames": self.signal_fields, "ts_col": "timestamp_ms", "granularity": "month"},
            "results": {"logical": self.results_file, "dir": self.results_dir, "fieldnames": self.result_fields, "ts_col": "close_time_ms", "granularity": "month"},
            "review_candidates": {"logical": self.review_candidates_file, "dir": self.review_candidates_dir, "fieldnames": None, "ts_col": None, "granularity": "day"},
            "market_review": {"logical": self.market_review_file, "dir": self.market_review_dir, "fieldnames": self.market_review_fields, "ts_col": "review_ts_ms", "granularity": "day"},
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
        is_short = side_u == "SHORT" or "short_exhaustion_retest" in strategy_l
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

    def _ensure_csv_files(self):
        self._ensure_storage_layout()

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

    def filter_symbols(self, symbols: List[str], tickers: Dict[str, Dict]) -> List[str]:
        min_qv = float(self.cfg["scanner"]["min_quote_volume_usdt_24h"])
        min_price = float(self.cfg["scanner"]["min_price"])
        kept = []
        for s in symbols:
            t = tickers.get(s)
            if not t:
                continue
            qv = float(t.get("quoteVolume", 0))
            price = float(t.get("lastPrice", 0))
            if qv >= min_qv and price >= min_price:
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

    def funding(self, symbol: str) -> float:
        data = self.get("/fapi/v1/premiumIndex", {"symbol": symbol})
        return float(data.get("lastFundingRate", 0.0)) * 100.0

    def avg(self, vals: List[float]) -> float:
        return sum(vals) / max(len(vals), 1)

    def calc_oi_jump_pct(self, symbol: str) -> Optional[float]:
        hist = self.oi_hist(symbol, period="5m", limit=6)
        if len(hist) < 2:
            return None
        prev = hist[-2]["oi_value"]
        curr = hist[-1]["oi_value"]
        if prev <= 0:
            return None
        return (curr - prev) / prev * 100.0

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

    def trend_15m(self, bars_15m_closed: List[Dict]) -> str:
        if len(bars_15m_closed) < 8:
            return "flat"
        highs = [b["high"] for b in bars_15m_closed[-4:]]
        lows = [b["low"] for b in bars_15m_closed[-4:]]
        if highs[-1] >= highs[-2] >= highs[-3] and lows[-1] >= lows[-2] >= lows[-3]:
            return "up"
        if highs[-1] <= highs[-2] <= highs[-3] and lows[-1] <= lows[-2] <= lows[-3]:
            return "down"
        return "flat"

    def trend_1h(self, bars_1h_closed: List[Dict]) -> str:
        if len(bars_1h_closed) < 6:
            return "flat"
        closes = [b["close"] for b in bars_1h_closed[-6:]]
        if closes[-1] > closes[-3] > closes[-5]:
            return "up"
        if closes[-1] < closes[-3] < closes[-5]:
            return "down"
        return "flat"


    def _ensure_storage_layout(self):
        self.snapshots_dir.mkdir(parents=True, exist_ok=True)
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
        self._ensure_header(self.snapshot_index_file, self.snapshot_fields)
        self._ensure_table_seed("market_review")

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

    def _interval_to_ms(self, interval: str) -> int:
        unit = interval[-1]
        value = int(interval[:-1])
        if unit == "m":
            return value * 60 * 1000
        if unit == "h":
            return value * 60 * 60 * 1000
        if unit == "d":
            return value * 24 * 60 * 60 * 1000
        return 0

    def _fetch_klines_range(self, symbol: str, interval: str, start_ms: int, end_ms: int, limit: int = 150) -> List[Dict]:
        try:
            data = self.get("/fapi/v1/klines", {
                "symbol": symbol,
                "interval": interval,
                "startTime": int(start_ms),
                "endTime": int(end_ms),
                "limit": int(limit),
            })
        except Exception:
            return []
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

    def _get_entry_reference_for_outcome(self, row: Dict):
        for key in ("breakout_level", "signal_price", "signal_high", "signal_low"):
            try:
                val = float(row.get(key) or 0.0)
            except Exception:
                val = 0.0
            if val > 0:
                return val, f"entry_ref_from_{key}"
        return None, "missing_entry_reference"

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
            rows = [self._enrich_pending_row_for_daily_review(r) for r in rows]
            self.write_csv(self.pending_file, rows, fieldnames=self.pending_fields)
            return synced_row
        return None

    def _derive_review_integrity_fields(self, row: Dict) -> Dict:
        status = str(row.get("status", "") or "").upper()
        sem_ok = "Y"
        sem_issue = ""
        if status == "CONFIRMED" and not str(row.get("confirmed_ts_ms", "") or "").strip():
            sem_ok = "N"
            sem_issue = "confirmed_without_confirmed_ts"
        if str(row.get("is_sent_signal", "N") or "N").upper() == "Y" and not str(row.get("sent_ts_ms", "") or "").strip():
            sem_ok = "N"
            sem_issue = sem_issue or "sent_without_sent_ts"
        review_eligible = "Y" if sem_ok == "Y" else "N"
        exclusion = "" if review_eligible == "Y" else sem_issue
        return {
            "semantic_consistency": sem_ok,
            "semantic_issue": sem_issue,
            "review_eligible": review_eligible,
            "review_exclusion_reason": exclusion,
        }

    def _compute_close_outcome_metrics(self, row: Dict) -> Optional[Dict]:
        try:
            status = str(row.get("status", "") or "").upper()
            if status == "PENDING":
                return {
                    "outcome_1h_available": "N",
                    "outcome_2h_available": "N",
                }
            close_ts = int(float(row.get("closed_ts_ms") or 0))
            if close_ts <= 0:
                return None
            symbol = str(row.get("symbol", "") or "").strip()
            side = str(row.get("side", "") or "").upper().strip()
            if not symbol or side not in {"LONG", "SHORT"}:
                return None
            now_ms = int(time.time() * 1000)
            if now_ms < close_ts + 2 * 60 * 60 * 1000:
                return {
                    "outcome_1h_available": "N",
                    "outcome_2h_available": "N",
                    "post_close_outcome_notes": "not_enough_post_close_time",
                }
            entry_ref, entry_note = self._get_entry_reference_for_outcome(row)
            if not entry_ref or entry_ref <= 0:
                return {
                    "review_eligible": "N",
                    "review_exclusion_reason": "missing_entry_reference",
                    "outcome_1h_available": "N",
                    "outcome_2h_available": "N",
                    "post_close_outcome_notes": entry_note,
                }
            bars = self._fetch_klines_range(symbol, "5m", close_ts, close_ts + 4 * 60 * 60 * 1000 + self._interval_to_ms("5m"), limit=120)
            if not bars:
                return None

            def compute_window(window_ms: int):
                subset = [b for b in bars if int(b.get("open_time", 0)) >= close_ts and int(b.get("open_time", 0)) < close_ts + window_ms]
                if not subset:
                    return None
                best_favor = -1e18
                best_adverse = -1e18
                t_favor = None
                t_adverse = None
                for b in subset:
                    hi = float(b.get("high", entry_ref))
                    lo = float(b.get("low", entry_ref))
                    if side == "LONG":
                        favor_candidate = (hi - entry_ref) / entry_ref * 100.0
                        adverse_candidate = (entry_ref - lo) / entry_ref * 100.0
                    else:
                        favor_candidate = (entry_ref - lo) / entry_ref * 100.0
                        adverse_candidate = (hi - entry_ref) / entry_ref * 100.0
                    if favor_candidate > best_favor:
                        best_favor = favor_candidate
                        t_favor = (int(b.get("open_time", close_ts)) - close_ts) / 60000.0
                    if adverse_candidate > best_adverse:
                        best_adverse = adverse_candidate
                        t_adverse = (int(b.get("open_time", close_ts)) - close_ts) / 60000.0
                return {"favor": max(best_favor, 0.0), "adverse": max(best_adverse, 0.0), "time_to_favor_m": t_favor, "time_to_adverse_m": t_adverse, "subset": subset}

            w1 = compute_window(60 * 60 * 1000)
            w2 = compute_window(2 * 60 * 60 * 1000)
            w4 = compute_window(4 * 60 * 60 * 1000)
            if not w1 or not w2:
                return None
            breakout = None
            try:
                breakout = float(row.get("breakout_level"))
            except Exception:
                breakout = None

            def reclaim_flag(window):
                if breakout is None or breakout <= 0 or not window:
                    return "UNKNOWN"
                subset = window.get("subset") or []
                if side == "LONG":
                    hit = any(float(b.get("low", breakout)) <= breakout for b in subset)
                else:
                    hit = any(float(b.get("high", breakout)) >= breakout for b in subset)
                return "Y" if hit else "N"

            entry_window_m = 10
            signal_open = int(float(row.get("signal_open_time") or 0))
            entry_feasible = ""
            entry_slippage = ""
            entry_exec_note = ""
            if signal_open > 0:
                e_bars = self._fetch_klines_range(symbol, "1m", signal_open, signal_open + entry_window_m * 60 * 1000 + self._interval_to_ms("1m"), limit=30)
                if e_bars:
                    touches = [b for b in e_bars if float(b.get("low", entry_ref)) <= entry_ref <= float(b.get("high", entry_ref))]
                    if touches:
                        entry_feasible = "Y"
                        entry_slippage = "0.0000"
                        entry_exec_note = f"entry still reachable within {entry_window_m}m"
                    else:
                        entry_feasible = "N"
                        entry_exec_note = f"entry not reached within {entry_window_m}m"

            no_meaningful_fast = bool((w2["favor"] < 1.0) or ((w2["time_to_favor_m"] or 9999) > 60))
            regret_valid = "Y"
            regret_reason = "valid_missed_continuation"
            if entry_feasible == "N":
                regret_valid, regret_reason = "N", "entry_not_feasible"
            elif w2["adverse"] > max(w2["favor"] * 0.8, 1.0):
                regret_valid, regret_reason = "N", "adverse_too_large"
            elif no_meaningful_fast:
                regret_valid, regret_reason = "N", "no_meaningful_fast_move"

            outcome_code = "WAIT_TOO_SHORT_CANDIDATE" if regret_valid == "Y" else "NO_MEANINGFUL_MOVE"
            if str(row.get("status", "")).upper() == "INVALIDATED" and regret_valid == "N":
                outcome_code = "KILL_CORRECT"

            return {
                "close_anchor_time_ms": str(close_ts) if status != "CONFIRMED" else "",
                "close_capture_basis": "true_close" if status != "CONFIRMED" else "not_due_yet",
                "future_1h_max_favor_pct": f"{w1['favor']:.4f}",
                "future_1h_max_adverse_pct": f"{w1['adverse']:.4f}",
                "future_2h_max_favor_pct": f"{w2['favor']:.4f}",
                "future_2h_max_adverse_pct": f"{w2['adverse']:.4f}",
                "future_4h_max_favor_pct": f"{(w4 or w2)['favor']:.4f}",
                "future_4h_max_adverse_pct": f"{(w4 or w2)['adverse']:.4f}",
                "reclaim_breakout_2h_YN": reclaim_flag(w2),
                "reclaim_breakout_4h_YN": reclaim_flag(w4),
                "outcome_1h_available": "Y",
                "outcome_2h_available": "Y",
                "outcome_1h_summary": f"favor={w1['favor']:.2f}% adverse={w1['adverse']:.2f}%",
                "outcome_2h_summary": f"favor={w2['favor']:.2f}% adverse={w2['adverse']:.2f}%",
                "post_close_outcome_notes": entry_note,
                "outcome_conclusion_code": outcome_code,
                "entry_feasible_YN": entry_feasible,
                "entry_feasible_window_minutes": str(entry_window_m),
                "entry_slippage_pct": entry_slippage,
                "entry_execution_note": entry_exec_note,
                "time_to_max_favor_minutes": "" if w2["time_to_favor_m"] is None else str(int(round(w2["time_to_favor_m"]))),
                "time_to_max_adverse_minutes": "" if w2["time_to_adverse_m"] is None else str(int(round(w2["time_to_adverse_m"]))),
                "regret_valid_YN": regret_valid,
                "regret_filter_reason": regret_reason,
            }
        except Exception as e:
            return {
                "post_close_outcome_notes": f"outcome_compute_error:{e}",
                "outcome_1h_available": "N",
                "outcome_2h_available": "N",
            }

    def _enrich_pending_row_for_daily_review(self, row: Dict) -> Dict:
        row = dict(row)
        row.update(self._derive_review_integrity_fields(row))
        outcome = self._compute_close_outcome_metrics(row)
        if outcome:
            row.update(outcome)
            if row.get("review_eligible", "Y") == "Y" and str(outcome.get("outcome_2h_available", "N")).upper() != "Y":
                row["review_eligible"] = "N"
                row["review_exclusion_reason"] = "outcome_2h_unavailable"
        return self._normalize_row_for_fields(row, self.pending_fields)

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

    def _sanitize_filename(self, value: str) -> str:
        return re.sub(r"[^A-Za-z0-9._-]+", "_", value).strip("_") or "snapshot"

    def _draw_candles(self, ax, bars: List[Dict], candle_width: float = 0.6):
        for idx, bar in enumerate(bars):
            op = float(bar["open"])
            cl = float(bar["close"])
            hi = float(bar["high"])
            lo = float(bar["low"])
            ax.vlines(idx, lo, hi, linewidth=1.0)
            lower = min(op, cl)
            height = max(abs(cl - op), max((hi - lo) * 0.003, 1e-12))
            try:
                import matplotlib.patches as mpatches
                rect = mpatches.Rectangle((idx - candle_width / 2, lower), candle_width, height, fill=False, linewidth=1.0)
                ax.add_patch(rect)
            except Exception:
                pass

    def _mark_signal_bar(self, ax, bars: List[Dict], ts_ms: int):
        if not bars:
            return
        target_idx = None
        for idx, bar in enumerate(bars):
            if int(bar.get("open_time", 0)) == int(ts_ms):
                target_idx = idx
                break
        if target_idx is None:
            candidates = [i for i, bar in enumerate(bars) if int(bar.get("open_time", 0)) <= int(ts_ms)]
            if candidates:
                target_idx = candidates[-1]
        if target_idx is None:
            target_idx = len(bars) - 1
        ax.axvline(target_idx, linestyle="--", linewidth=1.0)

    def save_review_snapshot(
        self,
        symbol: str,
        side: str,
        strategy: str,
        stage: str,
        ts_ms: int,
        breakout_level: Optional[float] = None,
        entry_ref: Optional[float] = None,
        stop: Optional[float] = None,
        tp1: Optional[float] = None,
        tp2: Optional[float] = None,
        signal_id: str = "",
        pending_id: str = "",
        outcome: str = "",
        note: str = "",
    ) -> Optional[str]:
        cfg = self.cfg.get("review_snapshots", {})
        if not cfg.get("enabled", False):
            return None

        stage_toggle = {
            "pre_pending": cfg.get("save_pre_pending", True),
            "pending": cfg.get("save_pending", True),
            "confirmed": cfg.get("save_confirmed", True),
            "closed": cfg.get("save_closed", True),
        }
        stage_key = stage.split(":", 1)[0]
        if not stage_toggle.get(stage_key, True):
            return None

        try:
            import matplotlib
            matplotlib.use("Agg")
            import matplotlib.pyplot as plt
            import matplotlib.patches as mpatches
        except Exception as e:
            print(f"[snapshot warn] matplotlib unavailable: {e}")
            return None

        try:
            context_interval = self.cfg["scanner"].get("interval_1h", "1h")
            mid_interval = self.cfg["scanner"].get("interval_15m", "15m")
            entry_interval = self.cfg["scanner"].get("interval_5m", "5m")
            context_bars_n = int(cfg.get("context_1h_bars", 24))
            mid_bars_n = int(cfg.get("context_15m_bars", 32))
            entry_bars_n = int(cfg.get("entry_5m_bars", 48))

            context_bars = self.klines(symbol, context_interval, limit=context_bars_n + 2)[:-1]
            mid_bars = self.klines(symbol, mid_interval, limit=mid_bars_n + 2)[:-1]
            entry_bars = self.klines(symbol, entry_interval, limit=entry_bars_n + 2)[:-1]
            if len(context_bars) < 6 or len(mid_bars) < 8 or len(entry_bars) < 8:
                return None

            day_dir = self.snapshots_dir / datetime.now(timezone.utc).strftime("%Y%m%d")
            day_dir.mkdir(parents=True, exist_ok=True)
            base_name = self._sanitize_filename(f"{symbol}_{side}_{strategy}_{stage}_{ts_ms}")
            file_path = day_dir / f"{base_name}.png"

            fig, axes = plt.subplots(3, 1, figsize=(15, 11), constrained_layout=False)
            ctx_ax, mid_ax, ent_ax = axes
            self._draw_candles(ctx_ax, context_bars)
            self._draw_candles(mid_ax, mid_bars)
            self._draw_candles(ent_ax, entry_bars)
            self._mark_signal_bar(ctx_ax, context_bars, ts_ms)
            self._mark_signal_bar(mid_ax, mid_bars, ts_ms)
            self._mark_signal_bar(ent_ax, entry_bars, ts_ms)

            for ax, bars, label in ((ctx_ax, context_bars, context_interval), (mid_ax, mid_bars, mid_interval), (ent_ax, entry_bars, entry_interval)):
                if breakout_level is not None:
                    ax.axhline(float(breakout_level), linestyle='--', linewidth=1.0)
                if entry_ref is not None:
                    ax.axhline(float(entry_ref), linestyle='-.', linewidth=1.0)
                if stop is not None:
                    ax.axhline(float(stop), linestyle=':', linewidth=1.0)
                if tp1 is not None:
                    ax.axhline(float(tp1), linestyle=':', linewidth=1.0)
                if tp2 is not None:
                    ax.axhline(float(tp2), linestyle=':', linewidth=1.0)
                ax.set_title(f"{symbol} {side} | {strategy} | {label} | {stage}")
                ax.set_xlabel("bars")
                ax.set_ylabel("price")
                if bars:
                    closes = [b["close"] for b in bars]
                    lows = [b["low"] for b in bars]
                    highs = [b["high"] for b in bars]
                    lo = min(lows)
                    hi = max(highs)
                    pad = max((hi - lo) * 0.05, max(abs(c) for c in closes) * 0.002)
                    ax.set_ylim(lo - pad, hi + pad)

            fig.suptitle(f"{symbol} {side} {stage} | {note[:180]}")
            try:
                fig.tight_layout(rect=[0, 0.02, 1, 0.965])
            except Exception:
                pass
            fig.savefig(file_path, dpi=int(cfg.get("dpi", 100)), bbox_inches="tight")
            plt.close(fig)

            self.append_csv(
                self.snapshot_index_file,
                {
                    "snapshot_ts_ms": int(time.time() * 1000),
                    "stage": stage,
                    "symbol": symbol,
                    "side": side,
                    "strategy": strategy,
                    "signal_id": signal_id,
                    "pending_id": pending_id,
                    "setup_id": pending_id or signal_id,
                    "outcome": outcome,
                    "image_path": str(file_path),
                    "context_interval": context_interval,
                    "entry_interval": f"{mid_interval}|{entry_interval}",
                    "breakout_level": "" if breakout_level is None else f"{breakout_level}",
                    "entry_ref": "" if entry_ref is None else f"{entry_ref}",
                    "stop": "" if stop is None else f"{stop}",
                    "tp1": "" if tp1 is None else f"{tp1}",
                    "tp2": "" if tp2 is None else f"{tp2}",
                    "note": note,
                },
                fieldnames=self.snapshot_fields,
            )
            return str(file_path)
        except Exception as e:
            print(f"[snapshot warn] {symbol} {side} {stage}: {e}")
            return None

    def _local_day_from_ms(self, ts_ms: int) -> str:
        return datetime.fromtimestamp(max(int(ts_ms), 0) / 1000.0, tz=timezone.utc).astimezone().strftime("%Y-%m-%d")

    def _bar_interval_ms_for_strategy(self, strategy: str) -> int:
        if strategy == "short_exhaustion_retest":
            return 15 * 60 * 1000
        return 5 * 60 * 1000

    def _review_case_day(self, created_ts_ms: int) -> str:
        try:
            from zoneinfo import ZoneInfo
            tz = ZoneInfo(self.review_case_timezone)
        except Exception:
            tz = timezone.utc
        return datetime.fromtimestamp(max(int(created_ts_ms), 0) / 1000.0, tz=timezone.utc).astimezone(tz).strftime("%Y-%m-%d")

    def _review_register_stage(self, case_day: str, case_id: str, stage: str, image_path: Optional[str], note: str = ""):
        # Delegated to review_service — tries multiple stage name aliases.
        review_svc._review_register_stage(self, case_day, case_id, stage, image_path, note=note)

    def _capture_and_register_case_stage(self, pending_row: Dict, stage: str, ts_ms: int, note: str = "", signal_row: Optional[Dict] = None):
        if not self.review_runtime:
            return None
        try:
            case_day = self._review_case_day(int(pending_row.get("created_ts_ms") or 0))
            case_id = pending_row.get("pending_id") or pending_row.get("setup_id", "")
            strategy = pending_row.get("strategy", "")
            try:
                if case_id and not self.review_runtime._load_case(case_day, case_id):
                    self.review_runtime.ensure_case(pending_row)
            except Exception:
                pass
            image_path = self.save_review_snapshot(
                symbol=pending_row.get("symbol", ""),
                side=pending_row.get("side", ""),
                strategy=strategy,
                stage={"pre_pending":"pre_pending","pending_open":"pending","entry_or_confirm":"confirmed","case_close":"closed"}[stage],
                ts_ms=int(ts_ms),
                breakout_level=float(pending_row.get("breakout_level") or 0.0) if pending_row.get("breakout_level") not in (None, "") else None,
                entry_ref=float((signal_row or {}).get("entry_ref") or 0.0) if (signal_row or {}).get("entry_ref") not in (None, "") else None,
                stop=float((signal_row or {}).get("stop") or 0.0) if (signal_row or {}).get("stop") not in (None, "") else None,
                tp1=float((signal_row or {}).get("tp1") or 0.0) if (signal_row or {}).get("tp1") not in (None, "") else None,
                tp2=float((signal_row or {}).get("tp2") or 0.0) if (signal_row or {}).get("tp2") not in (None, "") else None,
                signal_id=(signal_row or {}).get("signal_id", ""),
                pending_id=case_id,
                outcome=(signal_row or {}).get("status", ""),
                note=note,
            )
            self._review_register_stage(case_day, case_id, stage, image_path, note=note)
            return image_path
        except Exception as e:
            print(f"[review_case warn] capture_stage {stage} {pending_row.get('pending_id','')}: {e}")
            return None

    def _review_register_pending_case(self, pending_row: Dict):
        # Delegated to review_service.
        review_svc._review_register_pending_case(self, pending_row)

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

    def collect_due_case_close_fallbacks(self):
        # Delegated to review_service.
        review_svc.collect_due_case_close_fallbacks(self)

    def build_daily_review_pack(self, case_day: str, debug: bool = False):
        # Delegated to review_service — includes backfill before build.
        return review_svc.build_daily_review_pack(self, case_day, debug=debug)

        try:
            all_rows = self.read_csv(self.pending_file)
            if all_rows:
                enriched = [self._enrich_pending_row_for_daily_review(r) for r in all_rows]
                self.write_csv(self.pending_file, enriched, fieldnames=self.pending_fields)
        except Exception as e:
            print(f"[review_case warn] enrich pending rows before build failed: {e}")

        pending_path = self.pending_dir / f"pending_{case_day}.csv"
        signals_path = self.signals_dir / f"signals_{case_day[:7]}.csv"
        results_path = self.results_dir / f"results_{case_day[:7]}.csv"

        out_dir = self.review_daily_exports_dir if not debug else self.review_daily_exports_dir / "debug"

        cmd = [
            sys.executable,
            str(self.review_builder_script),
            "--date", case_day,
            "--workspace", str(self.review_case_workspace),
            "--pending", str(pending_path),
            "--signals", str(signals_path),
            "--results", str(results_path),
            "--snapshot-index", str(self.snapshot_index_file),
            "--out-dir", str(out_dir),
            "--tz", self.review_case_timezone,
            "--fallback-hours", str(self.review_case_fallback_close_hours),
        ]
        try:
            print("[review_case] building daily pack:", " ".join(cmd))
            subprocess.run(cmd, cwd=str(self.project_dir), check=True)
            if debug:
                debug_dir = self.review_daily_exports_dir / "debug"
                debug_dir.mkdir(parents=True, exist_ok=True)
                canonical = debug_dir / f"daily_review_{case_day}.docx"
                if canonical.exists():
                    ts_tag = datetime.now().strftime("%Y%m%d_%H%M%S")
                    debug_path = debug_dir / f"daily_review_debug_{case_day}_{ts_tag}.docx"
                    try:
                        canonical.rename(debug_path)
                    except Exception:
                        shutil.copy2(canonical, debug_path)
                        try:
                            canonical.unlink()
                        except Exception:
                            pass
                    print(f"[review_case] debug export written: {debug_path}")
            return True
        except Exception as e:
            print(f"[review_case warn] build daily pack failed: {e}")
            return False

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
        synced_pending = self._sync_confirmed_pending_row(
            s.setup_id,
            confirmed_ts_ms=int(s.timestamp_ms),
            note=s.reason or "signal confirmed",
        )
        self.sync_pending_send_decision(s.setup_id, "SENT", sent_ts_ms=int(s.timestamp_ms))
        self.save_review_snapshot(
            symbol=s.symbol,
            side=s.side,
            strategy=s.strategy,
            stage="confirmed",
            ts_ms=s.timestamp_ms,
            breakout_level=s.breakout_level,
            entry_ref=s.entry_ref,
            stop=s.stop,
            tp1=s.tp1,
            tp2=s.tp2,
            signal_id=s.signal_id,
            note=s.reason,
        )
        if self.review_runtime:
            pending_row = synced_pending or self._find_pending_by_setup(s.setup_id)
            if pending_row:
                case_day = self._review_case_day(int(pending_row.get("created_ts_ms") or 0))
                case_id = pending_row.get("pending_id") or pending_row.get("setup_id", "")
                try:
                    self.review_runtime.register_sent_signal(case_day, case_id, row)
                except Exception as e:
                    print(f"[review_case warn] register_sent_signal {case_id}: {e}")
                try:
                    already_captured = self.review_runtime.has_captured_stage(case_day, case_id, "entry_or_confirm")
                except Exception:
                    already_captured = False
                if not already_captured:
                    self._capture_and_register_case_stage(pending_row, "entry_or_confirm", int(s.timestamp_ms), note=s.reason, signal_row=row)

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
        try:
            self.save_review_snapshot(
                symbol=p.symbol,
                side=p.side,
                strategy=p.strategy,
                stage="pending",
                ts_ms=p.signal_open_time,
                breakout_level=p.breakout_level,
                entry_ref=p.entry_ref,
                stop=p.stop,
                tp1=p.tp1,
                tp2=p.tp2,
                pending_id=p.pending_id,
                note=p.reason,
            )
        except Exception as _snap_e:
            print(f"[snapshot warn] save_pending {p.pending_id}: {_snap_e}")
        if self.review_runtime:
            try:
                self._review_register_pending_case(pending_row)
            except Exception as e:
                print(f"[review_case warn] register pending {p.pending_id}: {e}")

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
            changed_row = None
            for row in rows:
                row_setup = row.get("setup_id") or row.get("pending_id", "")
                if row_setup == setup_id:
                    changed_row = dict(row)
                    break
            if changed_row is not None:
                changed_row = self._enrich_pending_row_for_daily_review(changed_row)
                for idx, row in enumerate(rows):
                    row_setup = row.get("setup_id") or row.get("pending_id", "")
                    if row_setup == setup_id:
                        rows[idx] = changed_row
                        break
            self.write_csv(self.pending_file, rows, fieldnames=self.pending_fields)
        return changed

    def format_close_message(self, signal_row: Dict, outcome: str, r_multiple: float, close_reason: str) -> str:
        side = signal_row.get("side", "UNKNOWN")
        side_icon = "[LONG]" if side == "LONG" else "[SHORT]"
        symbol = signal_row.get("symbol", "")
        return (
            f"✅ CLOSED {side_icon} #{symbol} | {outcome} | R={r_multiple:.2f}\n"
            f"Reason: {close_reason}\n"
            f"Strategy: {signal_row.get('strategy', '')}"
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
            if closed_row is not None:
                closed_row = self._enrich_pending_row_for_daily_review(closed_row)
                for idx, row in enumerate(rows):
                    if row.get("pending_id") == pending_id:
                        rows[idx] = closed_row
                        break
            self.write_csv(self.pending_file, rows, fieldnames=self.pending_fields)
            if self.review_runtime and closed_row:
                case_day = self._review_case_day(int(closed_row.get("created_ts_ms") or 0))
                case_id = closed_row.get("pending_id") or closed_row.get("setup_id", "")
                if status == "CONFIRMED":
                    try:
                        self.review_runtime.register_confirmed(case_day, case_id, closed_row)
                    except Exception as e:
                        print(f"[review_case warn] register_confirmed pending {pending_id}: {e}")
                    stage_ts = int(closed_row.get("closed_ts_ms") or int(time.time() * 1000))
                    self._capture_and_register_case_stage(closed_row, "entry_or_confirm", stage_ts, note=close_reason)
                else:
                    try:
                        self.review_runtime.register_close(case_day, case_id, closed_row)
                    except Exception as e:
                        print(f"[review_case warn] register_close pending {pending_id}: {e}")
                    stage_ts = int(closed_row.get("closed_ts_ms") or int(time.time() * 1000))
                    self._capture_and_register_case_stage(closed_row, "case_close", stage_ts, note=close_reason)
        return changed

    def close_signal(self, signal_row: Dict, outcome: str, r_multiple: float, bars_checked: int, close_reason: str, mfe_pct: float = 0.0, mae_pct: float = 0.0):
        results = self.read_csv(self.results_file)
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

        try:
            self.save_review_snapshot(
                symbol=signal_row.get("symbol", ""),
                side=signal_row.get("side", ""),
                strategy=self.infer_legacy_strategy(signal_row),
                stage="closed",
                ts_ms=int(result_row.get("close_time_ms") or signal_row.get("timestamp_ms") or 0),
                breakout_level=float(signal_row.get("breakout_level") or 0.0) if signal_row.get("breakout_level") else None,
                entry_ref=float(signal_row.get("entry_ref") or 0.0) if signal_row.get("entry_ref") else None,
                stop=float(signal_row.get("stop") or 0.0) if signal_row.get("stop") else None,
                tp1=float(signal_row.get("tp1") or 0.0) if signal_row.get("tp1") else None,
                tp2=float(signal_row.get("tp2") or 0.0) if signal_row.get("tp2") else None,
                signal_id=signal_row.get("signal_id", ""),
                outcome=outcome,
                note=f"{outcome} | r={r_multiple:.2f} | {close_reason}",
            )
        except Exception as e:
            print(f"[snapshot warn] close_signal {signal_row.get('signal_id')}: {e}")

        if self.review_runtime:
            pending_row = self._find_pending_by_setup(signal_row.get("setup_id", signal_row.get("signal_id", "")))
            if pending_row:
                case_day = self._review_case_day(int(pending_row.get("created_ts_ms") or 0))
                case_id = pending_row.get("pending_id") or pending_row.get("setup_id", "")
                try:
                    self.review_runtime.register_close(case_day, case_id, result_row)
                except Exception as e:
                    print(f"[review_case warn] register_close signal {case_id}: {e}")
                self._capture_and_register_case_stage(pending_row, "case_close", int(result_row.get("close_time_ms") or int(time.time() * 1000)), note=f"{outcome} | {close_reason}", signal_row=signal_row)

        signals = self.read_csv(self.signals_file)
        for row in signals:
            if row.get("signal_id") == signal_row.get("signal_id"):
                row["status"] = outcome
        self.write_csv(self.signals_file, signals, fieldnames=self.signal_fields)

        print(f"[close] {signal_row.get('symbol','')} {signal_row.get('side','')} | {outcome} | r={r_multiple:.2f} | {close_reason}")
        if self.cfg.get("telegram", {}).get("send_close_notifications", True):
            try:
                self.telegram_send(self.format_close_message(signal_row, outcome, r_multiple, close_reason))
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

        return (
            f"{side_icon} #{s.symbol} | ${s.price:.6g} | Score {s.score/10:.1f}/10\n\n"
            f"Entry: {s.entry_ref:.6g}\n"
            f"Stop: {s.stop:.6g} ({s.sl_distance_pct:.2f}%)\n"
            f"TP1: {s.tp1:.6g} ({s.tp1_distance_pct:.2f}%)\n"
            f"TP2: {s.tp2:.6g} ({s.tp2_distance_pct:.2f}%)\n\n"
            f"OI(5m): {s.oi_jump_pct:+.2f}%\n"
            f"Vol: {s.vol_ratio:.2f}x\n"
            f"Funding: {s.funding_pct:+.4f}%\n"
            f"Retest waited: {s.retest_bars_waited} bars\n"
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
        if not token or not chat_id or "YOUR_" in token or "YOUR_" in chat_id or "CUA_BAN" in token or "CUA_BAN" in chat_id:
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

    def find_retest_long(
        self,
        breakout_level: float,
        future_bars: List[Dict],
        tolerance_pct: float,
        reject_confirm_ratio: float,
        max_deep_retest_pct: float,
    ):
        tol = breakout_level * self._pct_to_fraction(tolerance_pct)
        max_deep = breakout_level * self._pct_to_fraction(max_deep_retest_pct)
        zone_low = breakout_level - tol
        zone_high = breakout_level + tol
        invalid_low = breakout_level - max_deep

        for idx, bar in enumerate(future_bars, start=1):
            if bar["low"] < invalid_low and bar["close"] < breakout_level:
                return {"state": "INVALIDATED", "bar_index": idx}

            touched = bar["low"] <= zone_high and bar["high"] >= zone_low
            if not touched:
                continue

            total = max(bar["high"] - bar["low"], 1e-12)
            close_reject = (bar["close"] - bar["low"]) / total
            if bar["low"] < invalid_low:
                return {"state": "INVALIDATED", "bar_index": idx}

            if bar["close"] >= breakout_level and close_reject >= reject_confirm_ratio:
                return {
                    "state": "CONFIRMED",
                    "bar_index": idx,
                    "entry_ref": bar["close"],
                    "entry_low": zone_low,
                    "entry_high": zone_high,
                    "retest_low": bar["low"],
                }

            if bar["close"] < breakout_level:
                return {"state": "INVALIDATED", "bar_index": idx}

        return {"state": "WAITING"}

    def find_retest_short(
        self,
        breakdown_level: float,
        future_bars: List[Dict],
        tolerance_pct: float,
        reject_confirm_ratio: float,
        max_deep_retest_pct: float,
    ):
        tol = breakdown_level * self._pct_to_fraction(tolerance_pct)
        max_deep = breakdown_level * self._pct_to_fraction(max_deep_retest_pct)
        zone_low = breakdown_level - tol
        zone_high = breakdown_level + tol
        invalid_high = breakdown_level + max_deep

        for idx, bar in enumerate(future_bars, start=1):
            if bar["high"] > invalid_high and bar["close"] > breakdown_level:
                return {"state": "INVALIDATED", "bar_index": idx}

            touched = bar["low"] <= zone_high and bar["high"] >= zone_low
            if not touched:
                continue

            total = max(bar["high"] - bar["low"], 1e-12)
            close_reject = (bar["high"] - bar["close"]) / total
            if bar["high"] > invalid_high:
                return {"state": "INVALIDATED", "bar_index": idx}

            if bar["close"] <= breakdown_level and close_reject >= reject_confirm_ratio:
                return {
                    "state": "CONFIRMED",
                    "bar_index": idx,
                    "entry_ref": bar["close"],
                    "entry_low": zone_low,
                    "entry_high": zone_high,
                    "retest_high": bar["high"],
                }

            if bar["close"] > breakdown_level:
                return {"state": "INVALIDATED", "bar_index": idx}

        return {"state": "WAITING"}

    def infer_legacy_strategy(self, row: Dict) -> str:
        strategy = row.get("strategy", "").strip()
        if strategy:
            return strategy
        return "long_breakout_retest"

    def detect_1h_exhaustion(self, bars_1h_closed: List[Dict]) -> Optional[Dict]:
        cfg = self.cfg.get("short_exhaustion_retest", {})
        lookback = int(cfg.get("exhaustion_1h_lookback_bars", 12))
        min_top_tests = int(cfg.get("exhaustion_1h_min_top_tests", 2))
        tolerance_pct = float(cfg.get("exhaustion_1h_resistance_tolerance_pct", 0.35)) / 100.0
        breakout_accept_pct = float(cfg.get("exhaustion_1h_breakout_accept_pct", 0.15)) / 100.0
        min_upper_wick_ratio = float(cfg.get("exhaustion_1h_min_upper_wick_ratio", 0.35))

        bars = bars_1h_closed[-lookback:]
        if len(bars) < max(lookback - 2, 8):
            return None

        resistance_ref = max(b["high"] for b in bars)
        tolerance_abs = resistance_ref * tolerance_pct
        tests = [b for b in bars if abs(b["high"] - resistance_ref) <= tolerance_abs]
        top_tests = len(tests)
        no_clean_acceptance = all(b["close"] <= resistance_ref * (1 + breakout_accept_pct) for b in bars)

        rejection_bars = [
            b for b in tests
            if self.upper_wick_ratio(b) >= min_upper_wick_ratio and b["close"] < resistance_ref
        ]
        has_failed_breakout = len(rejection_bars) >= 1

        closes = [b["close"] for b in bars]
        push_advances = []
        for idx in range(1, len(closes)):
            adv = closes[idx] - closes[idx - 1]
            if adv > 0:
                push_advances.append(adv)
        weakening_push = False
        if len(push_advances) >= 2 and push_advances[-1] <= push_advances[-2]:
            weakening_push = True

        up_body_last2 = [self.candle_body_ratio(b) for b in bars[-2:]]
        last2_near_high = sum(1 for b in bars[-2:] if (b["high"] - b["close"]) / max(b["high"] - b["low"], 1e-12) <= 0.20)
        bullish_last2 = sum(1 for b in bars[-2:] if b["close"] > b["open"])
        strong_bullish_impulse = bullish_last2 == 2 and last2_near_high == 2 and min(up_body_last2) >= 0.55

        score = 0.0
        if top_tests >= min_top_tests:
            score += min(10.0, 6.0 + 2.0 * (top_tests - min_top_tests + 1))
        if no_clean_acceptance:
            score += 8.0
        if has_failed_breakout:
            score += min(9.0, 5.0 + 2.0 * len(rejection_bars))
        if weakening_push:
            score += 8.0
        if strong_bullish_impulse:
            score = max(score - 12.0, 0.0)

        passed = top_tests >= min_top_tests and no_clean_acceptance and has_failed_breakout and not strong_bullish_impulse
        if not passed:
            return None

        reason_tags = [
            f"1h_top_tests={top_tests}",
            "1h_failed_breakout",
            "1h_no_accept_above_resistance",
        ]
        if weakening_push:
            reason_tags.append("1h_weakening_push")

        return {
            "passed": True,
            "resistance_ref": resistance_ref,
            "top_tests": top_tests,
            "has_failed_breakout": has_failed_breakout,
            "has_clear_rejection": has_failed_breakout,
            "has_weakening_push": weakening_push,
            "score_1h_exhaustion": min(score, 35.0),
            "reason_tags": reason_tags,
        }

    def detect_15m_breakdown_after_exhaustion(self, bars_15m_closed: List[Dict]) -> Optional[Dict]:
        cfg = self.cfg.get("short_exhaustion_retest", {})
        lookback = int(cfg.get("breakdown_15m_lookback_bars", 12))
        min_break_distance_pct = float(cfg.get("breakdown_15m_min_break_distance_pct", 0.15)) / 100.0
        min_body_ratio = float(cfg.get("breakdown_15m_min_body_ratio", 0.45))
        min_volume_ratio = float(cfg.get("breakdown_15m_min_volume_ratio", 1.10))

        bars = bars_15m_closed[-max(lookback + 2, 16):]
        if len(bars) < lookback + 1:
            return None

        signal_bar = bars[-1]
        support_window = bars[-(lookback + 1):-1]
        support_ref = min(b["low"] for b in support_window)
        break_distance_pct = max((support_ref - signal_bar["close"]) / max(support_ref, 1e-12) * 100.0, 0.0)
        body_ratio = self.candle_body_ratio(signal_bar)
        vol_ratio = self.volume_ratio_generic(bars, recent_bars=1, base_bars=min(6, len(bars) - 1))
        close_near_low = (signal_bar["close"] - signal_bar["low"]) / max(signal_bar["high"] - signal_bar["low"], 1e-12)

        passed = (
            signal_bar["close"] < support_ref * (1 - min_break_distance_pct)
            and body_ratio >= min_body_ratio
            and vol_ratio >= min_volume_ratio
            and close_near_low <= 0.45
        )
        if not passed:
            return None

        score = 0.0
        score += 12.0
        score += min(6.0, break_distance_pct / max(min_break_distance_pct * 100.0, 1e-12) * 3.0)
        score += min(6.0, body_ratio / max(min_body_ratio, 1e-12) * 3.0)
        score += min(6.0, vol_ratio / max(min_volume_ratio, 1e-12) * 3.0)

        return {
            "passed": True,
            "breakdown_level": support_ref,
            "support_ref": support_ref,
            "break_bar_open_time": signal_bar["open_time"],
            "break_distance_pct": break_distance_pct,
            "body_ratio": body_ratio,
            "vol_ratio": vol_ratio,
            "score_15m_breakdown": min(score, 30.0),
            "reason_tags": ["15m_clean_breakdown", f"15m_break_dist={break_distance_pct:.2f}%"],
            "signal_bar": signal_bar,
        }

    def _reset_round_detect_funnel(self):
        self._round_detect_funnel = {
            "long_breakout_retest": {},
            "short_exhaustion_retest": {},
        }

    def _funnel_hit(self, strategy: str, key: str, n: int = 1):
        funnel = getattr(self, "_round_detect_funnel", None)
        if funnel is None:
            return
        bucket = funnel.setdefault(strategy, {})
        bucket[key] = bucket.get(key, 0) + n

    def _print_detect_funnel_summary(self):
        funnel = getattr(self, "_round_detect_funnel", None) or {}
        order = {
            "long_breakout_retest": [
                "symbols_seen",
                "data_ok",
                "oi_ok",
                "breakout_ok",
                "candle_ok",
                "regime_ok",
                "funding_ok",
                "new_pending",
                "blocked_duplicate",
                "fail_data",
                "fail_oi",
                "fail_breakout",
                "fail_candle",
                "fail_regime",
                "fail_funding",
            ],
            "short_exhaustion_retest": [
                "symbols_seen",
                "data_ok",
                "exhaustion_ok",
                "breakdown_ok",
                "new_pending",
                "blocked_duplicate",
                "fail_disabled",
                "fail_data",
                "fail_exhaustion",
                "fail_breakdown",
            ],
        }
        for strategy, keys in order.items():
            bucket = funnel.get(strategy, {})
            parts = [f"{k}={bucket.get(k, 0)}" for k in keys if bucket.get(k, 0) or k in ("symbols_seen", "new_pending")]
            print(f"[detect funnel] {strategy} | " + " | ".join(parts))

    def build_pending_long_setup(self, symbol: str) -> Optional[PendingSetup]:
        return strategy_build_pending_long_setup(self, symbol, PendingSetup)

    def build_pending_short_exhaustion_setup(self, symbol: str) -> Optional[PendingSetup]:
        return strategy_build_pending_short_exhaustion_setup(self, symbol, PendingSetup)

    def build_pending_setups_for_symbol(self, symbol: str) -> List[PendingSetup]:
        setups: List[PendingSetup] = []
        long_setup = self.build_pending_long_setup(symbol)
        if long_setup is not None:
            setups.append(long_setup)
        short_setup = self.build_pending_short_exhaustion_setup(symbol)
        if short_setup is not None:
            setups.append(short_setup)
        return setups

    def process_pending_setups(self) -> List[Signal]:
        retest_cfg = self.cfg["retest"]
        risk_cfg = self.cfg["risk"]
        short_cfg = self.cfg.get("short_exhaustion_retest", {})
        long_oi_cfg = self.cfg.get("long_breakout_retest", self.cfg.get("legacy_5m_retest", {}))

        rows = self.read_csv(self.pending_file)
        confirmed: List[Signal] = []

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
            strategy = self.infer_legacy_strategy(row)
            score_oi = float(row.get("score_oi") or 0.0)
            score_exhaustion = float(row.get("score_exhaustion") or 0.0)
            score_breakout = float(row.get("score_breakout") or 0.0)
            score_retest = float(row.get("score_retest") or 0.0)
            reason_tags = row.get("reason_tags", "") or ""

            try:
                if strategy == "short_exhaustion_retest":
                    max_bars = int(short_cfg.get("retest_15m_max_bars", 3))
                    tolerance_pct = float(short_cfg.get("retest_15m_tolerance_pct", 0.20))
                    reject_confirm_ratio = float(short_cfg.get("retest_15m_reject_confirm_ratio", 0.35))
                    stop_buffer_pct = float(short_cfg.get("stop_buffer_pct", 0.30))
                    max_deep_retest_pct = float(short_cfg.get("retest_15m_max_deep_retest_pct", 0.25))
                    min_risk_pct = float(short_cfg.get("min_risk_pct", risk_cfg["min_risk_pct"])) / 100.0
                    tp1_r = float(short_cfg.get("tp1_r_multiple", risk_cfg["tp1_r_multiple"]))
                    tp2_r = float(short_cfg.get("tp2_r_multiple", risk_cfg["tp2_r_multiple"]))
                    bars_all = self.klines(symbol, self.cfg["scanner"]["interval_15m"], limit=max_bars + 20)
                else:
                    cfg_retest_max = int(retest_cfg["retest_max_bars"])
                    hard_max_bars = int(long_oi_cfg.get("hard_max_retest_wait_bars", 8))
                    max_bars = min(cfg_retest_max, max(hard_max_bars, 1))
                    tolerance_pct = float(retest_cfg["retest_tolerance_pct"])
                    reject_confirm_ratio = float(retest_cfg["retest_reject_confirm_ratio"])
                    stop_buffer_pct = float(retest_cfg["stop_buffer_pct"])
                    max_deep_retest_pct = float(retest_cfg.get("max_deep_retest_pct", tolerance_pct))
                    min_risk_pct = float(risk_cfg["min_risk_pct"]) / 100.0
                    tp1_r = float(risk_cfg["tp1_r_multiple"])
                    tp2_r = float(risk_cfg["tp2_r_multiple"])
                    bars_all = self.klines(symbol, self.cfg["scanner"]["interval_5m"], limit=max_bars + 30)

                closed_bars = bars_all[:-1]
                future_bars = [b for b in closed_bars if b["open_time"] > signal_open_time][:max_bars]

                if not future_bars:
                    continue

                if side == "LONG":
                    retest = self.find_retest_long(
                        breakout_level,
                        future_bars,
                        tolerance_pct,
                        reject_confirm_ratio,
                        max_deep_retest_pct,
                    )
                else:
                    retest = self.find_retest_short(
                        breakout_level,
                        future_bars,
                        tolerance_pct,
                        reject_confirm_ratio,
                        max_deep_retest_pct,
                    )

                state = retest["state"]
                if state == "WAITING" and len(future_bars) < max_bars:
                    continue
                if state == "INVALIDATED":
                    self.close_pending(row["pending_id"], "INVALIDATED", "retest invalidated", int(retest.get("bar_index", 0)))
                    continue
                if state == "WAITING":
                    self.close_pending(row["pending_id"], "EXPIRED_WAIT", "retest wait expired", len(future_bars))
                    continue

                retest_bars_waited = int(retest["bar_index"])
                entry_ref = float(retest["entry_ref"])
                entry_low = float(retest["entry_low"])
                entry_high = float(retest["entry_high"])

                if side == "LONG":
                    soft_max_bars = int(long_oi_cfg.get("soft_max_retest_wait_bars", 4))
                    hard_max_bars = int(long_oi_cfg.get("hard_max_retest_wait_bars", 8))
                    max_stale_runup_pct = float(long_oi_cfg.get("max_stale_runup_pct", 3.5))
                    max_close_below_breakout = int(long_oi_cfg.get("max_close_below_breakout_bars", 1))
                    max_post_break_chop_pct = float(long_oi_cfg.get("max_post_break_chop_pct", 3.5))
                    min_post_break_acceptance_ratio = float(long_oi_cfg.get("min_post_break_acceptance_ratio", 0.55))

                    pre_retest_bars = future_bars[:max(retest_bars_waited - 1, 0)]
                    bars_to_retest = future_bars[:retest_bars_waited]
                    if retest_bars_waited > hard_max_bars:
                        self.close_pending(row["pending_id"], "INVALIDATED", f"stale_retest waited {retest_bars_waited} bars", retest_bars_waited)
                        continue

                    if pre_retest_bars:
                        highest_pre_retest = max(b["high"] for b in pre_retest_bars)
                        lowest_pre_retest = min(b["low"] for b in pre_retest_bars)
                        stale_runup_pct = max((highest_pre_retest - breakout_level) / max(breakout_level, 1e-12) * 100.0, 0.0)
                        close_below_breakout = sum(1 for b in pre_retest_bars if b["close"] < breakout_level)
                        post_break_chop_pct = max((highest_pre_retest - lowest_pre_retest) / max(breakout_level, 1e-12) * 100.0, 0.0)
                    else:
                        stale_runup_pct = 0.0
                        close_below_breakout = 0
                        post_break_chop_pct = 0.0

                    impulse_ref_high = max(b["high"] for b in bars_to_retest)
                    impulse_ref_low = min(b["low"] for b in bars_to_retest)
                    acceptance_floor = impulse_ref_low + min_post_break_acceptance_ratio * max(impulse_ref_high - impulse_ref_low, 1e-12)
                    retest_low_val = float(retest["retest_low"])

                    if stale_runup_pct > max_stale_runup_pct:
                        self.close_pending(row["pending_id"], "INVALIDATED", f"stale_breakout runup {stale_runup_pct:.2f}%", retest_bars_waited)
                        continue
                    if close_below_breakout > max_close_below_breakout:
                        self.close_pending(row["pending_id"], "INVALIDATED", f"lost_acceptance closes_below_break {close_below_breakout}", retest_bars_waited)
                        continue
                    if post_break_chop_pct > max_post_break_chop_pct:
                        self.close_pending(row["pending_id"], "INVALIDATED", f"post_break_chop {post_break_chop_pct:.2f}%", retest_bars_waited)
                        continue
                    if retest_low_val < acceptance_floor:
                        self.close_pending(row["pending_id"], "INVALIDATED", f"acceptance_floor_lost {retest_low_val:.6f}<{acceptance_floor:.6f}", retest_bars_waited)
                        continue

                    raw_stop = retest_low_val * (1 - stop_buffer_pct / 100.0)
                    min_stop = entry_ref * (1 - min_risk_pct)
                    stop = min(raw_stop, min_stop)
                    stop_was_forced_min_risk = "yes" if stop == min_stop and abs(min_stop - raw_stop) > 1e-12 else "no"
                    risk = max(entry_ref - stop, 1e-12)
                    tp1 = entry_ref + tp1_r * risk
                    tp2 = entry_ref + tp2_r * risk
                    retest_depth_pct = max((breakout_level - retest_low_val) / max(breakout_level, 1e-12) * 100.0, 0.0)

                    freshness_score = 20.0 if retest_bars_waited <= 2 else 16.0 if retest_bars_waited <= 4 else 10.0 if retest_bars_waited <= 6 else 4.0
                    acceptance_score = 5.0
                    if close_below_breakout == 0:
                        acceptance_score += 3.0
                    if post_break_chop_pct <= max_post_break_chop_pct * 0.6:
                        acceptance_score += 3.0
                    if retest_low_val >= breakout_level:
                        acceptance_score += 4.0
                    if stale_runup_pct <= max_stale_runup_pct * 0.4:
                        acceptance_score += 2.0
                    score_retest = min(25.0, freshness_score + acceptance_score)
                    final_reason = reason.replace("pending", "retest hold") + f" + retest {score_retest:.0f}/25"
                    score = min(100.0, score_oi + score_breakout + score_retest)
                    confidence = max(0.0, min(0.99, score / 100.0))
                else:
                    raw_stop = float(retest["retest_high"]) * (1 + stop_buffer_pct / 100.0)
                    max_stop = entry_ref * (1 + min_risk_pct)
                    stop = max(raw_stop, max_stop)
                    stop_was_forced_min_risk = "yes" if stop == max_stop and abs(max_stop - raw_stop) > 1e-12 else "no"
                    risk = max(stop - entry_ref, 1e-12)
                    tp1 = entry_ref - tp1_r * risk
                    tp2 = entry_ref - tp2_r * risk
                    retest_depth_pct = max((float(retest["retest_high"]) - breakout_level) / max(breakout_level, 1e-12) * 100.0, 0.0)
                    if strategy == "short_exhaustion_retest":
                        score_retest = 35.0
                        score = min(100.0, score_exhaustion + score_breakout + score_retest)
                        confidence = max(0.0, min(0.99, score / 100.0))
                        final_reason = reason.replace("pending", "retest fail") + f" + 15m retest {score_retest:.0f}/35"
                    else:
                        score_retest = 25.0
                        score = min(100.0, max(score, score_breakout + score_retest))
                        confidence = max(0.0, min(0.99, score / 100.0))
                        final_reason = reason.replace("pending", "retest reject")

                signal_id = f"{symbol}-{side}-{signal_open_time}-{retest_bars_waited}"
                btc_ctx = self.get_btc_context()
                sl_distance_pct = abs(entry_ref - stop) / max(entry_ref, 1e-12) * 100.0
                tp1_distance_pct = abs(tp1 - entry_ref) / max(entry_ref, 1e-12) * 100.0
                tp2_distance_pct = abs(tp2 - entry_ref) / max(entry_ref, 1e-12) * 100.0
                break_distance_pct = abs(entry_ref - breakout_level) / max(breakout_level, 1e-12) * 100.0
                risk_pct_real = sl_distance_pct
                manual_eval = self.evaluate_manual_tradable(side, entry_ref, stop, tp1)

                signal = Signal(
                    signal_id=signal_id,
                    timestamp_ms=signal_open_time,
                    symbol=symbol,
                    side=side,
                    score=score,
                    confidence=confidence,
                    reason=final_reason,
                    breakout_level=breakout_level,
                    entry_low=entry_low,
                    entry_high=entry_high,
                    entry_ref=entry_ref,
                    stop=stop,
                    tp1=tp1,
                    tp2=tp2,
                    price=price,
                    oi_jump_pct=oi_jump_pct,
                    funding_pct=funding_pct,
                    vol_ratio=vol_ratio,
                    retest_bars_waited=retest_bars_waited,
                    config_version=str(self.cfg.get("config_version", "")),
                    strategy=strategy,
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
                    retest_depth_pct=retest_depth_pct,
                    score_oi=score_oi,
                    score_exhaustion=score_exhaustion,
                    score_breakout=score_breakout,
                    score_retest=score_retest,
                    reason_tags=reason_tags,
                    stop_was_forced_min_risk=stop_was_forced_min_risk,
                    manual_tradable=manual_eval["manual_tradable"],
                    manual_trade_note=manual_eval["manual_trade_note"],
                    regime_label=self._normalize_regime_label_value(
                        row.get("regime_label", ""), row.get("market_regime", ""), row.get("btc_regime", "")
                    ),
                    regime_fit_for_strategy=self._derive_regime_fit_for_strategy(
                        strategy, side, row.get("regime_label") or row.get("market_regime") or row.get("btc_regime") or "unclear_mixed"
                    ),
                    dispatch_action=row.get("dispatch_action") or "not_evaluated",
                    dispatch_confidence_band="not_evaluated",
                    dispatch_reason="not_evaluated",
                    status="OPEN",
                )
                if strategy == "short_exhaustion_retest":
                    min_send = float(short_cfg.get("score_min_send", 70.0))
                    if signal.score < min_send:
                        self.close_pending(row["pending_id"], "REJECTED_SCORE", f"score below min_send {min_send}", retest_bars_waited)
                        continue
                confirmed.append(signal)
                self.close_pending(row["pending_id"], "CONFIRMED", "signal confirmed", retest_bars_waited)
            except Exception as e:
                print(f"[pending warn] {row.get('pending_id')}: {e}")

        return confirmed

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
                strategy = self.infer_legacy_strategy(row)

                interval = self.cfg["scanner"]["interval_15m"] if strategy == "short_exhaustion_retest" else self.cfg["scanner"]["interval_5m"]
                bars = self.klines(symbol, interval, limit=max_bars_after_entry + 40)
                closed_bars = bars[:-1]
                future_bars = [b for b in closed_bars if b["open_time"] > signal_ts][:max_bars_after_entry]

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

                if outcome is None and len(future_bars) >= max_bars_after_entry:
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
        self.print_breakdown(rows, label="strategy", key_fn=lambda r: r.get("strategy", "legacy_5m_retest"))
        self.print_breakdown(rows, label="btc_regime", key_fn=lambda r: r.get("btc_regime", "unknown"))
        self.print_strategy_pipeline_summary()
        self.print_pending_reason_breakdown()
        self.print_pending_age_summary()
        self.print_snapshot_stage_summary()
        self.print_score_component_summary()
        self.print_result_breakdown_by_score_bucket()
        self.print_outcome_breakdown_by_strategy_side()
        self.print_manual_trading_diagnostics()
        self.export_review_candidates()
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
            strategy = row.get("strategy", "legacy_5m_retest") or "legacy_5m_retest"
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
            strategy = row.get("strategy", "legacy_5m_retest") or "legacy_5m_retest"
            side = row.get("side", "UNKNOWN") or "UNKNOWN"
            status = (row.get("status", "") or "").upper()
            g = ensure(strategy, side)
            g["signals"] += 1
            if status == "OPEN":
                g["open"] += 1

        for row in result_rows:
            strategy = row.get("strategy", "legacy_5m_retest") or "legacy_5m_retest"
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

                    strategy = cell(row_vals, "strategy", "legacy_5m_retest") or "legacy_5m_retest"
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

    def print_snapshot_stage_summary(self):
        cfg = self._observability_cfg()
        if not cfg.get("enabled", True):
            return

        rows = self.read_csv(self.snapshot_index_file)
        if not rows:
            print("SnapshotSummary | no snapshots yet")
            return
        groups: Dict[str, int] = {}
        for row in rows:
            key = f"{row.get('strategy','unknown')}|{row.get('stage','unknown')}"
            groups[key] = groups.get(key, 0) + 1
        for key in sorted(groups.keys()):
            strategy, stage = key.split("|", 1)
            print(f"Snapshot[{strategy}] | stage={stage} | total={groups[key]}")

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

    def export_market_opportunity_review(self, symbols: List[str], tickers: Dict[str, Dict]):
        cfg = self._observability_cfg()
        if not cfg.get("enabled", True):
            return
        top_n = int(cfg.get("market_review_top_n", 20))
        min_abs_change = float(cfg.get("market_review_min_abs_change_pct", 4.0))
        today = datetime.now(timezone.utc)
        out_path = self.market_review_dir / f"{today.strftime('%Y-%m-%d')}.csv"

        ranked = []
        for sym in symbols:
            t = tickers.get(sym)
            if not t:
                continue
            try:
                chg = float(t.get("priceChangePercent", 0.0))
                qv = float(t.get("quoteVolume", 0.0))
                ranked.append((sym, chg, qv))
            except Exception:
                continue
        ranked.sort(key=lambda x: (abs(x[1]), x[2]), reverse=True)
        ranked = [r for r in ranked if abs(r[1]) >= min_abs_change][:top_n]

        pending_rows = self.read_csv(self.pending_file)
        signal_rows = self.read_csv(self.signals_file)
        today_str = today.strftime('%Y-%m-%d')

        rows = []
        for sym, chg, qv in ranked:
            pend_for_sym = [r for r in pending_rows if r.get("symbol") == sym]
            sig_for_sym = [r for r in signal_rows if r.get("symbol") == sym]
            strategy_hint = "long_breakout_retest" if chg >= 0 else "short_exhaustion_retest"
            side_hint = "LONG_OPPORTUNITY" if chg >= 0 else "SHORT_OPPORTUNITY"
            opp_type = "TOP_GAINER" if chg >= 0 else "TOP_LOSER"
            bot_status = "NOT_DETECTED"
            miss_stage = "SCAN"
            miss_type = "BAD_MISS"
            reason_code = "not_detected"
            reason_text = "No pending or sent signal for top mover"
            setup_id = ""
            signal_id = ""

            if sig_for_sym:
                latest_sig = sorted(sig_for_sym, key=lambda r: self._to_int(r.get("timestamp_ms"), 0), reverse=True)[0]
                bot_status = "SENT"
                miss_stage = "SEND"
                miss_type = "CAUGHT"
                reason_code = "sent"
                reason_text = latest_sig.get("reason", "signal sent")
                setup_id = latest_sig.get("setup_id", "")
                signal_id = latest_sig.get("signal_id", "")
            elif pend_for_sym:
                latest_pen = sorted(pend_for_sym, key=lambda r: self._to_int(r.get("created_ts_ms"), 0), reverse=True)[0]
                setup_id = latest_pen.get("setup_id", latest_pen.get("pending_id", ""))
                status = (latest_pen.get("status", "") or "").upper()
                send_decision = (latest_pen.get("send_decision", "") or "").upper()
                skip_reason = latest_pen.get("skip_reason", "") or latest_pen.get("close_reason", "")
                if send_decision == "SKIPPED_TOP_N":
                    bot_status = "CONFIRMED_SKIPPED_SELECTION"
                    miss_stage = "SELECTION"
                    miss_type = "BAD_MISS"
                    reason_code = "skipped_top_n"
                    reason_text = skip_reason or "Confirmed but skipped by top_n selection"
                elif status == "PENDING":
                    bot_status = "PENDING"
                    miss_stage = "RETEST_CONFIRM"
                    miss_type = "WATCHING"
                    reason_code = "pending"
                    reason_text = latest_pen.get("reason", "Waiting for retest confirmation")
                elif status == "CONFIRMED":
                    bot_status = "CONFIRMED_NOT_SENT"
                    miss_stage = "SELECTION"
                    miss_type = "BAD_MISS"
                    reason_code = "confirmed_not_sent"
                    reason_text = skip_reason or latest_pen.get("close_reason", "confirmed but not sent")
                elif status in ("REJECTED_SCORE", "REJECTED_RULE"):
                    bot_status = "REJECTED"
                    miss_stage = "SETUP_LOGIC"
                    miss_type = "BAD_MISS"
                    reason_code = status.lower()
                    reason_text = latest_pen.get("close_reason", status.lower())
                elif status in ("INVALIDATED", "EXPIRED_WAIT"):
                    bot_status = status
                    miss_stage = "RETEST_CONFIRM"
                    miss_type = "GOOD_MISS" if abs(chg) < (min_abs_change + 2.0) else "BAD_MISS"
                    reason_code = status.lower()
                    reason_text = latest_pen.get("close_reason", status.lower())

            manual_tradable = "yes" if abs(chg) >= min_abs_change else "no"
            improvement_candidate = "yes" if bot_status not in ("SENT", "PENDING") and manual_tradable == "yes" else "no"
            rows.append({
                "review_ts_ms": int(time.time() * 1000),
                "review_date": today_str,
                "symbol": sym,
                "side_hint": side_hint,
                "strategy_hint": strategy_hint,
                "market_regime": self.current_market_snapshot.get("market_regime", "unknown"),
                "opportunity_type": opp_type,
                "bot_status": bot_status,
                "miss_stage": miss_stage,
                "miss_type": miss_type,
                "reason_code": reason_code,
                "reason_text": reason_text,
                "setup_id": setup_id,
                "signal_id": signal_id,
                "manual_tradable": manual_tradable,
                "improvement_candidate": improvement_candidate,
                "note": f"24h_change={chg:.2f}% quote_volume={qv:.0f}",
            })

        self.write_csv(out_path, rows, fieldnames=self.market_review_fields)

    def export_review_candidates(self):
        cfg = self._observability_cfg()
        if not cfg.get("enabled", True):
            return
        if not cfg.get("export_review_candidates", True):
            return

        max_rows = int(cfg.get("review_candidates_max_rows", 50))
        out_path = self.review_candidates_dir / f"{datetime.now(timezone.utc).strftime("%Y-%m-%d")}.csv"

        signals = {r.get("signal_id", ""): r for r in self.read_csv(self.signals_file)}
        snapshots = self.read_csv(self.snapshot_index_file)
        latest_snapshot_by_key: Dict[str, Dict] = {}
        for row in snapshots:
            key = f"{row.get('signal_id','')}|{row.get('pending_id','')}|{row.get('stage','')}"
            latest_snapshot_by_key[key] = row

        review_rows: List[Dict] = []
        for row in self.read_csv(self.results_file):
            sig = signals.get(row.get("signal_id", ""), {})
            score = self._to_float(sig.get("score"), 0.0)
            outcome = row.get("outcome", "")
            category = ""
            if score >= 90 and outcome in ("LOSS_STOP", "EXPIRED"):
                category = "high_score_failed"
            elif score < 80 and outcome in ("WIN_TP1", "WIN_TP2"):
                category = "low_score_won"
            elif outcome == "LOSS_STOP":
                category = "all_stops"
            if not category:
                continue
            snap = latest_snapshot_by_key.get(f"{row.get('signal_id','')}||closed", {})
            review_rows.append({
                "category": category,
                "signal_id": row.get("signal_id", ""),
                "pending_id": "",
                "symbol": row.get("symbol", ""),
                "side": row.get("side", ""),
                "strategy": row.get("strategy", ""),
                "score": f"{score:.2f}",
                "outcome": outcome,
                "r_multiple": row.get("r_multiple", ""),
                "status": outcome,
                "close_reason": row.get("close_reason", ""),
                "image_path": snap.get("image_path", ""),
            })

        for row in self.read_csv(self.pending_file):
            status = (row.get("status", "") or "").upper()
            if status not in ("INVALIDATED", "EXPIRED_WAIT"):
                continue
            category = "pending_invalidated" if status == "INVALIDATED" else "pending_expired"
            snap = latest_snapshot_by_key.get(f"|{row.get('pending_id','')}|pending", {})
            review_rows.append({
                "category": category,
                "signal_id": "",
                "pending_id": row.get("pending_id", ""),
                "symbol": row.get("symbol", ""),
                "side": row.get("side", ""),
                "strategy": row.get("strategy", ""),
                "score": row.get("score", ""),
                "outcome": "",
                "r_multiple": "",
                "status": status,
                "close_reason": row.get("close_reason", ""),
                "image_path": snap.get("image_path", ""),
            })

        review_rows.sort(key=lambda r: (r.get("category", ""), self._to_float(r.get("score"), 0.0)), reverse=True)
        fieldnames = ["category", "signal_id", "pending_id", "symbol", "side", "strategy", "score", "outcome", "r_multiple", "status", "close_reason", "image_path"]
        self.write_csv(out_path, review_rows[:max_rows], fieldnames=fieldnames)
        print(f"ReviewCandidates | exported={min(len(review_rows), max_rows)} | file={out_path.name}")

    def _infer_strategy_from_row(self, row: Dict) -> str:
        strategy = (row.get("strategy", "") or "").strip()
        if strategy:
            return strategy
        reason = (row.get("reason", "") or row.get("manual_trade_note", "") or "").lower()
        side = (row.get("side", "") or "").upper()
        if "exhaustion" in reason or "retest fail" in reason or side == "SHORT":
            return "short_exhaustion_retest" if side == "SHORT" else "legacy_5m_retest"
        return "legacy_5m_retest"

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
            strategy = row.get('strategy', 'legacy_5m_retest') or 'legacy_5m_retest'
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
            strategy = row.get('strategy', 'legacy_5m_retest') or 'legacy_5m_retest'
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
            key = f"{row.get('strategy','legacy_5m_retest')}|{row.get('side','UNKNOWN')}"
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
        print(f"[startup] strategy.long_breakout_retest.enabled={self.cfg.get('strategy', {}).get('long_breakout_retest', {}).get('enabled', True)}")
        print(f"[startup] strategy.short_exhaustion_retest.enabled={self.cfg.get('strategy', {}).get('short_exhaustion_retest', {}).get('enabled', False)}")
        print(f"[startup] short_exhaustion_retest.retest_15m_max_bars={self.cfg.get('short_exhaustion_retest', {}).get('retest_15m_max_bars', 3)}")
        print(f"[startup] short_exhaustion_retest.score_min_send={self.cfg.get('short_exhaustion_retest', {}).get('score_min_send', 70)}")
        print(f"[startup] btc_sentiment.bullish_threshold_pct={self.cfg.get('btc_sentiment', {}).get('bullish_threshold_pct', 1.0)}")
        print(f"[startup] btc_sentiment.bearish_threshold_pct={self.cfg.get('btc_sentiment', {}).get('bearish_threshold_pct', -1.0)}")
        print(f"[startup] review_snapshots.enabled={self.cfg.get('review_snapshots', {}).get('enabled', False)}")
        print(f"[startup] review_snapshots.output_dir={self.cfg.get('review_snapshots', {}).get('output_dir', 'review_snapshots')}")
        print(f"[startup] review_case_system.enabled={self.review_case_system_enabled}")
        print(f"[startup] review_case_system.workspace_dir={self.review_case_workspace}")
        print(f"[startup] review_case_system.builder_script={self.review_builder_script}")
        print(f"[startup] observability.enabled={self.cfg.get('observability', {}).get('enabled', True)}")
        print(f"[startup] storage.data_dir={self.data_dir}")
        print(f"[startup] storage.pending_dir={self.pending_dir}")
        print(f"[startup] storage.signals_dir={self.signals_dir}")
        print(f"[startup] storage.results_dir={self.results_dir}")
        print(f"[startup] observability.review_candidates_file={self.cfg.get('observability', {}).get('review_candidates_file', 'review_candidates.csv')}")
        print(f"[startup] observability.score_component_summary={self.cfg.get('observability', {}).get('score_component_summary', True)}")
        print(f"[startup] observability.market_review_top_n={self.cfg.get('observability', {}).get('market_review_top_n', 20)}")
        print("=" * 72)

    def scan_once(self):
        self.round_idx += 1
        self._reset_round_detect_funnel()
        now_ms = int(time.time() * 1000)
        print(f"[round start] idx={self.round_idx} time_ms={now_ms}")

        tickers = self.load_24h_tickers()
        symbols = self.filter_symbols(self.load_symbols(), tickers)
        self.build_market_snapshot(symbols, tickers)
        regime = classify_regime(self, now_ms=now_ms)
        self.current_regime = regime
        print(
            f"Regime | label={regime.regime_label} | conf={regime.regime_confidence} | "
            f"long_fit={regime.regime_fit_long_breakout} | short_fit={regime.regime_fit_short_exhaustion} | "
            f"note={regime.regime_note}"
        )

        self.evaluate_open_signals()
        confirmed = self.process_pending_setups()
        sent_count = 0
        skipped_top_n = 0

        if confirmed:
            confirmed.sort(key=lambda x: x.score, reverse=True)
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
                strategy_fit = getattr(s, "regime_fit_for_strategy", "MEDIUM") or "MEDIUM"
                if s.dispatch_action == "MAIN_SIGNAL" and strategy_fit == "LOW":
                    s.dispatch_action = "WATCHLIST"
                    s.dispatch_confidence_band = "MEDIUM"
                    s.dispatch_reason = f"{dispatch.dispatch_reason}|regime_downgrade_low_fit"

                if s.dispatch_action == "MAIN_SIGNAL":
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

                if dispatch.dispatch_action == "WATCHLIST":
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

        self.export_market_opportunity_review(symbols, tickers)
        self.collect_due_case_close_fallbacks()
        self.print_stats()

        new_pending = 0

        for sym in symbols:
            try:
                setups = self.build_pending_setups_for_symbol(sym)
                for p in setups:
                    p.regime_label = regime.regime_label
                    if p.side == "LONG":
                        p.regime_fit_for_strategy = regime.regime_fit_long_breakout
                    elif p.side == "SHORT":
                        p.regime_fit_for_strategy = regime.regime_fit_short_exhaustion
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

        self._print_detect_funnel_summary()
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
        simulation_prev_snapshots = getattr(self, "enable_snapshots", True)
        self.enable_snapshots = False
        try:
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
        finally:
            self.enable_snapshots = simulation_prev_snapshots

    def run_forever(self):
        sec = int(self.cfg["scanner"]["loop_seconds"])
        while True:
            try:
                self.scan_once()
            except Exception as e:
                print(f"[fatal loop warn] {e}")
            time.sleep(sec)


def load_config(path: str) -> Dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("config_path", nargs="?", default="config.yaml")
    parser.add_argument("--simulate-case", dest="simulate_case", default="")
    parser.add_argument("--build-review-pack", dest="build_review_pack", default="")
    args = parser.parse_args()

    cfg = load_config(args.config_path)
    scanner = BinanceScanner(cfg)
    scanner._write_runtime_build_marker()
    scanner.startup_print(args.config_path)

    if args.simulate_case:
        scanner.run_simulation_case(args.simulate_case)
        return
    if args.build_review_pack:
        ok = scanner.build_daily_review_pack(args.build_review_pack)
        print("REVIEW_PACK_OK" if ok else "REVIEW_PACK_FAILED")
        return

    scanner.run_forever()


if __name__ == "__main__":
    main()
