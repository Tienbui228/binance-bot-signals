"""
scripts/run_signature_measurement.py
CLI entry point for the R2-lite offline signature measurement pipeline.

Usage:
  python scripts/run_signature_measurement.py [--config research_config.yaml] [--run-id my_run]

Pipeline:
  1. Load config
  2. Determine 30-day historical window
  3. Load + filter universe symbols
  4. Fetch + cache all data (klines, top/global ratios, OI, basis) for each symbol
  5. Fetch BTCUSDT data (always, for market context)
  6. Compute cross-symbol context (alt breadth, BTC 24h change series)
  7. Per symbol: align data, compute all features, evaluate rule, build events
  8. Collect all events across symbols
  9. Apply 60-min cooldown dedup filter
 10. Compute outcomes (future path, payoff, outcome class, move persistence)
 11. Run baseline rules for comparison
 12. Build 4 boards + decision code
 13. Write outputs: rule_event_log.csv, rule_measurement_details.csv, rule_measurement_summary.md

Offline research only. Live runtime files are not imported or modified.
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import yaml

# r2_lite root = parent of scripts/
R2_LITE_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(R2_LITE_ROOT))

from research.signature_measurement.io import ResearchCache, ResearchFetcher, align_symbol_data
from research.signature_measurement.proxy_features import enrich_proxy_features, compute_data_quality_flags
from research.signature_measurement.classifier import (
    compute_compression_score, compute_breakout_level,
    compute_price_structure_features, compute_breakout_quality,
    classify_regime_label_candidate, classify_btc_24h_band, classify_alt_breadth_band,
    compute_symbol_volatility_band, compute_alt_breadth_series, compute_btc_24h_series,
)
from research.signature_measurement.rule_engine import evaluate_candidate_rule, evaluate_baseline_rule
from research.signature_measurement.event_builder import build_candidate_events, apply_first_fire_cooldown
from research.signature_measurement.outcome_engine import (
    compute_future_path_metrics, compute_payoff_metrics,
    classify_outcome_class, classify_move_persistence,
)
from research.signature_measurement.report_builder import (
    build_board_a, build_board_b, build_board_c, build_board_d,
    determine_decision_code, render_markdown_report,
    write_event_log_csv, write_measurement_details_csv,
)
from research.signature_measurement.contracts import F

log = logging.getLogger("signature_measurement")


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------

def load_config(path: str) -> Dict:
    p = Path(path)
    if not p.is_absolute():
        p = R2_LITE_ROOT / p
    with open(p, "r") as fh:
        return yaml.safe_load(fh)


def _resolve(rel_or_abs: str) -> Path:
    """Resolve path from config: if relative, anchor to R2_LITE_ROOT."""
    p = Path(rel_or_abs)
    return p if p.is_absolute() else R2_LITE_ROOT / p


# ---------------------------------------------------------------------------
# Window determination
# ---------------------------------------------------------------------------

def compute_window(window_days: int) -> tuple[int, int]:
    """Return (start_ms, end_ms) for the last N days."""
    now_ms = int(time.time() * 1000)
    start_ms = now_ms - window_days * 24 * 3600 * 1000
    return start_ms, now_ms


# ---------------------------------------------------------------------------
# Per-symbol feature pipeline
# ---------------------------------------------------------------------------

def process_symbol(
    symbol: str,
    data: Dict[str, pd.DataFrame],
    cfg: Dict,
    btc_24h_series: pd.Series,
    alt_breadth_series: pd.Series,
) -> pd.DataFrame:
    """
    Align raw data, compute all features, and return a feature-enriched DataFrame.
    Returns empty DataFrame if data is insufficient.
    """
    rm_cfg = cfg["research_measurement"]

    df = align_symbol_data(data)
    if df.empty:
        log.warning("%s: empty aligned data, skipping", symbol)
        return pd.DataFrame()

    df = df.sort_values("open_time").reset_index(drop=True)

    # Price structure
    df = compute_price_structure_features(df, atr_proxy_bars=rm_cfg.get("atr_proxy_bars", 20))

    # Compression score
    df["compression_score"] = compute_compression_score(
        df,
        recent_bars=rm_cfg.get("compression_recent_bars", 6),
        prev_bars=rm_cfg.get("compression_prev_bars", 12),
    )

    # Breakout level
    df["breakout_level"] = compute_breakout_level(
        df, lookback=rm_cfg.get("breakout_level_lookback", 12)
    )

    # Volume features + proxy features (z-scores, imbalances)
    df = enrich_proxy_features(
        df,
        roll_z_n=rm_cfg.get("roll_z_n", 96),
        roll_baseline_n=rm_cfg.get("roll_baseline_n", 288),
    )

    # Breakout quality (needs breakout_level, volume_ratio_3bar, oi_delta_3bar_pct, body_ratio, upper_wick_ratio)
    df = compute_breakout_quality(df)

    # Volatility band
    df["volatility_band"] = compute_symbol_volatility_band(df)

    # --- Join cross-symbol market context ---
    # BTC 24h change
    if not btc_24h_series.empty:
        df_indexed = df.set_index("open_time")
        btc_aligned = btc_24h_series.reindex(df_indexed.index, method="nearest", tolerance=5*60*1000)
        df["btc_24h_change_pct"] = btc_aligned.values
    else:
        df["btc_24h_change_pct"] = float("nan")

    # Alt breadth
    if not alt_breadth_series.empty:
        df_indexed = df.set_index("open_time")
        breadth_aligned = alt_breadth_series.reindex(df_indexed.index, method="nearest", tolerance=5*60*1000)
        df["alt_breadth_pct"] = breadth_aligned.values
    else:
        df["alt_breadth_pct"] = float("nan")

    # Regime label
    def _regime(row):
        btc = row.get("btc_24h_change_pct")
        alt = row.get("alt_breadth_pct")
        if pd.isna(btc) or pd.isna(alt):
            return "unclear_mixed"
        return classify_regime_label_candidate(float(btc), float(alt))

    df["regime_label_candidate"] = df.apply(_regime, axis=1)

    # BTC + breadth bands
    df["btc_24h_band"] = df["btc_24h_change_pct"].apply(
        lambda x: classify_btc_24h_band(float(x)) if pd.notna(x) else "unknown"
    )
    df["alt_breadth_band"] = df["alt_breadth_pct"].apply(
        lambda x: classify_alt_breadth_band(float(x)) if pd.notna(x) else "unknown"
    )

    # Data quality flags
    df = compute_data_quality_flags(df)

    return df


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def run_pipeline(cfg: Dict, run_id: str) -> None:
    rm_cfg = cfg["research_measurement"]
    uni_cfg = cfg["universe"]
    cache_cfg = cfg["cache"]
    out_cfg = cfg["output"]

    period = rm_cfg.get("timeframe", "5m")
    side = rm_cfg.get("side", "LONG")
    rule_family = rm_cfg.get("rule_family", "long_breakout_retest_signature")
    rule_version = rm_cfg.get("rule_version", "v1")
    measurement_version = rm_cfg.get("measurement_version", "v1")
    universe_version = uni_cfg.get("version", "v1")
    cooldown_min = rm_cfg.get("cooldown_minutes", 60)
    adverse_floor = rm_cfg.get("adverse_floor", 0.10)
    rule_v1_cfg = rm_cfg.get("rule_v1", {})
    outcome_thresholds = rm_cfg.get("outcome_thresholds", {})

    window_days = uni_cfg.get("window_days", 30)
    start_ms, end_ms = compute_window(window_days)

    start_dt = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
    end_dt = datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")

    log.info("=== R2-LITE MEASUREMENT RUN %s ===", run_id)
    log.info("Window: %s → %s (%d days)", start_dt, end_dt, window_days)

    # Output directory
    out_dir = _resolve(out_cfg.get("base_dir", "data/research_outputs")) / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    # Fetcher + cache
    cache = ResearchCache(str(_resolve(cache_cfg.get("base_dir", "data/research_cache/binance_proxy"))))
    fetcher = ResearchFetcher(
        cache=cache,
        rate_limit_sleep=cache_cfg.get("rate_limit_sleep_sec", 0.25),
        max_retries=cache_cfg.get("max_retries", 3),
        retry_sleep=cache_cfg.get("retry_sleep_sec", 2.0),
    )

    # --- Universe ---
    log.info("Loading universe...")
    all_symbols = fetcher.load_universe(
        quote_asset=uni_cfg.get("quote_asset", "USDT"),
        exclude=uni_cfg.get("exclude_symbols", []),
    )
    symbols = fetcher.filter_by_volume(
        all_symbols,
        min_qv=float(uni_cfg.get("min_quote_volume_usdt_24h", 50_000_000)),
        max_count=int(uni_cfg.get("max_symbols", 200)),
    )
    # Always ensure BTCUSDT is fetched for market context
    btc_sym = "BTCUSDT"
    if btc_sym not in symbols:
        symbols_with_btc = [btc_sym] + symbols
    else:
        symbols_with_btc = symbols
    log.info("Universe: %d symbols (+ BTCUSDT)", len(symbols))

    # --- Fetch all raw data ---
    log.info("Fetching historical data for all symbols (cached where possible)...")
    all_klines: Dict[str, pd.DataFrame] = {}
    all_symbol_data: Dict[str, Dict[str, pd.DataFrame]] = {}

    for i, sym in enumerate(symbols_with_btc):
        if i % 20 == 0:
            log.info("  %d/%d symbols fetched", i, len(symbols_with_btc))
        data = fetcher.fetch_symbol_data(sym, period, start_ms, end_ms)
        all_klines[sym] = data.get("klines", pd.DataFrame())
        all_symbol_data[sym] = data

    # --- Cross-symbol market context ---
    log.info("Computing market context (alt breadth, BTC 24h series)...")
    btc_24h_series = compute_btc_24h_series(all_klines.get(btc_sym, pd.DataFrame()))
    alt_breadth_series = compute_alt_breadth_series(
        {s: all_klines[s] for s in symbols}  # exclude BTC from breadth calc
    )

    # --- Per-symbol feature pipeline + rule evaluation ---
    log.info("Processing symbols: features + rule evaluation...")
    all_raw_events: List[Dict[str, Any]] = []

    for sym in symbols:
        df = process_symbol(
            sym,
            all_symbol_data.get(sym, {}),
            cfg,
            btc_24h_series=btc_24h_series,
            alt_breadth_series=alt_breadth_series,
        )
        if df.empty:
            continue

        signal_mask = evaluate_candidate_rule(df, rule_v1_cfg)
        fire_count = signal_mask.sum()
        if fire_count == 0:
            continue

        log.debug("%s: %d raw fires", sym, fire_count)
        events = build_candidate_events(
            df=df,
            symbol=sym,
            signal_mask=signal_mask,
            rule_family=rule_family,
            rule_version=rule_version,
            measurement_version=measurement_version,
            universe_version=universe_version,
            side=side,
            timeframe=period,
        )
        all_raw_events.extend(events)

    log.info("Total raw fires across all symbols: %d", len(all_raw_events))

    # --- Overlap dedup ---
    log.info("Applying %d-min cooldown dedup...", cooldown_min)
    kept_events, fire_stats = apply_first_fire_cooldown(all_raw_events, cooldown_minutes=cooldown_min)
    log.info("Kept: %d / Dropped: %d", fire_stats["kept_fire_count"], fire_stats["dropped_overlap_count"])

    # --- Outcome computation ---
    log.info("Computing outcomes...")
    kept_events = compute_future_path_metrics(kept_events, all_klines)
    kept_events = compute_payoff_metrics(kept_events, adverse_floor=adverse_floor)
    kept_events = classify_outcome_class(kept_events, outcome_thresholds)
    kept_events = classify_move_persistence(kept_events)

    # --- Build report boards ---
    log.info("Building measurement boards...")
    events_df = pd.DataFrame(kept_events)

    board_a = build_board_a(events_df, fire_stats)
    board_b = build_board_b(events_df)
    board_c = build_board_c(events_df)
    board_d = build_board_d(events_df)

    # --- Baseline comparison (optional) ---
    baseline_dfs: List[pd.DataFrame] = []
    for bl_cfg in rm_cfg.get("baselines", []):
        bl_name = bl_cfg.get("name", "baseline")
        bl_events: List[Dict] = []
        for sym in symbols:
            df = process_symbol(sym, all_symbol_data.get(sym, {}), cfg,
                                btc_24h_series=btc_24h_series,
                                alt_breadth_series=alt_breadth_series)
            if df.empty:
                continue
            bl_mask = evaluate_baseline_rule(df, bl_cfg)
            if bl_mask.sum() == 0:
                continue
            evs = build_candidate_events(
                df=df, symbol=sym, signal_mask=bl_mask,
                rule_family=bl_name,
                rule_version=bl_cfg.get("rule_version", "baseline_v1"),
                measurement_version=measurement_version,
                universe_version=universe_version, side=side, timeframe=period,
            )
            bl_events.extend(evs)
        bl_kept, bl_stats = apply_first_fire_cooldown(bl_events, cooldown_minutes=cooldown_min)
        bl_kept = compute_future_path_metrics(bl_kept, all_klines)
        bl_kept = compute_payoff_metrics(bl_kept, adverse_floor=adverse_floor)
        bl_kept = classify_outcome_class(bl_kept, outcome_thresholds)
        if bl_kept:
            bl_df = pd.DataFrame(bl_kept)
            bl_df[F.BASELINE_NAME] = bl_name
            baseline_dfs.append(bl_df)
            ba_bl = build_board_a(bl_df, bl_stats)
            log.info("Baseline [%s]: kept=%d, a_or_better=%.1f%%",
                     bl_name, bl_stats["kept_fire_count"],
                     ba_bl.get("a_or_better_rate_pct", 0))

    baseline_df = pd.concat(baseline_dfs, ignore_index=True) if baseline_dfs else None

    decision = determine_decision_code(events_df, board_a, baseline_df)

    run_meta = {
        "rule_family": rule_family,
        "rule_version": rule_version,
        "measurement_version": measurement_version,
        "universe_version": universe_version,
        "side": side,
        "window_start": start_dt,
        "window_end": end_dt,
        "run_ts": datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
    }

    md = render_markdown_report(board_a, board_b, board_c, board_d, decision, run_meta)

    # --- Write outputs (Section 18) ---
    log.info("Writing outputs to %s", out_dir)
    write_event_log_csv(kept_events, out_dir / "rule_event_log.csv")
    write_measurement_details_csv(kept_events, out_dir / "rule_measurement_details.csv")
    (out_dir / "rule_measurement_summary.md").write_text(md, encoding="utf-8")

    if out_cfg.get("emit_feature_slice_csv") and board_c:
        import csv
        with open(out_dir / "rule_feature_slice.csv", "w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=["dimension", "slice", "events",
                                                "a_or_better_rate_pct", "fail_rate_pct",
                                                "median_1h_favor_pct", "median_1h_adverse_pct"])
            w.writeheader()
            for dim, slices in board_c.items():
                for s in slices:
                    w.writerow({"dimension": dim, **s})

    if out_cfg.get("emit_regime_slice_csv") and board_d:
        import csv
        with open(out_dir / "rule_regime_slice.csv", "w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=["dimension", "slice", "events",
                                                "a_plus_rate_pct", "a_or_better_rate_pct",
                                                "fail_rate_pct", "median_4h_favor_pct",
                                                "median_4h_adverse_pct"])
            w.writeheader()
            for dim, slices in board_d.items():
                for s in slices:
                    w.writerow({"dimension": dim, **s})

    if baseline_df is not None:
        baseline_df.to_csv(out_dir / "baseline_measurement_details.csv", index=False)

    log.info("=== Run complete. Decision: %s ===", decision.get("rule_decision_code"))
    log.info("Outputs: %s", out_dir)

    # Print summary to stdout
    print(f"\n{'='*60}")
    print(f"R2-LITE MEASUREMENT COMPLETE")
    print(f"Rule:     {rule_family} {rule_version}")
    print(f"Window:   {start_dt} → {end_dt}")
    print(f"Events:   kept={fire_stats['kept_fire_count']}, eligible={board_a.get('measurement_eligible_events')}")
    print(f"A+:       {board_a.get('a_plus_rate_pct', 0):.1f}%   A-or-better: {board_a.get('a_or_better_rate_pct', 0):.1f}%   Fail: {board_a.get('fail_rate_pct', 0):.1f}%")
    print(f"Decision: {decision.get('rule_decision_code')}")
    print(f"Outputs:  {out_dir}")
    print(f"{'='*60}\n")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="R2-lite offline signature measurement pipeline"
    )
    parser.add_argument(
        "--config", default=str(R2_LITE_ROOT / "research_config.yaml"),
        help="Path to research_config.yaml (default: r2_lite/research_config.yaml)"
    )
    parser.add_argument(
        "--run-id", default=None,
        help="Run identifier for output directory. Default: timestamp."
    )
    parser.add_argument(
        "--log-level", default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    run_id = args.run_id or datetime.now(tz=timezone.utc).strftime("run_%Y%m%d_%H%M%S")
    cfg = load_config(args.config)
    run_pipeline(cfg, run_id)


if __name__ == "__main__":
    main()
