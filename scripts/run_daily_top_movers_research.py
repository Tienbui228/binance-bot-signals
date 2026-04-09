#!/usr/bin/env python3
"""
scripts/run_daily_top_movers_research.py

Phase R1 Daily Top Movers Research — main orchestration script.

Usage:
  python3 scripts/run_daily_top_movers_research.py --day 2026-04-04

Produces 4 required artifacts:
  1. data/research_output/top_movers/YYYY-MM-DD/report/daily_top_mover_research_pack.docx
  2. data/research_output/top_movers/YYYY-MM-DD/csv/daily_research_summary.csv
  3. data/research_output/top_movers/YYYY-MM-DD/csv/daily_signature_candidates.csv
  4. data/research_output/top_movers/rollups/signature_evidence_ledger.csv  (daily replace/upsert)

Architecture boundary: does NOT touch live runtime files.
"""

import argparse
import os
import sys
import traceback
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from research.top_movers.research_binance_client import ResearchBinanceClient
from research.top_movers.io import (
    ensure_output_dirs, day_window_ms, lookback_start_ms,
    load_or_fetch, write_csv, write_markdown, csv_path, dated_csv_path, report_path,
)
from research.top_movers.daily_selector import (
    select_daily_top_movers, selection_to_movers_list_rows,
)
from research.top_movers.canonical_move import build_canonical_move
from research.top_movers.proxy_features import build_proxy_features
from research.top_movers.anchor_detector import detect_anchors
from research.top_movers.case_builder import (
    make_case_id, build_case_row, build_anchor_snapshot_rows, apply_image_results,
)
from research.top_movers.image_renderer import render_images
from research.top_movers.report_builder import build_report
from research.top_movers.docx_report_builder import build_docx_pack
from research.top_movers.signature_ledger import (
    extract_signature_candidates,
    write_candidates_csv,   # always writes header, even for 0 rows
    upsert_ledger,          # daily replace/upsert by research_day
    build_daily_research_summary,
    build_daily_journal_row,
    CANDIDATE_SCHEMA,
)

DOCX_FILENAME = "daily_top_mover_research_pack.docx"


def fetch_token_raw_data(client, symbol, research_day, day_start_ms, day_end_ms, lb_start_ms) -> Dict:
    """Fetch or load cached raw data for one token.
    Includes 4h post-day extension for retrospective outcome measurement.
    """
    post_day_end_ms = day_end_ms + 4 * 3600 * 1000

    def _fetch(key, fn): return load_or_fetch(research_day, f"{symbol}_{key}", fn)

    bars_5m      = _fetch("klines_5m",          lambda: client.get_klines(symbol, "5m",  lb_start_ms, day_end_ms, limit=500))
    bars_5m_ext  = _fetch("klines_5m_ext",       lambda: client.get_klines(symbol, "5m",  day_end_ms, post_day_end_ms, limit=50))
    bars_15m     = _fetch("klines_15m",          lambda: client.get_klines(symbol, "15m", lb_start_ms, day_end_ms, limit=200))
    bars_1h      = _fetch("klines_1h",           lambda: client.get_klines(symbol, "1h",  lb_start_ms, day_end_ms, limit=60))
    oi_hist      = _fetch("oi_hist_5m",          lambda: client.get_oi_hist(symbol, "5m", lb_start_ms, day_end_ms, limit=500))
    top_account  = _fetch("top_account_ratio_5m",lambda: client.get_top_account_ratio(symbol, "5m", lb_start_ms, day_end_ms, limit=500))
    top_position = _fetch("top_position_ratio_5m",lambda: client.get_top_position_ratio(symbol, "5m", lb_start_ms, day_end_ms, limit=500))
    global_acc   = _fetch("global_account_5m",   lambda: client.get_global_account_ratio(symbol, "5m", lb_start_ms, day_end_ms, limit=500))
    taker        = _fetch("taker_ratio_5m",       lambda: client.get_taker_ratio(symbol, "5m", lb_start_ms, day_end_ms, limit=500))
    basis        = _fetch("basis_5m",             lambda: client.get_basis(symbol, "5m",  lb_start_ms, day_end_ms, limit=500))

    # Merge bars_5m + extension (deduplicate by open_time)
    seen = {b["open_time"] for b in bars_5m}
    bars_5m_extended = list(bars_5m)
    for b in bars_5m_ext:
        if b["open_time"] not in seen:
            bars_5m_extended.append(b); seen.add(b["open_time"])
    bars_5m_extended.sort(key=lambda b: b["open_time"])

    return {
        "bars_5m": bars_5m,
        "bars_5m_extended": bars_5m_extended,
        "bars_15m": bars_15m,
        "bars_1h": bars_1h,
        "proxy": {
            "oi_hist": oi_hist, "top_account": top_account, "top_position": top_position,
            "global_account": global_acc, "taker": taker, "basis": basis,
        },
    }


def process_token(client, token_bar, side, rank, research_day, day_start_ms, day_end_ms,
                  lb_start_ms, selection_context):
    symbol = token_bar.symbol
    bucket = "top_gainers" if side == "LONG" else "top_losers"
    print(f"\n[{symbol}] {side} rank={rank}")
    try:
        raw = fetch_token_raw_data(client, symbol, research_day, day_start_ms, day_end_ms, lb_start_ms)
        bars_5m = raw["bars_5m"]; bars_5m_ext = raw["bars_5m_extended"]
        bars_15m = raw["bars_15m"]; bars_1h = raw["bars_1h"]

        data_ok = len(bars_5m) >= 50
        data_note = "" if data_ok else f"insufficient_5m_bars:{len(bars_5m)}"

        token_bar_dict = {
            "daily_return_pct": token_bar.daily_return_pct,
            "day_open": token_bar.day_open, "day_close": token_bar.day_close,
            "day_high": token_bar.day_high, "day_low": token_bar.day_low,
        }
        move = build_canonical_move(symbol, side, research_day, bars_5m, token_bar_dict, day_start_ms)
        if not data_ok: move.data_quality_ok = False; move.data_quality_note = data_note

        proxy = build_proxy_features(symbol, bars_5m, raw["proxy"])
        anchors = detect_anchors(bars_5m=bars_5m, bars_15m=bars_15m, move=move,
                                  oi_delta_series=proxy.oi_delta_3bar,
                                  volume_ratio_series=proxy.volume_ratio_3bar)

        case_id = make_case_id(research_day, symbol, side)
        image_results = render_images(case_id, research_day, symbol, side, anchors, bars_5m, bars_15m, bars_1h)

        case_row = build_case_row(
            case_id=case_id, move=move, anchors=anchors, proxy=proxy,
            bars_5m_extended=bars_5m_ext,
            research_regime=selection_context.get("research_regime","unclear_mixed"),
            btc_24h=selection_context.get("btc_24h_change_pct", 0.0),
            alt_breadth_pct=selection_context.get("alt_breadth_pct", 0.0),
            top_mover_rank=rank, top_mover_bucket=bucket, image_results=image_results,
        )

        anchor_rows = build_anchor_snapshot_rows(case_id, research_day, symbol, anchors, proxy)
        anchor_rows = apply_image_results(anchor_rows, image_results)

        imgs_ok = sum(1 for r in image_results.values() if r.get("created"))
        print(f"  [{symbol}] Done. Images={imgs_ok}/5 Decision={case_row.get('decision_grade')} Eligible={case_row.get('research_eligible_YN')}")
        return case_row, anchor_rows, image_results
    except Exception as e:
        print(f"  [{symbol}] ERROR: {e}"); traceback.print_exc()
        return None, [], {}


def main():
    parser = argparse.ArgumentParser(description="Phase R1 Daily Top Movers Research")
    parser.add_argument("--day", required=True, help="Research day YYYY-MM-DD (UTC)")
    parser.add_argument("--top-n", type=int, default=10)
    parser.add_argument("--rate-limit-sleep", type=float, default=0.10)
    args = parser.parse_args()

    research_day = args.day
    try: datetime.strptime(research_day, "%Y-%m-%d")
    except ValueError: print(f"ERROR: Invalid day '{research_day}'"); sys.exit(1)

    print(f"\n{'='*60}\nPhase R1 Daily Top Movers Research: {research_day}\n{'='*60}\n")

    ensure_output_dirs(research_day)
    client = ResearchBinanceClient(rate_limit_sleep=args.rate_limit_sleep)
    day_start_ms, day_end_ms = day_window_ms(research_day)
    lb_start_ms = lookback_start_ms(day_start_ms)

    # === Step 1: Select top movers ===
    print("=== Step 1: Selecting top movers ===")
    selection = select_daily_top_movers(client, research_day, top_n=args.top_n)
    write_csv(csv_path(research_day, "daily_top_movers_list.csv"),
              selection_to_movers_list_rows(selection))

    selection_context = {
        "research_regime": selection.research_regime,
        "btc_24h_change_pct": selection.btc_24h_change_pct,
        "alt_breadth_pct": selection.alt_breadth_pct,
        "total_eligible_alts": selection.total_eligible_alts,
        "positive_alts": selection.positive_alts,
    }

    # === Step 2: Process each token ===
    print(f"\n=== Step 2: Processing {len(selection.all_tokens)} tokens ===")
    all_case_rows: List[Dict] = []
    all_anchor_rows: List[Dict] = []
    all_image_results: Dict[str, Dict] = {}

    for (token_bar, side), rank in zip(selection.all_tokens,
                                       list(range(1, args.top_n + 1)) * 2):
        case_row, anchor_rows, img_results = process_token(
            client=client, token_bar=token_bar, side=side, rank=rank,
            research_day=research_day, day_start_ms=day_start_ms, day_end_ms=day_end_ms,
            lb_start_ms=lb_start_ms, selection_context=selection_context,
        )
        if case_row is not None:
            all_case_rows.append(case_row)
            all_anchor_rows.extend(anchor_rows)
            cid = case_row.get("case_id","")
            if cid: all_image_results[cid] = img_results

    # === Step 3: Save core CSVs ===
    print(f"\n=== Step 3: Saving CSVs ===")
    write_csv(dated_csv_path(research_day, "daily_case_dataset_{}.csv"), all_case_rows)  # canonical
    write_csv(csv_path(research_day, "daily_top_mover_cases.csv"), all_case_rows)         # compat mirror
    write_csv(csv_path(research_day, "daily_top_mover_anchor_snapshots.csv"), all_anchor_rows)

    # === Step 4: Signature candidates + ledger ===
    print(f"\n=== Step 4: Signature extraction + ledger daily replace ===")
    sig_candidates = extract_signature_candidates(all_case_rows, research_day)

    # Always write candidates CSV with correct schema header (even if 0 rows)
    write_candidates_csv(
        csv_path(research_day, "daily_signature_candidates.csv"),
        sig_candidates,
    )
    write_candidates_csv(
        dated_csv_path(research_day, "daily_signature_candidates_{}.csv"),
        sig_candidates,
    )
    print(f"  Signature candidates: {len(sig_candidates)}" +
          (" (no repeated pattern met threshold)" if not sig_candidates else ""))

    # Daily replace/upsert: purge stale rows for today, insert current run's rows
    upsert_ledger(sig_candidates, research_day)
    print(f"  Ledger updated for {research_day}: {len(sig_candidates)} rows (stale rows purged)")

    # Daily summary
    daily_summary = build_daily_research_summary(
        research_day, selection_context, all_case_rows, sig_candidates)
    write_csv(csv_path(research_day, "daily_research_summary.csv"), [daily_summary])
    write_csv(dated_csv_path(research_day, "daily_research_summary_{}.csv"), [daily_summary])

    journal_row = build_daily_journal_row(research_day, all_case_rows, sig_candidates)
    write_csv(csv_path(research_day, "research_daily_journal.csv"), [journal_row])

    # === Step 5: Build DOCX pack ===
    print(f"\n=== Step 5: Building DOCX research pack ===")
    docx_out = os.path.join("data/research_output/top_movers", research_day, "report", DOCX_FILENAME)
    try:
        build_docx_pack(
            research_day=research_day, selection_context=selection_context,
            cases=all_case_rows, anchor_rows=all_anchor_rows,
            signature_candidates=sig_candidates, output_path=docx_out,
            image_results_all=all_image_results,
        )
        print(f"  Saved: {DOCX_FILENAME}")
    except Exception as e:
        print(f"  DOCX build failed: {e}"); traceback.print_exc()

    # === Step 6: Markdown summary ===
    images_created = sum(
        1 for rows in all_image_results.values()
        for r in rows.values() if r.get("created")
    )
    report_md = build_report(
        research_day=research_day, selection_context=selection_context,
        cases=all_case_rows, anchor_rows=all_anchor_rows, images_created=images_created,
        sig_candidates=sig_candidates,
    )
    write_markdown(report_path(research_day), report_md)

    # === Step 7: Unified analysis pack ===
    print(f"\n=== Step 7: Unified analysis pack ===")
    from research.top_movers.unified_analysis_pack_builder import build_unified_analysis_pack
    unified_out = os.path.join(
        "data/research_output/top_movers", research_day, "report",
        f"R1_unified_analysis_pack_{research_day}.docx",
    )
    try:
        build_unified_analysis_pack(
            research_day=research_day,
            cases=all_case_rows,
            sig_candidates=sig_candidates,
            daily_summary=daily_summary,
            output_path=unified_out,
            window_days=7,
        )
        print(f"  Saved: R1_unified_analysis_pack_{research_day}.docx")
    except Exception as e:
        print(f"  Unified pack build failed: {e}"); traceback.print_exc()


    # === Step 8: Analysis bundle (manifest + xlsx) ===
    print(f'\n=== Step 8: Analysis bundle ===')
    from research.top_movers.analysis_bundle_builder import build_manifest, build_xlsx_bundle
    _bundle_dir = os.path.join('data/research_output/top_movers', research_day, 'report')
    _manifest_out = os.path.join(_bundle_dir, f'analysis_bundle_manifest_{research_day}.md')
    try:
        build_manifest(
            research_day=research_day, cases=all_case_rows,
            sig_candidates=sig_candidates, daily_summary=daily_summary,
            output_path=_manifest_out,
        )
        print(f'  Saved: analysis_bundle_manifest_{research_day}.md')
    except Exception as e:
        print(f'  Manifest failed: {e}'); traceback.print_exc()
    _xlsx_out = os.path.join(_bundle_dir, f'R1_analysis_bundle_{research_day}.xlsx')
    try:
        build_xlsx_bundle(
            research_day=research_day, cases=all_case_rows,
            sig_candidates=sig_candidates, daily_summary=daily_summary,
            output_path=_xlsx_out, window_days=7,
        )
        print(f'  Saved: R1_analysis_bundle_{research_day}.xlsx')
    except Exception as e:
        print(f'  xlsx failed: {e}'); traceback.print_exc()

    # === Summary ===
    print(f"\n{'='*60}")
    print(f"Phase R1 Complete: {research_day}")
    print(f"  Cases:      {len(all_case_rows)} / {len(selection.all_tokens)}")
    print(f"  Eligible:   {sum(1 for c in all_case_rows if c.get('research_eligible_YN')=='Y')}")
    print(f"  Images:     {images_created} / {len(all_case_rows) * 5}")
    print(f"  Signatures: {len(sig_candidates)} (0 = no repeated pattern today, not an error)")
    grade_counts: Dict[str,int] = {}
    for c in all_case_rows:
        g = c.get("decision_grade","?")
        grade_counts[g] = grade_counts.get(g, 0) + 1
    print(f"\nDecision grades (case-level, independent of repeated signatures):")
    for g, cnt in sorted(grade_counts.items(), key=lambda x:-x[1]):
        print(f"    {g}: {cnt}")
    print(f"\nOutputs:")
    print(f"  {docx_out}")
    print(f"  {csv_path(research_day, 'daily_research_summary.csv')}")
    print(f"  {csv_path(research_day, 'daily_signature_candidates.csv')}")
    print(f"  data/research_output/top_movers/rollups/signature_evidence_ledger.csv")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
