"""
research/top_movers/analysis_bundle_builder.py

Builds two companion artifacts per research day:
  1. analysis_bundle_manifest_YYYY-MM-DD.md  — reading guide + artifact table
  2. R1_analysis_bundle_YYYY-MM-DD.xlsx      — all CSV/ledger data in one workbook

xlsx sheets:
  - case_dataset         canonical one-row-per-case (same source as DOCX report)
  - daily_summary        day-level metrics (key/value)
  - signature_candidates today's repeated pattern candidates
  - ledger_snapshot      7-day deduped cross-day view (one row per signature_key)
  - ledger_raw           full raw ledger (all rows, all days)

Downstream-only. Does not touch live runtime, lifecycle, or strategy files.
"""

import csv
import os
from datetime import datetime, timezone
from typing import Dict, List, Optional

from research.top_movers.io import OUTPUT_BASE
from research.top_movers.signature_ledger import (
    LEDGER_PATH,
    load_and_normalize_ledger_rows,
    build_ledger_snapshot_for_report,
)

try:
    import openpyxl
    from openpyxl.styles import Font, PatternFill, Alignment
    HAS_OPENPYXL = True
except ImportError:
    HAS_OPENPYXL = False

# ---------------------------------------------------------------------------
# Shared constants
# ---------------------------------------------------------------------------

_HEADER_FILL  = "2E75B6"   # blue
_ALT_FILL     = "F2F2F2"   # light grey for alternating rows
_HEADER_FONT  = "FFFFFF"   # white

# Fields shown in ledger snapshot sheet (same semantics as report snapshot)
_LEDGER_SNAP_FIELDS = [
    "signature_candidate_code", "first_seen_date", "last_seen_date",
    "support_days_count", "recent_support_days_count",
    "latest_validation_status", "current_role",
]

# Fields shown in ledger_raw sheet (key fields from full ledger CSV)
_LEDGER_RAW_FIELDS = [
    "research_day", "signature_candidate_code", "signature_key",
    "support_count_day", "confidence_day", "decision_grade_day",
    "validation_status_day", "first_seen_date", "last_seen_date",
]

# Fields for signature_candidates sheet
_SIG_CAND_FIELDS = [
    "research_day", "signature_candidate_code", "signature_key",
    "support_count", "support_share_pct", "dominant_side",
    "dominant_move_class", "dominant_participation_pattern",
    "dominant_structural_quality", "decision_grade", "confidence",
    "validation_status", "caution_flag", "next_action",
    "maps_to_existing_strategy_family", "improvement_target_layer_mode",
    "median_1h_favor", "median_4h_favor", "notes",
]


# ---------------------------------------------------------------------------
# xlsx helpers
# ---------------------------------------------------------------------------

def _xl_header_fill():
    return PatternFill(fill_type="solid", fgColor=_HEADER_FILL)

def _xl_alt_fill():
    return PatternFill(fill_type="solid", fgColor=_ALT_FILL)

def _xl_header_font():
    return Font(bold=True, color=_HEADER_FONT)

def _xl_write_sheet(ws, headers: List[str], rows: List[List]):
    """Write headers + data rows to a worksheet with basic styling."""
    # Header row
    for col_idx, header in enumerate(headers, 1):
        cell = ws.cell(row=1, column=col_idx, value=header)
        cell.fill = _xl_header_fill()
        cell.font = _xl_header_font()
        cell.alignment = Alignment(wrap_text=False)

    # Data rows
    for row_idx, row_data in enumerate(rows, 2):
        fill = _xl_alt_fill() if row_idx % 2 == 0 else None
        for col_idx, value in enumerate(row_data, 1):
            cell = ws.cell(row=row_idx, column=col_idx, value=str(value) if value is not None else "")
            if fill:
                cell.fill = fill

    # Freeze top row
    ws.freeze_panes = "A2"

    # Auto-width (approximate)
    for col_idx, header in enumerate(headers, 1):
        col_letter = ws.cell(row=1, column=col_idx).column_letter
        _data_lens = [len(str(ws.cell(row=r, column=col_idx).value or "")) for r in range(2, min(ws.max_row + 1, 52))]
        max_len = max([len(str(header))] + _data_lens) if _data_lens else len(str(header))
        ws.column_dimensions[col_letter].width = min(max(max_len + 2, 10), 45)


def _rows_from_dicts(dicts: List[Dict], fields: List[str]) -> List[List]:
    return [[d.get(f, "") for f in fields] for d in dicts]


def _case_fields(cases: List[Dict]) -> List[str]:
    """Return ordered field list from case rows, preserving canonical insertion order."""
    if not cases:
        return []
    return list(cases[0].keys())


# ---------------------------------------------------------------------------
# Manifest builder
# ---------------------------------------------------------------------------

def build_manifest(
    research_day: str,
    cases: List[Dict],
    sig_candidates: List[Dict],
    daily_summary: Dict,
    output_path: str,
) -> str:
    """Build analysis_bundle_manifest_YYYY-MM-DD.md"""

    eligible_count  = sum(1 for c in cases if c.get("research_eligible_YN") == "Y")
    health          = daily_summary.get("overall_research_health", "—")
    regime          = daily_summary.get("research_regime", "—")
    improve_count   = daily_summary.get("old_strategy_improvement_cases_count", 0)
    new_thesis      = daily_summary.get("new_strategy_thesis_cases_count", 0)
    short_today     = daily_summary.get("short_intervention_candidate_today", "N")
    short_reason    = daily_summary.get("short_intervention_reason", "none")
    top_sig         = daily_summary.get("top_candidate_signature_1", "none today")

    ledger_rows = 0
    if os.path.exists(LEDGER_PATH):
        with open(LEDGER_PATH, newline="", encoding="utf-8") as f:
            ledger_rows = sum(1 for _ in csv.DictReader(f))

    bundle_dir = os.path.dirname(output_path)

    lines = [
        f"# R1 Analysis Bundle Manifest — {research_day}",
        f"",
        f"Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        f"",
        f"---",
        f"",
        f"## Day Summary",
        f"",
        f"| Field | Value |",
        f"|---|---|",
        f"| Research Day | {research_day} |",
        f"| Research Regime | {regime} |",
        f"| Overall Health | {health} |",
        f"| Cases Built | {len(cases)} |",
        f"| Eligible Cases | {eligible_count} |",
        f"| Signature Candidates (repeated) | {len(sig_candidates)} |",
        f"| Top Signature | {top_sig} |",
        f"| Improvement Candidates (case-level) | {improve_count} |",
        f"| New Thesis Candidates (case-level) | {new_thesis} |",
        f"| Short Intervention Today | {short_today} |",
        f"| Short Intervention Reason | {short_reason} |",
        f"",
        f"---",
        f"",
        f"## Artifacts in This Bundle",
        f"",
        f"| File | Type | Rows | Description |",
        f"|---|---|---|---|",
        f"| `daily_case_dataset_{research_day}.csv` | canonical case data | {len(cases)} | one-row-per-case; source of truth for all boards |",
        f"| `daily_research_summary_{research_day}.csv` | day summary | 1 | 33+ day-level metrics |",
        f"| `daily_signature_candidates_{research_day}.csv` | repeated patterns | {len(sig_candidates)} | patterns with ≥2 eligible cases |",
        f"| `signature_evidence_ledger.csv` | rolling ledger | {ledger_rows} | cross-day evidence (all days) |",
        f"| `R1_analysis_bundle_{research_day}.xlsx` | xlsx bundle | — | all above in one workbook |",
        f"",
        f"---",
        f"",
        f"## How to Use This Bundle",
        f"",
        f"Send to ChatGPT in this order for best context:",
        f"1. This manifest file",
        f"2. `R1_analysis_bundle_{research_day}.xlsx` (or the individual CSVs)",
        f"",
        f"---",
        f"",
        f"## Critical Semantic Rules (read before interpreting)",
        f"",
        f"| Rule | Clarification |",
        f"|---|---|",
        f"| repeated signatures ≠ strategy proof | A repeated signature = ≥2 eligible cases share the same (side, pre_move_sig, participation, structural_quality). It is a pattern to track, NOT a validated strategy or a live rule change recommendation. |",
        f"| case-level theses ≠ repeated patterns | A case can have decision_grade = NEW_STRATEGY_THESIS_CANDIDATE even when zero repeated signatures exist today. These are per-case conclusions, independent of the signature evidence board. |",
        f"| research families under investigation ≠ live strategy families | R1 research families (long_breakout_retest, short_exhaustion_retest) are the mapping targets. Do not confuse NEW_STRATEGY_THESIS_CANDIDATE cases with changes to the live bot strategy families. |",
        f"| CLEAN_WITH_VISUAL_GAPS health | Research data is analysis-ready (eligible/proxy/outcome complete) but ≥1 chart image could not be rendered. Outcome fields are unaffected — they come from price data. Do not treat as PARTIAL. |",
        f"| data_confidence = case data trust | HIGH/MEDIUM/LOW trust in the completeness and interpretability of this case's data inputs. |",
        f"| intervention_confidence = intervention readiness | Whether this case is credible evidence for an intervention discussion. Separate from data quality. |",
        f"| anchor_conflict_flag = Y | One or more anchors used a fallback detection (no clean signal found). Treat anchor-derived features with caution for these cases. |",
        f"",
        f"## Readiness Levels",
        f"",
        f"| Level | Meaning |",
        f"|---|---|",
        f"| descriptive_only | Not enough repetition or strategy relevance. Observe only. |",
        f"| keep_tracking | Interesting but insufficient for any intervention. |",
        f"| old_strategy_improvement_candidate | Points to an existing strategy family + identifiable improvement layer. Requires further validation. |",
        f"| new_strategy_thesis_candidate | Potential new family, not yet validated. Needs multi-day repeated support before any action. |",
        f"",
        f"---",
        f"",
        f"## Sheet Guide (xlsx)",
        f"",
        f"| Sheet | Contents | Use |",
        f"|---|---|---|",
        f"| case_dataset | All {len(cases)} cases with full field set | Primary source of truth — trace any board row back here |",
        f"| daily_summary | 33+ day-level fields | Day health, dominant patterns, short intervention flag |",
        f"| signature_candidates | {len(sig_candidates)} repeated pattern candidates | Cross-case pattern evidence |",
        f"| ledger_snapshot | 7-day deduped view (one row per signature_key) | Cross-day tracking |",
        f"| ledger_raw | Full raw ledger (all days) | Audit and history |",
        f"",
        f"---",
        f"",
        f"*This manifest is downstream-only. It does not modify live bot config, strategy, lifecycle, or runtime files.*",
    ]

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    return output_path


# ---------------------------------------------------------------------------
# xlsx bundle builder
# ---------------------------------------------------------------------------

def build_xlsx_bundle(
    research_day: str,
    cases: List[Dict],
    sig_candidates: List[Dict],
    daily_summary: Dict,
    output_path: str,
    window_days: int = 7,
) -> str:
    """
    Build R1_analysis_bundle_YYYY-MM-DD.xlsx with 5 sheets.
    Requires openpyxl: pip install openpyxl
    """
    if not HAS_OPENPYXL:
        raise ImportError(
            "openpyxl is required for xlsx bundle: pip install openpyxl"
        )

    wb = openpyxl.Workbook()
    wb.remove(wb.active)  # remove default blank sheet

    # ------------------------------------------------------------------
    # Sheet 1: case_dataset
    # ------------------------------------------------------------------
    ws_cases = wb.create_sheet("case_dataset")
    if cases:
        fields = _case_fields(cases)
        rows   = _rows_from_dicts(cases, fields)
        _xl_write_sheet(ws_cases, fields, rows)
    else:
        ws_cases.cell(row=1, column=1, value="No cases for this day")

    # ------------------------------------------------------------------
    # Sheet 2: daily_summary (key/value layout)
    # ------------------------------------------------------------------
    ws_summary = wb.create_sheet("daily_summary")
    _xl_write_sheet(ws_summary, ["Field", "Value"],
                    [[k, str(v) if v is not None else ""] for k, v in daily_summary.items()])

    # ------------------------------------------------------------------
    # Sheet 3: signature_candidates
    # ------------------------------------------------------------------
    ws_sigs = wb.create_sheet("signature_candidates")
    if sig_candidates:
        # Use predefined display fields; fall back to all fields if some missing
        available = set(sig_candidates[0].keys())
        display_fields = [f for f in _SIG_CAND_FIELDS if f in available]
        extra = [f for f in sig_candidates[0].keys() if f not in display_fields]
        all_fields = display_fields + extra
        rows = _rows_from_dicts(sig_candidates, all_fields)
        _xl_write_sheet(ws_sigs, all_fields, rows)
    else:
        ws_sigs.cell(row=1, column=1, value="No repeated signature candidates today")
        ws_sigs.cell(row=2, column=1, value="(zero candidates = no cross-case pattern met threshold — valid)")

    # ------------------------------------------------------------------
    # Sheet 4: ledger_snapshot (7-day deduped, one row per signature_key)
    # ------------------------------------------------------------------
    ws_snap = wb.create_sheet("ledger_snapshot")
    normalized = load_and_normalize_ledger_rows(research_day, window_days=window_days, as_of_day=research_day)
    snapshot   = build_ledger_snapshot_for_report(normalized, research_day)
    if snapshot:
        snap_fields = _LEDGER_SNAP_FIELDS + [
            f for f in snapshot[0].keys()
            if f not in _LEDGER_SNAP_FIELDS and not f.startswith("_")
        ]
        rows = _rows_from_dicts(snapshot, snap_fields)
        _xl_write_sheet(ws_snap, snap_fields, rows)
    else:
        ws_snap.cell(row=1, column=1, value="No ledger data in rolling window")
        ws_snap.cell(row=2, column=1, value=f"(window: {window_days} days ending {research_day})")

    # ------------------------------------------------------------------
    # Sheet 5: ledger_raw (full raw ledger — all days)
    # ------------------------------------------------------------------
    ws_raw = wb.create_sheet("ledger_raw")
    if os.path.exists(LEDGER_PATH):
        with open(LEDGER_PATH, newline="", encoding="utf-8") as f:
            raw_rows = [dict(r) for r in csv.DictReader(f)
                        if r.get("research_day", "") <= research_day]
        if raw_rows:
            # Show key fields first, then remaining
            available = set(raw_rows[0].keys())
            key_first = [f for f in _LEDGER_RAW_FIELDS if f in available]
            rest = [f for f in raw_rows[0].keys() if f not in key_first]
            all_fields = key_first + rest
            sorted_rows = sorted(raw_rows, key=lambda r: r.get("research_day", ""), reverse=True)
            rows = _rows_from_dicts(sorted_rows, all_fields)
            _xl_write_sheet(ws_raw, all_fields, rows)
        else:
            ws_raw.cell(row=1, column=1, value="Ledger file exists but is empty")
    else:
        ws_raw.cell(row=1, column=1, value="signature_evidence_ledger.csv not found yet")

    # ------------------------------------------------------------------
    # Save
    # ------------------------------------------------------------------
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    wb.save(output_path)
    return output_path
