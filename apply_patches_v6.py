#!/usr/bin/env python3
"""
apply_patches_v6.py — 3 tracks:
  Track 1: as-of-day filter in all ledger consumers
  Track 2: status/role semantic sync in data layer
  Track 3: image gap observability with specific reasons
Run from project root.
"""
import re

def read(p): return open(p, encoding="utf-8").read()
def write(p, c): open(p, "w", encoding="utf-8").write(c); print(f"  saved: {p}")

def patch(path, old, new, label="", required=True):
    c = read(path)
    if old not in c:
        print(f"  {'WARN' if required else 'SKIP'} not found: {label}")
        return False
    write(path, c.replace(old, new, 1))
    print(f"  OK: {label}")
    return True

SL = "research/top_movers/signature_ledger.py"
DR = "research/top_movers/docx_report_builder.py"
UA = "research/top_movers/unified_analysis_pack_builder.py"
AB = "research/top_movers/analysis_bundle_builder.py"

# =============================================================================
print("\n=== Track 1+2: signature_ledger.py ===")
# =============================================================================

# T1-SL-1: add as_of_day parameter
patch(SL,
    "def load_and_normalize_ledger_rows(research_day: str, window_days: int = 7) -> List[Dict]:",
    "def load_and_normalize_ledger_rows(research_day: str, window_days: int = 7, as_of_day: Optional[str] = None) -> List[Dict]:",
    "add as_of_day param to load_and_normalize_ledger_rows")

# T1-SL-2: filter rows by as_of_day when loading
patch(SL,
    "    all_rows: List[Dict] = []\n"
    "    with open(LEDGER_PATH, \"r\", newline=\"\", encoding=\"utf-8\") as f:\n"
    "        reader = csv.DictReader(f)\n"
    "        for row in reader:\n"
    "            all_rows.append(dict(row))",
    "    all_rows: List[Dict] = []\n"
    "    with open(LEDGER_PATH, \"r\", newline=\"\", encoding=\"utf-8\") as f:\n"
    "        reader = csv.DictReader(f)\n"
    "        for row in reader:\n"
    "            if as_of_day and row.get(\"research_day\", \"\") > as_of_day:\n"
    "                continue  # as-of-day filter: exclude future rows\n"
    "            all_rows.append(dict(row))",
    "add as-of-day row filter in load_and_normalize_ledger_rows")

# T2-SL: sync latest_validation_status with current_role after role derivation
patch(SL,
    "        elif support_days >= 2:\n"
    "            row[\"current_role\"] = \"tracking\"\n"
    "        else:\n"
    "            row[\"current_role\"] = \"first_observation\"",
    "        elif support_days >= 2:\n"
    "            row[\"current_role\"] = \"tracking\"\n"
    "        else:\n"
    "            row[\"current_role\"] = \"first_observation\"\n"
    "        # Sync: latest_validation_status must not contradict current_role\n"
    "        if row[\"current_role\"] in (\"first_observation\", \"stale\"):\n"
    "            row[\"latest_validation_status\"] = row[\"current_role\"]",
    "sync latest_validation_status with current_role to eliminate contradictions")

# =============================================================================
print("\n=== Track 1+3: docx_report_builder.py ===")
# =============================================================================

# T1-DR-1: pass as_of_day=research_day to load_and_normalize_ledger_rows
patch(DR,
    "    normalized_ledger_rows = load_and_normalize_ledger_rows(research_day, window_days=7)\n"
    "    doc=Document()",
    "    normalized_ledger_rows = load_and_normalize_ledger_rows(research_day, window_days=7, as_of_day=research_day)\n"
    "    doc=Document()",
    "pass as_of_day to ledger load in build_docx_pack")

# T1-DR-2: update ledger snapshot section header to show as-of date
patch(DR,
    '    _h2(doc, "14d. Cross-Day Ledger Snapshot")',
    '    _h2(doc, f"14d. Cross-Day Ledger Snapshot (as of {research_day})")',
    "update ledger snapshot header to show as-of date")

# T3-DR-1: add _IMG_KEY_TO_ANCHOR constant after _IMAGE_DEFS
patch(DR,
    "_IMAGE_DEFS = [\n"
    "    (\"P0_context_1h\",              \"P0 Context (1h)\"),",
    "# Maps image key to primary anchor code (for missing-reason lookup)\n"
    "_IMG_KEY_TO_ANCHOR = {\n"
    "    \"P0_context_1h\":            \"P0\",\n"
    "    \"P0_P1_setup_15m\":          \"P0\",\n"
    "    \"P1_ignition_5m\":           \"P1\",\n"
    "    \"P2_P3_break_expansion_5m\": \"P2\",\n"
    "    \"P4_resolution_15m\":        \"P4\",\n"
    "}\n"
    "\n"
    "_IMAGE_DEFS = [\n"
    "    (\"P0_context_1h\",              \"P0 Context (1h)\"),",
    "add _IMG_KEY_TO_ANCHOR constant")

# T3-DR-2: update _s17_case_appendix signature to accept image_results_all
patch(DR,
    "def _s17_case_appendix(doc, cases, anchor_rows, research_day):",
    "def _s17_case_appendix(doc, cases, anchor_rows, research_day, image_results_all=None):",
    "add image_results_all param to _s17_case_appendix")

# T3-DR-3: update _s17_case_appendix call in build_docx_pack
patch(DR,
    "    _s17_case_appendix(doc, cases, anchor_rows, research_day)",
    "    _s17_case_appendix(doc, cases, anchor_rows, research_day, image_results_all)",
    "pass image_results_all to _s17_case_appendix in build_docx_pack")

# T3-DR-4: show specific missing reason instead of generic "not available"
patch(DR,
    "                _p(doc, f\"  [Image not available: {img_key}]\", size=8)",
    "                _img_res = (image_results_all or {}).get(cid, {}).get(img_key, {})\n"
    "                _reason = _img_res.get(\"reason\", \"\") or \"reason_unknown\"\n"
    "                _p(doc, f\"  [Missing: {img_key} | reason: {_reason}]\", size=8)",
    "show specific missing reason in case appendix")

# =============================================================================
print("\n=== Track 1: unified_analysis_pack_builder.py ===")
# =============================================================================

# T1-UA-1: pass as_of_day to load_and_normalize_ledger_rows
patch(UA,
    "    normalized_ledger_rows = load_and_normalize_ledger_rows(research_day, window_days=window_days)",
    "    normalized_ledger_rows = load_and_normalize_ledger_rows(research_day, window_days=window_days, as_of_day=research_day)",
    "pass as_of_day to ledger load in build_unified_analysis_pack")

# T1-UA-2: update _u6_raw_ledger signature to accept as_of_day
patch(UA,
    "def _u6_raw_ledger(doc) -> None:",
    "def _u6_raw_ledger(doc, as_of_day: str = None) -> None:",
    "add as_of_day param to _u6_raw_ledger")

# T1-UA-3: update _u6_raw_ledger header to be explicit
patch(UA,
    '    _h2(doc, "6. Raw Ledger Appendix")',
    '    _h2(doc, f"6. Raw Ledger Appendix (as of {as_of_day})" if as_of_day else "6. Raw Ledger Appendix")',
    "update raw ledger appendix header to show as-of date")

# T1-UA-4: filter raw ledger rows by as_of_day
patch(UA,
    "        with open(LEDGER_PATH, newline=\"\", encoding=\"utf-8\") as f:\n"
    "            all_rows = list(csv.DictReader(f))",
    "        with open(LEDGER_PATH, newline=\"\", encoding=\"utf-8\") as f:\n"
    "            all_rows = [dict(r) for r in csv.DictReader(f)\n"
    "                        if not as_of_day or r.get(\"research_day\", \"\") <= as_of_day]",
    "filter raw ledger rows by as_of_day in _u6_raw_ledger")

# T1-UA-5: update call site for _u6_raw_ledger
patch(UA,
    "    _u6_raw_ledger(doc)",
    "    _u6_raw_ledger(doc, as_of_day=research_day)",
    "pass as_of_day to _u6_raw_ledger call")

# T1-UA-6: update ledger snapshot header
patch(UA,
    '    _h2(doc, "5. Cross-Day Ledger Snapshot")',
    '    _h2(doc, f"5. Cross-Day Ledger Snapshot (as of {research_day})")',
    "update ledger snapshot header in unified pack")

# =============================================================================
print("\n=== Track 1: analysis_bundle_builder.py ===")
# =============================================================================

# T1-AB-1: pass as_of_day to load_and_normalize_ledger_rows
patch(AB,
    "    normalized = load_and_normalize_ledger_rows(research_day, window_days=window_days)",
    "    normalized = load_and_normalize_ledger_rows(research_day, window_days=window_days, as_of_day=research_day)",
    "pass as_of_day to ledger load in build_xlsx_bundle")

# T1-AB-2: filter raw ledger rows in ledger_raw sheet
patch(AB,
    "        with open(LEDGER_PATH, newline=\"\", encoding=\"utf-8\") as f:\n"
    "            reader = csv.DictReader(f)\n"
    "            raw_rows = list(reader)",
    "        with open(LEDGER_PATH, newline=\"\", encoding=\"utf-8\") as f:\n"
    "            raw_rows = [dict(r) for r in csv.DictReader(f)\n"
    "                        if r.get(\"research_day\", \"\") <= research_day]",
    "filter raw ledger rows by research_day in xlsx ledger_raw sheet")

# =============================================================================
print("\n=== Done. ===")
print("\nRebuild 3 days to validate:")
print("  python3 scripts/run_daily_top_movers_research.py --day 2026-04-06")
print("  python3 scripts/run_daily_top_movers_research.py --day 2026-04-07")
print("  python3 scripts/run_daily_top_movers_research.py --day 2026-04-08")
print("\nValidation checklist:")
print("  1. Day-06 pack: no 2026-04-08 rows in any ledger section")
print("  2. Day-07 pack: no 2026-04-08 rows in any ledger section")
print("  3. Status/Role no longer contradictory (tracking+first_observation gone)")
print("  4. Missing images show specific reason (e.g. p4_not_reached_yet)")
print("  5. short_intervention_today remains N on all 3 days")
