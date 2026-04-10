#!/usr/bin/env python3
"""
apply_patches_v12.py — narrow markdown + wording fixes:
  1. build_report: auto-load ledger if normalized_ledger_rows not passed
  2. Remove duplicate markdown sections (idempotent)
  3. _section_decision_bridge: use build_intervention_shortlist for identity fix
  4. build_measurement_decision_card: acknowledge isolated thesis in evidence note
  5. Decision Card: improve Why Not Others wording

Files touched: signature_ledger.py, report_builder.py
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
RB = "research/top_movers/report_builder.py"

# =============================================================================
print("\n=== Fix 1: build_measurement_decision_card — acknowledge isolated thesis ===")
# =============================================================================
patch(SL,
    "    improve   = [c for c in eligible\n"
    "                 if c.get(\"decision_grade\") == \"OLD_STRATEGY_IMPROVEMENT_CANDIDATE\"]\n"
    "    thesis    = [c for c in eligible\n"
    "                 if c.get(\"decision_grade\") == \"NEW_STRATEGY_THESIS_CANDIDATE\"]",
    "    improve   = [c for c in eligible\n"
    "                 if c.get(\"decision_grade\") == \"OLD_STRATEGY_IMPROVEMENT_CANDIDATE\"]\n"
    "    thesis    = [c for c in eligible\n"
    "                 if c.get(\"decision_grade\") == \"NEW_STRATEGY_THESIS_CANDIDATE\"]\n"
    "    thesis_count = len(thesis)",
    "track thesis_count separately in decision card")

# Fix evidence note to include isolated thesis cases
patch(SL,
    "    if improve_count == 0 and strong_sig_count == 0:\n"
    "        evidence_note = \"No improvement candidates or repeated signatures today.\"\n"
    "    elif improve_count > 0 and strong_sig_count == 0:\n"
    "        evidence_note = (f\"{improve_count} case-level improvement candidate(s); \"\n"
    "                         f\"no repeated cross-day signature yet.\")\n"
    "    elif improve_count == 0 and strong_sig_count > 0:\n"
    "        evidence_note = (f\"No case-level improvement candidates; \"\n"
    "                         f\"{strong_sig_count} repeated signature(s) with MEDIUM+ confidence.\")\n"
    "    else:\n"
    "        evidence_note = (f\"{improve_count} improvement candidate(s) + \"\n"
    "                         f\"{strong_sig_count} repeated signature(s). \"\n"
    "                         f\"Cross-day support: {repeated_sig_count} signature(s) in repeated_candidate role.\")",
    "    if improve_count == 0 and strong_sig_count == 0 and thesis_count == 0:\n"
    "        evidence_note = \"No improvement candidates, repeated signatures, or new thesis cases today.\"\n"
    "    elif improve_count == 0 and strong_sig_count == 0 and thesis_count > 0:\n"
    "        # Isolated new thesis: visible but not action-ready\n"
    "        _thesis_fams = list(set(\n"
    "            c.get(\"candidate_strategy_family_name\",\"\") for c in thesis\n"
    "            if c.get(\"candidate_strategy_family_name\",\"\") not in (\"\",\"under_investigation\")\n"
    "        ))\n"
    "        _fam_str = \", \".join(_thesis_fams[:2]) if _thesis_fams else \"unnamed\"\n"
    "        evidence_note = (f\"{thesis_count} isolated new-thesis candidate(s) observed \"\n"
    "                         f\"(family: {_fam_str}). Not action-ready — needs multi-day repetition \"\n"
    "                         \"before hypothesis stage.\")\n"
    "    elif improve_count > 0 and strong_sig_count == 0:\n"
    "        evidence_note = (f\"{improve_count} case-level improvement candidate(s); \"\n"
    "                         f\"no repeated cross-day signature yet.\")\n"
    "    elif improve_count == 0 and strong_sig_count > 0:\n"
    "        evidence_note = (f\"No case-level improvement candidates; \"\n"
    "                         f\"{strong_sig_count} repeated signature(s) with MEDIUM+ confidence.\")\n"
    "    else:\n"
    "        evidence_note = (f\"{improve_count} improvement candidate(s) + \"\n"
    "                         f\"{strong_sig_count} repeated signature(s). \"\n"
    "                         f\"Cross-day support: {repeated_sig_count} signature(s) in repeated_candidate role.\")",
    "improve evidence_note: surface isolated thesis cases clearly")

# Fix Why Not Others to mention repeated short sig when present
patch(SL,
    "    other_families = set(c.get(\"maps_to_existing_strategy_family\", \"\")\n"
    "                         for c in eligible\n"
    "                         if c.get(\"maps_to_existing_strategy_family\",\"\") not in (\"\", \"—\", chosen_family))\n"
    "    why_not = (f\"Other observed families ({', '.join(sorted(other_families)[:3]) or 'none'}) \"\n"
    "               \"have fewer improvement candidates or lower proxy completeness.\")",
    "    other_families = set(c.get(\"maps_to_existing_strategy_family\", \"\")\n"
    "                         for c in eligible\n"
    "                         if c.get(\"maps_to_existing_strategy_family\",\"\") not in (\"\", \"—\", \"none\", chosen_family))\n"
    "    _other_str = ', '.join(sorted(other_families)[:3]) if other_families else 'none'\n"
    "    _sig_note = \"\"\n"
    "    if sig_candidates:\n"
    "        _sig_fams = list(set(s.get(\"maps_to_existing_strategy_family\",\"\") or\n"
    "                             s.get(\"dominant_side\",\"\") for s in sig_candidates\n"
    "                             if s.get(\"confidence\") in (\"HIGH\",\"MEDIUM\")))\n"
    "        if _sig_fams:\n"
    "            _sig_note = (f\" Note: repeated signature(s) exist ({', '.join(_sig_fams[:2])}) \"\n"
    "                         \"but no action-ready improvement candidates for that family today.\")\n"
    "    why_not = (f\"No other action-ready families today (observed: {_other_str}).\"\n"
    "               + _sig_note)",
    "improve Why Not Others: mention repeated sigs without action-ready candidates")

# =============================================================================
print("\n=== Fix 2: report_builder.py — auto-load ledger + dedup + fix identity ===")
# =============================================================================

# 2a. Add ledger import at top of build_report body (auto-load if not passed)
patch(RB,
    "    ok_count = sum(1 for c in cases if c.get(\"data_quality_ok\") == \"Y\")\n"
    "    warn_count = total - ok_count\n"
    "    missing_img_count = sum(1 for c in cases if c.get(\"full_visual_complete_YN\") != \"Y\")",
    "    ok_count = sum(1 for c in cases if c.get(\"data_quality_ok\") == \"Y\")\n"
    "    warn_count = total - ok_count\n"
    "    missing_img_count = sum(1 for c in cases if c.get(\"full_visual_complete_YN\") != \"Y\")\n"
    "    # Auto-load ledger if not passed — ensures markdown gets real data\n"
    "    if normalized_ledger_rows is None:\n"
    "        try:\n"
    "            from research.top_movers.signature_ledger import ledger_rows_as_of\n"
    "            normalized_ledger_rows = ledger_rows_as_of(research_day)\n"
    "        except Exception:\n"
    "            normalized_ledger_rows = []\n"
    "    _lr = normalized_ledger_rows",
    "auto-load ledger in build_report when not passed by caller")

# 2b. Fix _section_decision_bridge to use build_intervention_shortlist for identity
patch(RB,
    "def _section_decision_bridge(cases: List[Dict], sig_candidates: List[Dict]) -> str:\n"
    "    \"\"\"Minimal markdown stubs for the decision bridge (text-only, no tables).\"\"\"\n"
    "    from collections import Counter\n"
    "    eligible = [c for c in cases if c.get(\"decision_grade\") in (\n"
    "        \"OLD_STRATEGY_IMPROVEMENT_CANDIDATE\", \"NEW_STRATEGY_THESIS_CANDIDATE\",\n"
    "    ) and c.get(\"research_eligible_YN\") == \"Y\"]\n"
    "    lines = [\"## 8. Decision Bridge Summary\\n\"]\n"
    "    if eligible:\n"
    "        families = Counter(\n"
    "            (c.get(\"maps_to_existing_strategy_family\",\"?\"), c.get(\"improvement_target_layer\",\"?\"))\n"
    "            for c in eligible\n"
    "        )\n"
    "        lines.append(f\"**Intervention candidates: {len(eligible)} eligible case(s)**\\n\")\n"
    "        for (fam, layer), cnt in families.most_common(2):\n"
    "            lines.append(f\"  - `{fam}` / `{layer}`: {cnt} case(s)\")\n"
    "        lines.append(\"\")\n"
    "    else:\n"
    "        lines.append(\"**Intervention candidates:** none today.\\n\")",
    "def _section_decision_bridge(cases: List[Dict], sig_candidates: List[Dict],\n"
    "                              normalized_ledger_rows: Optional[List[Dict]] = None) -> str:\n"
    "    \"\"\"Minimal markdown for the decision bridge — uses shortlist helper for identity fix.\"\"\"\n"
    "    from research.top_movers.signature_ledger import build_intervention_shortlist\n"
    "    eligible = [c for c in cases if c.get(\"research_eligible_YN\") == \"Y\"]\n"
    "    shortlist = build_intervention_shortlist(eligible, sig_candidates, normalized_ledger_rows or [])\n"
    "    lines = [\"## 8. Decision Bridge Summary\\n\"]\n"
    "    if shortlist:\n"
    "        lines.append(f\"**Intervention candidates: {len(shortlist)} family/layer group(s)**\\n\")\n"
    "        for row in shortlist[:3]:\n"
    "            fam   = row.get(\"strategy_family\", \"?\")\n"
    "            layer = row.get(\"issue_layer\", \"?\")\n"
    "            cnt   = row.get(\"case_count_today\", 0)\n"
    "            rdy   = row.get(\"readiness\", \"?\")\n"
    "            lines.append(f\"  - `{fam}` / `{layer}`: {cnt} case(s) — {rdy}\")\n"
    "        lines.append(\"\")\n"
    "    else:\n"
    "        lines.append(\"**Intervention candidates:** none today.\\n\")",
    "fix _section_decision_bridge: use build_intervention_shortlist for identity")

# 2c. Update call site of _section_decision_bridge to pass _lr
patch(RB,
    "    sections.append(_section_decision_bridge(cases, sig_candidates or []))\n"
    "    _lr = normalized_ledger_rows or []",
    "    sections.append(_section_decision_bridge(cases, sig_candidates or [], _lr))\n"
    "    _lr = normalized_ledger_rows or []",
    "pass _lr to _section_decision_bridge call site",
    required=False)

# Try alternate call site (if _lr was set before this call)
patch(RB,
    "    sections.append(_section_decision_bridge(cases, sig_candidates or []))",
    "    sections.append(_section_decision_bridge(cases, sig_candidates or [], _lr))",
    "pass _lr to _section_decision_bridge (alternate site)",
    required=False)

# 2d. Deduplicate: remove double-wired markdown sections if present
# The issue is sections appear twice. Fix by removing the old _section_ledger_snapshot_md
# calls that were wired by v11 if they appear before the new canonical wire.
content = read(RB)
# Remove any duplicate wiring blocks that may exist
# Detect and remove repeated section calls
lines = content.split('\n')
seen_anchors = set()
dedup_lines = []
skip_next = 0
for i, line in enumerate(lines):
    if skip_next > 0:
        skip_next -= 1
        continue
    # Detect duplicate section wires
    stripped = line.strip()
    if stripped.startswith('sections.append(_section_ledger_snapshot_md('):
        key = 'ledger_snap'
        if key in seen_anchors:
            skip_next = 0  # skip this line
            continue
        seen_anchors.add(key)
    elif stripped.startswith('sections.append(_section_promotion_rules_md('):
        key = 'promo_rules'
        if key in seen_anchors:
            skip_next = 0
            continue
        seen_anchors.add(key)
    elif stripped.startswith('sections.append(_section_semantic_warning_md('):
        key = 'sem_warn'
        if key in seen_anchors:
            skip_next = 0
            continue
        seen_anchors.add(key)
    elif stripped.startswith('sections.append(_section_decision_card_md('):
        key = 'dec_card'
        if key in seen_anchors:
            skip_next = 0
            continue
        seen_anchors.add(key)
    elif stripped.startswith('sections.append(_section_trusted_weak_deferred_md('):
        key = 'twd'
        if key in seen_anchors:
            skip_next = 0
            continue
        seen_anchors.add(key)
    dedup_lines.append(line)

dedup_content = '\n'.join(dedup_lines)
if dedup_content != content:
    write(RB, dedup_content)
    print("  OK: removed duplicate section wires from report_builder.py")
else:
    print("  OK: no duplicates found")

# =============================================================================
print("\n=== Syntax check ===")
import ast
for fpath in [SL, RB]:
    try:
        ast.parse(read(fpath))
        print(f"  SYNTAX OK: {fpath}")
    except SyntaxError as e:
        print(f"  SYNTAX ERROR {fpath}: {e}")

print("\n=== Done. ===")
print("Rebuild 3 days:")
print("  python3 scripts/run_daily_top_movers_research.py --day 2026-04-06")
print("  python3 scripts/run_daily_top_movers_research.py --day 2026-04-07")
print("  python3 scripts/run_daily_top_movers_research.py --day 2026-04-08")
print()
print("Validation:")
print("  1. Markdown snapshot shows real rows (not 'No ledger data')")
print("  2. No duplicate sections in markdown")
print("  3. Decision Bridge shows thesis family name, not 'none/not_applicable'")
print("  4. Evidence note: isolated thesis says 'not action-ready' not 'no candidates'")
print("  5. Why Not Others: mentions repeated sigs without action-ready candidates")
