#!/usr/bin/env python3
"""
apply_patches_v4.py — final semantic alignment pass. Run from project root.

Fixes:
  1. Health label in daily pack (_s2_integrity + _s3_exec_summary)
  2. Markdown signature section rewrite (remove verdict-based zero-candidate text)
  3. Ledger reconcile moved to data layer (load_and_normalize_ledger_rows)
     + removed from view layer (build_ledger_snapshot_for_report)
"""
import re

def read(path):
    return open(path, encoding="utf-8").read()

def write(path, content):
    open(path, "w", encoding="utf-8").write(content)
    print(f"  saved: {path}")

DR = "research/top_movers/docx_report_builder.py"
RB = "research/top_movers/report_builder.py"
SL = "research/top_movers/signature_ledger.py"

# =============================================================================
print("\n=== 1. docx_report_builder.py — fix health in _s2_integrity ===")
# =============================================================================
content = read(DR)

# Fix _s2_integrity: missing_img is a LIST, use bool(missing_img)
# Unique anchor: followed by t=_make_table(doc, ["Metric","Value"]
before = len(content)
content = re.sub(
    r'(health\s*=\s*["\']CLEAN["\']\s+if\s+er>=0\.\d+\s+and\s+pr>=0\.\d+[^\n]+\n)'
    r'(\s+t\s*=\s*_make_table\(doc,\s*\["Metric","Value"\])',
    lambda m: (
        '    health = ("CLEAN_WITH_VISUAL_GAPS" if missing_img else "CLEAN") '
        'if er>=0.75 and pr>=0.60 else "PARTIAL" if er>=0.40 or pr>=0.40 else "WEAK"\n'
        + m.group(2)
    ),
    content,
    count=1,
)
print(f"  _s2_integrity: {'OK' if len(content) != before else 'SKIP — no match'}")

# Fix _s3_exec_summary: missing_img is an INT count, use missing_img>0
# Unique anchor: followed by blank line then `    improve=`
before = len(content)
content = re.sub(
    r'(health\s*=\s*["\']CLEAN["\']\s+if\s+er>=0\.\d+\s+and\s+pr>=0\.\d+[^\n]+\n)'
    r'(\n\s+improve\s*=)',
    lambda m: (
        '    health = ("CLEAN_WITH_VISUAL_GAPS" if missing_img>0 else "CLEAN") '
        'if er>=0.75 and pr>=0.60 else "PARTIAL" if er>=0.40 or pr>=0.40 else "WEAK"\n'
        + m.group(2)
    ),
    content,
    count=1,
)
print(f"  _s3_exec_summary: {'OK' if len(content) != before else 'SKIP — no match'}")

write(DR, content)

# =============================================================================
print("\n=== 2. report_builder.py — rewrite _section_candidate_signatures ===")
# =============================================================================
content = read(RB)

NEW_SIG_FUNC = (
    'def _section_candidate_signatures(cases: List[Dict], sig_candidates: Optional[List[Dict]] = None) -> str:\n'
    '    """\n'
    '    Show repeated signature candidates only (cross-case pattern evidence).\n'
    '    verdict-based setup_signature_candidate logic removed from this section.\n'
    '    """\n'
    '    sig_candidates = sig_candidates or []\n'
    '    lines = ["## 6. Repeated Signature Candidates\\n"]\n'
    '\n'
    '    if sig_candidates:\n'
    '        top_code = sig_candidates[0].get("signature_candidate_code", "-")\n'
    '        lines.append(f"**Repeated signature candidates (>=2 eligible cases): {len(sig_candidates)}**")\n'
    '        lines.append(f"Top: `{top_code}`\\n")\n'
    '        for s in sig_candidates:\n'
    '            code  = s.get("signature_candidate_code", "")\n'
    '            n     = s.get("support_count", "?")\n'
    '            side  = s.get("dominant_side", "?")\n'
    '            grade = s.get("decision_grade", "")\n'
    '            conf  = s.get("confidence", "")\n'
    '            nxt   = s.get("next_action", "")\n'
    '            lines.append(f"- `{code}` | N={n} | {side} | grade={grade} | conf={conf} | next={nxt}")\n'
    '        lines.append("")\n'
    '    else:\n'
    '        lines.append(\n'
    '            "**Repeated signature candidates: 0**  "\n'
    '            "(no cross-case pattern met threshold today - valid and expected on heterogeneous mover days)\\n"\n'
    '        )\n'
    '\n'
    '    return "\\n".join(lines) + "\\n"\n'
)

# Replace entire function using lookahead for next function def
before = content
content = re.sub(
    r'def _section_candidate_signatures\(.*?(?=\n\ndef _section_footer)',
    lambda m: NEW_SIG_FUNC.rstrip('\n'),
    content,
    flags=re.DOTALL,
    count=1,
)
if content != before:
    print("  _section_candidate_signatures: OK")
else:
    print("  WARN: _section_candidate_signatures not found — check file manually")

# Rename section header in markdown for consistency
content = content.replace(
    '    lines = ["## 6. Candidate Setup Signatures\\n"]',
    '    lines = ["## 6. Repeated Signature Candidates\\n"]',
)

write(RB, content)

# =============================================================================
print("\n=== 3. signature_ledger.py — move reconcile to data layer ===")
# =============================================================================
content = read(SL)

# 3a. Add reconcile INSIDE load_and_normalize_ledger_rows, before current_role derivation
# Unique anchor: the three row assignments before try:
OLD_DATA = (
    '        row["support_days_count"] = support_days\n'
    '        row["recent_support_days_count"] = recent_days\n'
    '        row["latest_validation_status"] = latest_status\n'
    '        try:'
)
NEW_DATA = (
    '        row["support_days_count"] = support_days\n'
    '        row["recent_support_days_count"] = recent_days\n'
    '        # Data-layer reconcile: status stuck at first_observation despite multi-day support\n'
    '        if latest_status == "first_observation" and support_days >= 2:\n'
    '            latest_status = "tracking"\n'
    '        row["latest_validation_status"] = latest_status\n'
    '        try:'
)
if OLD_DATA in content:
    content = content.replace(OLD_DATA, NEW_DATA, 1)
    print("  load_and_normalize_ledger_rows: reconcile added to data layer OK")
else:
    print("  WARN: data-layer reconcile anchor not found — check manually")

# 3b. Remove view-layer reconcile from build_ledger_snapshot_for_report
OLD_VIEW = (
    '    snapshot = list(latest_by_sig.values())\n'
    '    # Safe reconcile: if support_days >= 2 but status still first_observation, upgrade view\n'
    '    for _r in snapshot:\n'
    '        if (_r.get(\'support_days_count\', 0) or 0) >= 2 and _r.get(\'latest_validation_status\') == \'first_observation\':\n'
    '            _r[\'latest_validation_status\'] = \'tracking\'\n'
    '    _role_order = {"repeated_candidate": 0, "tracking": 1, "first_observation": 2, "stale": 3}'
)
NEW_VIEW = (
    '    snapshot = list(latest_by_sig.values())\n'
    '    _role_order = {"repeated_candidate": 0, "tracking": 1, "first_observation": 2, "stale": 3}'
)
if OLD_VIEW in content:
    content = content.replace(OLD_VIEW, NEW_VIEW, 1)
    print("  build_ledger_snapshot_for_report: view-layer reconcile removed OK")
else:
    # Try without the reconcile block (if it was never added or already removed)
    print("  build_ledger_snapshot_for_report: view-layer reconcile not found — may already be clean")

write(SL, content)

# =============================================================================
print("\n=== Done. ===\n")
print("Run: python3 scripts/run_daily_top_movers_research.py --day 2026-04-08")
print("Check all 4 artifacts agree on:")
print("  health = CLEAN_WITH_VISUAL_GAPS")
print("  signature candidates = 1 | top = SIG_SHORT_VOL_DIVWRN_SHAL")
print("  markdown: no zero-candidate wording")
