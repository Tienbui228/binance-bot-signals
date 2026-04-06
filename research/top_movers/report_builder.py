"""
research/top_movers/report_builder.py

Builds the daily markdown research report.

Sections:
  0. Header + Data Quality Gate
  1. Move Archetype Distribution (move_class breakdown)
  2. Pre-Move Signature Board (compression_score / pre_move_signature)
  3. Participation Pattern Board (large vs crowd proxy)
  4. Structural Quality Board (structural_quality distribution)
  5. Repeated Clues Board (patterns appearing in 3+ tokens)
  6. Candidate Setup Signatures (setup_signature_candidate tokens)
  7. Research Footer (metadata, deferred items)

Rules:
  - Summaries aggregate from truth-clean cases only
  - Does NOT repair missing fields
  - If data quality is poor, banner reflects it clearly
  - Does NOT claim to fix live bot behavior
"""

from typing import Dict, List, Optional


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _pct(n: int, total: int) -> str:
    if total == 0:
        return "0%"
    return f"{round(100.0 * n / total)}%"


def _fmt(val, ndigits: int = 4) -> str:
    if val is None or val == "":
        return "—"
    try:
        return str(round(float(val), ndigits))
    except Exception:
        return str(val)


def _safe_float(val) -> Optional[float]:
    try:
        return float(val)
    except Exception:
        return None


def _count_by(cases: List[Dict], field: str) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for c in cases:
        val = str(c.get(field) or "unknown")
        counts[val] = counts.get(val, 0) + 1
    return dict(sorted(counts.items(), key=lambda x: -x[1]))


def _avg_field(cases: List[Dict], field: str) -> Optional[float]:
    vals = [_safe_float(c.get(field)) for c in cases]
    vals = [v for v in vals if v is not None]
    if not vals:
        return None
    return sum(vals) / len(vals)


# ---------------------------------------------------------------------------
# Section builders
# ---------------------------------------------------------------------------

def _section_header(
    research_day: str,
    research_regime: str,
    btc_24h: float,
    alt_breadth_pct: float,
    total_cases: int,
    data_quality_summary: Dict,
) -> str:
    ok_count = data_quality_summary.get("ok", 0)
    warn_count = data_quality_summary.get("warn", 0)

    quality_banner = "✅ DATA QUALITY: GOOD" if warn_count == 0 else f"⚠️ DATA QUALITY: {warn_count} TOKENS WITH ISSUES"

    return f"""# Daily Top Movers Research Report
## {research_day}

---

### Research Context
| Field | Value |
|---|---|
| Research Day | {research_day} |
| BTC 24h Change | {btc_24h:+.2f}% |
| Alt Breadth | {alt_breadth_pct:.1f}% |
| Research Regime | `{research_regime}` |
| Total Cases | {total_cases} |
| Data Quality OK | {ok_count} / {total_cases} |

### {quality_banner}

"""


def _section_data_gate(cases: List[Dict]) -> str:
    ok = [c for c in cases if c.get("data_quality_ok") == "Y"]
    bad = [c for c in cases if c.get("data_quality_ok") != "Y"]
    lines = ["## 0. Data Quality Gate\n"]

    if bad:
        lines.append(f"**{len(bad)} token(s) excluded from optimization summaries:**\n")
        for c in bad:
            lines.append(f"  - `{c['symbol']}` — {c.get('data_quality_note', 'unknown')}")
        lines.append("")

    lines.append(f"**Truth-clean cases for analysis: {len(ok)} / {len(cases)}**\n")
    lines.append("")

    if not ok:
        lines.append("⛔ No truth-clean cases available. Cannot draw optimization conclusions.\n")

    return "\n".join(lines)


def _section_move_archetype(cases: List[Dict]) -> str:
    ok = [c for c in cases if c.get("data_quality_ok") == "Y"]
    if not ok:
        return "## 1. Move Archetype Distribution\n\n_No truth-clean cases._\n\n"

    counts = _count_by(ok, "move_class")
    total = len(ok)
    lines = [
        "## 1. Move Archetype Distribution\n",
        f"Based on {total} truth-clean cases.\n",
        "| Move Class | Count | Share |",
        "|---|---|---|",
    ]
    for cls, cnt in counts.items():
        lines.append(f"| `{cls}` | {cnt} | {_pct(cnt, total)} |")

    lines.append("")
    lines.append(f"**Most common: `{list(counts.keys())[0]}`** ({list(counts.values())[0]} cases)\n")
    return "\n".join(lines) + "\n"


def _section_pre_move_signature(cases: List[Dict]) -> str:
    ok = [c for c in cases if c.get("data_quality_ok") == "Y"]
    if not ok:
        return "## 2. Pre-Move Signature Board\n\n_No truth-clean cases._\n\n"

    counts = _count_by(ok, "pre_move_signature")
    total = len(ok)

    avg_cs = _avg_field(ok, "compression_score")

    lines = [
        "## 2. Pre-Move Signature Board\n",
        f"| Pre-Move Signature | Count | Share |",
        "|---|---|---|",
    ]
    for sig, cnt in counts.items():
        lines.append(f"| `{sig}` | {cnt} | {_pct(cnt, total)} |")

    lines.append("")
    lines.append(f"**Average compression_score (truth-clean): {_fmt(avg_cs, 3)}**\n")

    # Per-side breakdown
    long_cs = [c for c in ok if c.get("side") == "LONG"]
    short_cs = [c for c in ok if c.get("side") == "SHORT"]
    avg_long = _avg_field(long_cs, "compression_score")
    avg_short = _avg_field(short_cs, "compression_score")
    lines.append(f"  LONG average: {_fmt(avg_long, 3)} | SHORT average: {_fmt(avg_short, 3)}\n")

    return "\n".join(lines) + "\n"


def _section_participation_pattern(cases: List[Dict]) -> str:
    ok = [c for c in cases if c.get("data_quality_ok") == "Y"]
    if not ok:
        return "## 3. Participation Pattern Board\n\n_No truth-clean cases._\n\n"

    counts = _count_by(ok, "participation_pattern")
    total = len(ok)

    avg_large = _avg_field(ok, "large_participant_proxy")
    avg_crowd = _avg_field(ok, "crowd_participation_proxy")
    avg_overheat = _avg_field(ok, "crowd_overheat_proxy")

    lines = [
        "## 3. Participation Pattern Board\n",
        "| Pattern | Count | Share |",
        "|---|---|---|",
    ]
    for pat, cnt in counts.items():
        lines.append(f"| `{pat}` | {cnt} | {_pct(cnt, total)} |")

    lines.append("")
    lines.append("**Average proxy values (truth-clean):**\n")
    lines.append(f"  large_participant_proxy: {_fmt(avg_large)}")
    lines.append(f"  crowd_participation_proxy: {_fmt(avg_crowd)}")
    lines.append(f"  crowd_overheat_proxy: {_fmt(avg_overheat)}\n")

    no_proxy = sum(1 for c in ok if not c.get("large_participant_proxy"))
    if no_proxy > 0:
        lines.append(f"_Note: {no_proxy} cases missing proxy data (endpoint not available for this symbol)._\n")

    return "\n".join(lines) + "\n"


def _section_structural_quality(cases: List[Dict]) -> str:
    ok = [c for c in cases if c.get("data_quality_ok") == "Y"]
    if not ok:
        return "## 4. Structural Quality Board\n\n_No truth-clean cases._\n\n"

    sq_counts = _count_by(ok, "structural_quality")
    bq_counts = _count_by(ok, "break_quality_band")
    total = len(ok)

    lines = [
        "## 4. Structural Quality Board\n",
        "**Structural Quality:**\n",
        "| Quality | Count | Share |",
        "|---|---|---|",
    ]
    for q, cnt in sq_counts.items():
        lines.append(f"| `{q}` | {cnt} | {_pct(cnt, total)} |")

    lines.append("")
    lines.append("**Break Quality Band:**\n")
    lines.append("| Band | Count | Share |")
    lines.append("|---|---|---|")
    for band, cnt in bq_counts.items():
        lines.append(f"| `{band}` | {cnt} | {_pct(cnt, total)} |")

    avg_bq = _avg_field(ok, "break_quality_score")
    lines.append(f"\n**Average break_quality_score: {_fmt(avg_bq, 3)}**\n")

    return "\n".join(lines) + "\n"


def _section_repeated_clues(cases: List[Dict]) -> str:
    ok = [c for c in cases if c.get("data_quality_ok") == "Y"]
    if not ok:
        return "## 5. Repeated Clues Board\n\n_No truth-clean cases._\n\n"

    lines = [
        "## 5. Repeated Clues Board\n",
        "Patterns appearing in 3+ tokens today:\n",
    ]

    threshold = 3
    found_any = False

    for field in ["move_class", "pre_move_signature", "participation_pattern", "break_quality_band"]:
        counts = _count_by(ok, field)
        for val, cnt in counts.items():
            if cnt >= threshold:
                found_any = True
                lines.append(f"- **`{field}={val}`**: {cnt} / {len(ok)} tokens ({_pct(cnt, len(ok))})")

    if not found_any:
        lines.append("_No pattern appeared in 3+ tokens today._")

    lines.append("")
    return "\n".join(lines) + "\n"


def _section_candidate_signatures(cases: List[Dict]) -> str:
    candidates = [
        c for c in cases
        if c.get("research_verdict") == "setup_signature_candidate"
        and c.get("data_quality_ok") == "Y"
    ]
    monitoring = [
        c for c in cases
        if c.get("research_verdict") == "worth_monitoring"
        and c.get("data_quality_ok") == "Y"
    ]

    lines = ["## 6. Candidate Setup Signatures\n"]

    if candidates:
        lines.append(f"**{len(candidates)} setup signature candidate(s):**\n")
        for c in candidates:
            lines.append(
                f"- `{c['symbol']}` {c['side']} | "
                f"move={c.get('move_class', '?')} | "
                f"pre_sig={c.get('pre_move_signature', '?')} | "
                f"bq={c.get('break_quality_band', '?')} | "
                f"comp={_fmt(c.get('compression_score'), 3)} | "
                f"large_proxy={_fmt(c.get('large_participant_proxy'), 3)}"
            )
    else:
        lines.append("_No setup signature candidates today._\n")

    if monitoring:
        lines.append(f"\n**{len(monitoring)} worth monitoring:**\n")
        for c in monitoring:
            lines.append(
                f"- `{c['symbol']}` {c['side']} | "
                f"sq={c.get('structural_quality', '?')} | "
                f"pattern={c.get('participation_pattern', '?')}"
            )

    lines.append("")

    # Decision note (generated from truth-clean subset only)
    ok_cases = [c for c in cases if c.get("data_quality_ok") == "Y"]
    if ok_cases:
        candidate_count = len(candidates)
        total_ok = len(ok_cases)
        lines.append("**Daily Decision Note** (truth-clean subset only):\n")
        if candidate_count >= 3:
            lines.append(
                f"Today produced {candidate_count}/{total_ok} setup signature candidates. "
                "Consider reviewing for repeating patterns in next session.\n"
            )
        else:
            lines.append(
                f"Today produced {candidate_count}/{total_ok} setup signature candidates. "
                "Insufficient for pattern generalization — continue observation.\n"
            )

    return "\n".join(lines) + "\n"


def _section_footer(
    research_day: str,
    total_cases: int,
    total_anchors: int,
    images_created: int,
    images_total: int,
    deferred_notes: List[str],
) -> str:
    lines = [
        "## 7. Research Footer\n",
        "| Field | Value |",
        "|---|---|",
        f"| Research Day | {research_day} |",
        f"| Total cases | {total_cases} |",
        f"| Total anchor rows | {total_anchors} (expected: {total_cases * 5}) |",
        f"| Images rendered | {images_created} / {images_total} |",
        f"| Anchor detect method | AUTO_V1 |",
        "",
        "**Deferred items:**\n",
    ]
    if deferred_notes:
        for note in deferred_notes:
            lines.append(f"- {note}")
    else:
        lines.append("- None\n")

    lines.append("")
    lines.append(
        "_This report is downstream-only. "
        "It does not modify live bot config, lifecycle, or strategy files._\n"
    )

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def build_report(
    research_day: str,
    selection_context: Dict,
    cases: List[Dict],
    anchor_rows: List[Dict],
    images_created: int = 0,
) -> str:
    """Build the full markdown research report.

    selection_context: dict with btc_24h_change_pct, alt_breadth_pct,
                       research_regime, total_eligible_alts, positive_alts
    cases: list of case row dicts from case_builder
    anchor_rows: list of anchor snapshot rows
    images_created: count of successfully rendered images
    """
    total = len(cases)
    images_total = total * 4  # 4 images per token

    ok_count = sum(1 for c in cases if c.get("data_quality_ok") == "Y")
    warn_count = total - ok_count

    btc_24h = selection_context.get("btc_24h_change_pct", 0.0) or 0.0
    alt_breadth = selection_context.get("alt_breadth_pct", 0.0) or 0.0
    regime = selection_context.get("research_regime", "unclear_mixed")

    sections = []

    sections.append(_section_header(
        research_day=research_day,
        research_regime=regime,
        btc_24h=float(btc_24h),
        alt_breadth_pct=float(alt_breadth),
        total_cases=total,
        data_quality_summary={"ok": ok_count, "warn": warn_count},
    ))

    sections.append(_section_data_gate(cases))
    sections.append(_section_move_archetype(cases))
    sections.append(_section_pre_move_signature(cases))
    sections.append(_section_participation_pattern(cases))
    sections.append(_section_structural_quality(cases))
    sections.append(_section_repeated_clues(cases))
    sections.append(_section_candidate_signatures(cases))

    deferred = [
        "Live post-fix pre_pending/pending_open capture validation (requires fresh CUT_MS rows)",
        "alignment_bonus for large_participant_proxy (deferred to v1.1)",
        "Sprint 3B regime expansion (not approved)",
    ]
    sections.append(_section_footer(
        research_day=research_day,
        total_cases=total,
        total_anchors=len(anchor_rows),
        images_created=images_created,
        images_total=images_total,
        deferred_notes=deferred,
    ))

    return "\n".join(sections)
