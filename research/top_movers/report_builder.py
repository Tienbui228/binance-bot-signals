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

    # Compute health label consistent with canonical summary
    _ok_c = data_quality_summary.get("ok", 0)
    _missing_img = data_quality_summary.get("missing_img", 0)
    _er = _ok_c / max(total_cases, 1)
    if _er >= 0.75:
        _hl = "CLEAN_WITH_VISUAL_GAPS" if _missing_img > 0 else "CLEAN"
    elif _er >= 0.40:
        _hl = "PARTIAL"
    else:
        _hl = "WEAK"
    quality_banner = f"Research Health: {_hl}" + (
        f" | ⚠️ {warn_count} token(s) with data issues" if warn_count else ""
    )

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


def _section_candidate_signatures(cases: List[Dict], sig_candidates: Optional[List[Dict]] = None) -> str:
    """
    Show repeated signature candidates only (cross-case pattern evidence).
    verdict-based setup_signature_candidate logic removed from this section.
    """
    sig_candidates = sig_candidates or []
    lines = ["## 6. Repeated Signature Candidates\n"]

    if sig_candidates:
        top_code = sig_candidates[0].get("signature_candidate_code", "-")
        lines.append(f"**Repeated signature candidates (>=2 eligible cases): {len(sig_candidates)}**")
        lines.append(f"Top: `{top_code}`\n")
        for s in sig_candidates:
            code  = s.get("signature_candidate_code", "")
            n     = s.get("support_count", "?")
            side  = s.get("dominant_side", "?")
            grade = s.get("decision_grade", "")
            conf  = s.get("confidence", "")
            nxt   = s.get("next_action", "")
            lines.append(f"- `{code}` | N={n} | {side} | grade={grade} | conf={conf} | next={nxt}")
        lines.append("")
    else:
        lines.append(
            "**Repeated signature candidates: 0**  "
            "(no cross-case pattern met threshold today - valid and expected on heterogeneous mover days)\n"
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



def _section_decision_bridge(cases: List[Dict], sig_candidates: List[Dict]) -> str:
    """Minimal markdown stubs for the decision bridge (text-only, no tables)."""
    from collections import Counter
    eligible = [c for c in cases if c.get("decision_grade") in (
        "OLD_STRATEGY_IMPROVEMENT_CANDIDATE", "NEW_STRATEGY_THESIS_CANDIDATE",
    ) and c.get("research_eligible_YN") == "Y"]
    lines = ["## 8. Decision Bridge Summary\n"]
    if eligible:
        families = Counter(
            (c.get("maps_to_existing_strategy_family","?"), c.get("improvement_target_layer","?"))
            for c in eligible
        )
        lines.append(f"**Intervention candidates: {len(eligible)} eligible case(s)**\n")
        for (fam, layer), cnt in families.most_common(2):
            lines.append(f"  - `{fam}` / `{layer}`: {cnt} case(s)")
        lines.append("")
    else:
        lines.append("**Intervention candidates:** none today.\n")
    flags = []
    if any(c.get("structural_quality") == "runaway_no_base" for c in cases): flags.append("RUNAWAY_NO_BASE")
    if any(c.get("structural_quality") == "dirty_break" for c in cases): flags.append("DIRTY_BREAK")
    if any(c.get("participation_pattern") == "low_participation_move" for c in cases): flags.append("LOW_PARTICIPATION_BREAK")
    if any(c.get("participation_pattern") == "crowd_chase_dominant"
           and c.get("structural_quality") not in ("clean_base_break","repeated_test_then_break","exhaustion_spike")
           for c in cases): flags.append("CROWD_CHASE_DOMINANT")
    flag_str = ', '.join(flags) if flags else 'none.'
    lines.append(f"**Anti-pattern flags today:** {flag_str}\n")
    lines.append("_Cross-day ledger snapshot and promotion rules available in the DOCX pack._\n")
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def build_report(
    research_day: str,
    selection_context: Dict,
    cases: List[Dict],
    anchor_rows: List[Dict],
    images_created: int = 0,
    sig_candidates: Optional[List[Dict]] = None,
) -> str:
    """Build the full markdown research report.

    selection_context: dict with btc_24h_change_pct, alt_breadth_pct,
                       research_regime, total_eligible_alts, positive_alts
    cases: list of case row dicts from case_builder
    anchor_rows: list of anchor snapshot rows
    images_created: count of successfully rendered images
    """
    total = len(cases)
    images_total = total * 5  # 5 images per token

    ok_count = sum(1 for c in cases if c.get("data_quality_ok") == "Y")
    warn_count = total - ok_count
    missing_img_count = sum(1 for c in cases if c.get("full_visual_complete_YN") != "Y")

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
        data_quality_summary={"ok": ok_count, "warn": warn_count, "missing_img": missing_img_count},
    ))

    sections.append(_section_data_gate(cases))
    sections.append(_section_move_archetype(cases))
    sections.append(_section_pre_move_signature(cases))
    sections.append(_section_participation_pattern(cases))
    sections.append(_section_structural_quality(cases))
    sections.append(_section_repeated_clues(cases))
    sections.append(_section_candidate_signatures(cases, sig_candidates or []))
    sections.append(_section_decision_bridge(cases, sig_candidates or []))

    deferred = [
        "alignment_bonus for large_participant_proxy (deferred to R1 v1.1)",
        "Multi-day anchor QA sample for P0-P4 correctness (not yet run)",
        "Fresh-case validation: visual gap reduction as proxy endpoint coverage improves",
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
