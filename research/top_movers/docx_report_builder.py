"""
research/top_movers/docx_report_builder.py

Builds the full daily research pack DOCX using python-docx.

Fix: Sections 3 and 10 now explicitly distinguish:
  A. Repeated signature candidates (require >= threshold repeated cases → may be zero)
  B. Case-level strategy theses (per-case interpretation → may exist even when A = 0)
"""

import os
from datetime import datetime, timezone
from typing import Dict, List, Optional

from docx import Document
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.oxml import OxmlElement
from docx.oxml.ns import qn
from docx.shared import Inches, Pt, RGBColor

from research.top_movers.io import image_path
from research.top_movers.signature_ledger import (
    load_and_normalize_ledger_rows,
    build_ledger_snapshot_for_report,
    validate_ledger_semantics,
    build_intervention_shortlist,
)

# Maps image key to primary anchor code (for missing-reason lookup)
_IMG_KEY_TO_ANCHOR = {
    "P0_context_1h":            "P0",
    "P0_P1_setup_15m":          "P0",
    "P1_ignition_5m":           "P1",
    "P2_P3_break_expansion_5m": "P2",
    "P4_resolution_15m":        "P4",
}

_IMAGE_DEFS = [
    ("P0_context_1h",              "P0 Context (1h)"),
    ("P0_P1_setup_15m",            "P0→P1 Setup (15m)"),
    ("P1_ignition_5m",             "P1 Ignition (5m)"),
    ("P2_P3_break_expansion_5m",   "P2→P3 Break+Expansion (5m)"),
    ("P4_resolution_15m",          "P4 Resolution (15m)"),
]

_GRADE_COLORS = {
    "OLD_STRATEGY_IMPROVEMENT_CANDIDATE": "2E75B6",
    "NEW_STRATEGY_THESIS_CANDIDATE":      "70AD47",
    "KEEP_TRACKING":                      "FFC000",
    "DESCRIPTIVE_ONLY":                   "808080",
    "NOT_RELIABLE_YET":                   "C00000",
}
_HEALTH_COLORS = {"CLEAN": "70AD47", "CLEAN_WITH_VISUAL_GAPS": "92D050", "PARTIAL": "FFC000", "WEAK": "C00000"}
_YN_COLORS     = {"Y": "70AD47", "N": "C00000"}
_DQ_COLORS     = {"CLEAN": "70AD47", "PARTIAL": "FFC000", "WEAK": "C00000"}


def _shade(cell, hex_color):
    tc = cell._tc; tcPr = tc.get_or_add_tcPr()
    shd = OxmlElement("w:shd")
    shd.set(qn("w:val"),"clear"); shd.set(qn("w:color"),"auto"); shd.set(qn("w:fill"), hex_color)
    tcPr.append(shd)

def _white_bold(cell):
    for p in cell.paragraphs:
        for r in p.runs: r.bold=True; r.font.color.rgb=RGBColor(0xFF,0xFF,0xFF)

def _font(cell, bold=False, size=8):
    for p in cell.paragraphs:
        for r in p.runs: r.bold=bold; r.font.size=Pt(size)

def _make_table(doc, headers, col_w, fill="2E75B6"):
    if sum(col_w) > 6.5: col_w = [w * 6.5 / sum(col_w) for w in col_w]
    t = doc.add_table(rows=1, cols=len(headers)); t.style="Table Grid"
    tbl = t._tbl; tblPr = tbl.find(qn("w:tblPr")) or OxmlElement("w:tblPr")
    tblW = OxmlElement("w:tblW"); tblW.set(qn("w:w"),str(int(sum(col_w)*1440))); tblW.set(qn("w:type"),"dxa"); tblPr.append(tblW)
    for i, cell in enumerate(t.rows[0].cells):
        tc=cell._tc; tcPr=tc.get_or_add_tcPr()
        tcW=OxmlElement("w:tcW"); tcW.set(qn("w:w"),str(int(col_w[i]*1440))); tcW.set(qn("w:type"),"dxa"); tcPr.insert(0,tcW)
    for i,h in enumerate(headers):
        cell=t.rows[0].cells[i]; cell.text=h; _shade(cell,fill); _white_bold(cell)
        cell.paragraphs[0].alignment=WD_ALIGN_PARAGRAPH.CENTER
    return t

def _add_row(t, vals, hi_col=-1, hi_map=None):
    row=t.add_row()
    for i,v in enumerate(vals):
        cell=row.cells[i]; cell.text=str(v) if v is not None else "—"; _font(cell)
        if hi_map and i==hi_col:
            color=hi_map.get(str(v))
            if color:
                _shade(cell,color)
                for p in cell.paragraphs:
                    for r in p.runs: r.font.color.rgb=RGBColor(0xFF,0xFF,0xFF)

def _h1(doc, t): doc.add_heading(t, level=1)
def _h2(doc, t): doc.add_heading(t, level=2)
def _p(doc, t, bold=False, size=10, italic=False):
    p=doc.add_paragraph(); r=p.add_run(t); r.bold=bold; r.font.size=Pt(size); r.italic=italic; return p

def _dom(cases, f):
    vs=[c.get(f,"") for c in cases if c.get(f)]; return max(set(vs),key=vs.count) if vs else "—"
def _pct(n,total): return f"{round(100*n/max(total,1))}%"
def _fmt(v, nd=3):
    if v is None or v=="" or v=="None": return "—"
    try: return str(round(float(v),nd))
    except: return str(v)


# ---------------------------------------------------------------------------
# Section 1 — Data Quality Gate
# ---------------------------------------------------------------------------

def _s1_data_quality(doc, cases):
    _h2(doc, "1. Data Quality Gate")
    eligible  = sum(1 for c in cases if c.get("research_eligible_YN")=="Y")
    full_vis  = sum(1 for c in cases if c.get("full_visual_complete_YN")=="Y")
    proxy_ok  = sum(1 for c in cases if c.get("proxy_complete_YN")=="Y")
    outcome_ok= sum(1 for c in cases if c.get("outcome_complete_YN")=="Y")
    _p(doc, f"Research eligible: {eligible}/{len(cases)}  |  Full visual: {full_vis}/{len(cases)}  |  Proxy ≥50%: {proxy_ok}/{len(cases)}  |  Outcomes: {outcome_ok}/{len(cases)}")
    t=_make_table(doc, ["Symbol","Side","Res.Elig","FullVis","Proxy","Outcome","DQ Flag","Note"],
                  [0.9,0.5,0.8,0.7,0.6,0.7,0.8,2.7])
    for c in cases:
        _add_row(t, [c.get("symbol"),c.get("side"),c.get("research_eligible_YN"),
            c.get("full_visual_complete_YN"),c.get("proxy_complete_YN"),c.get("outcome_complete_YN"),
            c.get("data_quality_flag"),c.get("data_quality_note") or ""],
            hi_col=2, hi_map=_YN_COLORS)
    doc.add_paragraph()


# ---------------------------------------------------------------------------
# Section 2 — Research Integrity Panel
# ---------------------------------------------------------------------------

def _s2_integrity(doc, cases, image_results_all):
    _h2(doc, "2. Research Integrity Panel")
    eligible    = [c for c in cases if c.get("research_eligible_YN")=="Y"]
    missing_img = [c.get("symbol","") for c in cases if c.get("full_visual_complete_YN")!="Y"]
    missing_prx = [c.get("symbol","") for c in cases if c.get("proxy_complete_YN")!="Y"]
    missing_out = [c.get("symbol","") for c in cases if c.get("outcome_complete_YN")!="Y"]
    n=len(cases); er=len(eligible)/n if n else 0; pr=(n-len(missing_prx))/n if n else 0; ir=(n-len(missing_img))/n if n else 0
    health = ("CLEAN_WITH_VISUAL_GAPS" if missing_img else "CLEAN") if er>=0.75 and pr>=0.60 else "PARTIAL" if er>=0.40 or pr>=0.40 else "WEAK"
    t=_make_table(doc, ["Metric","Value"], [3.0,4.7], fill="404040")
    _add_row(t, ["Research health", health], hi_col=1, hi_map=_HEALTH_COLORS)
    _add_row(t, ["Research-eligible", f"{len(eligible)}/{n}"])
    _add_row(t, ["Missing any image (full_visual_complete=N)", ", ".join(missing_img) or "none"])
    _add_row(t, ["Missing proxy history (proxy_complete=N)",   ", ".join(missing_prx) or "none"])
    _add_row(t, ["Missing outcome horizon (outcome_complete=N)",", ".join(missing_out) or "none"])
    doc.add_paragraph()


# ---------------------------------------------------------------------------
# Section 3 — Executive Research Summary
# Note: distinguish repeated sig candidates vs case-level theses (fix 3)
# ---------------------------------------------------------------------------

def _s3_exec_summary(doc, cases, selection_context, signature_candidates):
    _h2(doc, "3. Executive Research Summary")
    eligible=[c for c in cases if c.get("research_eligible_YN")=="Y"]
    if not eligible: _p(doc,"No eligible cases today."); return

    n=len(cases); missing_prx=sum(1 for c in cases if c.get("proxy_complete_YN")!="Y")
    missing_img=sum(1 for c in cases if c.get("full_visual_complete_YN")!="Y")
    er=len(eligible)/n if n else 0; pr=(n-missing_prx)/n if n else 0; ir=(n-missing_img)/n if n else 0
    health = ("CLEAN_WITH_VISUAL_GAPS" if missing_img>0 else "CLEAN") if er>=0.75 and pr>=0.60 else "PARTIAL" if er>=0.40 or pr>=0.40 else "WEAK"

    improve=sum(1 for c in eligible if c.get("strategy_action_type")=="improve_existing")
    create =sum(1 for c in eligible if c.get("strategy_action_type")=="create_new")
    observe=sum(1 for c in eligible if c.get("strategy_action_type")=="keep_observing")

    n_sigs = len(signature_candidates)
    top_sig = signature_candidates[0].get("signature_candidate_code","—") if signature_candidates else "none today"

    t=_make_table(doc, ["Key","Value"], [3.0,4.7])
    rows=[
        ("Research health", health),
        ("Regime", selection_context.get("research_regime","—")),
        ("BTC 24h", f"{_fmt(selection_context.get('btc_24h_change_pct'),2)}%"),
        ("Alt Breadth", f"{_fmt(selection_context.get('alt_breadth_pct'),1)}%"),
        ("Dominant move class", _dom(eligible,"move_class")),
        ("Dominant pre-move signature", _dom(eligible,"pre_move_signature")),
        ("Dominant participation", _dom(eligible,"participation_pattern")),
        ("Dominant structural quality", _dom(eligible,"structural_quality")),
        ("improve_existing (case-level)", str(improve)),
        ("create_new / new thesis (case-level)", str(create)),
        ("keep_observing (case-level)", str(observe)),
        (f"Repeated sig candidates (≥{2} cases)", str(n_sigs)),
        ("Top repeated signature", top_sig),
    ]
    for k,v in rows:
        _add_row(t, [k,v], hi_col=1, hi_map={"CLEAN":"70AD47","PARTIAL":"FFC000","WEAK":"C00000"})
    doc.add_paragraph()

    # Distinction note (fix 3A)
    _p(doc,
       "DISTINCTION — Repeated Signature Candidates vs Case-Level Strategy Theses:",
       bold=True, size=9)
    _p(doc,
       "  A. Repeated signature candidates (Section 10): require >= 2 eligible cases to share the "
       "same (side, pre_move_signature, participation_pattern, structural_quality). "
       "Zero candidates today means no pattern repeated — this is valid and expected on low-volume days.",
       size=9, italic=True)
    _p(doc,
       "  B. Case-level strategy theses (Section 13): derived from individual case interpretation. "
       "A case may have decision_grade = NEW_STRATEGY_THESIS_CANDIDATE or OLD_STRATEGY_IMPROVEMENT_CANDIDATE "
       "even when zero repeated signature candidates exist. These are per-case conclusions, not confirmed repeated patterns.",
       size=9, italic=True)
    doc.add_paragraph()


# ---------------------------------------------------------------------------
# Sections 4-9
# ---------------------------------------------------------------------------

def _s4_selection_board(doc, cases):
    _h2(doc, "4. Selection Board")
    t=_make_table(doc, ["#","Symbol","Side","Return%","Rank","Regime","Eligible","FullVis"],
                  [0.4,1.0,0.5,0.8,0.5,1.5,0.7,0.7])
    for i,c in enumerate(cases,1):
        _add_row(t, [i,c.get("symbol"),c.get("side"),_fmt(c.get("daily_return_pct"),2),
            c.get("top_mover_rank"),c.get("research_regime",""),
            c.get("research_eligible_YN"),c.get("full_visual_complete_YN")],
            hi_col=6, hi_map=_YN_COLORS)
    doc.add_paragraph()


def _s5_move_archetype(doc, cases):
    _h2(doc, "5. Move Archetype Distribution")
    eligible=[c for c in cases if c.get("research_eligible_YN")=="Y"]
    counts: Dict[str,int]={}
    for c in eligible: mc=c.get("move_class","—"); counts[mc]=counts.get(mc,0)+1
    t=_make_table(doc, ["Move Class","Count","Share"], [3.5,0.8,0.8])
    for mc,cnt in sorted(counts.items(), key=lambda x:-x[1]):
        _add_row(t, [mc, cnt, _pct(cnt,len(eligible))])
    doc.add_paragraph()


def _s6_pre_move_sig(doc, cases):
    _h2(doc, "6. Pre-Move Signature Board")
    eligible=[c for c in cases if c.get("research_eligible_YN")=="Y"]
    groups: Dict[str,list]={}
    for c in eligible: groups.setdefault(c.get("pre_move_signature","—"),[]).append(c)
    t=_make_table(doc, ["Signature","Count","Share","Dominant Side"], [2.5,0.7,0.7,1.5])
    for sig,cs in sorted(groups.items(), key=lambda x:-len(x[1])):
        _add_row(t, [sig,len(cs),_pct(len(cs),len(eligible)),_dom(cs,"side")])
    doc.add_paragraph()


def _s7_participation(doc, cases):
    _h2(doc, "7. Participation Pattern Board")
    eligible=[c for c in cases if c.get("research_eligible_YN")=="Y"]
    counts: Dict[str,int]={}
    for c in eligible: counts[c.get("participation_pattern","—")]=counts.get(c.get("participation_pattern","—"),0)+1
    t=_make_table(doc, ["Pattern","Count","Share"], [3.5,0.8,0.8])
    for pat,cnt in sorted(counts.items(), key=lambda x:-x[1]):
        _add_row(t, [pat,cnt,_pct(cnt,len(eligible))])
    doc.add_paragraph()


def _s8_structural_quality(doc, cases):
    _h2(doc, "8. Structural Quality Board")
    eligible=[c for c in cases if c.get("research_eligible_YN")=="Y"]
    counts: Dict[str,int]={}
    for c in eligible: counts[c.get("structural_quality","—")]=counts.get(c.get("structural_quality","—"),0)+1
    t=_make_table(doc, ["Structural Quality","Count","Share"], [3.5,0.8,0.8])
    for sq,cnt in sorted(counts.items(), key=lambda x:-x[1]):
        _add_row(t, [sq,cnt,_pct(cnt,len(eligible))])
    doc.add_paragraph()


def _s9_outcome_quality(doc, cases):
    _h2(doc, "9. Outcome Quality Board")
    eligible=[c for c in cases if c.get("research_eligible_YN")=="Y"]
    groups: Dict[str,list]={}
    for c in eligible: groups.setdefault(c.get("resolution_label","—"),[]).append(c)
    t=_make_table(doc, ["Resolution Label","Count","Share","Avg f1h","Avg f4h","Avg a4h"],
                  [2.5,0.6,0.6,0.9,0.9,0.9])
    for rl,cs in sorted(groups.items(), key=lambda x:-len(x[1])):
        _add_row(t, [rl,len(cs),_pct(len(cs),len(eligible)),
            _fmt(_avg_field(cs,"future_1h_max_favor_pct")),
            _fmt(_avg_field(cs,"future_4h_max_favor_pct")),
            _fmt(_avg_field(cs,"future_4h_max_adverse_pct"))])
    doc.add_paragraph()


# ---------------------------------------------------------------------------
# Section 10 — Signature Evidence Board
# Note: explicitly states distinction and handles zero-candidate case (fix 3B)
# ---------------------------------------------------------------------------

def _s10_signature_evidence(doc, signature_candidates):
    _h2(doc, "10. Signature Evidence Board")

    # Distinction note (fix 3B)
    _p(doc,
       "Repeated signature candidates: require >= 2 eligible cases with the same "
       "(side, pre_move_signature, participation_pattern, structural_quality). "
       "Zero entries is valid — it means no cross-case pattern repeated today.",
       size=9, italic=True)
    _p(doc,
       "Individual cases may still have decision_grade = NEW_STRATEGY_THESIS_CANDIDATE or "
       "OLD_STRATEGY_IMPROVEMENT_CANDIDATE (see Section 13). Those are case-level theses, "
       "not confirmed repeated signatures.",
       size=9, italic=True)

    if not signature_candidates:
        _p(doc, f"No repeated signature candidates today (threshold: ≥2 cases).")
        _p(doc, "signature_candidates_count = 0  |  reason = no_repeated_pattern_met_threshold", size=9)
        doc.add_paragraph(); return

    _p(doc, f"signature_candidates_count = {len(signature_candidates)}")
    t=_make_table(doc,
        ["Code","Description","N","Share%","Side","MClass","PartPat","Struct","Med1hF","Med4hF","Action","Conf","NextAct"],
        [1.3,2.5,0.4,0.6,0.5,1.5,1.8,1.5,0.7,0.7,1.2,0.6,1.5])
    for s in signature_candidates:
        _add_row(t, [
            s.get("signature_candidate_code"), s.get("signature_description"),
            s.get("support_count"), s.get("support_share_pct"),
            s.get("dominant_side"), s.get("dominant_move_class"),
            s.get("dominant_participation_pattern"), s.get("dominant_structural_quality"),
            _fmt(s.get("median_1h_favor")), _fmt(s.get("median_4h_favor")),
            s.get("maps_to_existing_strategy_family"), s.get("confidence"), s.get("next_action"),
        ])
    doc.add_paragraph()


# ---------------------------------------------------------------------------
# Sections 11-18
# ---------------------------------------------------------------------------

def _s11_strategy_mapping(doc, cases):
    _h2(doc, "11. Strategy Mapping Board")
    eligible=[c for c in cases if c.get("research_eligible_YN")=="Y"]
    t=_make_table(doc, ["Symbol","Side","Move Class","Maps To","Fit","Layer","Hypothesis","Cand?"],
                  [0.9,0.5,1.8,1.5,0.5,1.3,2.0,0.6])
    for c in eligible:
        _add_row(t, [c.get("symbol"),c.get("side"),c.get("move_class"),
            c.get("maps_to_existing_strategy_family"),c.get("existing_strategy_fit_confidence"),
            c.get("improvement_target_layer"),c.get("improvement_hypothesis"),
            c.get("existing_strategy_improvement_candidate_YN")])
    doc.add_paragraph()


def _s12_new_strategy(doc, cases):
    _h2(doc, "12. Research Families Under Investigation")
    eligible=[c for c in cases if c.get("new_strategy_candidate_flag")=="Y"]
    _p(doc,
       "These are exploratory research families under investigation — NOT actual NEW_STRATEGY_THESIS_CANDIDATE cases. "
       "A case reaching this board means its move pattern did not map to an existing strategy and showed "
       "some structural interest. It does NOT mean a new strategy is validated or recommended.",
       size=9, italic=True)
    if not eligible: _p(doc,"No exploratory families today."); doc.add_paragraph(); return
    t=_make_table(doc, ["Symbol","Side","Candidate Family","Trigger","Env","Invalid","Conf"],
                  [0.9,0.5,1.8,2.0,1.3,1.5,0.7])
    for c in eligible:
        _add_row(t, [c.get("symbol"),c.get("side"),c.get("candidate_strategy_family_name"),
            c.get("candidate_trigger_description"),c.get("candidate_environment"),
            c.get("candidate_invalid_pattern"),c.get("new_strategy_confidence")])
    doc.add_paragraph()


def _s13_decision_grade(doc, cases):
    _h2(doc, "13. Decision Grade Board")
    _p(doc, "Decision grades are per-case (case-level theses). These exist independently of "
            "repeated signature candidates in Section 10.", size=9, italic=True)
    eligible=[c for c in cases if c.get("research_eligible_YN")=="Y"]
    t=_make_table(doc, ["Symbol","Side","Decision Grade","Action","Conf","Prx%","Out%","Vis","Caution"],
                  [0.9,0.5,2.2,1.4,0.7,0.6,0.6,0.6,0.7])
    for c in eligible:
        _add_row(t, [c.get("symbol"),c.get("side"),c.get("decision_grade"),
            c.get("strategy_action_type"),c.get("classification_confidence"),
            _fmt(c.get("proxy_completeness_score"),0),
            _fmt(c.get("outcome_completeness_score"),0),
            c.get("full_visual_complete_YN"),c.get("caution_flag")],
            hi_col=2, hi_map=_GRADE_COLORS)
    doc.add_paragraph()


def _s14_trap_caution(doc, cases):
    _h2(doc, "14. Trap / Caution Board")
    traps=_detect_traps(cases)
    if not traps: _p(doc,"No trap patterns today."); doc.add_paragraph(); return
    t=_make_table(doc, ["Trap Code","Description","Affected","Why Risky","Caution"], [1.5,2.0,1.5,2.0,1.7])
    for tr in traps:
        _add_row(t, [tr["trap_code"],tr["description"],tr["affected"],tr["why_risky"],tr["caution"]])
    doc.add_paragraph()


def _s15_review_queue(doc, cases):
    _h2(doc, "15. Human Review Queue")
    queue=[c for c in cases if c.get("decision_grade") in
           ("OLD_STRATEGY_IMPROVEMENT_CANDIDATE","NEW_STRATEGY_THESIS_CANDIDATE")
           or c.get("caution_flag")=="Y"]
    if not queue: _p(doc,"No cases require immediate review."); doc.add_paragraph(); return
    t=_make_table(doc, ["Case ID","Symbol","Side","Decision Grade","Reason"], [2.0,1.0,0.5,2.5,2.7])
    for c in queue:
        reason=[]
        if c.get("decision_grade") in ("OLD_STRATEGY_IMPROVEMENT_CANDIDATE","NEW_STRATEGY_THESIS_CANDIDATE"):
            reason.append(c.get("decision_grade",""))
        if c.get("caution_flag")=="Y": reason.append("caution_flag")
        _add_row(t, [c.get("case_id"),c.get("symbol"),c.get("side"),c.get("decision_grade")," | ".join(reason)])
    doc.add_paragraph()


def _s16_case_registry(doc, cases):
    _h2(doc, "16. Case Registry Preview")
    t=_make_table(doc, ["Case ID","Symbol","Side","Return%","Move Class","Struct","Flow Phase","Resolution","Grade"],
                  [1.8,0.9,0.5,0.7,1.5,1.5,1.8,1.8,2.2])
    for c in cases:
        _add_row(t, [c.get("case_id"),c.get("symbol"),c.get("side"),_fmt(c.get("daily_return_pct"),2),
            c.get("move_class"),c.get("structural_quality"),c.get("flow_phase_code"),
            c.get("resolution_label"),c.get("decision_grade")],
            hi_col=8, hi_map=_GRADE_COLORS)
    doc.add_paragraph()


def _s17_case_appendix(doc, cases, anchor_rows, research_day, image_results_all=None):
    _h1(doc, "17. Full Case Detail Appendix")
    _p(doc, "One section per token. 5 images embedded. v2 taxonomy labels.", size=9)

    anchor_by_case: Dict[str,List[Dict]]={}
    for row in anchor_rows:
        anchor_by_case.setdefault(row.get("case_id",""),[]).append(row)

    for c in cases:
        doc.add_page_break()
        cid=c.get("case_id",""); sym=c.get("symbol",""); side=c.get("side","")
        _h2(doc, f"{sym} {side} | {cid}")
        _p(doc, f"Return: {_fmt(c.get('daily_return_pct'),2)}%  |  Move: {c.get('move_class')}  |  "
                f"Struct: {c.get('structural_quality')}  |  Grade: {c.get('decision_grade')}", bold=True)
        _p(doc, f"Elig={c.get('research_eligible_YN')}  FullVis={c.get('full_visual_complete_YN')}  "
                f"Proxy={c.get('proxy_complete_YN')}  Outcome={c.get('outcome_complete_YN')}  "
                f"Caution={c.get('caution_flag')}", size=9)
        if c.get("full_visual_complete_YN") != "Y":
            _p(doc,
               "Note: full_visual_complete = N means one or more chart images could not be rendered. "
               "This does NOT affect outcome/resolution fields (which are computed from price data, not images). "
               "P4 anchor availability and P4 image availability are separate — see anchor table above.",
               size=8, italic=True)
        t1=c.get("case_takeaway_1",""); t2=c.get("case_takeaway_2","")
        if t1: _p(doc, f"► {t1}")
        if t2: _p(doc, f"  {t2}", size=9)

        _p(doc, "Anchor Timeline:", bold=True, size=9)
        at=_make_table(doc, ["Anchor","Ts ms","Close","Quality","Image"], [0.8,1.8,1.2,1.8,1.0], fill="404040")
        for code in ["P0","P1","P2","P3","P4"]:
            a=next((r for r in anchor_by_case.get(cid,[]) if r.get("anchor_code")==code), None)
            if a:
                q=""
                if code=="P0": q=f"comp={_fmt(a.get('compression_score'))}"
                elif code=="P2": q=f"bq={a.get('break_quality_band','')} ({_fmt(a.get('break_quality_score'))})"
                elif code=="P3": q=f"ext={_fmt(a.get('directional_extension_pct'))}%"
                _add_row(at, [code,a.get("anchor_ts_ms"),a.get("bar_close"),q,a.get("image_created_YN","N")])
        doc.add_paragraph()

        _p(doc, "Proxy & Outcome:", bold=True, size=9)
        pt=_make_table(doc, ["Field","Value","Field","Value"], [2.0,1.5,2.0,1.5], fill="404040")
        pairs=[
            ("flow_phase",c.get("flow_phase_code")), ("resolution",c.get("resolution_label")),
            ("participation",c.get("participation_pattern")), ("pre_sig",c.get("pre_move_signature")),
            ("large_proxy",_fmt(c.get("large_participant_proxy"))), ("crowd_proxy",_fmt(c.get("crowd_participation_proxy"))),
            ("f1h_favor",_fmt(c.get("future_1h_max_favor_pct"))), ("f4h_favor",_fmt(c.get("future_4h_max_favor_pct"))),
        ]
        for i in range(0,len(pairs),2):
            k1,v1=pairs[i]; k2,v2=pairs[i+1] if i+1<len(pairs) else ("","")
            _add_row(pt, [k1,v1,k2,v2])
        doc.add_paragraph()

        _p(doc, "Charts:", bold=True, size=9)
        for img_key, img_label in _IMAGE_DEFS:
            fp=image_path(research_day, cid, img_key)
            _p(doc, f"  {img_label}", size=8)
            if os.path.exists(fp):
                try:
                    doc.add_picture(fp, width=Inches(6.5))
                    doc.paragraphs[-1].alignment=WD_ALIGN_PARAGRAPH.CENTER
                except Exception as e:
                    _p(doc, f"  [Embed failed: {e}]", size=8)
            else:
                _img_res = (image_results_all or {}).get(cid, {}).get(img_key, {})
                _reason = _img_res.get("reason", "") or "reason_unknown"
                _p(doc, f"  [Missing: {img_key} | reason: {_reason}]", size=8)
            doc.add_paragraph()


def _s18_footer(doc, cases, signature_candidates):
    doc.add_page_break(); _h1(doc, "Research Action Footer")
    eligible=[c for c in cases if c.get("research_eligible_YN")=="Y"]
    keep  = [s.get("signature_candidate_code","") for s in signature_candidates if s.get("strategy_action_type")=="keep_observing"]
    imp   = [c.get("case_id","") for c in eligible if c.get("strategy_action_type")=="improve_existing"]
    # Only surface case-level new thesis candidates (NEW_STRATEGY_THESIS_CANDIDATE), not exploratory families
    new_t = list(set(c.get("case_id","") for c in eligible
                     if c.get("decision_grade")=="NEW_STRATEGY_THESIS_CANDIDATE"))
    not_yet=[c.get("case_id","") for c in eligible if c.get("decision_grade")=="NOT_RELIABLE_YET"]
    t=_make_table(doc, ["Action","Items"], [2.0,6.6])
    _add_row(t, ["Keep tracking (repeated sigs):", ", ".join(keep) or "none today"])
    _add_row(t, ["Improve existing (case-level):", ", ".join(imp) or "none today"])
    _add_row(t, ["New thesis case-level (decision_grade=NEW_STRATEGY_THESIS_CANDIDATE):",
                 ", ".join(new_t) or "none today"])
    _add_row(t, ["Not conclude yet:",              ", ".join(not_yet) or "none"])


# ---------------------------------------------------------------------------
# Trap detection
# ---------------------------------------------------------------------------

def _detect_traps(cases):
    traps=[]
    runaway=[c.get("symbol","") for c in cases if c.get("structural_quality")=="runaway_no_base"]
    if runaway:
        traps.append({"trap_code":"RUNAWAY_NO_BASE","description":"Break without compression base",
                      "affected":", ".join(runaway[:5]),"why_risky":"Entry chases, not positions",
                      "caution":"Do not treat as signature candidate"})
    crowd=[c.get("symbol","") for c in cases if c.get("participation_pattern")=="crowd_chase_dominant"
           and c.get("structural_quality") not in ("clean_base_break","repeated_test_then_break")]
    if crowd:
        traps.append({"trap_code":"CROWD_CHASE_DOMINANT","description":"Crowd-driven without clean structure",
                      "affected":", ".join(crowd[:5]),"why_risky":"Reverses quickly without large participant support",
                      "caution":"Require large proxy confirmation"})
    lowpart=[c.get("symbol","") for c in cases if c.get("participation_pattern")=="low_participation_move"]
    if lowpart:
        traps.append({"trap_code":"LOW_PARTICIPATION_MOVE","description":"Minimal flow participation at break",
                      "affected":", ".join(lowpart[:5]),"why_risky":"Low participation breaks often fail",
                      "caution":"Mark descriptive_only until confirmed"})
    weak=[c.get("symbol","") for c in cases if (c.get("proxy_completeness_score") or 0)<40]
    if len(weak)>=3:
        traps.append({"trap_code":"WEAK_PROXY","description":f"{len(weak)} tokens proxy < 40%",
                      "affected":", ".join(weak[:5]),"why_risky":"Unreliable flow classification",
                      "caution":"Treat conclusions as provisional"})
    return traps


def _avg_field(cases, f):
    vs=[]
    for c in cases:
        v=c.get(f)
        if v not in (None,"","None"):
            try: vs.append(float(v))
            except: pass
    return round(sum(vs)/len(vs),4) if vs else None



# ---------------------------------------------------------------------------
# New sections (Decision Bridge — inserted after Section 14)
# ---------------------------------------------------------------------------

_ROLE_COLORS_DB = {
    "repeated_candidate": "70AD47",
    "tracking":           "FFC000",
    "first_observation":  "2E75B6",
    "stale":              "808080",
}

_ANTIPATTERN_WHY = {
    "RUNAWAY_NO_BASE":         "Entry chases price with no base; high reversal risk",
    "DIRTY_BREAK":             "Break quality insufficient; unclear participation",
    "LOW_PARTICIPATION_BREAK": "Minimal flow at break; often fails without follow-through",
    "CROWD_CHASE_DOMINANT":    "Crowd-driven without large participant confirmation; reverses quickly",
}


def _snew_semantic_warning(doc, normalized_ledger_rows):
    warnings = validate_ledger_semantics(normalized_ledger_rows)
    if not warnings:
        return
    _h2(doc, "14a. Ledger Semantic Warnings")
    tbl = doc.add_table(rows=1, cols=1); tbl.style = "Table Grid"
    cell = tbl.rows[0].cells[0]; _shade(cell, "FFF2CC")
    cell.paragraphs[0].clear()
    for w in warnings:
        p = cell.add_paragraph(f"\u26a0  {w}")
        for r in p.runs:
            r.font.size = Pt(9); r.bold = True
    doc.add_paragraph()


def _snew_intervention_shortlist(doc, cases, sig_candidates, normalized_ledger_rows):
    _h2(doc, "14b. Strategy Intervention Shortlist")
    eligible = [c for c in cases if c.get("research_eligible_YN") == "Y"]
    shortlist = build_intervention_shortlist(eligible, sig_candidates, normalized_ledger_rows)
    if not shortlist:
        _p(doc, "No intervention candidates today."); doc.add_paragraph(); return
    t = _make_table(doc,
        ["Strategy Family", "Issue Layer", "Cases", "Repeated?", "Cross-Day", "Evidence Source", "Readiness"],
        [1.8, 1.5, 0.6, 0.7, 0.8, 1.5, 2.3])
    for row in shortlist:
        _add_row(t, [row.get("strategy_family",""), row.get("issue_layer",""),
            row.get("case_count_today",0), str(row.get("repeated_support_today",0)),
            row.get("cross_day_support",0), row.get("evidence_source",""), row.get("readiness","")])
    doc.add_paragraph()


def _snew_antipattern_board(doc, cases):
    _h2(doc, "14c. Anti-Pattern / Downgrade Board")
    patterns = []
    runaway = [c for c in cases if c.get("structural_quality") == "runaway_no_base"]
    if runaway:
        patterns.append({"code":"RUNAWAY_NO_BASE","affected":", ".join(c.get("symbol","") for c in runaway[:5]),
            "why":_ANTIPATTERN_WHY["RUNAWAY_NO_BASE"],"repeated":"Y" if len(runaway)>=2 else "N"})
    dirty = [c for c in cases if c.get("structural_quality") == "dirty_break"]
    if dirty:
        patterns.append({"code":"DIRTY_BREAK","affected":", ".join(c.get("symbol","") for c in dirty[:5]),
            "why":_ANTIPATTERN_WHY["DIRTY_BREAK"],"repeated":"Y" if len(dirty)>=2 else "N"})
    lowpart = [c for c in cases if c.get("participation_pattern") == "low_participation_move"]
    if lowpart:
        patterns.append({"code":"LOW_PARTICIPATION_BREAK","affected":", ".join(c.get("symbol","") for c in lowpart[:5]),
            "why":_ANTIPATTERN_WHY["LOW_PARTICIPATION_BREAK"],"repeated":"Y" if len(lowpart)>=2 else "N"})
    crowd = [c for c in cases if c.get("participation_pattern") == "crowd_chase_dominant"
             and c.get("structural_quality") not in ("clean_base_break","repeated_test_then_break","exhaustion_spike")]
    if crowd:
        patterns.append({"code":"CROWD_CHASE_DOMINANT","affected":", ".join(c.get("symbol","") for c in crowd[:5]),
            "why":_ANTIPATTERN_WHY["CROWD_CHASE_DOMINANT"],"repeated":"Y" if len(crowd)>=2 else "N"})
    if not patterns:
        _p(doc, "No anti-pattern flags today."); doc.add_paragraph(); return
    t = _make_table(doc, ["Pattern Code","Affected Symbols","Why Downgrade","Repeated Today","Note"],
                    [1.6,1.5,2.4,0.9,1.8])
    for pt in patterns:
        _add_row(t, [pt["code"],pt["affected"],pt["why"],pt["repeated"],"do not use as strategy thesis"])
    doc.add_paragraph()


def _snew_ledger_snapshot(doc, normalized_ledger_rows, research_day):
    _h2(doc, f"14d. Cross-Day Ledger Snapshot (as of {research_day})")
    snapshot = build_ledger_snapshot_for_report(normalized_ledger_rows, research_day)
    if not snapshot:
        _p(doc, "No ledger data available."); doc.add_paragraph(); return
    t = _make_table(doc,
        ["Code","First Seen","Last Seen","Support Days","Recent 7d","Status","Role"],
        [2.0,0.9,0.9,0.9,0.8,1.3,1.4])
    for row in snapshot:
        role = row.get("current_role","")
        r = t.add_row()
        vals = [row.get("signature_candidate_code", row.get("signature_key","")[:25]),
                row.get("first_seen_date",""), row.get("last_seen_date",""),
                row.get("support_days_count",0), row.get("recent_support_days_count",0),
                row.get("latest_validation_status",""), role]
        for i, v in enumerate(vals):
            cell = r.cells[i]; cell.text = str(v) if v is not None else "\u2014"; _font(cell)
            if i == 6:
                color = _ROLE_COLORS_DB.get(role)
                if color:
                    _shade(cell, color)
                    for p in cell.paragraphs:
                        for run in p.runs: run.font.color.rgb = RGBColor(0xFF,0xFF,0xFF)
            elif role == "stale":
                _shade(cell, "D9D9D9")
    doc.add_paragraph()


def _snew_promotion_rules(doc):
    _h2(doc, "14e. Readiness Promotion Rules")
    t = _make_table(doc, ["Readiness Level","Meaning"], [2.5,5.8], fill="404040")
    for level, meaning in [
        ("descriptive_only",                  "Not enough repetition or strategy relevance."),
        ("keep_tracking",                      "Interesting but insufficient for intervention."),
        ("old_strategy_improvement_candidate", "Points to existing strategy + identifiable improvement layer."),
        ("new_strategy_thesis_candidate",      "New family candidate — not yet validated, needs multi-day support."),
    ]:
        _add_row(t, [level, meaning])
    _p(doc, "Note: Repeated signatures (Section 10) and case-level theses (Section 13) are independent. Do not conflate.",
       size=9, italic=True)
    doc.add_paragraph()


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def build_docx_pack(research_day, selection_context, cases, anchor_rows, signature_candidates,
                    output_path, image_results_all=None) -> str:
    if image_results_all is None: image_results_all={}
    normalized_ledger_rows = load_and_normalize_ledger_rows(research_day, window_days=7, as_of_day=research_day)
    doc=Document()

    _h1(doc, "Daily Top Movers Research Pack")
    _p(doc, f"Research Day: {research_day}", bold=True)
    _p(doc, f"Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    _p(doc, f"Regime: {selection_context.get('research_regime','—')}")
    _p(doc, f"BTC 24h: {_fmt(selection_context.get('btc_24h_change_pct'),2)}%  |  Alt Breadth: {_fmt(selection_context.get('alt_breadth_pct'),1)}%")
    eligible_n=sum(1 for c in cases if c.get("research_eligible_YN")=="Y")
    _p(doc, f"Cases: {len(cases)}  |  Eligible: {eligible_n}  |  Signatures (repeated): {len(signature_candidates)}")
    doc.add_page_break()

    _s1_data_quality(doc, cases)
    _s2_integrity(doc, cases, image_results_all)
    _s3_exec_summary(doc, cases, selection_context, signature_candidates)
    _s4_selection_board(doc, cases)
    _s5_move_archetype(doc, cases)
    _s6_pre_move_sig(doc, cases)
    _s7_participation(doc, cases)
    _s8_structural_quality(doc, cases)
    _s9_outcome_quality(doc, cases)
    _s10_signature_evidence(doc, signature_candidates)
    _s11_strategy_mapping(doc, cases)
    _s12_new_strategy(doc, cases)
    _s13_decision_grade(doc, cases)
    _s14_trap_caution(doc, cases)
    _snew_semantic_warning(doc, normalized_ledger_rows)
    _snew_intervention_shortlist(doc, cases, signature_candidates, normalized_ledger_rows)
    _snew_antipattern_board(doc, cases)
    _snew_ledger_snapshot(doc, normalized_ledger_rows, research_day)
    _snew_promotion_rules(doc)
    _s15_review_queue(doc, cases)
    _s16_case_registry(doc, cases)
    _s17_case_appendix(doc, cases, anchor_rows, research_day, image_results_all)
    _s18_footer(doc, cases, signature_candidates)

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    doc.save(output_path)
    return output_path
