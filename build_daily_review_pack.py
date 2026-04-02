
#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from docx import Document
from docx.enum.section import WD_SECTION
from docx.enum.style import WD_STYLE_TYPE
from docx.enum.table import WD_ALIGN_VERTICAL, WD_TABLE_ALIGNMENT
from docx.enum.text import WD_ALIGN_PARAGRAPH, WD_BREAK
from docx.oxml import OxmlElement
from docx.oxml.ns import qn
from docx.shared import Inches, Pt
from zoneinfo import ZoneInfo


def read_csv_rows(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def safe_float(v) -> Optional[float]:
    try:
        if v in (None, "", "NA", "N/A"):
            return None
        return float(v)
    except Exception:
        return None


def safe_int(v) -> Optional[int]:
    try:
        if v in (None, "", "NA", "N/A"):
            return None
        return int(float(v))
    except Exception:
        return None


def yn(v: str) -> str:
    return "Y" if str(v or "").strip().upper() == "Y" else "N"


def fmt_ts_ms(ts_ms: Optional[int], tz_name: str) -> str:
    if not ts_ms:
        return ""
    tz = ZoneInfo(tz_name)
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).astimezone(tz).strftime("%Y-%m-%d %H:%M:%S")


def case_owner_day(ts_ms: Optional[int], tz_name: str) -> str:
    if not ts_ms:
        return ""
    tz = ZoneInfo(tz_name)
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).astimezone(tz).strftime("%Y-%m-%d")


def infer_case_close_type(row: Dict[str, str], fallback_hours: int) -> str:
    explicit = (row.get("case_close_type") or "").strip()
    if explicit:
        return explicit
    status = (row.get("status") or "").upper()
    if status in {"INVALIDATED", "EXPIRED_WAIT"}:
        return "true_close"
    if status in {"PENDING", "CONFIRMED"}:
        created = safe_int(row.get("created_ts_ms"))
        if created:
            due = created + fallback_hours * 3600 * 1000
            now_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
            if status == "PENDING" and now_ms >= due:
                return "fallback_4h_snapshot"
        return "not_due_yet"
    return "true_close" if safe_int(row.get("closed_ts_ms")) else "not_due_yet"


def set_cell_text(cell, text: str, bold: bool = False):
    cell.text = ""
    p = cell.paragraphs[0]
    r = p.add_run(text or "")
    r.bold = bold
    p.paragraph_format.space_after = Pt(0)
    cell.vertical_alignment = WD_ALIGN_VERTICAL.CENTER


def shade_cell(cell, fill: str):
    tc_pr = cell._tc.get_or_add_tcPr()
    shd = OxmlElement("w:shd")
    shd.set(qn("w:fill"), fill)
    tc_pr.append(shd)


def set_repeat_table_header(row):
    trPr = row._tr.get_or_add_trPr()
    tblHeader = OxmlElement('w:tblHeader')
    tblHeader.set(qn('w:val'), "true")
    trPr.append(tblHeader)


def set_cell_margins(cell, top=100, start=120, bottom=100, end=120):
    tc = cell._tc
    tcPr = tc.get_or_add_tcPr()
    tcMar = tcPr.first_child_found_in("w:tcMar")
    if tcMar is None:
        tcMar = OxmlElement("w:tcMar")
        tcPr.append(tcMar)
    for m, val in (("top", top), ("start", start), ("bottom", bottom), ("end", end)):
        node = tcMar.find(qn(f"w:{m}"))
        if node is None:
            node = OxmlElement(f"w:{m}")
            tcMar.append(node)
        node.set(qn("w:w"), str(val))
        node.set(qn("w:type"), "dxa")


def ensure_styles(doc: Document):
    styles = doc.styles

    if "CaseTitle" not in styles:
        style = styles.add_style("CaseTitle", WD_STYLE_TYPE.PARAGRAPH)
        style.base_style = styles["Heading 2"]
        style.font.size = Pt(13)
        style.font.bold = True

    if "MiniLabel" not in styles:
        style = styles.add_style("MiniLabel", WD_STYLE_TYPE.PARAGRAPH)
        style.base_style = styles["Body Text"]
        style.font.size = Pt(9)
        style.font.bold = True

    if "MetaText" not in styles:
        style = styles.add_style("MetaText", WD_STYLE_TYPE.PARAGRAPH)
        style.base_style = styles["Body Text"]
        style.font.size = Pt(9)

    if "CodeNote" not in styles:
        style = styles.add_style("CodeNote", WD_STYLE_TYPE.PARAGRAPH)
        style.base_style = styles["Body Text"]
        style.font.name = "Courier New"
        style.font.size = Pt(9)


def load_snapshot_index(path: Path) -> Dict[Tuple[str, str], Dict[str, str]]:
    rows = read_csv_rows(path)
    out: Dict[Tuple[str, str], Dict[str, str]] = {}
    for row in rows:
        pid = row.get("pending_id") or row.get("setup_id") or ""
        stage = row.get("stage") or ""
        if pid and stage:
            out[(pid, stage)] = row
    return out


def try_load_workspace_case(workspace: Path, case_day: str, case_id: str) -> Dict:
    if not workspace.exists():
        return {}
    # Best-effort recursive search; safe but bounded.
    candidates = []
    direct = workspace / case_day / f"{case_id}.json"
    if direct.exists():
        candidates.append(direct)
    # fallback bounded recursive search
    for p in workspace.rglob("*.json"):
        name = p.name
        if case_id in name or p.stem == case_id:
            candidates.append(p)
            if len(candidates) >= 10:
                break
    for c in candidates:
        try:
            data = json.loads(c.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                return data
        except Exception:
            continue
    return {}


def stage_meta_from_case(case_meta: Dict, stage_key: str) -> Tuple[str, str, str]:
    if not case_meta:
        return "", "", ""
    stages = case_meta.get("stages") or {}
    item = stages.get(stage_key) or case_meta.get(stage_key) or {}
    if not isinstance(item, dict):
        return "", "", ""
    status = str(item.get("stage_status") or item.get("status") or "")
    ctype = str(item.get("stage_content_type") or item.get("content_type") or "")
    note = str(item.get("note") or "")
    return status, ctype, note


def infer_stage(row: Dict[str, str], case_meta: Dict, snapshots: Dict[Tuple[str, str], Dict[str, str]], stage_key: str) -> Tuple[str, str, str, Optional[Path]]:
    pid = row.get("pending_id") or row.get("setup_id") or ""
    stage_map = {
        "pre_pending": "pre_pending",
        "pending_open": "pending",
        "entry_or_confirm": "confirmed",
        "case_close": "closed",
    }
    snap_stage = stage_map[stage_key]
    snap_row = snapshots.get((pid, snap_stage))
    img_path = None
    if snap_row and snap_row.get("image_path"):
        p = Path(snap_row["image_path"])
        if p.exists():
            img_path = p

    status, ctype, note = stage_meta_from_case(case_meta, stage_key)
    if status:
        return status, ctype or ("chart_snapshot" if img_path else "none"), note, img_path

    # fallback inference from row + snapshot availability
    final_status = (row.get("status") or "").upper()
    if stage_key in {"pre_pending", "pending_open"}:
        if img_path:
            return "captured", "chart_snapshot", note or (snap_row.get("note") if snap_row else ""), img_path
        return "missing_unexpected", "none", "MISSING UNEXPECTED", None

    if stage_key == "entry_or_confirm":
        if img_path:
            return "captured", "chart_snapshot", note or (snap_row.get("note") if snap_row else ""), img_path
        if final_status in {"CONFIRMED", "SENT"} or yn(row.get("is_confirmed")) == "Y" or yn(row.get("is_sent_signal")) == "Y":
            return "missing_unexpected", "none", "MISSING UNEXPECTED", None
        return "not_applicable", "none", "NOT APPLICABLE", None

    if stage_key == "case_close":
        close_type = infer_case_close_type(row, 4)
        if img_path:
            return "captured", "chart_snapshot", note or (snap_row.get("note") if snap_row else "") or row.get("close_reason", ""), img_path
        if close_type == "not_due_yet":
            return "not_reached_yet", "none", "NOT REACHED YET", None
        return "missing_unexpected", "none", "MISSING UNEXPECTED", None

    return "", "", "", img_path


def add_kv_table(doc: Document, items: List[Tuple[str, str]], widths=(1.8, 4.9)):
    tbl = doc.add_table(rows=0, cols=2)
    tbl.alignment = WD_TABLE_ALIGNMENT.LEFT
    tbl.style = "Table Grid"
    for k, v in items:
        row = tbl.add_row()
        row.cells[0].width = Inches(widths[0])
        row.cells[1].width = Inches(widths[1])
        set_cell_text(row.cells[0], k, bold=True)
        set_cell_text(row.cells[1], v)
        shade_cell(row.cells[0], "F2F4F7")
        for c in row.cells:
            set_cell_margins(c)
    doc.add_paragraph("")


def add_stage_block(doc: Document, stage_title: str, status: str, content_type: str, note: str, image_path: Optional[Path], extra_lines: Optional[List[str]] = None):
    p = doc.add_paragraph(style="CaseTitle")
    p.add_run(stage_title)
    meta = doc.add_paragraph(style="MetaText")
    meta.add_run(f"Stage status: {status or '-'} | Content type: {content_type or '-'}")
    if extra_lines:
        for line in extra_lines:
            if line:
                doc.add_paragraph(line, style="MetaText")
    if note:
        doc.add_paragraph(f"Note: {note}", style="MetaText")
    if image_path and image_path.exists():
        try:
            doc.add_picture(str(image_path), width=Inches(6.3))
            cap = doc.add_paragraph(style="MetaText")
            cap.alignment = WD_ALIGN_PARAGRAPH.CENTER
            cap.add_run(image_path.name)
        except Exception as e:
            doc.add_paragraph(f"[image insert failed] {image_path.name}: {e}", style="CodeNote")
    doc.add_paragraph("")


def add_summary_table(doc: Document, summary_items: List[Tuple[str, str]]):
    tbl = doc.add_table(rows=1, cols=2)
    tbl.style = "Table Grid"
    tbl.alignment = WD_TABLE_ALIGNMENT.LEFT
    hdr = tbl.rows[0]
    set_repeat_table_header(hdr)
    set_cell_text(hdr.cells[0], "Metric", bold=True)
    set_cell_text(hdr.cells[1], "Value", bold=True)
    shade_cell(hdr.cells[0], "D9EAF7")
    shade_cell(hdr.cells[1], "D9EAF7")
    for c in hdr.cells:
        set_cell_margins(c)
    for k, v in summary_items:
        row = tbl.add_row()
        set_cell_text(row.cells[0], k)
        set_cell_text(row.cells[1], v)
        for c in row.cells:
            set_cell_margins(c)
    doc.add_paragraph("")


def build_doc(args):
    tz_name = args.tz
    rows = read_csv_rows(Path(args.pending))
    snapshot_idx = load_snapshot_index(Path(args.snapshot_index))

    owner_rows = []
    for row in rows:
        cts = safe_int(row.get("created_ts_ms"))
        if case_owner_day(cts, tz_name) == args.date:
            owner_rows.append(row)

    owner_rows.sort(key=lambda r: safe_int(r.get("created_ts_ms")) or 0)

    total = len(owner_rows)
    review_eligible = sum(1 for r in owner_rows if yn(r.get("review_eligible")) == "Y")
    excluded = total - review_eligible
    semantic_bad = sum(1 for r in owner_rows if yn(r.get("semantic_consistency")) == "N")
    missing_pre = 0
    missing_pending = 0
    missing_entry = 0
    missing_close = 0

    # precompute stage states for summary
    precomputed = []
    for row in owner_rows:
        case_id = row.get("pending_id") or row.get("setup_id") or ""
        case_meta = try_load_workspace_case(Path(args.workspace), args.date, case_id)
        pre = infer_stage(row, case_meta, snapshot_idx, "pre_pending")
        pend = infer_stage(row, case_meta, snapshot_idx, "pending_open")
        ent = infer_stage(row, case_meta, snapshot_idx, "entry_or_confirm")
        clo = infer_stage(row, case_meta, snapshot_idx, "case_close")
        if pre[0] == "missing_unexpected":
            missing_pre += 1
        if pend[0] == "missing_unexpected":
            missing_pending += 1
        if ent[0] == "missing_unexpected":
            missing_entry += 1
        if clo[0] == "missing_unexpected":
            missing_close += 1
        precomputed.append((row, case_meta, pre, pend, ent, clo))

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"daily_review_{args.date}.docx"

    doc = Document()
    ensure_styles(doc)
    section = doc.sections[0]
    section.left_margin = Inches(0.8)
    section.right_margin = Inches(0.8)
    section.top_margin = Inches(0.7)
    section.bottom_margin = Inches(0.7)

    title = doc.add_paragraph()
    title.alignment = WD_ALIGN_PARAGRAPH.CENTER
    r = title.add_run(f"Daily Signal Review {args.date}")
    r.bold = True
    r.font.size = Pt(17)

    sub = doc.add_paragraph(style="MetaText")
    sub.alignment = WD_ALIGN_PARAGRAPH.CENTER
    sub.add_run("Lifecycle evidence + short-horizon outcome review pack")

    add_summary_table(doc, [
        ("Total cases", str(total)),
        ("Review eligible", str(review_eligible)),
        ("Excluded", str(excluded)),
        ("Semantic inconsistency", str(semantic_bad)),
        ("missing_unexpected: pre_pending", str(missing_pre)),
        ("missing_unexpected: pending_open", str(missing_pending)),
        ("missing_unexpected: entry_or_confirm", str(missing_entry)),
        ("missing_unexpected: case_close", str(missing_close)),
    ])

    # Registry preview
    doc.add_paragraph("Case Registry Preview", style="CaseTitle")
    preview_tbl = doc.add_table(rows=1, cols=6)
    preview_tbl.style = "Table Grid"
    preview_tbl.alignment = WD_TABLE_ALIGNMENT.LEFT
    hdr = preview_tbl.rows[0]
    set_repeat_table_header(hdr)
    for i, h in enumerate(["Case ID", "Symbol", "Side", "Strategy", "Status", "Outcome"]):
        set_cell_text(hdr.cells[i], h, bold=True)
        shade_cell(hdr.cells[i], "D9EAF7")
        set_cell_margins(hdr.cells[i])
    for row, _, _, _, _, _ in precomputed:
        rr = preview_tbl.add_row()
        vals = [
            row.get("pending_id") or row.get("setup_id") or "",
            row.get("symbol", ""),
            row.get("side", ""),
            row.get("strategy", ""),
            row.get("status", ""),
            row.get("outcome_conclusion_code", "") or row.get("close_reason", ""),
        ]
        for i, v in enumerate(vals):
            set_cell_text(rr.cells[i], v)
            set_cell_margins(rr.cells[i])
    doc.add_paragraph("")

    for idx, (row, case_meta, pre, pend, ent, clo) in enumerate(precomputed, 1):
        if idx > 1:
            doc.add_page_break()

        case_id = row.get("pending_id") or row.get("setup_id") or ""
        p = doc.add_paragraph(style="CaseTitle")
        p.add_run(f"Case {idx}: {case_id}")

        status_final = row.get("status", "")
        close_reason = row.get("close_reason", "")
        created_local = fmt_ts_ms(safe_int(row.get("created_ts_ms")), tz_name)
        signal_local = fmt_ts_ms(safe_int(row.get("signal_open_time")), tz_name)
        confirmed_local = fmt_ts_ms(safe_int(row.get("confirmed_ts_ms")), tz_name)
        sent_local = fmt_ts_ms(safe_int(row.get("sent_ts_ms")), tz_name)
        close_local = fmt_ts_ms(safe_int(row.get("closed_ts_ms")), tz_name)
        fallback_due = ""
        cts = safe_int(row.get("created_ts_ms"))
        if cts:
            fallback_due = fmt_ts_ms(cts + int(args.fallback_hours) * 3600 * 1000, tz_name)

        add_kv_table(doc, [
            ("Summary", f"{row.get('symbol','')} | {row.get('side','')} | {row.get('strategy','')} | status_final={status_final} | close_reason={close_reason}"),
            ("Times", f"signal={signal_local} | created={created_local} | confirmed={confirmed_local} | sent={sent_local} | close={close_local} | fallback_due={fallback_due}"),
            ("Semantics", f"is_confirmed={yn(row.get('is_confirmed'))} | is_sent_signal={yn(row.get('is_sent_signal'))} | review_eligible={yn(row.get('review_eligible'))} | semantic_consistency={yn(row.get('semantic_consistency'))}"),
            ("Review integrity", f"review_exclusion_reason={row.get('review_exclusion_reason','')} | semantic_issue={row.get('semantic_issue','')}"),
            ("Close anchoring", f"case_close_type={infer_case_close_type(row, int(args.fallback_hours))} | close_anchor_time={fmt_ts_ms(safe_int(row.get('close_anchor_time_ms')), tz_name)} | close_capture_basis={row.get('close_capture_basis','')}"),
            ("Tradeability / outcome", f"entry_feasible={row.get('entry_feasible_YN','')} | slippage={row.get('entry_slippage_pct','')} | regret_valid={row.get('regret_valid_YN','')} | outcome={row.get('outcome_conclusion_code','')}"),
        ])

        add_stage_block(doc, "Pre Pending", pre[0], pre[1], pre[2], pre[3])
        add_stage_block(doc, "Pending Open", pend[0], pend[1], pend[2], pend[3])

        entry_extra = []
        if row.get("confirm_fail_detail"):
            entry_extra.append(f"confirm_fail_detail: {row.get('confirm_fail_detail')}")
        add_stage_block(doc, "Entry Or Confirm", ent[0], ent[1], ent[2], ent[3], entry_extra)

        close_extra = []
        if row.get("close_trigger_detail"):
            close_extra.append(f"close_trigger_detail: {row.get('close_trigger_detail')}")
        if row.get("invalidation_detail"):
            close_extra.append(f"invalidation_detail: {row.get('invalidation_detail')}")
        add_stage_block(doc, "Case Close", clo[0], clo[1], clo[2], clo[3], close_extra)

        doc.add_paragraph("Post-Close Outcome", style="CaseTitle")
        add_kv_table(doc, [
            ("1h outcome", f"available={yn(row.get('outcome_1h_available'))} | favor={row.get('future_1h_max_favor_pct','')} | adverse={row.get('future_1h_max_adverse_pct','')} | summary={row.get('outcome_1h_summary','')}"),
            ("2h outcome", f"available={yn(row.get('outcome_2h_available'))} | favor={row.get('future_2h_max_favor_pct','')} | adverse={row.get('future_2h_max_adverse_pct','')} | summary={row.get('outcome_2h_summary','')}"),
            ("4h reference", f"favor={row.get('future_4h_max_favor_pct','')} | adverse={row.get('future_4h_max_adverse_pct','')}"),
            ("Reclaim", f"2h={row.get('reclaim_breakout_2h_YN','')} | 4h={row.get('reclaim_breakout_4h_YN','')}"),
            ("Execution reality", f"entry_feasible={row.get('entry_feasible_YN','')} | entry_window={row.get('entry_feasible_window_minutes','')}m | entry_note={row.get('entry_execution_note','')}"),
            ("Time-to-move", f"time_to_max_favor={row.get('time_to_max_favor_minutes','')}m | time_to_max_adverse={row.get('time_to_max_adverse_minutes','')}m"),
            ("Regret filter", f"regret_valid={row.get('regret_valid_YN','')} | reason={row.get('regret_filter_reason','')}"),
            ("Conclusion", f"{row.get('outcome_conclusion_code','')} | notes={row.get('post_close_outcome_notes','')}"),
        ])

        doc.add_paragraph("Review placeholders", style="CaseTitle")
        doc.add_paragraph(
            "human_review_status=PENDING | verdict_code= | root_cause_code= | action_candidate_code=",
            style="MetaText",
        )

    doc.save(out_path)
    print(f"[review_case builder] wrote {out_path}")


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--date", required=True)
    p.add_argument("--workspace", required=True)
    p.add_argument("--pending", required=True)
    p.add_argument("--signals", required=False, default="")
    p.add_argument("--results", required=False, default="")
    p.add_argument("--snapshot-index", required=False, default="")
    p.add_argument("--out-dir", required=True)
    p.add_argument("--tz", required=False, default="UTC")
    p.add_argument("--fallback-hours", required=False, default="4")
    return p.parse_args()


if __name__ == "__main__":
    build_doc(parse_args())
