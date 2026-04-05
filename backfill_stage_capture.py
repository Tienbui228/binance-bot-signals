#!/usr/bin/env python3
"""
backfill_stage_capture.py  (v2)
--------------------------------
Fixes pre_pending and pending_open stage coverage for historical cases.

Root cause (confirmed):
  save_pending() had wrong method name (_register_pending_case — Sprint 3A.2 bug).
  This caused AttributeError, so _review_register_pending_case was never called.

  BUT: later when a case was confirmed/closed, _capture_and_register_case_stage
  called ensure_case() internally, creating case_meta with DEFAULT stage values:
    pre_pending  = "missing_unexpected"  <- default, never updated
    pending_open = "missing_unexpected"  <- default, never updated

  So case_meta EXISTS for all 45 cases, but stages are still wrong.
  The first backfill run skipped these because case_meta already existed.

This v2 fixes existing case_metas where pre_pending/pending_open = missing_unexpected,
setting them to capture_failed (accurate: snapshot was not taken, not an engine bug).

Usage:
  # Dry run first
  python3 backfill_stage_capture.py --update-existing --dry-run

  # Run for real
  python3 backfill_stage_capture.py --update-existing

  # Also create case_meta for any row with no file at all
  python3 backfill_stage_capture.py --update-existing --create-missing
"""
from __future__ import annotations
import argparse, csv, json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

STAGES_TO_FIX = ("pre_pending", "pending_open")


def safe_int(v, d=0):
    try:
        return int(float(v)) if v not in (None,"","NA") else d
    except Exception:
        return d

def yn(v): return "Y" if str(v or "").strip().upper()=="Y" else "N"

def owner_day(ts_ms, tz):
    if not ts_ms: return ""
    return datetime.fromtimestamp(ts_ms/1000.,tz=timezone.utc).astimezone(tz).strftime("%Y-%m-%d")

def fmt_local(ts_ms, tz):
    if not ts_ms: return ""
    return datetime.fromtimestamp(ts_ms/1000.,tz=timezone.utc).astimezone(tz).strftime("%Y-%m-%d %H:%M:%S")

def read_pending(pending_dir: Path) -> List[Dict]:
    rows=[]
    for f in sorted(pending_dir.glob("pending_*.csv")):
        try:
            with open(f,newline="",encoding="utf-8-sig") as fh:
                rows.extend(csv.DictReader(fh))
        except Exception as e:
            print(f"  [warn] {f.name}: {e}")
    return rows

def meta_path(workspace: Path, case_day: str, case_id: str) -> Path:
    return workspace/"cases"/case_day/case_id/"case_meta.json"

def capture_failed(note="pre_fix_backfill_v2"):
    return {"stage_status":"capture_failed","stage_content_type":"none","image_path":"","note":note}

def not_applicable():
    return {"stage_status":"not_applicable","stage_content_type":"none","image_path":"","note":"not_applicable"}

def not_reached():
    return {"stage_status":"not_reached_yet","stage_content_type":"none","image_path":"","note":"not_reached_yet","case_close_type":"not_due_yet"}


def fix_existing(path: Path, dry_run: bool, stats: Dict):
    """Update pre_pending/pending_open missing_unexpected → capture_failed."""
    try:
        meta = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        stats["errors"]+=1; return
    stages = meta.get("stages",{})
    changed = False
    for stage in STAGES_TO_FIX:
        if stages.get(stage,{}).get("stage_status")=="missing_unexpected":
            if not dry_run:
                stages[stage] = capture_failed("pre_fix_backfill_v2 — was missing_unexpected, "
                                               "snapshot was never taken (Sprint 3A.2 bug)")
            changed=True; stats["stages"]+=1
    if changed:
        stats["updated"]+=1
        if dry_run:
            print(f"  [dry] would fix: {path.parent.name}")
        else:
            meta["stages"]=stages
            path.write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding="utf-8")


def infer_close_type(row, fallback_hours):
    e=(row.get("case_close_type") or "").strip()
    if e: return e
    s=(row.get("status") or "").upper()
    if s in {"INVALIDATED","EXPIRED_WAIT"}: return "true_close"
    c=safe_int(row.get("created_ts_ms"))
    if c and s=="PENDING":
        due=c+fallback_hours*3600*1000
        if int(datetime.now(tz=timezone.utc).timestamp()*1000)>=due:
            return "fallback_4h_snapshot"
    return "not_due_yet"


def build_new_meta(row, tz, fallback_hours):
    cid=row.get("pending_id") or row.get("setup_id") or ""
    cts=safe_int(row.get("created_ts_ms"))
    sms=safe_int(row.get("signal_open_time"))
    day=owner_day(cts,tz)
    status=(row.get("status") or "PENDING").upper()
    is_conf=yn(row.get("is_confirmed"))=="Y"
    close_type=infer_close_type(row,fallback_hours)
    stages={
        "pre_pending":  capture_failed("pre_fix_backfill_v2 — new"),
        "pending_open": capture_failed("pre_fix_backfill_v2 — new"),
        "entry_or_confirm": capture_failed("pre_fix_backfill_v2") if is_conf else not_applicable(),
        "case_close": (
            {**capture_failed("pre_fix_backfill_v2"),"case_close_type":close_type}
            if close_type in("true_close","fallback_4h_snapshot") else
            {**not_reached(),"case_close_type":"not_due_yet"}
        ),
    }
    return {
        "case_id":cid,"symbol":row.get("symbol",""),"side":row.get("side",""),
        "strategy":row.get("strategy",""),"case_day":day,
        "signal_time_local":fmt_local(sms,tz),"created_time_local":fmt_local(cts,tz),
        "confirmed_time_local":fmt_local(safe_int(row.get("confirmed_ts_ms")),tz),
        "sent_time_local":fmt_local(safe_int(row.get("sent_ts_ms")),tz),
        "close_time_local":fmt_local(safe_int(row.get("closed_ts_ms")),tz) if close_type!="not_due_yet" else "",
        "fallback_close_due_time_local":fmt_local(cts+fallback_hours*3600000,tz) if cts else "",
        "status_final":status,"close_reason":str(row.get("close_reason") or ""),
        "is_confirmed":"Y" if is_conf else "N","is_sent_signal":yn(row.get("is_sent_signal")),
        "case_close_type":close_type,"slot_bundle_complete":"Y",
        "evidence_ready_for_review":"none","lifecycle_complete":"N",
        "human_review_status":"PENDING","verdict_code":"","root_cause_code":"",
        "action_candidate_code":"","review_notes_short":"pre_fix_backfill_v2",
        "has_case_close_image":"N","stages":stages,
    }


def run(pending_dir, workspace, tz_name, fallback_hours,
        update_existing, create_missing, dry_run):
    tz=ZoneInfo(tz_name)
    print(f"[backfill v2] pending_dir     : {pending_dir}")
    print(f"[backfill v2] workspace       : {workspace}")
    print(f"[backfill v2] update_existing : {update_existing}")
    print(f"[backfill v2] create_missing  : {create_missing}")
    print(f"[backfill v2] dry_run         : {dry_run}")
    print()

    rows=read_pending(pending_dir)
    print(f"[backfill v2] total rows: {len(rows)}")

    stats={"updated":0,"stages":0,"created":0,"skipped":0,"no_id":0,"errors":0}

    for row in rows:
        cid=row.get("pending_id") or row.get("setup_id") or ""
        cts=safe_int(row.get("created_ts_ms"))
        if not cid or not cts:
            stats["no_id"]+=1; continue
        day=owner_day(cts,tz)
        if not day:
            stats["no_id"]+=1; continue
        mp=meta_path(workspace,day,cid)
        if mp.exists():
            if update_existing:
                fix_existing(mp,dry_run,stats)
            else:
                stats["skipped"]+=1
        else:
            if create_missing:
                if dry_run:
                    print(f"  [dry] would create: {mp}")
                    stats["created"]+=1
                else:
                    try:
                        mp.parent.mkdir(parents=True,exist_ok=True)
                        mp.write_text(json.dumps(build_new_meta(row,tz,fallback_hours),
                                                  ensure_ascii=False,indent=2),encoding="utf-8")
                        stats["created"]+=1
                    except Exception as e:
                        print(f"  [error] create {cid}: {e}")
            else:
                stats["skipped"]+=1

    print()
    print("[backfill v2] Results:")
    print(f"  cases updated (stages fixed) : {stats['updated']}")
    print(f"  stages fixed (missing→failed): {stats['stages']}")
    print(f"  cases created (new meta)     : {stats['created']}")
    print(f"  skipped                      : {stats['skipped']}")
    print(f"  no_id / no_date              : {stats['no_id']}")
    if stats["errors"]: print(f"  errors: {stats['errors']}")
    print()
    if dry_run:
        print("[backfill v2] Dry run — no files written.")
    else:
        print("[backfill v2] Done. Rebuild report to verify.")
        print("  Expected: missing_unexpected pre_pending/pending_open = 0")


def parse_args():
    p=argparse.ArgumentParser()
    p.add_argument("--pending-dir",     default="data/pending")
    p.add_argument("--workspace",       default="review_workspace")
    p.add_argument("--tz",              default="Asia/Ho_Chi_Minh")
    p.add_argument("--fallback-hours",  type=int, default=4)
    p.add_argument("--update-existing", action="store_true",
                   help="Fix existing case_metas with missing_unexpected stages")
    p.add_argument("--create-missing",  action="store_true",
                   help="Create case_meta for rows with no file at all")
    p.add_argument("--dry-run",         action="store_true")
    return p.parse_args()

if __name__=="__main__":
    args=parse_args()
    if not args.update_existing and not args.create_missing:
        print("Nothing to do — use --update-existing and/or --create-missing")
        print("Example: python3 backfill_stage_capture.py --update-existing --create-missing")
    else:
        run(Path(args.pending_dir),Path(args.workspace),args.tz,
            args.fallback_hours,args.update_existing,args.create_missing,args.dry_run)
