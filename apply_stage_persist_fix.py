#!/usr/bin/env python3
"""
apply_stage_persist_fix.py
---------------------------
Fixes ongoing pre_pending/pending_open capture/persist coverage in oi_scanner.py.

Root cause:
  _capture_and_register_case_stage() swallows ALL exceptions.
  If register_stage_image() raises or fails, the stage stays "missing_unexpected"
  and _review_register_pending_case() never notices.

Fix:
  After calling _capture_and_register_case_stage(), check if the stage was actually
  updated in case_meta. If it's still "missing_unexpected", explicitly call
  register_stage_image(None) to force it to "capture_failed".

  This guarantees: pre_pending and pending_open are NEVER left as missing_unexpected
  after save_pending() runs, regardless of whether snapshot capture succeeds.

Run from project root:
  python3 apply_stage_persist_fix.py
"""
from pathlib import Path
import shutil

TARGET = Path("oi_scanner.py")
BACKUP = Path("oi_scanner.py.bak_stagepersist")

OLD = '''    def _review_register_pending_case(self, pending_row: Dict):
        if not self.review_runtime:
            return
        try:
            rec = self.review_runtime.ensure_case(pending_row)
            bar_ms = self._bar_interval_ms_for_strategy(pending_row.get("strategy", ""))
            pre_ts = max(int(pending_row.get("signal_open_time") or 0) - bar_ms, 0)
            self._capture_and_register_case_stage(pending_row, "pre_pending", pre_ts, note="pre_pending context")
            self._capture_and_register_case_stage(pending_row, "pending_open", int(pending_row.get("signal_open_time") or 0), note=pending_row.get("reason", "pending_open"))
        except Exception as e:
            print(f"[review_case warn] register_pending_case {pending_row.get('pending_id','')}: {e}")'''

NEW = '''    def _review_register_pending_case(self, pending_row: Dict):
        if not self.review_runtime:
            return
        try:
            self.review_runtime.ensure_case(pending_row)
            case_day = self._review_case_day(int(pending_row.get("created_ts_ms") or 0))
            case_id  = pending_row.get("pending_id") or pending_row.get("setup_id", "")
            bar_ms   = self._bar_interval_ms_for_strategy(pending_row.get("strategy", ""))
            pre_ts   = max(int(pending_row.get("signal_open_time") or 0) - bar_ms, 0)
            pend_ts  = int(pending_row.get("signal_open_time") or 0)
            for stage, ts, note in [
                ("pre_pending",  pre_ts,  "pre_pending context"),
                ("pending_open", pend_ts, pending_row.get("reason", "pending_open")),
            ]:
                # Attempt snapshot capture + stage registration
                self._capture_and_register_case_stage(pending_row, stage, ts, note=note)
                # Stage persist guarantee: _capture_and_register_case_stage swallows
                # all exceptions. If stage is still "missing_unexpected", force it to
                # "capture_failed" so the report never shows a false missing_unexpected.
                try:
                    case_dict = self.review_runtime._load_case_dict(case_day, case_id)
                    if case_dict:
                        current = case_dict.get("stages", {}).get(stage, {}).get("stage_status", "")
                        if current == "missing_unexpected":
                            self.review_runtime.register_stage_image(
                                case_day, case_id, stage, None,
                                note=f"{note} — capture_failed_fallback",
                            )
                except Exception:
                    pass
        except Exception as e:
            print(f"[review_case warn] register_pending_case {pending_row.get('pending_id','')}: {e}")'''


def main():
    if not TARGET.exists():
        print(f"ERROR: {TARGET} not found. Run from project root.")
        return

    content = TARGET.read_text(encoding="utf-8")

    count = content.count(OLD)
    if count == 0:
        print(f"[stage persist fix] Target not found — already applied or file differs.")
        return
    if count > 1:
        print(f"[stage persist fix] ERROR: target found {count} times — ambiguous. Aborting.")
        return

    shutil.copy2(TARGET, BACKUP)
    print(f"[stage persist fix] Backup: {BACKUP}")

    content = content.replace(OLD, NEW, 1)
    TARGET.write_text(content, encoding="utf-8")
    print(f"[stage persist fix] Patched: {TARGET}")
    print()
    print("Next steps:")
    print("  1. Restart scanner")
    print("  2. Wait 2-3 rounds")
    print("  3. Check logs for '[review_case warn]' to confirm no more errors")
    print("  4. Run: python3 backfill_stage_capture.py --update-existing --create-missing")
    print("  5. Rebuild report and verify missing_unexpected counts = 0")


if __name__ == "__main__":
    main()
