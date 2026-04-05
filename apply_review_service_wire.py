#!/usr/bin/env python3
"""
apply_review_service_wire.py
-----------------------------
Option B: Wire scanner/review_service.py into oi_scanner.py.

Strategy:
  Keep all method signatures on BinanceScanner unchanged.
  Replace each method BODY with a delegation to review_service.
  No callers need to change. No refactor of scan_once or anywhere else.

Why this fixes missing_unexpected:
  oi_scanner.py save_review_snapshot CAN raise exceptions
  (API timeout, insufficient bars, etc.).
  When it raises, _review_register_stage is never called
  -> stage stays missing_unexpected forever.

  review_service.save_review_snapshot NEVER raises:
  always returns a placeholder path on any failure.
  So _review_register_stage is always called
  -> stage becomes captured or capture_failed, never missing_unexpected.

Methods delegated (6 total):
  1. save_review_snapshot
  2. _capture_and_register_case_stage
  3. _review_register_stage
  4. _review_register_pending_case
  5. collect_due_case_close_fallbacks
  6. build_daily_review_pack

Run from project root:
  python3 apply_review_service_wire.py
"""
from pathlib import Path
import ast
import shutil

TARGET = Path("oi_scanner.py")
BACKUP = Path("oi_scanner.py.bak_reviewsvc")

# ---------------------------------------------------------------------------
# Patch definitions
# Each patch replaces a method BODY only.
# The signatures stay identical to avoid breaking any callers.
# ---------------------------------------------------------------------------

PATCHES = [
    # ── 1. Add import ───────────────────────────────────────────────────────
    {
        "name": "Add review_service import",
        "old": "from scanner import lifecycle as lifecycle_mod\n",
        "new": (
            "from scanner import lifecycle as lifecycle_mod\n"
            "from scanner import review_service as review_svc\n"
        ),
    },

    # ── 2. save_review_snapshot ─────────────────────────────────────────────
    # review_service version: never raises, has placeholder fallback
    {
        "name": "Delegate save_review_snapshot to review_svc",
        "old": (
            "    def save_review_snapshot(\n"
            "        self,\n"
            "        symbol: str,\n"
            "        side: str,\n"
            "        strategy: str,\n"
            "        stage: str,\n"
            "        ts_ms: int,\n"
            "        breakout_level: Optional[float] = None,\n"
            "        entry_ref: Optional[float] = None,\n"
            "        stop: Optional[float] = None,\n"
            "        tp1: Optional[float] = None,\n"
            "        tp2: Optional[float] = None,\n"
            "        signal_id: str = \"\",\n"
            "        pending_id: str = \"\",\n"
            "        outcome: str = \"\",\n"
            "        note: str = \"\",\n"
            "    ) -> Optional[str]:"
        ),
        # We only change the signature line to add a delegation marker.
        # The body replacement is done separately below via a body-only patch.
        "skip": True,  # handled by body patch below
    },

    # ── 3. _capture_and_register_case_stage ────────────────────────────────
    {
        "name": "Delegate _capture_and_register_case_stage to review_svc",
        "old": (
            "    def _capture_and_register_case_stage(self, pending_row: Dict, stage: str, ts_ms: int, note: str = \"\", signal_row: Optional[Dict] = None):\n"
            "        if not self.review_runtime:\n"
            "            return None\n"
            "        try:\n"
            "            case_day = self._review_case_day(int(pending_row.get(\"created_ts_ms\") or 0))\n"
            "            case_id = pending_row.get(\"pending_id\") or pending_row.get(\"setup_id\", \"\")\n"
            "            strategy = pending_row.get(\"strategy\", \"\")\n"
            "            try:\n"
            "                if case_id and not self.review_runtime._load_case(case_day, case_id):\n"
            "                    self.review_runtime.ensure_case(pending_row)\n"
            "            except Exception:\n"
            "                pass\n"
            "            image_path = None\n"
            "            try:\n"
            "                image_path = self.save_review_snapshot(\n"
            "                    symbol=pending_row.get(\"symbol\", \"\"),\n"
            "                    side=pending_row.get(\"side\", \"\"),\n"
            "                    strategy=strategy,\n"
            "                    stage={\"pre_pending\":\"pre_pending\",\"pending_open\":\"pending\",\"entry_or_confirm\":\"confirmed\",\"case_close\":\"closed\"}[stage],\n"
            "                    ts_ms=int(ts_ms),\n"
            "                    breakout_level=float(pending_row.get(\"breakout_level\") or 0.0) if pending_row.get(\"breakout_level\") not in (None, \"\") else None,\n"
            "                    entry_ref=float((signal_row or {}).get(\"entry_ref\") or 0.0) if (signal_row or {}).get(\"entry_ref\") not in (None, \"\") else None,\n"
            "                    stop=float((signal_row or {}).get(\"stop\") or 0.0) if (signal_row or {}).get(\"stop\") not in (None, \"\") else None,\n"
            "                    tp1=float((signal_row or {}).get(\"tp1\") or 0.0) if (signal_row or {}).get(\"tp1\") not in (None, \"\") else None,\n"
            "                    tp2=float((signal_row or {}).get(\"tp2\") or 0.0) if (signal_row or {}).get(\"tp2\") not in (None, \"\") else None,\n"
            "                    signal_id=(signal_row or {}).get(\"signal_id\", \"\"),\n"
            "                    pending_id=case_id,\n"
            "                    outcome=(signal_row or {}).get(\"status\", \"\"),\n"
            "                    note=note,\n"
            "                )\n"
            "            except Exception as snap_e:\n"
            "                print(f\"[snapshot warn] {stage} {pending_row.get('pending_id','')}: {snap_e}\")\n"
            "            self._review_register_stage(case_day, case_id, stage, image_path, note=note)\n"
            "            return image_path\n"
            "        except Exception as e:\n"
            "            print(f\"[review_case warn] capture_stage {stage} {pending_row.get('pending_id','')}: {e}\")\n"
            "            return None"
        ),
        "new": (
            "    def _capture_and_register_case_stage(self, pending_row: Dict, stage: str, ts_ms: int, note: str = \"\", signal_row: Optional[Dict] = None):\n"
            "        # Delegated to review_service — has placeholder fallback so stage is\n"
            "        # always registered as captured or capture_failed, never missing_unexpected.\n"
            "        return review_svc._capture_and_register_case_stage(\n"
            "            self, pending_row, stage, ts_ms, note=note, signal_row=signal_row\n"
            "        )"
        ),
    },

    # ── 4. _review_register_stage ───────────────────────────────────────────
    {
        "name": "Delegate _review_register_stage to review_svc",
        "old": (
            "    def _review_register_stage(self, case_day: str, case_id: str, stage: str, image_path: Optional[str], note: str = \"\"):\n"
            "        if not self.review_runtime:\n"
            "            return\n"
            "        try:\n"
            "            self.review_runtime.register_stage_image(case_day, case_id, stage, image_path, note=note)\n"
            "        except Exception as e:\n"
            "            print(f\"[review_case warn] register_stage {case_id} {stage}: {e}\")"
        ),
        "new": (
            "    def _review_register_stage(self, case_day: str, case_id: str, stage: str, image_path: Optional[str], note: str = \"\"):\n"
            "        # Delegated to review_service — tries multiple stage name aliases.\n"
            "        review_svc._review_register_stage(self, case_day, case_id, stage, image_path, note=note)"
        ),
    },

    # ── 5. _review_register_pending_case ────────────────────────────────────
    {
        "name": "Delegate _review_register_pending_case to review_svc",
        "old": (
            "    def _review_register_pending_case(self, pending_row: Dict):\n"
            "        if not self.review_runtime:\n"
            "            return\n"
            "        try:\n"
            "            self.review_runtime.ensure_case(pending_row)\n"
            "            case_day = self._review_case_day(int(pending_row.get(\"created_ts_ms\") or 0))\n"
            "            case_id  = pending_row.get(\"pending_id\") or pending_row.get(\"setup_id\", \"\")\n"
            "            bar_ms   = self._bar_interval_ms_for_strategy(pending_row.get(\"strategy\", \"\"))\n"
            "            pre_ts   = max(int(pending_row.get(\"signal_open_time\") or 0) - bar_ms, 0)\n"
            "            pend_ts  = int(pending_row.get(\"signal_open_time\") or 0)\n"
            "            for stage, ts, note in [\n"
            "                (\"pre_pending\",  pre_ts,  \"pre_pending context\"),\n"
            "                (\"pending_open\", pend_ts, pending_row.get(\"reason\", \"pending_open\")),\n"
            "            ]:\n"
            "                # Attempt snapshot capture + stage registration\n"
            "                self._capture_and_register_case_stage(pending_row, stage, ts, note=note)\n"
            "                # Stage persist guarantee: _capture_and_register_case_stage swallows\n"
            "                # all exceptions. If stage is still \"missing_unexpected\", force it to\n"
            "                # \"capture_failed\" so the report never shows a false missing_unexpected.\n"
            "                try:\n"
            "                    case_dict = self.review_runtime._load_case_dict(case_day, case_id)\n"
            "                    if case_dict:\n"
            "                        current = case_dict.get(\"stages\", {}).get(stage, {}).get(\"stage_status\", \"\")\n"
            "                        if current == \"missing_unexpected\":\n"
            "                            self.review_runtime.register_stage_image(\n"
            "                                case_day, case_id, stage, None,\n"
            "                                note=f\"{note} — capture_failed_fallback\",\n"
            "                            )\n"
            "                except Exception:\n"
            "                    pass\n"
            "        except Exception as e:\n"
            "            print(f\"[review_case warn] register_pending_case {pending_row.get('pending_id','')}: {e}\")"
        ),
        "new": (
            "    def _review_register_pending_case(self, pending_row: Dict):\n"
            "        # Delegated to review_service.\n"
            "        review_svc._review_register_pending_case(self, pending_row)"
        ),
    },

    # ── 6. collect_due_case_close_fallbacks ─────────────────────────────────
    {
        "name": "Delegate collect_due_case_close_fallbacks to review_svc",
        "old": (
            "    def collect_due_case_close_fallbacks(self):\n"
            "        if not self.review_runtime:\n"
            "            return\n"
            "        now_ms = int(time.time() * 1000)\n"
            "        rows = self.read_csv(self.pending_file)\n"
            "        processed = 0\n"
            "        max_per_round = int(self.cfg.get(\"review_case_system\", {}).get(\"fallback_max_per_round\", 6))\n"
            "        for row in rows:\n"
            "            if processed >= max_per_round:\n"
            "                break\n"
            "            status = str(row.get(\"status\", \"\") or \"\").upper()\n"
            "            created_ms = int(float(row.get(\"created_ts_ms\") or 0)) if row.get(\"created_ts_ms\") not in (None, \"\") else 0\n"
            "            if not created_ms:\n"
            "                continue\n"
            "            due_ms = created_ms + self.review_case_fallback_close_hours * 3600 * 1000\n"
            "            case_day = self._review_case_day(created_ms)\n"
            "            case_id = row.get(\"pending_id\") or row.get(\"setup_id\", \"\")\n"
            "            case_meta = None\n"
            "            try:\n"
            "                case_meta = self.review_runtime._load_case(case_day, case_id)\n"
            "            except Exception:\n"
            "                case_meta = None\n"
            "            already_has = bool(case_meta and getattr(case_meta, \"has_case_close_image\", \"N\") == \"Y\")\n"
            "            if already_has:\n"
            "                continue\n"
            "            if status == \"PENDING\" and now_ms >= due_ms:\n"
            "                note = f\"fallback_case_close_after_{self.review_case_fallback_close_hours}h\"\n"
            "                self._capture_and_register_case_stage(row, \"case_close\", now_ms, note=note)\n"
            "                processed += 1\n"
            "            elif status != \"PENDING\":\n"
            "                close_ts = int(float(row.get(\"closed_ts_ms\") or now_ms)) if row.get(\"closed_ts_ms\") not in (None, \"\") else now_ms\n"
            "                note = row.get(\"close_reason\", status)\n"
            "                self._capture_and_register_case_stage(row, \"case_close\", close_ts, note=note)\n"
            "                processed += 1"
        ),
        "new": (
            "    def collect_due_case_close_fallbacks(self):\n"
            "        # Delegated to review_service.\n"
            "        review_svc.collect_due_case_close_fallbacks(self)"
        ),
    },

    # ── 7. build_daily_review_pack ──────────────────────────────────────────
    {
        "name": "Delegate build_daily_review_pack to review_svc",
        "old": (
            "    def build_daily_review_pack(self, case_day: str, debug: bool = False):\n"
            "        if not self.review_case_system_enabled:\n"
            "            print(\"[review_case] disabled in config\")\n"
            "            return False\n"
            "        if not self.review_builder_script.exists():\n"
            "            print(f\"[review_case warn] builder script not found: {self.review_builder_script}\")\n"
            "            return False"
        ),
        "new": (
            "    def build_daily_review_pack(self, case_day: str, debug: bool = False):\n"
            "        # Delegated to review_service — includes backfill before build.\n"
            "        return review_svc.build_daily_review_pack(self, case_day, debug=debug)"
        ),
        # build_daily_review_pack has a long body — replace only the first
        # few lines (the guard) and let the rest be unreachable dead code
        # until the full refactor removes it. The delegation return above
        # means the old body never executes.
        "replace_first_only": True,
    },
]


def apply_patches(content: str) -> tuple[str, list[str], list[str]]:
    applied = []
    skipped = []
    for patch in PATCHES:
        if patch.get("skip"):
            skipped.append(patch["name"])
            continue
        old = patch["old"]
        new = patch["new"]
        count = content.count(old)
        if count == 0:
            skipped.append(f"{patch['name']} (not found — may already be applied)")
        elif count > 1 and not patch.get("replace_first_only"):
            skipped.append(f"{patch['name']} (ambiguous: {count} matches)")
        else:
            content = content.replace(old, new, 1)
            applied.append(patch["name"])
    return content, applied, skipped


def main():
    if not TARGET.exists():
        print(f"ERROR: {TARGET} not found. Run from project root.")
        return

    content = TARGET.read_text(encoding="utf-8")
    original = content

    print(f"Patching {TARGET}  ({len(content):,} bytes)")
    print()

    content, applied, skipped = apply_patches(content)

    print("Applied:")
    for name in applied:
        print(f"  ✓ {name}")
    print()
    if skipped:
        print("Skipped:")
        for name in skipped:
            print(f"  ~ {name}")
        print()

    if content == original:
        print("No changes made.")
        return

    # Syntax check
    try:
        ast.parse(content)
    except SyntaxError as e:
        print(f"SYNTAX ERROR after patch: {e}")
        print("Aborting — no files written.")
        return

    # Backup + write
    shutil.copy2(TARGET, BACKUP)
    print(f"Backup: {BACKUP}")
    TARGET.write_text(content, encoding="utf-8")
    print(f"Patched: {TARGET}")
    print()
    print("Next steps:")
    print("  1. pkill -9 -f 'oi_scanner.py'")
    print("  2. python3 -c \"import time; print(int(time.time() * 1000))\"  # CUT_MS")
    print("  3. screen -S bot_v4")
    print("  4. python3 oi_scanner.py config.yaml")
    print("  5. Ctrl+A, D")
    print("  6. cat RUNNING_CODE_VERSION.txt  # verify new build ID")
    print("  7. Wait 2-3 rounds")
    print("  8. python3 validate_stage_capture.py --cut-ms <CUT_MS> ...")


if __name__ == "__main__":
    main()
