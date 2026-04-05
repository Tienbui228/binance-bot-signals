#!/usr/bin/env python3
"""
run_daily_export.py
--------------------
Phase 4: Auto daily export for Daily Review Report V2.

DESIGN RULE: This script is an EXTERNAL runner.
It must never be imported or called from inside oi_scanner.py.
The scanner keeps running regardless of whether this script succeeds or fails.

What it does:
  - Reads config.yaml to get all paths (workspace, pending_dir, tz, etc.)
  - Calls build_daily_review_pack.py for today's date (or --date if given)
  - Writes one DOCX per day: daily_review_YYYY-MM-DD.docx
  - Overwrites if report for same day already exists (idempotent)
  - Exits 0 on success, non-zero on failure

Usage:
  # Run for today
  python3 run_daily_export.py

  # Run for a specific date
  python3 run_daily_export.py --date 2026-04-04

  # Use a different config
  python3 run_daily_export.py config.yaml

  # Dry run — show what would happen, do not build
  python3 run_daily_export.py --dry-run

Cron example (23:30 local time every day):
  30 23 * * * cd /path/to/binance_bot_signals && python3 run_daily_export.py >> logs/daily_export.log 2>&1

Systemd timer: see comments at bottom of this file.

Exit codes:
  0  success — DOCX written
  1  build failed (see log output for details)
  2  configuration error (review_case_system disabled, missing files, etc.)
  3  timeout

Deferred validation note:
  pre_pending and pending_open live capture coverage is not yet proven.
  Backfill has set historical cases to capture_failed (honest).
  Confirm fresh post-patch cases before claiming full stage coverage.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

try:
    import yaml
except ImportError:
    print("[daily_export] ERROR: pyyaml not installed — pip install pyyaml")
    sys.exit(2)

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

BUILD_TIMEOUT_SECONDS = 300


# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------

def load_config(config_path: Path) -> dict:
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def resolve_paths(config_path: Path, cfg: dict, case_day: str) -> dict:
    """Resolve all file paths needed by build_daily_review_pack.py."""
    project_dir = config_path.resolve().parent
    storage_cfg = cfg.get("storage", {})
    review_cfg  = cfg.get("review_case_system", {})
    snap_cfg    = cfg.get("review_snapshots", {})

    data_dir     = project_dir / str(storage_cfg.get("data_dir",    "data"))
    pending_dir  = data_dir    / str(storage_cfg.get("pending_dir",  "pending"))
    signals_dir  = data_dir    / str(storage_cfg.get("signals_dir",  "signals"))
    results_dir  = data_dir    / str(storage_cfg.get("results_dir",  "results"))

    month = case_day[:7]   # YYYY-MM

    return {
        "project_dir":     project_dir,
        "pending_path":    pending_dir  / f"pending_{case_day}.csv",
        "signals_path":    signals_dir  / f"signals_{month}.csv",
        "results_path":    results_dir  / f"results_{month}.csv",
        "snapshot_index":  project_dir  / str(snap_cfg.get("index_file", "review_snapshots.csv")),
        "workspace":       project_dir  / str(review_cfg.get("workspace_dir", "review_workspace")),
        "out_dir":         project_dir  / str(review_cfg.get("daily_exports_dir", "review_exports")),
        "builder_script":  project_dir  / str(review_cfg.get("builder_script", "build_daily_review_pack.py")),
        "tz_name":         str(review_cfg.get("timezone", "Asia/Ho_Chi_Minh")),
        "fallback_hours":  int(review_cfg.get("fallback_close_hours", 4)),
        "enabled":         bool(review_cfg.get("enabled", False)),
    }


# ---------------------------------------------------------------------------
# Export runner
# ---------------------------------------------------------------------------

def run_export(
    config_path: Path,
    case_day: str,
    dry_run: bool = False,
    verbose: bool = False,
) -> int:
    """
    Build the daily report for case_day.
    Returns exit code: 0=ok, 1=build failed, 2=config error, 3=timeout.
    """
    generated_at = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[daily_export] date={case_day}  generated_at={generated_at}")

    # Load config
    if not config_path.exists():
        print(f"[daily_export] ERROR: config not found: {config_path}")
        return 2
    try:
        cfg = load_config(config_path)
    except Exception as e:
        print(f"[daily_export] ERROR: could not load config: {e}")
        return 2

    paths = resolve_paths(config_path, cfg, case_day)

    # Check review_case_system enabled
    if not paths["enabled"]:
        print(f"[daily_export] review_case_system.enabled=false — nothing to export")
        return 0

    # Check builder script exists
    builder = paths["builder_script"]
    if not builder.exists():
        print(f"[daily_export] ERROR: builder script not found: {builder}")
        return 2

    # Check pending file exists
    pending = paths["pending_path"]
    if not pending.exists():
        print(f"[daily_export] WARN: pending file not found: {pending}")
        print(f"[daily_export] Will build report with 0 rows for {case_day}")
        # Do not abort — builder handles empty gracefully

    # Build command
    cmd = [
        sys.executable,
        str(builder),
        "--date",           case_day,
        "--workspace",      str(paths["workspace"]),
        "--pending",        str(pending),
        "--signals",        str(paths["signals_path"]),
        "--results",        str(paths["results_path"]),
        "--snapshot-index", str(paths["snapshot_index"]),
        "--out-dir",        str(paths["out_dir"]),
        "--tz",             paths["tz_name"],
        "--fallback-hours", str(paths["fallback_hours"]),
    ]

    if verbose or dry_run:
        print(f"[daily_export] cmd: {' '.join(cmd)}")

    if dry_run:
        print(f"[daily_export] dry-run — not building")
        out_file = paths["out_dir"] / f"daily_review_{case_day}.docx"
        print(f"[daily_export] would write: {out_file}")
        return 0

    # Run builder
    t0 = time.time()
    try:
        result = subprocess.run(
            cmd,
            cwd=str(paths["project_dir"]),
            capture_output=True,
            text=True,
            timeout=BUILD_TIMEOUT_SECONDS,
        )
        elapsed = time.time() - t0

        # Print builder output
        if result.stdout:
            for line in result.stdout.strip().splitlines():
                print(f"[daily_export] builder: {line}")
        if result.stderr:
            for line in result.stderr.strip().splitlines():
                print(f"[daily_export] builder stderr: {line}")

        if result.returncode != 0:
            print(f"[daily_export] FAILED exit={result.returncode} elapsed={elapsed:.1f}s")
            return 1

        # Verify output file was written
        out_file = paths["out_dir"] / f"daily_review_{case_day}.docx"
        if not out_file.exists():
            print(f"[daily_export] ERROR: builder exited 0 but output file not found: {out_file}")
            return 1

        size_kb = out_file.stat().st_size / 1024
        print(f"[daily_export] OK: {out_file}  ({size_kb:.0f} KB)  elapsed={elapsed:.1f}s")
        return 0

    except subprocess.TimeoutExpired:
        print(f"[daily_export] TIMEOUT after {BUILD_TIMEOUT_SECONDS}s — builder killed")
        return 3

    except Exception as e:
        print(f"[daily_export] ERROR: {e}")
        return 1


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Daily Review Report V2 — auto export runner (Phase 4)."
    )
    parser.add_argument(
        "config_path",
        nargs="?",
        default="config.yaml",
        help="Path to config.yaml (default: config.yaml in current directory)",
    )
    parser.add_argument(
        "--date",
        default="",
        help="Report date YYYY-MM-DD (default: today in configured timezone)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would happen without building the report",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print full build command",
    )

    args = parser.parse_args()
    config_path = Path(args.config_path)

    # Resolve case_day
    if args.date:
        case_day = args.date
    else:
        # Use configured timezone for "today"
        try:
            cfg = load_config(config_path)
            tz_name = cfg.get("review_case_system", {}).get("timezone", "Asia/Ho_Chi_Minh")
            tz = ZoneInfo(tz_name)
        except Exception:
            tz = ZoneInfo("Asia/Ho_Chi_Minh")
        case_day = datetime.now(tz=timezone.utc).astimezone(tz).strftime("%Y-%m-%d")

    exit_code = run_export(
        config_path=config_path,
        case_day=case_day,
        dry_run=args.dry_run,
        verbose=args.verbose,
    )
    sys.exit(exit_code)


if __name__ == "__main__":
    main()


# ---------------------------------------------------------------------------
# Systemd timer setup (copy-paste reference)
# ---------------------------------------------------------------------------
#
# File: /etc/systemd/system/binance_daily_export.service
# --------------------------------------------------------
# [Unit]
# Description=Binance Bot Daily Review Export
# After=network.target
#
# [Service]
# Type=oneshot
# User=YOUR_USER
# WorkingDirectory=/path/to/binance_bot_signals
# ExecStart=/usr/bin/python3 run_daily_export.py
# StandardOutput=append:/path/to/binance_bot_signals/logs/daily_export.log
# StandardError=append:/path/to/binance_bot_signals/logs/daily_export.log
#
# File: /etc/systemd/system/binance_daily_export.timer
# -------------------------------------------------------
# [Unit]
# Description=Run Binance daily review export at 23:30 local time
#
# [Timer]
# OnCalendar=*-*-* 23:30:00
# Persistent=true
#
# [Install]
# WantedBy=timers.target
#
# Enable:
#   systemctl daemon-reload
#   systemctl enable --now binance_daily_export.timer
#   systemctl list-timers binance_daily_export.timer
