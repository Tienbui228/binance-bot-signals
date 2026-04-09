#!/usr/bin/env python3
"""
repair_ledger.py — one-time ledger semantic repair.
Run from project root: python3 repair_ledger.py

What this does:
  - Fixes first_seen_date / last_seen_date for all rows using raw research_day facts
  - Creates a timestamped backup before any mutation
  - Prints before/after examples for corrupted rows
  - Runs invariant validation after repair
  - Idempotent: safe to run more than once

What this does NOT do:
  - Does not delete rows
  - Does not change strategy logic
  - Does not touch live runtime files
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from research.top_movers.signature_ledger import (
    repair_ledger_history,
    validate_ledger_invariants,
    LEDGER_PATH,
)

print("=" * 60)
print("Ledger Semantic Repair")
print("=" * 60)
print(f"Ledger: {LEDGER_PATH}")

if not os.path.exists(LEDGER_PATH):
    print("ERROR: ledger file not found — nothing to repair.")
    sys.exit(1)

summary = repair_ledger_history()

print()
print("=" * 60)
print("Repair Summary")
print("=" * 60)
print(f"  Status:           {summary['status']}")
print(f"  Backup created:   {summary.get('backup_path','N/A')}")
print(f"  Total rows:       {summary.get('total_rows', 0)}")
print(f"  Rows repaired:    {summary.get('rows_repaired', 0)}")
print(f"  Unique sigs:      {summary.get('unique_signatures', 0)}")

examples = summary.get("examples", [])
if examples:
    print()
    print("  Examples (first 6 repaired rows):")
    for ex in examples:
        print(f"    {ex['code']} [{ex['research_day']}]")
        print(f"      first_seen: {ex['old_first'] or '(empty)'!r} -> {ex['new_first']}")
        print(f"      last_seen:  {ex['old_last'] or '(empty)'!r}  -> {ex['new_last']}")

violations = summary.get("post_repair_violations", [])
print()
if violations:
    print(f"  WARNING: {len(violations)} invariant violation(s) after repair:")
    for v in violations:
        print(f"    {v}")
    sys.exit(2)
else:
    print("  PASS: all invariants satisfied after repair.")
    print("  Ledger is now safe for research interpretation.")
    sys.exit(0)
