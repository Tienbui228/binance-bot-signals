#!/usr/bin/env python3
"""apply_patches_v5.py — P0 ledger semantic fix. Run from project root."""
import re

def read(p): return open(p, encoding="utf-8").read()
def write(p, c): open(p, "w", encoding="utf-8").write(c); print(f"  saved: {p}")

def patch(path, old, new, label="", required=True):
    c = read(path)
    if old not in c:
        print(f"  {'WARN' if required else 'SKIP'} not found: {label}")
        return False
    write(path, c.replace(old, new, 1))
    print(f"  OK: {label}")
    return True

SL = "research/top_movers/signature_ledger.py"

# =============================================================================
print("\n=== 1. Add shutil import ===")
# =============================================================================
patch(SL,
    "import csv\nimport os\nimport statistics",
    "import csv\nimport os\nimport shutil\nimport statistics",
    "add shutil import")

# =============================================================================
print("\n=== 2. Replace upsert_ledger — derive first/last from raw facts ===")
# =============================================================================
content = read(SL)

NEW_UPSERT = (
    'def upsert_ledger(new_candidates: List[Dict], research_day: str) -> None:\n'
    '    """\n'
    '    Daily replace/upsert for signature_evidence_ledger.csv.\n'
    '\n'
    '    first_seen_date / last_seen_date are derived from raw research_day facts only.\n'
    '    Never trust stored field values — they may be stale from prior buggy writes.\n'
    '    All retained historical rows are corrected on each write.\n'
    '    """\n'
    '    os.makedirs(os.path.dirname(LEDGER_PATH), exist_ok=True)\n'
    '\n'
    '    kept: Dict[tuple, Dict] = {}\n'
    '    actual_days_by_sig: Dict[str, set] = {}\n'
    '\n'
    '    if os.path.exists(LEDGER_PATH):\n'
    '        with open(LEDGER_PATH, "r", newline="", encoding="utf-8") as f:\n'
    '            for row in csv.DictReader(f):\n'
    '                day = row.get("research_day","")\n'
    '                sig = row.get("signature_key","")\n'
    '                if day == research_day:\n'
    '                    continue  # PURGE stale rows for this day\n'
    '                kept[(day, sig)] = row\n'
    '                if sig and day:\n'
    '                    actual_days_by_sig.setdefault(sig, set()).add(day)\n'
    '\n'
    '    # Include today\'s new candidates in day tracking\n'
    '    for cand in new_candidates:\n'
    '        sk = cand.get("signature_key","")\n'
    '        if sk:\n'
    '            actual_days_by_sig.setdefault(sk, set()).add(research_day)\n'
    '\n'
    '    # Canonical first/last seen from raw research_day facts — never from stored fields\n'
    '    first_seen_by_sig = {sig: min(days) for sig, days in actual_days_by_sig.items()}\n'
    '    last_seen_by_sig  = {sig: max(days) for sig, days in actual_days_by_sig.items()}\n'
    '\n'
    '    # Correct first/last seen on all retained historical rows\n'
    '    for (day, sig), row in kept.items():\n'
    '        if sig in first_seen_by_sig:\n'
    '            row["first_seen_date"] = first_seen_by_sig[sig]\n'
    '            row["last_seen_date"]  = last_seen_by_sig[sig]\n'
    '\n'
    '    # Insert new candidates for today\n'
    '    for cand in new_candidates:\n'
    '        sig_key = cand.get("signature_key","")\n'
    '        key     = (research_day, sig_key)\n'
    '        new_row = _candidate_to_ledger_row(cand)\n'
    '        new_row["first_seen_date"] = first_seen_by_sig.get(sig_key, research_day)\n'
    '        new_row["last_seen_date"]  = last_seen_by_sig.get(sig_key, research_day)\n'
    '        kept[key] = new_row\n'
    '\n'
    '    all_rows = sorted(kept.values(), key=lambda r: (r.get("research_day",""), r.get("signature_key","")))\n'
    '    all_keys = list(dict.fromkeys(\n'
    '        LEDGER_SCHEMA + [k for k in (all_rows[0].keys() if all_rows else []) if k not in LEDGER_SCHEMA]\n'
    '    ))\n'
    '\n'
    '    with open(LEDGER_PATH, "w", newline="", encoding="utf-8") as f:\n'
    '        writer = csv.DictWriter(f, fieldnames=all_keys, extrasaction="ignore")\n'
    '        writer.writeheader()\n'
    '        for row in all_rows:\n'
    '            writer.writerow({k: row.get(k,"") for k in all_keys})\n'
)

before = content
content = re.sub(
    r'def upsert_ledger\(new_candidates.*?(?=\n# ---------------------------------------------------------------------------\n# Daily research summary)',
    NEW_UPSERT,
    content,
    flags=re.DOTALL,
    count=1,
)
if content != before:
    print("  OK: upsert_ledger replaced")
else:
    print("  WARN: upsert_ledger pattern not matched — check manually")
write(SL, content)

# =============================================================================
print("\n=== 3. Fix extract_signature_candidates — load ledger history ===")
# =============================================================================
patch(SL,
    '    if not eligible:\n'
    '        return []\n'
    '\n'
    '    groups: Dict[str, List[Dict]] = {}',
    '    if not eligible:\n'
    '        return []\n'
    '\n'
    '    # Load historical ledger to derive validation_status from true history\n'
    '    _hist_days_by_sig: Dict[str, set] = {}\n'
    '    if os.path.exists(LEDGER_PATH):\n'
    '        with open(LEDGER_PATH, "r", newline="", encoding="utf-8") as _f:\n'
    '            for _row in csv.DictReader(_f):\n'
    '                _sk = _row.get("signature_key", "")\n'
    '                _rd = _row.get("research_day", "")\n'
    '                if _sk and _rd and _rd != research_day:\n'
    '                    _hist_days_by_sig.setdefault(_sk, set()).add(_rd)\n'
    '\n'
    '    groups: Dict[str, List[Dict]] = {}',
    "add ledger history load in extract_signature_candidates")

# =============================================================================
print("\n=== 4. Fix validation_status — use ledger history ===")
# =============================================================================
patch(SL,
    '"validation_status": "tracking" if sig_conf in ("HIGH","MEDIUM") else "first_observation",',
    '"validation_status": "tracking" if (sig_conf in ("HIGH","MEDIUM") or bool(_hist_days_by_sig.get(sig_key))) else "first_observation",',
    "validation_status: seen-before check from ledger history")

# =============================================================================
print("\n=== 5. Append repair + invariant functions ===")
# =============================================================================

REPAIR_FUNCS = '''

# ---------------------------------------------------------------------------
# Ledger repair and invariant validation (downstream-only)
# ---------------------------------------------------------------------------

def validate_ledger_invariants_from_rows(rows: List[Dict]) -> List[str]:
    """Check semantic invariants on ledger rows. Returns list of violation strings."""
    violations = []
    for row in rows:
        sk   = row.get("signature_key", "")
        rd   = row.get("research_day", "")
        fs   = row.get("first_seen_date", "")
        ls   = row.get("last_seen_date", "")
        code = row.get("signature_candidate_code", sk[:25] if sk else "?")
        if fs and ls and fs > ls:
            violations.append(f"{code} [{rd}]: first_seen ({fs}) > last_seen ({ls})")
        if rd and fs and rd < fs:
            violations.append(f"{code} [{rd}]: research_day ({rd}) < first_seen ({fs})")
        if rd and ls and rd > ls:
            violations.append(f"{code} [{rd}]: research_day ({rd}) > last_seen ({ls})")
    return violations


def validate_ledger_invariants() -> List[str]:
    """Load ledger from disk and run invariant checks. Returns violation strings."""
    if not os.path.exists(LEDGER_PATH):
        return []
    with open(LEDGER_PATH, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    return validate_ledger_invariants_from_rows(rows)


def repair_ledger_history(window_days: int = 7) -> Dict:
    """
    One-time idempotent repair of signature_evidence_ledger.csv.

    Rules:
      - first_seen_date = min(research_day) per signature_key across ALL rows
      - last_seen_date  = max(research_day) per signature_key across ALL rows
      - Does NOT delete any rows
      - Does NOT change strategy or scoring fields
      - Creates a timestamped backup before mutating
      - Idempotent: running twice gives the same result
      - Prints a concise repair summary

    Returns dict with: status, backup_path, total_rows, rows_repaired, examples,
                       post_repair_violations
    """
    if not os.path.exists(LEDGER_PATH):
        print("  repair_ledger_history: no ledger file found, nothing to repair")
        return {"status": "no_file", "rows_repaired": 0}

    # Backup
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    backup_path = LEDGER_PATH + f".bak_{ts}"
    shutil.copy(LEDGER_PATH, backup_path)
    print(f"  Backup: {backup_path}")

    # Load all rows
    with open(LEDGER_PATH, newline="", encoding="utf-8") as f:
        all_rows = list(csv.DictReader(f))

    if not all_rows:
        return {"status": "empty", "rows_repaired": 0, "backup_path": backup_path}

    # Group by signature_key: collect all raw research_days
    days_by_sig: Dict[str, set] = {}
    for row in all_rows:
        sk = row.get("signature_key", "")
        rd = row.get("research_day", "")
        if sk and rd:
            days_by_sig.setdefault(sk, set()).add(rd)

    first_by_sig = {sig: min(days) for sig, days in days_by_sig.items()}
    last_by_sig  = {sig: max(days) for sig, days in days_by_sig.items()}

    repaired = 0
    examples: List[Dict] = []

    for row in all_rows:
        sk = row.get("signature_key", "")
        if not sk:
            continue
        old_first = row.get("first_seen_date", "")
        old_last  = row.get("last_seen_date", "")
        new_first = first_by_sig.get(sk, "")
        new_last  = last_by_sig.get(sk, "")
        changed = (old_first != new_first) or (old_last != new_last)
        if changed:
            row["first_seen_date"] = new_first
            row["last_seen_date"]  = new_last
            repaired += 1
            if len(examples) < 6:
                examples.append({
                    "sig_key":    sk,
                    "code":       row.get("signature_candidate_code", sk[:25]),
                    "research_day": row.get("research_day", ""),
                    "old_first":  old_first,  "new_first": new_first,
                    "old_last":   old_last,   "new_last":  new_last,
                })

    # Write back
    all_keys = list(all_rows[0].keys())
    with open(LEDGER_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=all_keys, extrasaction="ignore")
        writer.writeheader()
        for row in all_rows:
            writer.writerow({k: row.get(k, "") for k in all_keys})

    violations = validate_ledger_invariants_from_rows(all_rows)

    summary = {
        "status":                 "repaired",
        "backup_path":            backup_path,
        "total_rows":             len(all_rows),
        "rows_repaired":          repaired,
        "unique_signatures":      len(days_by_sig),
        "examples":               examples,
        "post_repair_violations": violations,
    }

    print(f"  Total rows: {len(all_rows)} | Repaired: {repaired} | Sigs: {len(days_by_sig)}")
    if violations:
        print(f"  WARNING: {len(violations)} invariant violation(s) remain after repair")
        for v in violations[:5]:
            print(f"    {v}")
    else:
        print("  All invariants pass after repair.")

    return summary
'''

content = read(SL)
if "def repair_ledger_history(" in content:
    print("  SKIP: repair functions already present")
else:
    write(SL, content + REPAIR_FUNCS)
    print("  OK: repair + invariant functions appended")

# =============================================================================
print("\n=== 6. Create repair_ledger.py standalone runner ===")
# =============================================================================

REPAIR_RUNNER = '''#!/usr/bin/env python3
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
'''

with open("repair_ledger.py", "w", encoding="utf-8") as f:
    f.write(REPAIR_RUNNER)
print("  OK: repair_ledger.py created in project root")

# =============================================================================
print("\n=== Done. Run sequence: ===")
print("  1. python3 repair_ledger.py")
print("  2. python3 scripts/run_daily_top_movers_research.py --day 2026-04-08")
print("  3. Verify: SIG_SHORT_VOL_DIVWRN_SHAL shows tracking (not first_observation)")
print("  4. Verify: no first_seen > last_seen anywhere")
print()
