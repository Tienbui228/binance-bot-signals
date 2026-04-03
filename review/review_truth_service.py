"""
review/review_truth_service.py
--------------------------------
Phase F: canonical home for review stage truth functions.

Current state: thin re-export from scanner/review_service.py.
Logic has not been moved yet — this establishes the canonical import path.

Owns:
  - Stage capture (save_review_snapshot, _capture_and_register_case_stage)
  - Snapshot helpers

Must NOT:
  - Invent truth semantics
  - Repair lifecycle truth (confirmed_ts_ms, sent_ts_ms, closed_ts_ms)
  - Issue writes to pending/signal CSV

Note on _repair_confirmed_review_semantics:
  This function exists in scanner/review_service.py.
  It is labelled clearly as a repair helper.
  It must NOT be extended or made implicit.
  Future cleanup: move to an explicit audit helper, not a silent side effect.

Canonical stages (locked):
  pre_pending
  pending_open
  entry_or_confirm
  case_close
"""
from __future__ import annotations

from scanner.review_service import (  # noqa: F401
    save_review_snapshot,
    CANONICAL_STAGE_TO_SNAPSHOT_STAGE,
    SNAPSHOT_STAGE_TO_CANONICAL_STAGE,
    CANONICAL_STAGE_TO_RUNTIME_STAGE,
)
