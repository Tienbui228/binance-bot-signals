"""
lifecycle/case_truth_service.py
--------------------------------
Phase E: canonical home for all lifecycle truth writes.

Current state: thin re-export from scanner/lifecycle.py.
All production write paths in BinanceScanner (oi_scanner.py) are the
canonical owners for now. This module exists to establish the canonical
import path for future consolidation.

Write-path checklist (all verified PASS in Phase E):
  [x] save_pending            — BinanceScanner.save_pending (enrich_row_with_regime)
  [x] confirm/update pending  — BinanceScanner._mark_pending_confirmed_fields
  [x] invalidate/expire       — BinanceScanner.close_pending (enrich_row_with_regime)
  [x] save_signal             — BinanceScanner.save_signal (enrich_row_with_regime)
  [x] close/rewrite           — BinanceScanner.close_signal
  [x] sync_pending_send_decision — BinanceScanner.sync_pending_send_decision

Invariants:
  - confirmed_ts_ms and sent_ts_ms must remain distinct
  - closed_ts_ms must be a real timestamp or not_reached_yet, never fake
  - All writes must call enrich_row_with_regime before persisting

Future: oi_scanner.py class methods will be moved here one by one.
"""
from __future__ import annotations

# Re-export standalone helpers from scanner/lifecycle.py for callers
# that import from this canonical location.
from scanner.lifecycle import (  # noqa: F401
    mark_pending_confirmed_fields,
    sync_confirmed_pending_row,
    save_signal,
    save_pending,
    sync_pending_send_decision,
    close_pending,
    close_signal,
)
