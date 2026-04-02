"""
dispatch/dispatch_router.py
---------------------------
Phase C2: canonical location for dispatch routing.

BEHAVIOR: Identical to scanner/dispatch/router.py.
This module re-exports route_dispatch_v1 verbatim — no logic changes.

Migration path:
  Before C2: oi_scanner.py imports from scanner.dispatch.router
  After  C2: oi_scanner.py imports from dispatch.dispatch_router
  scanner/dispatch/router.py: kept as shim (imports from here)

Do not add routing logic here during C2.
Do not change thresholds, actions, or confidence bands.
"""
from scanner.dispatch.router import route_dispatch_v1  # verbatim re-export

__all__ = ["route_dispatch_v1"]
