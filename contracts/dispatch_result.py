"""
contracts/dispatch_result.py
Canonical typed contract for dispatch router output.
Dispatch routes only — it must not redetect strategy or invent semantics.

Valid dispatch_action values:
    NO_SEND
    WATCHLIST
    MAIN_SIGNAL
    not_evaluated  (only before dispatch has run)
"""
from dataclasses import dataclass


@dataclass
class DispatchResult:
    """Output of dispatch/dispatch_router.py."""
    dispatch_action: str = "not_evaluated"
    dispatch_confidence_band: str = "not_evaluated"
    dispatch_reason: str = "not_evaluated"
    publish_priority: int = 0
