"""
contracts/delivery_result.py
Canonical typed contract for delivery metadata output.
Delivery annotates entry reality. It must not overwrite setup_detected
and must not issue NO_SEND directly.
"""
from dataclasses import dataclass


@dataclass
class DeliveryResult:
    """Output of delivery/delivery_state_evaluator.py."""
    entry_state: str = "not_evaluated"
    delivery_band: str = "not_evaluated"
    manual_tradable: str = "not_evaluated"
    manual_trade_note: str = ""
    entry_distance_pct: float = 0.0
    retest_depth_pct: float = 0.0
    risk_pct_real: float = 0.0
    sl_distance_pct: float = 0.0
    tp1_distance_pct: float = 0.0
    tp2_distance_pct: float = 0.0
    delivery_note: str = ""
