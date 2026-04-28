from typing import Dict, Optional


_EXCLUSION_REASONS = {
    "room_too_small": "room_too_small",
    "rr_too_low": "rr_too_low",
    "breakdown_not_confirmed": "breakdown_not_confirmed",
    "retest_not_found": "retest_not_found",
    "liquidity_too_low": "liquidity_too_low",
    "base_invalid": "base_invalid",
    "peak_too_old": "peak_too_old",
    "false_break_reclaim": "false_break_reclaim",
    "insufficient_data": "insufficient_data",
    "invalid_anchor_order": "invalid_anchor_order",
    "ignition_not_found": "base_invalid",
    "ignition_not_found_before_peak": "base_invalid",
    "insufficient_bars_before_ignition": "base_invalid",
    "base_window_already_trending": "base_invalid",
    "market_cap_too_large": "market_cap_too_large",
    "market_cap_unknown": "market_cap_unknown",
}


def classify_exclusion(gate_results: Dict) -> Optional[str]:
    """Map gate failure results to an exclusion reason code.

    gate_results: dict with boolean values per gate name.
    Returns exclusion reason string, or None if no exclusion.
    """
    if not gate_results.get("G_D1_base_valid", True):
        reason = gate_results.get("base_invalid_reason", "base_invalid")
        return _EXCLUSION_REASONS.get(reason, "base_invalid")

    if not gate_results.get("G_D2_pump_pct", True):
        return "insufficient_data"

    if not gate_results.get("G_D3_pump_vol", True):
        return "insufficient_data"

    if not gate_results.get("G_D4_peak_age", True):
        return "peak_too_old"

    if not gate_results.get("G_D5_room", True):
        return "room_too_small"

    if not gate_results.get("G_D6_liquidity", True):
        return "liquidity_too_low"

    if not gate_results.get("G_D7_listing_age", True):
        return "liquidity_too_low"

    if not gate_results.get("G_D8_anchor_order", True):
        return "invalid_anchor_order"

    # Scan gates
    if not gate_results.get("false_break", True):
        return "false_break_reclaim"

    if not gate_results.get("G_H3_no_false_break", True):
        return "false_break_reclaim"

    if not gate_results.get("G_H4_room", True):
        return "room_too_small"

    if not gate_results.get("G_H5_rr", True):
        return "rr_too_low"

    if not gate_results.get("G_H6_anchor_order", True):
        return "invalid_anchor_order"

    return None
