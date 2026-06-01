"""
Coverage gate and registry health metrics (spec §7).

Coverage gate is a PRECONDITION — not one of g1-g6.
Evaluated before every signal check. If coverage is insufficient, concentration
is not trustworthy and the coin is skipped for this scan cycle.
"""

from typing import Dict, Optional, Tuple

from ._client import throttle_counter


def coverage_gate(
    coin: str,
    tracked_long_notional: float,
    oi_usd: float,
    coverage_min: float,
) -> Tuple[bool, float]:
    """Returns (ok, coverage_estimate).

    coverage_estimate = tracked_long_notional / oi_usd
    ok = True if estimate >= coverage_min.
    Returns (False, 0.0) if oi_usd is zero.
    """
    if oi_usd <= 0:
        return False, 0.0
    estimate = tracked_long_notional / oi_usd
    return estimate >= coverage_min, estimate


def registry_health(
    coin: str,
    registry: Dict[str, dict],
    coverage_estimate: float,
    prev_active_count: int,
    prev_registry_size: int,
) -> dict:
    """Compute registry health metrics for §13 logging.

    Returns dict suitable for inclusion in scan CSV row.
    """
    active_addresses = sum(
        1 for e in registry.values()
        if e.get("last_szi", 0.0) != 0.0
    )
    total_addresses = len(registry)

    new_address_rate = max(0, total_addresses - prev_registry_size)
    churn_rate = max(0, prev_active_count - active_addresses)  # addresses that went flat

    throttle_counts = throttle_counter.get_and_reset()
    throttle_count = sum(throttle_counts.values())

    return {
        "coverage_estimate": round(coverage_estimate, 4),
        "active_address_count": active_addresses,
        "total_address_count": total_addresses,
        "new_address_rate": new_address_rate,
        "registry_churn_rate": churn_rate,
        "throttle_count": throttle_count,
    }
