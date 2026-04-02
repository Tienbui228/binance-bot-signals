"""
veto/veto_engine.py
--------------------
Phase D: verbatim extraction of the cooldown gate from oi_scanner.py.

BEHAVIOR: Identical to BinanceScanner.should_send().
No new veto rules added. This is a stub that preserves the existing
45-minute cooldown-per-symbol behavior only.

V1 veto rules (future — not implemented yet):
    unclear_regime, duplicate_setup_block, cooldown_block,
    breadth_too_weak_for_long, breakout_environment_dead,
    broad_market_too_supportive_for_short,
    exhaustion_not_clean_enough, entry_chased_extreme

Must not:
- Add new rules beyond what existed inline
- Own strategy detection
- Own dispatch scoring
- Own lifecycle writes
"""
from __future__ import annotations

import time
from typing import Dict


def check_cooldown(
    symbol: str,
    side: str,
    sent_cache: Dict[str, float],
    ttl_seconds: int = 60 * 45,
) -> bool:
    """Return True if the symbol:side pair is allowed to send (not in cooldown).

    Verbatim logic from BinanceScanner.should_send().
    Mutates sent_cache in place when allowed (same as original).

    Args:
        symbol:      Trading symbol e.g. 'BTCUSDT'
        side:        'LONG' or 'SHORT'
        sent_cache:  The scanner's sent_cache dict (passed by reference)
        ttl_seconds: Cooldown window in seconds (default 45 minutes)

    Returns:
        True  = allowed to send (not in cooldown)
        False = blocked by cooldown
    """
    key = f"{symbol}:{side}"
    now = time.time()
    last = sent_cache.get(key, 0)
    if now - last < ttl_seconds:
        return False
    sent_cache[key] = now
    return True
