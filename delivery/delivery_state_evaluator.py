"""
delivery/delivery_state_evaluator.py
-------------------------------------
Phase D: verbatim extraction of evaluate_manual_tradable from oi_scanner.py.

BEHAVIOR: Identical to the inline method. No new rules added.
Config keys read: manual_review.min_sl_distance_pct,
                  manual_review.min_tp1_distance_pct,
                  manual_review.min_risk_reward_for_manual

Must not:
- Rewrite setup_detected
- Issue NO_SEND directly
- Own any lifecycle writes
"""
from __future__ import annotations

from typing import Dict


def evaluate_manual_tradable(
    side: str,
    entry_ref: float,
    stop: float,
    tp1: float,
    cfg: Dict,
) -> Dict[str, str]:
    """Return manual_tradable and manual_trade_note for a setup.

    Verbatim logic from BinanceScanner.evaluate_manual_tradable().
    cfg must be the full scanner config dict (same as self.cfg).
    """
    manual_cfg = cfg.get("manual_review", {})
    min_sl_distance_pct = float(manual_cfg.get("min_sl_distance_pct", 2.0))
    min_tp1_distance_pct = float(manual_cfg.get("min_tp1_distance_pct", 2.5))
    min_risk_reward_for_manual = float(manual_cfg.get("min_risk_reward_for_manual", 1.0))

    sl_distance_pct = abs(entry_ref - stop) / max(entry_ref, 1e-12) * 100.0
    tp1_distance_pct = abs(tp1 - entry_ref) / max(entry_ref, 1e-12) * 100.0
    rr_tp1 = tp1_distance_pct / max(sl_distance_pct, 1e-12)

    notes = []
    if sl_distance_pct < min_sl_distance_pct:
        notes.append("sl_too_tight")
    if tp1_distance_pct < min_tp1_distance_pct:
        notes.append("tp1_too_small")
    if rr_tp1 < min_risk_reward_for_manual:
        notes.append("rr_tp1_too_low")

    if notes:
        return {
            "manual_tradable": "no",
            "manual_trade_note": ",".join(notes) + ";better_for_auto",
        }

    return {
        "manual_tradable": "yes",
        "manual_trade_note": "good_for_manual",
    }
