"""
regime/regime_normalizer.py
----------------------------
Single shared write-enrichment helper for regime/fit attachment.

RULE: No production row write/update path may assign regime_label or
regime_fit_for_strategy directly. All production paths must call
enrich_row_with_regime() from this module instead.

This module owns:
  - The normalization map (legacy -> 3A labels)
  - The fit map (strategy_family + regime_label -> HIGH/MEDIUM/LOW)
  - enrich_row_with_regime(): the one function all write paths call

It does NOT own:
  - Regime classification (that is regime/regime_classifier.py)
  - CSV writes (that is lifecycle/case_truth_service.py)
  - Dispatch logic
"""
from __future__ import annotations

from typing import Dict

# ── 3A approved label set ──────────────────────────────────────────────────

VALID_3A_LABELS = frozenset({
    "trend_continuation_friendly",
    "broad_weakness_sell_pressure",
    "unclear_mixed",
})

# ── Normalization map ──────────────────────────────────────────────────────
# Maps any incoming label (legacy or raw) to one of the three 3A labels.

_NORMALIZE_MAP: Dict[str, str] = {
    # Already valid — keep as-is
    "trend_continuation_friendly": "trend_continuation_friendly",
    "broad_weakness_sell_pressure": "broad_weakness_sell_pressure",
    "unclear_mixed":                "unclear_mixed",
    # Legacy BULLISH family
    "bullish":          "trend_continuation_friendly",
    "bull":             "trend_continuation_friendly",
    "up":               "trend_continuation_friendly",
    "uptrend":          "trend_continuation_friendly",
    "trend":            "trend_continuation_friendly",
    "continuation":     "trend_continuation_friendly",
    "range_bullish":    "trend_continuation_friendly",
    "alt_momo":         "trend_continuation_friendly",
    # Legacy BEARISH family
    "bearish":          "broad_weakness_sell_pressure",
    "bear":             "broad_weakness_sell_pressure",
    "down":             "broad_weakness_sell_pressure",
    "downtrend":        "broad_weakness_sell_pressure",
    "sell_pressure":    "broad_weakness_sell_pressure",
    # Neutral / range / unknown -> unclear_mixed
    "neutral":          "unclear_mixed",
    "range":            "unclear_mixed",
    "mixed":            "unclear_mixed",
    "sideways":         "unclear_mixed",
    "sideway":          "unclear_mixed",
    "unknown":          "unclear_mixed",
    "volatile_chop":    "unclear_mixed",
    "chop":             "unclear_mixed",
}

# ── Fit map ───────────────────────────────────────────────────────────────
# (strategy_family_key, regime_label) -> fit value

_FIT_MAP: Dict[tuple, str] = {
    # Long families
    ("long",  "trend_continuation_friendly"):   "HIGH",
    ("long",  "broad_weakness_sell_pressure"):  "LOW",
    ("long",  "unclear_mixed"):                 "MEDIUM",
    # Short family
    ("short", "trend_continuation_friendly"):   "LOW",
    ("short", "broad_weakness_sell_pressure"):  "HIGH",
    ("short", "unclear_mixed"):                 "MEDIUM",
}

# Strategy family -> long/short key
_FAMILY_SIDE_KEY: Dict[str, str] = {
    "long_breakout_retest":   "long",
    "legacy_5m_retest":       "long",
    "short_exhaustion_retest": "short",
}


# ── Public API ─────────────────────────────────────────────────────────────

def normalize_regime_label(
    regime_label: str = "",
    market_regime: str = "",
    btc_regime: str = "",
) -> str:
    """Return a valid 3A regime label from any incoming value.

    Priority: regime_label > market_regime > btc_regime > fallback.
    Never returns a legacy label.
    """
    raw = str(regime_label or market_regime or btc_regime or "").strip().lower()
    if not raw:
        return "unclear_mixed"
    return _NORMALIZE_MAP.get(raw, "unclear_mixed")


def derive_regime_fit(strategy_family: str, side: str, regime_label: str) -> str:
    """Return HIGH, MEDIUM, or LOW fit for a strategy family + regime label.

    Falls back to side-based inference when strategy_family is unknown.
    Returns not_evaluated only when neither family nor side can be inferred.
    """
    label = normalize_regime_label(regime_label)
    family_key = _FAMILY_SIDE_KEY.get(str(strategy_family or "").lower())

    if family_key:
        return _FIT_MAP.get((family_key, label), "MEDIUM")

    # Fallback: infer from side
    side_u = str(side or "").upper()
    if side_u == "SHORT":
        return _FIT_MAP.get(("short", label), "MEDIUM")
    if side_u == "LONG":
        return _FIT_MAP.get(("long", label), "MEDIUM")

    return "not_evaluated"


def enrich_row_with_regime(
    row: Dict,
    strategy_family: str = "",
    side: str = "",
    regime_label: str = "",
    market_regime: str = "",
    btc_regime: str = "",
) -> Dict:
    """Attach normalized regime_label and regime_fit_for_strategy to a row dict.

    This is the ONE function all production row write/update paths must call.
    It mutates row in place and also returns it.

    Args:
        row:             The pending/signal row dict about to be written or
                         updated. Must be a plain dict (not a dataclass).
        strategy_family: One of the three locked family names.  If empty,
                         falls back to row['strategy'].
        side:            'LONG' or 'SHORT'.  Falls back to row['side'].
        regime_label:    Incoming regime label (may be legacy or 3A).
                         Falls back to row fields in priority order.
        market_regime:   Secondary fallback for label resolution.
        btc_regime:      Tertiary fallback for label resolution.

    Returns:
        The same row dict with regime_label and regime_fit_for_strategy set.

    Contract:
        - Only modifies regime_label and regime_fit_for_strategy.
        - Never wipes dispatch_*, confirmed_ts_ms, or any other field.
        - If regime_fit_for_strategy already has a valid value (HIGH/MEDIUM/LOW)
          and regime_label is already a valid 3A label, the existing values are
          preserved (idempotent on clean rows).
    """
    row = dict(row)  # defensive copy — caller gets the enriched version back

    # Resolve incoming label from all available sources
    incoming_label = (
        regime_label
        or row.get("regime_label", "")
        or market_regime
        or row.get("market_regime", "")
        or btc_regime
        or row.get("btc_regime", "")
    )

    normalized = normalize_regime_label(incoming_label)
    row["regime_label"] = normalized

    # Resolve strategy family and side from args or row
    resolved_family = strategy_family or row.get("strategy", "")
    resolved_side = side or row.get("side", "")

    # Only overwrite fit if it's missing or invalid
    existing_fit = str(row.get("regime_fit_for_strategy", "") or "")
    if existing_fit not in ("HIGH", "MEDIUM", "LOW"):
        row["regime_fit_for_strategy"] = derive_regime_fit(
            resolved_family, resolved_side, normalized
        )

    return row
