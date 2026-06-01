"""
Signal gates g1–g6 (spec §10).

Shadow mode: when hl_gates_enabled=False, detected=None but gates_pass is still computed
and should_shadow_alert is set so [SHADOW] Telegram alerts fire during §13 validation.

Insufficient-history guard (spec plan issue #3):
  Per-coin, not global. A coin that reached coverage only on day 4 won't have
  window_snapshots concentration snapshots until later. Block silently — no error.

Window: snapshot-count based (window_snapshots config). Logs window_time_h so §13
can determine if time-based window would be more appropriate post-validation.

Gate g6 (funding): only evaluated if FUNDING_UNIT_VERIFIED (always True after 2026-06-01
verification, but guarded for safety).
"""

from typing import Dict, List, Optional, Tuple

from .market_context import FUNDING_UNIT_VERIFIED


def evaluate(
    coin: str,
    concentration_history: List[Tuple[int, float]],  # [(ts_ms, real_concentration), ...]
    oi_history: List[float],                           # recent oi_usd values
    churn: dict,
    top_n_long_notional_history: List[float],
    coin_ctx: dict,
    cfg: dict,
    coverage_ok: bool,
    warmup_elapsed: bool,
) -> dict:
    """Evaluate all gates and return result dict.

    Always computes gates_pass.
    detected = gates_pass only when hl_gates_enabled=True AND coverage_ok AND not blocked.
    should_shadow_alert = gates_pass AND NOT hl_gates_enabled AND coverage_ok AND not blocked.
    """
    hl_cfg = cfg.get("hl_whale_accum", {})
    hl_gates_enabled = bool(hl_cfg.get("hl_gates_enabled", False))
    window_n = int(hl_cfg.get("window_snapshots", 14))
    conc_min = float(hl_cfg.get("conc_min", 0.15))
    jaccard_min = float(hl_cfg.get("jaccard_min", 0.50))
    price_vs_baseline_min = float(hl_cfg.get("price_vs_baseline_min", -0.02))
    price_trend_7v30_min = float(hl_cfg.get("price_trend_7v30_min", -0.02))
    funding_max = float(hl_cfg.get("funding_8h_pct_max", 0.03))
    scan_interval_min = float(hl_cfg.get("scan_interval_tier_b_min", 60))

    # --- Insufficient-history guard (per-coin, issue #3) ---
    if len(concentration_history) < window_n:
        window_time_h = round(len(concentration_history) * scan_interval_min / 60, 2)
        return {
            "blocked": "insufficient_history",
            "g1": False, "g2": False, "g3": False,
            "g4": False, "g5": False, "g6": False,
            "gates_pass": False,
            "detected": None,
            "should_shadow_alert": False,
            "window_time_h": window_time_h,
            "snapshots_available": len(concentration_history),
            "window_snapshots_needed": window_n,
        }

    if not warmup_elapsed:
        return {**_false_gates(), "blocked": "warmup", "detected": None, "should_shadow_alert": False,
                "snapshots_available": len(concentration_history)}

    # --- Gate computations ---
    conc_values = [v for _, v in concentration_history]
    curr_conc = conc_values[-1]
    window_conc = conc_values[-window_n:]

    # g1: same-cohort accumulation (churn metrics, cluster-keyed)
    top_n_rising = (
        len(top_n_long_notional_history) >= 2
        and top_n_long_notional_history[-1] > top_n_long_notional_history[-2]
    )
    g1 = (
        top_n_rising
        and churn.get("same_cohort_rising", False)
        and churn.get("topn_jaccard", 0.0) >= jaccard_min
    )

    # g2: real_concentration high
    g2 = curr_conc >= conc_min

    # g3: concentration rising over window
    g3 = window_conc[-1] > window_conc[0] if len(window_conc) >= 2 else False

    # g4: OI trend positive over window
    g4 = _oi_trend_positive(oi_history, window_n)

    # g5: price vs baseline and price trend 7v30
    mark_px = coin_ctx.get("mark_px", 0.0)
    price_baseline = coin_ctx.get("price_baseline", mark_px)
    close_30d = coin_ctx.get("close_30d", [])
    close_7d = coin_ctx.get("close_7d", [])

    price_vs_baseline = ((mark_px - price_baseline) / price_baseline) if price_baseline > 0 else 0.0
    mean_30d = (sum(close_30d) / len(close_30d)) if close_30d else price_baseline
    mean_7d = (sum(close_7d) / len(close_7d)) if close_7d else mark_px
    price_trend_7v30 = ((mean_7d - mean_30d) / mean_30d) if mean_30d > 0 else 0.0
    g5 = price_vs_baseline >= price_vs_baseline_min and price_trend_7v30 >= price_trend_7v30_min

    # g6: funding neutral — only if verified
    funding_8h = coin_ctx.get("funding_8h_pct")
    if not FUNDING_UNIT_VERIFIED or funding_8h is None:
        g6 = True  # gate disabled
    else:
        g6 = funding_8h < funding_max

    gates_pass = g1 and g2 and g3 and g4 and g5 and g6

    # Shadow vs active mode (issue #8)
    blocked = not coverage_ok
    detected: Optional[bool]
    if hl_gates_enabled and coverage_ok:
        detected = gates_pass
    else:
        detected = None

    should_shadow_alert = gates_pass and (not hl_gates_enabled) and coverage_ok

    window_time_h = round(window_n * scan_interval_min / 60, 2)

    return {
        "blocked": "coverage_fail" if not coverage_ok else None,
        "g1": g1, "g2": g2, "g3": g3, "g4": g4, "g5": g5, "g6": g6,
        "gates_pass": gates_pass,
        "detected": detected,
        "should_shadow_alert": should_shadow_alert,
        "price_vs_baseline": round(price_vs_baseline, 4),
        "price_trend_7v30": round(price_trend_7v30, 4),
        "funding_8h_pct": funding_8h,
        "real_concentration": curr_conc,
        "topn_jaccard": churn.get("topn_jaccard", 0.0),
        "window_time_h": window_time_h,
        "snapshots_available": len(concentration_history),
    }


def _oi_trend_positive(oi_history: List[float], window_n: int) -> bool:
    if len(oi_history) < 2:
        return False
    window = oi_history[-window_n:] if len(oi_history) >= window_n else oi_history
    return window[-1] > window[0]


def _false_gates() -> dict:
    return {
        "g1": False, "g2": False, "g3": False,
        "g4": False, "g5": False, "g6": False,
        "gates_pass": False,
        "snapshots_available": 0,
    }
