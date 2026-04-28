from typing import Dict, List


def classify_oi_regime(oi_hist: List[Dict],
                       current_price: float,
                       peak_high: float) -> Dict:
    """Classify OI regime from hourly OI history.

    Returns {oi_regime_label, oi_change_from_peak_pct, oi_change_1h_pct,
              oi_change_4h_pct, oi_context_note}.
    OI is context only — not a hard gate.
    """
    if not oi_hist or len(oi_hist) < 2:
        return _oi_result("OI_UNCLEAR_OR_MISSING", None, None, None,
                          "insufficient OI history")

    # Sort ascending by ts
    hist = sorted(oi_hist, key=lambda x: x["ts"])

    latest_oi = hist[-1]["oi_value"]
    peak_oi = max(h["oi_value"] for h in hist) if hist else latest_oi

    # oi_change_from_peak_pct
    oi_change_from_peak_pct = ((latest_oi - peak_oi) / peak_oi) if peak_oi > 0 else None

    # oi_change_1h_pct: latest vs 1 bar ago
    oi_change_1h_pct = None
    if len(hist) >= 2 and hist[-2]["oi_value"] > 0:
        oi_change_1h_pct = (hist[-1]["oi_value"] - hist[-2]["oi_value"]) / hist[-2]["oi_value"]

    # oi_change_4h_pct: latest vs 4 bars ago
    oi_change_4h_pct = None
    if len(hist) >= 5 and hist[-5]["oi_value"] > 0:
        oi_change_4h_pct = (hist[-1]["oi_value"] - hist[-5]["oi_value"]) / hist[-5]["oi_value"]

    # Price direction vs peak
    oi_price_down = current_price < peak_high * 0.95

    # OI direction (1h)
    oi_up = oi_change_1h_pct is not None and oi_change_1h_pct > 0.02
    oi_down = oi_change_1h_pct is not None and oi_change_1h_pct < -0.02
    oi_stable = oi_change_1h_pct is not None and abs(oi_change_1h_pct) < 0.02

    if oi_up and oi_price_down:
        label = "OI_EXPANSION_PRICE_DOWN"
        note = "New shorts entering. Thesis confirmation."
    elif oi_down and oi_price_down:
        label = "OI_CONTRACTION_AFTER_DUMP"
        note = "Long liquidation. Neutral to bearish."
    elif oi_stable and oi_price_down:
        label = "OI_STABLE_PRICE_FAILS_RETEST"
        note = "Context support for failed retest."
    else:
        label = "OI_UNCLEAR_OR_MISSING"
        note = "OI does not fit standard regime."

    return _oi_result(label, oi_change_from_peak_pct, oi_change_1h_pct,
                      oi_change_4h_pct, note)


def _oi_result(label, from_peak, h1, h4, note) -> Dict:
    return {
        "oi_regime_label": label,
        "oi_change_from_peak_pct": from_peak,
        "oi_change_1h_pct": h1,
        "oi_change_4h_pct": h4,
        "oi_context_note": note,
    }
