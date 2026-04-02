from dataclasses import dataclass
from typing import Any

@dataclass
class RegimeVerdict:
    regime_label: str = "unclear_mixed"
    regime_confidence: str = "LOW"
    regime_fit_long_breakout: str = "MEDIUM"
    regime_fit_short_exhaustion: str = "MEDIUM"
    regime_note: str = "fallback"


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value or 0.0)
    except Exception:
        return default


def classify_regime(scanner, now_ms: int) -> RegimeVerdict:
    snapshot = getattr(scanner, "current_market_snapshot", {}) or {}
    btc_regime = str(snapshot.get("btc_regime", "neutral") or "neutral").lower()
    btc_24h = _to_float(snapshot.get("btc_24h_change_pct", 0.0))
    btc_4h = _to_float(snapshot.get("btc_4h_change_pct", 0.0))
    btc_1h = _to_float(snapshot.get("btc_1h_change_pct", 0.0))
    breadth = _to_float(snapshot.get("alt_market_breadth_pct", 0.0))

    if (btc_regime == "bearish" and breadth <= 45.0) or (btc_24h <= -1.0 and btc_4h <= 0.0 and breadth <= 45.0):
        conf = "HIGH" if btc_24h <= -1.5 and breadth <= 40.0 else "MEDIUM"
        return RegimeVerdict(
            regime_label="broad_weakness_sell_pressure",
            regime_confidence=conf,
            regime_fit_long_breakout="LOW",
            regime_fit_short_exhaustion="HIGH",
            regime_note="btc_bearish+breadth_weak",
        )

    if breadth >= 55.0 and btc_24h >= -0.5 and btc_1h >= -0.4 and btc_4h >= -0.3:
        conf = "HIGH" if (btc_regime == "bullish" or btc_24h >= 1.0) and breadth >= 60.0 else "MEDIUM"
        return RegimeVerdict(
            regime_label="trend_continuation_friendly",
            regime_confidence=conf,
            regime_fit_long_breakout="HIGH",
            regime_fit_short_exhaustion="LOW",
            regime_note="btc_ok+breadth_supportive",
        )

    return RegimeVerdict(
        regime_label="unclear_mixed",
        regime_confidence="LOW",
        regime_fit_long_breakout="MEDIUM",
        regime_fit_short_exhaustion="MEDIUM",
        regime_note="mixed_state",
    )
