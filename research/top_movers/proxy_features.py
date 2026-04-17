"""
research/top_movers/proxy_features.py

Implements all proxy formulas from the Phase R1 spec.

Formulas implemented (exactly as specified):
  - top_acc_imbalance
  - top_pos_imbalance
  - global_acc_imbalance
  - global_acc_imbalance_delta
  - taker_imbalance
  - top_vs_global_divergence
  - large_participant_proxy
  - crowd_participation_proxy
  - crowd_overheat_proxy
  - flow_composite_signal
  - compression_score (for candidate windows)
  - break_quality_score / break_quality_band

Z-score rule:
  - per-symbol rolling z-score
  - window = 96 bars (96 x 5m = 8h)
  - min_periods = 48
  - clip to [-3, +3]
  - returns None if insufficient data

This module is downstream-only and does not import from the live bot.
Reuses scanner/market_math.py for wick/body helpers.
"""

import statistics
from dataclasses import dataclass, field
from typing import Dict, List, Optional

# Reuse allowed helpers from scanner/market_math.py
try:
    from scanner.market_math import (
        upper_wick_ratio,
        lower_wick_ratio,
        candle_body_ratio,
    )
except ImportError:
    # Fallback implementations if running standalone
    def upper_wick_ratio(bar):
        total = max(bar["high"] - bar["low"], 1e-12)
        body_high = max(bar["open"], bar["close"])
        return max(bar["high"] - body_high, 0.0) / total

    def lower_wick_ratio(bar):
        total = max(bar["high"] - bar["low"], 1e-12)
        body_low = min(bar["open"], bar["close"])
        return max(body_low - bar["low"], 0.0) / total

    def candle_body_ratio(bar):
        total = max(bar["high"] - bar["low"], 1e-12)
        return abs(bar["close"] - bar["open"]) / total


_EPS = 1e-12
_ZSCORE_WINDOW = 96
_ZSCORE_MIN_PERIODS = 48
_ZSCORE_CLIP = 3.0


# ---------------------------------------------------------------------------
# Z-score helpers
# ---------------------------------------------------------------------------

def rolling_zscore_series(
    values: List[Optional[float]],
    window: int = _ZSCORE_WINDOW,
    min_periods: int = _ZSCORE_MIN_PERIODS,
    clip_val: float = _ZSCORE_CLIP,
) -> List[Optional[float]]:
    """Per-symbol rolling z-score series. Clip to [-clip_val, +clip_val].

    Per spec:
      z(x)_t = (x_t - rolling_mean_t) / rolling_std_t
      window = 96, min_periods = 48, clip [-3, +3]
      Returns None if rolling_std == 0 or insufficient history.
    """
    n = len(values)
    result: List[Optional[float]] = [None] * n
    for i in range(n):
        val = values[i]
        if val is None:
            continue
        start = max(0, i - window + 1)
        window_vals = [v for v in values[start : i + 1] if v is not None]
        if len(window_vals) < min_periods:
            continue
        mean = sum(window_vals) / len(window_vals)
        variance = sum((v - mean) ** 2 for v in window_vals) / len(window_vals)
        std = variance ** 0.5
        if std < 1e-10:
            continue
        z = (val - mean) / std
        result[i] = max(-clip_val, min(clip_val, z))
    return result


def zscore_at(
    values: List[Optional[float]],
    idx: int,
    window: int = _ZSCORE_WINDOW,
    min_periods: int = _ZSCORE_MIN_PERIODS,
    clip_val: float = _ZSCORE_CLIP,
) -> Optional[float]:
    """Compute z-score at a single index (point-in-time)."""
    if idx < 0 or idx >= len(values) or values[idx] is None:
        return None
    start = max(0, idx - window + 1)
    window_vals = [v for v in values[start : idx + 1] if v is not None]
    if len(window_vals) < min_periods:
        return None
    mean = sum(window_vals) / len(window_vals)
    variance = sum((v - mean) ** 2 for v in window_vals) / len(window_vals)
    std = variance ** 0.5
    if std < 1e-10:
        return None
    z = (values[idx] - mean) / std
    return max(-clip_val, min(clip_val, z))


# ---------------------------------------------------------------------------
# Series alignment
# ---------------------------------------------------------------------------

def build_ts_to_idx(bars_5m: List[Dict]) -> Dict[int, int]:
    """Build a map from open_time to bar index."""
    return {b["open_time"]: i for i, b in enumerate(bars_5m)}


def align_to_5m(
    raw_series: List[Dict],
    ts_field: str,
    value_fields: List[str],
    bars_5m: List[Dict],
    ts_to_idx: Dict[int, int],
) -> Dict[str, List[Optional[float]]]:
    """Align a raw time-series to the 5m bars timeline.

    Returns a dict mapping field_name -> list aligned to bars_5m length.
    Missing bars are None (no forward-fill to avoid fake data).
    """
    n = len(bars_5m)
    result = {f: [None] * n for f in value_fields}

    # Build lookup from normalized ts to raw row
    lookup: Dict[int, Dict] = {}
    bucket = 5 * 60 * 1000
    for row in raw_series:
        ts = int(row.get(ts_field, 0))
        norm = (ts // bucket) * bucket
        lookup[norm] = row

    for i, bar in enumerate(bars_5m):
        ts = bar["open_time"]
        row = lookup.get(ts)
        if row is None:
            continue
        for f in value_fields:
            val = row.get(f)
            if val is not None:
                try:
                    result[f][i] = float(val)
                except (ValueError, TypeError):
                    pass

    return result


# ---------------------------------------------------------------------------
# OI delta
# ---------------------------------------------------------------------------

def compute_oi_delta_3bar_series(
    oi_aligned: List[Optional[float]],
) -> List[Optional[float]]:
    """Compute oi_delta_3bar_pct = (oi[i] - oi[i-3]) / max(oi[i-3], eps) * 100."""
    n = len(oi_aligned)
    result: List[Optional[float]] = [None] * n
    for i in range(3, n):
        curr = oi_aligned[i]
        prev = oi_aligned[i - 3]
        if curr is None or prev is None or prev <= 0:
            continue
        result[i] = (curr - prev) / prev * 100.0
    return result


# ---------------------------------------------------------------------------
# Volume ratio 3-bar
# ---------------------------------------------------------------------------

def compute_volume_ratio_3bar_series(
    bars_5m: List[Dict],
    recent_bars: int = 3,
    base_bars: int = 6,
) -> List[Optional[float]]:
    """volume_ratio_3bar = mean(vol[-3:]) / mean(vol[-9:-3]).

    Returns None where insufficient history.
    """
    n = len(bars_5m)
    result: List[Optional[float]] = [None] * n
    needed = recent_bars + base_bars
    for i in range(needed - 1, n):
        recent = [bars_5m[j]["volume"] for j in range(i - recent_bars + 1, i + 1)]
        base = [bars_5m[j]["volume"] for j in range(i - recent_bars - base_bars + 1, i - recent_bars + 1)]
        avg_recent = sum(recent) / len(recent)
        avg_base = sum(base) / max(len(base), 1)
        if avg_base <= 0:
            continue
        result[i] = avg_recent / avg_base
    return result


# ---------------------------------------------------------------------------
# Compression score
# ---------------------------------------------------------------------------

def compute_compression_score_for_window(
    window_bars: List[Dict],
    context_bars: List[Dict],
) -> Optional[float]:
    """Compute compression_score for a specific 6-bar window.

    window_bars: the 6 bars being evaluated
    context_bars: up to 12 bars BEFORE the window (for median_range_prev12)

    Returns score in [0, 1] or None if insufficient context.
    """
    if len(window_bars) < 6:
        return None

    prev12_bars = context_bars[-12:] if len(context_bars) >= 12 else context_bars
    if len(prev12_bars) < 6:
        return None

    ranges_last6 = [b["high"] - b["low"] for b in window_bars]
    ranges_prev12 = [b["high"] - b["low"] for b in prev12_bars]

    median_range_last6 = statistics.median(ranges_last6)
    median_range_prev12 = statistics.median(ranges_prev12)

    closes_last6 = [b["close"] for b in window_bars]
    close_span_last6 = max(closes_last6) - min(closes_last6)

    wick_sums = [upper_wick_ratio(b) + lower_wick_ratio(b) for b in window_bars]
    mean_wick_sum_last6 = sum(wick_sums) / len(wick_sums)

    denom = max(median_range_prev12, _EPS)

    range_contract = 1.0 - min(max(median_range_last6 / denom, 0.0), 1.5) / 1.5
    close_cluster = 1.0 - min(max(close_span_last6 / denom, 0.0), 1.5) / 1.5
    wick_cleanliness = 1.0 - min(max(mean_wick_sum_last6, 0.0), 1.0)

    score = (
        0.45 * range_contract
        + 0.35 * close_cluster
        + 0.20 * wick_cleanliness
    )
    return max(0.0, min(1.0, score))


# ---------------------------------------------------------------------------
# Break quality score
# ---------------------------------------------------------------------------

def compute_break_quality_score(
    bar: Dict,
    break_level: float,
    side: str,
    bars_before: List[Dict],
    oi_delta_3bar: Optional[float],
    volume_ratio_3bar: Optional[float],
) -> Dict:
    """Compute break_quality_score and break_quality_band.

    Per spec:
      break_distance = close_t - break_level  (reverse sign for SHORT)
      atr_proxy = median(range[-20:])
      break_strength = clip((break_distance / max(atr_proxy, eps)) / 1.5, 0, 1)
      body_quality = clip(body_ratio_t, 0, 1)
      wick_clean = 1 - clip(upper_wick_ratio_t, 0, 1)  [for LONG]
                   1 - clip(lower_wick_ratio_t, 0, 1)  [for SHORT]
      volume_support = clip(volume_ratio_3bar / 2.0, 0, 1)
      oi_support = clip(max(oi_delta_3bar_pct, 0) / 4.0, 0, 1)

    Returns dict with score, band, and components.
    """
    recent = bars_before[-20:] if len(bars_before) >= 20 else bars_before
    if not recent:
        return {
            "break_quality_score": None,
            "break_quality_band": "poor",
            "bq_components": {},
        }

    ranges = [b["high"] - b["low"] for b in recent]
    atr_proxy = statistics.median(ranges) if ranges else _EPS

    if side == "LONG":
        break_distance = bar["close"] - break_level
        wick_clean = 1.0 - min(max(upper_wick_ratio(bar), 0.0), 1.0)
    else:
        break_distance = break_level - bar["close"]
        wick_clean = 1.0 - min(max(lower_wick_ratio(bar), 0.0), 1.0)

    break_strength = min(max((break_distance / max(atr_proxy, _EPS)) / 1.5, 0.0), 1.0)
    body_quality = min(max(candle_body_ratio(bar), 0.0), 1.0)
    volume_support = (
        min(max((volume_ratio_3bar / 2.0), 0.0), 1.0)
        if volume_ratio_3bar is not None
        else 0.0
    )
    oi_support = (
        min(max(max(oi_delta_3bar, 0.0) / 4.0, 0.0), 1.0)
        if oi_delta_3bar is not None
        else 0.0
    )

    score = min(
        max(
            0.30 * break_strength
            + 0.20 * body_quality
            + 0.15 * wick_clean
            + 0.20 * volume_support
            + 0.15 * oi_support,
            0.0,
        ),
        1.0,
    )

    if score >= 0.75:
        band = "strong"
    elif score >= 0.60:
        band = "clean"
    elif score >= 0.45:
        band = "weak"
    else:
        band = "poor"

    return {
        "break_quality_score": score,
        "break_quality_band": band,
        "bq_components": {
            "break_strength": break_strength,
            "body_quality": body_quality,
            "wick_clean": wick_clean,
            "volume_support": volume_support,
            "oi_support": oi_support,
            "atr_proxy": atr_proxy,
        },
    }


# ---------------------------------------------------------------------------
# ProxyFeatures: aligned proxy series for one symbol
# ---------------------------------------------------------------------------

@dataclass
class ProxyFeatures:
    """All proxy series aligned to the 5m bar timeline for one symbol.

    Constructed once per symbol. Look up values by bar index using .at(idx).
    """

    symbol: str
    n: int  # number of 5m bars

    # Raw imbalance series
    top_acc_imbalance: List[Optional[float]] = field(default_factory=list)
    top_pos_imbalance: List[Optional[float]] = field(default_factory=list)
    global_acc_imbalance: List[Optional[float]] = field(default_factory=list)
    global_acc_imbalance_delta: List[Optional[float]] = field(default_factory=list)
    taker_imbalance: List[Optional[float]] = field(default_factory=list)
    basis_rate: List[Optional[float]] = field(default_factory=list)
    oi_value: List[Optional[float]] = field(default_factory=list)
    oi_delta_3bar: List[Optional[float]] = field(default_factory=list)
    volume_ratio_3bar: List[Optional[float]] = field(default_factory=list)

    # Z-score series
    z_top_pos: List[Optional[float]] = field(default_factory=list)
    z_top_acc: List[Optional[float]] = field(default_factory=list)
    z_oi_delta: List[Optional[float]] = field(default_factory=list)
    z_taker: List[Optional[float]] = field(default_factory=list)
    z_global_acc: List[Optional[float]] = field(default_factory=list)
    z_global_delta: List[Optional[float]] = field(default_factory=list)
    z_basis: List[Optional[float]] = field(default_factory=list)

    # Composite proxy series
    top_vs_global_divergence: List[Optional[float]] = field(default_factory=list)
    large_participant_proxy: List[Optional[float]] = field(default_factory=list)
    crowd_participation_proxy: List[Optional[float]] = field(default_factory=list)
    crowd_overheat_proxy: List[Optional[float]] = field(default_factory=list)

    # Data quality flag
    proxy_data_available: bool = False

    def at(self, idx: int) -> Dict:
        """Return a snapshot dict of all proxy values at bar index idx."""
        def _get(series, i):
            if not series or i < 0 or i >= len(series):
                return None
            return series[i]

        z_pos = _get(self.z_top_pos, idx)
        z_acc = _get(self.z_top_acc, idx)
        z_oid = _get(self.z_oi_delta, idx)
        z_tak = _get(self.z_taker, idx)
        z_g = _get(self.z_global_acc, idx)
        z_gd = _get(self.z_global_delta, idx)
        z_bas = _get(self.z_basis, idx)

        return {
            "top_acc_imbalance": _get(self.top_acc_imbalance, idx),
            "top_pos_imbalance": _get(self.top_pos_imbalance, idx),
            "global_acc_imbalance": _get(self.global_acc_imbalance, idx),
            "global_acc_imbalance_delta": _get(self.global_acc_imbalance_delta, idx),
            "taker_imbalance": _get(self.taker_imbalance, idx),
            "basis_rate": _get(self.basis_rate, idx),
            "oi_delta_3bar_pct": _get(self.oi_delta_3bar, idx),
            "volume_ratio_3bar": _get(self.volume_ratio_3bar, idx),
            "z_top_pos": z_pos,
            "z_top_acc": z_acc,
            "z_oi_delta": z_oid,
            "z_taker": z_tak,
            "z_global_acc": z_g,
            "z_global_delta": z_gd,
            "z_basis": z_bas,
            "top_vs_global_divergence": _get(self.top_vs_global_divergence, idx),
            "large_participant_proxy": _get(self.large_participant_proxy, idx),
            "crowd_participation_proxy": _get(self.crowd_participation_proxy, idx),
            "crowd_overheat_proxy": _get(self.crowd_overheat_proxy, idx),
        }


def _weighted_sum(components: List) -> Optional[float]:
    """Weighted sum of (weight, value) pairs. Renormalize if some are None."""
    available = [(w, v) for w, v in components if v is not None]
    if not available:
        return None
    total_w = sum(w for w, _ in available)
    if total_w < _EPS:
        return None
    return sum(w * v for w, v in available) / total_w


def build_proxy_features(
    symbol: str,
    bars_5m: List[Dict],
    raw: Dict,
) -> ProxyFeatures:
    """Build a ProxyFeatures object for one symbol.

    raw: dict with keys:
      oi_hist, top_account, top_position, global_account, taker, basis
    Each is a list of dicts from ResearchBinanceClient.
    """
    n = len(bars_5m)
    ts_to_idx = build_ts_to_idx(bars_5m)

    pf = ProxyFeatures(symbol=symbol, n=n)

    # --- OI value aligned ---
    oi_aligned_raw = align_to_5m(
        raw.get("oi_hist", []), "ts", ["oi_value"], bars_5m, ts_to_idx
    )
    pf.oi_value = oi_aligned_raw["oi_value"]
    pf.oi_delta_3bar = compute_oi_delta_3bar_series(pf.oi_value)

    # --- Volume ratio ---
    pf.volume_ratio_3bar = compute_volume_ratio_3bar_series(bars_5m)

    # --- Top account imbalance ---
    top_acc = align_to_5m(
        raw.get("top_account", []), "ts", ["long_pct", "short_pct"], bars_5m, ts_to_idx
    )
    pf.top_acc_imbalance = [
        (a - b) if a is not None and b is not None else None
        for a, b in zip(top_acc["long_pct"], top_acc["short_pct"])
    ]

    # --- Top position imbalance ---
    top_pos = align_to_5m(
        raw.get("top_position", []), "ts", ["long_pct", "short_pct"], bars_5m, ts_to_idx
    )
    pf.top_pos_imbalance = [
        (a - b) if a is not None and b is not None else None
        for a, b in zip(top_pos["long_pct"], top_pos["short_pct"])
    ]

    # --- Global account imbalance ---
    global_acc = align_to_5m(
        raw.get("global_account", []), "ts", ["long_pct", "short_pct"], bars_5m, ts_to_idx
    )
    pf.global_acc_imbalance = [
        (a - b) if a is not None and b is not None else None
        for a, b in zip(global_acc["long_pct"], global_acc["short_pct"])
    ]

    # --- Global imbalance delta ---
    pf.global_acc_imbalance_delta = [None] * n
    for i in range(1, n):
        curr = pf.global_acc_imbalance[i]
        prev = pf.global_acc_imbalance[i - 1]
        if curr is not None and prev is not None:
            pf.global_acc_imbalance_delta[i] = curr - prev

    # --- Taker imbalance ---
    taker = align_to_5m(
        raw.get("taker", []), "ts", ["buy_vol", "sell_vol"], bars_5m, ts_to_idx
    )
    pf.taker_imbalance = []
    for bv, sv in zip(taker["buy_vol"], taker["sell_vol"]):
        if bv is not None and sv is not None:
            total = bv + sv
            pf.taker_imbalance.append((bv - sv) / max(total, _EPS))
        else:
            pf.taker_imbalance.append(None)

    # --- Basis rate ---
    basis_raw = align_to_5m(
        raw.get("basis", []), "ts", ["basis_rate"], bars_5m, ts_to_idx
    )
    pf.basis_rate = basis_raw["basis_rate"]

    # --- Z-scores ---
    pf.z_top_pos = rolling_zscore_series(pf.top_pos_imbalance)
    pf.z_top_acc = rolling_zscore_series(pf.top_acc_imbalance)
    pf.z_oi_delta = rolling_zscore_series(pf.oi_delta_3bar)
    pf.z_taker = rolling_zscore_series(pf.taker_imbalance)
    pf.z_global_acc = rolling_zscore_series(pf.global_acc_imbalance)
    pf.z_global_delta = rolling_zscore_series(pf.global_acc_imbalance_delta)
    pf.z_basis = rolling_zscore_series(pf.basis_rate)

    # --- Top vs global divergence ---
    pf.top_vs_global_divergence = [
        (zp - zg) if zp is not None and zg is not None else None
        for zp, zg in zip(pf.z_top_pos, pf.z_global_acc)
    ]

    # --- Large participant proxy ---
    # 0.45*z(top_pos) + 0.20*z(top_acc) + 0.20*z(oi_delta) + 0.15*z(taker)
    pf.large_participant_proxy = [
        _weighted_sum([
            (0.45, pf.z_top_pos[i]),
            (0.20, pf.z_top_acc[i]),
            (0.20, pf.z_oi_delta[i]),
            (0.15, pf.z_taker[i]),
        ])
        for i in range(n)
    ]

    # --- Crowd participation proxy ---
    # 0.45*z(global_acc) + 0.20*z(global_delta) + 0.20*z(taker) + 0.15*z(basis)
    pf.crowd_participation_proxy = [
        _weighted_sum([
            (0.45, pf.z_global_acc[i]),
            (0.20, pf.z_global_delta[i]),
            (0.20, pf.z_taker[i]),
            (0.15, pf.z_basis[i]),
        ])
        for i in range(n)
    ]

    # --- Crowd overheat proxy ---
    # 0.70 * max(0, crowd) + 0.30 * max(0, z_basis)
    pf.crowd_overheat_proxy = [
        _compute_overheat(pf.crowd_participation_proxy[i], pf.z_basis[i])
        for i in range(n)
    ]

    # Check if proxy data was actually available
    has_data = any(v is not None for v in pf.top_pos_imbalance)
    pf.proxy_data_available = has_data

    return pf


def _compute_overheat(
    crowd: Optional[float], z_basis: Optional[float]
) -> Optional[float]:
    """crowd_overheat = 0.70*max(0, crowd) + 0.30*max(0, z_basis)"""
    parts = []
    if crowd is not None:
        parts.append((0.70, max(0.0, crowd)))
    if z_basis is not None:
        parts.append((0.30, max(0.0, z_basis)))
    if not parts:
        return None
    total_w = sum(w for w, _ in parts)
    return sum(w * v for w, v in parts) / total_w


def compute_flow_composite(
    large_proxy: Optional[float],
    z_oi_delta: Optional[float],
    z_taker: Optional[float],
    break_quality_score: Optional[float],
    overheat: Optional[float],
) -> Optional[float]:
    """flow_composite_signal =
        0.40 * large_participant_proxy
      + 0.20 * z(oi_delta_3bar_pct)
      + 0.15 * z(taker_imbalance)
      + 0.10 * break_quality_score
      - 0.15 * crowd_overheat_proxy
    """
    pos_parts = [
        (0.40, large_proxy),
        (0.20, z_oi_delta),
        (0.15, z_taker),
        (0.10, break_quality_score),
    ]
    neg_parts = [(0.15, overheat)]

    pos_available = [(w, v) for w, v in pos_parts if v is not None]
    neg_available = [(w, v) for w, v in neg_parts if v is not None]

    if not pos_available and not neg_available:
        return None

    total_w = sum(w for w, _ in pos_parts) + sum(w for w, _ in neg_parts)
    avail_w = sum(w for w, _ in pos_available) + sum(w for w, _ in neg_available)

    if avail_w < 0.30 * total_w:
        return None

    val = (
        sum(w * v for w, v in pos_available)
        - sum(w * v for w, v in neg_available)
    ) / avail_w * total_w / total_w

    # Re-normalize: just use available weighted sum directly
    pos_sum = sum(w * v for w, v in pos_available)
    neg_sum = sum(w * v for w, v in neg_available)
    return pos_sum - neg_sum


# ---------------------------------------------------------------------------
# BTC context fields (for Context / Layer 7 answer contract)
# ---------------------------------------------------------------------------

def compute_btc_context_fields(
    p2_ts_ms: int,
    btc_bars_15m: List[Dict],
    btc_bars_1h: List[Dict],
) -> Dict:
    """
    Compute three BTC context fields at the moment of P2 (break bar).

    btc_change_15m_pct
        % change of BTC close in the 15m bar containing p2_ts_ms
        vs the immediately preceding 15m bar.
        Null if bar not found or no previous bar.

    btc_change_1h_pct
        Same logic for 1h bars.

    market_volatility_proxy
        mean((high / low) - 1.0) across all BTC 1h bars provided.
        Represents intraday BTC volatility for the research session.
        Null if no 1h bars available.

    Null policy: conservative. Never invent defaults.
    """
    result: Dict = {
        "btc_change_15m_pct": None,
        "btc_change_1h_pct": None,
        "market_volatility_proxy": None,
    }

    def _find_bar_idx(bars: List[Dict], ts_ms: int) -> Optional[int]:
        """Index of bar whose [open_time, close_time] interval contains ts_ms."""
        for i, b in enumerate(bars):
            open_t  = b.get("open_time", 0)
            close_t = b.get("close_time", open_t)
            if open_t <= ts_ms <= close_t:
                return i
        return None

    def _pct_change(curr: Optional[float], prev: Optional[float]) -> Optional[float]:
        if curr is None or prev is None or abs(prev) < _EPS:
            return None
        return round((curr - prev) / prev * 100.0, 4)

    # btc_change_15m_pct
    if btc_bars_15m and p2_ts_ms:
        idx = _find_bar_idx(btc_bars_15m, p2_ts_ms)
        if idx is not None and idx > 0:
            result["btc_change_15m_pct"] = _pct_change(
                btc_bars_15m[idx].get("close"),
                btc_bars_15m[idx - 1].get("close"),
            )

    # btc_change_1h_pct
    if btc_bars_1h and p2_ts_ms:
        idx = _find_bar_idx(btc_bars_1h, p2_ts_ms)
        if idx is not None and idx > 0:
            result["btc_change_1h_pct"] = _pct_change(
                btc_bars_1h[idx].get("close"),
                btc_bars_1h[idx - 1].get("close"),
            )

    # market_volatility_proxy: mean((high/low) - 1) across provided 1h bars
    if btc_bars_1h:
        hl_ratios = []
        for b in btc_bars_1h:
            h = b.get("high")
            lo = b.get("low")
            if h is not None and lo is not None and lo > _EPS:
                hl_ratios.append(h / lo - 1.0)
        if hl_ratios:
            result["market_volatility_proxy"] = round(
                sum(hl_ratios) / len(hl_ratios), 6
            )

    return result
