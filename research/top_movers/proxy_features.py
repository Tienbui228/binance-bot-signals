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
# Wave 2 — retest volume decay ratio (L5 volume field)
# ---------------------------------------------------------------------------

def compute_retest_volume_decay_ratio(
    bars_5m: List[Dict],
    break_bar_idx: Optional[int],
    retest_start_idx: Optional[int],
    retest_extreme_idx: Optional[int],
) -> Optional[float]:
    """Volume decay during retest zone vs. break bar volume.

    Ratio = mean(volume in retest zone) / volume(break bar).
    < 1.0 → volume decaying during retest (healthy pullback).
    >= 1.0 → volume not decaying (risky — retest may not hold).

    Returns None if indices are invalid or break bar volume is zero.
    """
    if break_bar_idx is None or retest_start_idx is None or retest_extreme_idx is None:
        return None
    if break_bar_idx < 0 or break_bar_idx >= len(bars_5m):
        return None
    break_vol = bars_5m[break_bar_idx].get("volume")
    if not break_vol or break_vol <= _EPS:
        return None
    start = min(retest_start_idx, retest_extreme_idx)
    end = max(retest_start_idx, retest_extreme_idx) + 1
    retest_bars = bars_5m[start:end]
    if not retest_bars:
        return None
    mean_vol = sum(b["volume"] for b in retest_bars if b.get("volume") is not None) / len(retest_bars)
    return round(mean_vol / break_vol, 4)


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


# ---------------------------------------------------------------------------
# V4-3 — Layer 3 Exhaustion Measurement
# ---------------------------------------------------------------------------

_ONE_HOUR_MS = 3_600_000
_EMA_PERIOD_EXHAUST = 20
_WICK_CLUSTER_N = 5          # bars before P1 for wick cluster scan
_WICK_CLUSTER_THRESH = 0.4   # upper_wick_ratio threshold
_BLOWOFF_BASELINE_N = 20     # baseline bars for blowoff ratio
_BLOWOFF_MIN_BARS = 3        # minimum bars to compute blowoff


def _compute_ema(values: List[float], period: int) -> List[Optional[float]]:
    """EMA series. Seed = SMA of first `period` values. Returns same-length list."""
    n = len(values)
    result: List[Optional[float]] = [None] * n
    if n < period:
        return result
    k = 2.0 / (period + 1)
    ema = sum(values[:period]) / period
    result[period - 1] = ema
    for i in range(period, n):
        ema = values[i] * k + ema * (1 - k)
        result[i] = ema
    return result


def compute_exhaustion_pack(
    symbol: str,
    p0_price: Optional[float],
    p0_ts_ms: Optional[int],
    p1_ts_ms: Optional[int],
    p1_price: Optional[float],
    peak_close: Optional[float],
    p1_low: Optional[float],
    p2_ts_ms: Optional[int],
    bars_1h: List[Dict],
) -> Dict:
    """All 10 Layer 3 exhaustion fields. Returns EMPTY dict on guard failure.

    bars_1h: pre-fetched 1h klines (dicts with keys: open_time, high, low,
             close, volume). No new API call is made inside this function.
    """
    _EMPTY = {
        "pre_break_extension_pct_from_local_base": None,
        "pre_break_extension_pct_from_vwap":       None,
        "pre_break_extension_pct_from_ema":        None,
        "peak_upper_wick_ratio":                   None,
        "wick_cluster_count_last_n_bars":          0,
        "blowoff_volume_ratio":                    None,
        "close_location_near_low_score":           None,
        "failed_continuation_count":               0,
        "exhaustion_strength_raw":                 None,
        "exhaustion_strength_bucket":              None,
    }

    if not bars_1h or p1_ts_ms is None:
        return _EMPTY

    # Locate P1 bar — the 1h bar containing p1_ts_ms (5m open_time)
    p1_bar_idx = None
    for i, bar in enumerate(bars_1h):
        if bar["open_time"] <= p1_ts_ms < bar["open_time"] + _ONE_HOUR_MS:
            p1_bar_idx = i
            break
    if p1_bar_idx is None:
        return _EMPTY

    p1_1h_bar = bars_1h[p1_bar_idx]

    # Locate P2 bar
    p2_bar_idx = None
    if p2_ts_ms is not None:
        for i, bar in enumerate(bars_1h):
            if bar["open_time"] <= p2_ts_ms < bar["open_time"] + _ONE_HOUR_MS:
                p2_bar_idx = i
                break

    # Locate P0 bar
    p0_bar_idx = None
    if p0_ts_ms is not None:
        for i, bar in enumerate(bars_1h):
            if bar["open_time"] <= p0_ts_ms < bar["open_time"] + _ONE_HOUR_MS:
                p0_bar_idx = i
                break

    # [1] pre_break_extension_pct_from_local_base = (p1_price - p0_price) / p0_price * 100
    pbe_local = None
    if p1_price is not None and p0_price is not None and abs(p0_price) > _EPS:
        pbe_local = round((p1_price - p0_price) / p0_price * 100.0, 4)

    # [2] pre_break_extension_pct_from_vwap
    #     VWAP over bars_1h[p0_bar_idx .. p1_bar_idx] inclusive
    pbe_vwap = None
    if p0_bar_idx is not None:
        vwap_bars = bars_1h[p0_bar_idx: p1_bar_idx + 1]
        total_vol = sum(b["volume"] for b in vwap_bars)
        if total_vol > _EPS:
            vwap_val = sum(
                ((b["high"] + b["low"] + b["close"]) / 3.0) * b["volume"]
                for b in vwap_bars
            ) / total_vol
            if p1_price is not None and abs(vwap_val) > _EPS:
                pbe_vwap = round((p1_price - vwap_val) / vwap_val * 100.0, 4)

    # [3] pre_break_extension_pct_from_ema
    #     EMA20 of close, all bars_1h up to and including P1 bar (no lookahead)
    pbe_ema = None
    closes_to_p1 = [b["close"] for b in bars_1h[: p1_bar_idx + 1]]
    if len(closes_to_p1) >= _EMA_PERIOD_EXHAUST:
        ema_series = _compute_ema(closes_to_p1, _EMA_PERIOD_EXHAUST)
        ema_at_p1 = ema_series[p1_bar_idx] if p1_bar_idx < len(ema_series) else None
        if ema_at_p1 is not None and abs(ema_at_p1) > _EPS and p1_price is not None:
            pbe_ema = round((p1_price - ema_at_p1) / ema_at_p1 * 100.0, 4)

    # [4] peak_upper_wick_ratio = (p1_price - peak_close) / (p1_price - p1_low)
    #     Uses P1 5m bar OHLC directly (not the 1h bar)
    peak_upper_wick = None
    if p1_price is not None and peak_close is not None and p1_low is not None:
        bar_range = p1_price - p1_low
        if bar_range > _EPS:
            peak_upper_wick = round((p1_price - peak_close) / bar_range, 4)
        else:
            peak_upper_wick = 0.0  # doji bar

    # [5] wick_cluster_count_last_n_bars
    #     Count of 1h bars in [p1_bar_idx-5 : p1_bar_idx) with upper_wick_ratio >= 0.4
    wick_start = max(0, p1_bar_idx - _WICK_CLUSTER_N)
    wick_window = bars_1h[wick_start: p1_bar_idx]
    wick_cluster = sum(1 for b in wick_window if upper_wick_ratio(b) >= _WICK_CLUSTER_THRESH)

    # [6] blowoff_volume_ratio = P1_1h_bar volume / mean(baseline vols)
    #     Baseline = up to 20 bars before P1 bar
    blowoff_ratio = None
    bl_start = max(0, p1_bar_idx - _BLOWOFF_BASELINE_N)
    baseline_bars = bars_1h[bl_start: p1_bar_idx]
    if len(baseline_bars) >= _BLOWOFF_MIN_BARS:
        avg_vol = sum(b["volume"] for b in baseline_bars) / len(baseline_bars)
        if avg_vol > _EPS:
            blowoff_ratio = round(p1_1h_bar["volume"] / avg_vol, 4)

    # [7] close_location_near_low_score = 1 - (peak_close - p1_low) / (p1_price - p1_low)
    #     Score near 1 = close near low (bearish exhaustion); near 0 = close near high
    close_loc = None
    if p1_price is not None and peak_close is not None and p1_low is not None:
        bar_range = p1_price - p1_low
        if bar_range > _EPS:
            close_loc = round(
                max(0.0, min(1.0, 1.0 - (peak_close - p1_low) / bar_range)), 4
            )
        else:
            close_loc = 0.5  # doji — neutral

    # [8] failed_continuation_count
    #     Bars in bars_1h (p1_bar_idx+1 .. p2_bar_idx) where close < open
    failed_cont = 0
    if p2_bar_idx is not None and p2_bar_idx > p1_bar_idx + 1:
        between = bars_1h[p1_bar_idx + 1: p2_bar_idx]
        failed_cont = sum(1 for b in between if b["close"] < b["open"])

    # [9-10] exhaustion_strength_raw + bucket
    #        Weighted composite; weights renormalized automatically for None components
    _wick_clust_norm = min(wick_cluster / 5.0, 1.0)  # always float
    _blowoff_norm = (
        min(max((blowoff_ratio - 1.0) / 4.0, 0.0), 1.0)
        if blowoff_ratio is not None else None
    )
    _failed_norm = min(failed_cont / 3.0, 1.0)  # always float

    raw_score = _weighted_sum([
        (0.30, peak_upper_wick),
        (0.20, _wick_clust_norm),
        (0.25, _blowoff_norm),
        (0.15, close_loc),
        (0.10, _failed_norm),
    ])
    if raw_score is not None:
        raw_score = round(raw_score, 4)

    bucket = None
    if raw_score is not None:
        if raw_score >= 0.65:
            bucket = "STRONG"
        elif raw_score >= 0.35:
            bucket = "MODERATE"
        else:
            bucket = "WEAK"

    return {
        "pre_break_extension_pct_from_local_base": pbe_local,
        "pre_break_extension_pct_from_vwap":       pbe_vwap,
        "pre_break_extension_pct_from_ema":        pbe_ema,
        "peak_upper_wick_ratio":                   peak_upper_wick,
        "wick_cluster_count_last_n_bars":          wick_cluster,
        "blowoff_volume_ratio":                    blowoff_ratio,
        "close_location_near_low_score":           close_loc,
        "failed_continuation_count":               failed_cont,
        "exhaustion_strength_raw":                 raw_score,
        "exhaustion_strength_bucket":              bucket,
    }


# ---------------------------------------------------------------------------
# V4-5 — Layer 7 OI Regime + Funding Fields
# ---------------------------------------------------------------------------

def compute_oi_regime_pack(
    symbol: str,
    p3_ts_ms,
    p2_price,
    p3_price,
    research_day: str,
    client,
) -> Dict:
    """Compute 8 Layer 7 OI/funding fields. Makes at most 2 API calls when p3_ts_ms available."""
    from datetime import datetime, timezone, timedelta

    _EMPTY: Dict = {
        "oi_regime_label":           "OI_UNCLEAR_OR_MISSING",
        "oi_change_1h_pct":          None,
        "oi_change_4h_pct":          None,
        "oi_data_unavailable":       False,
        "oi_context_note":           "",
        "latest_funding_rate_raw":   None,
        "latest_funding_rate_pct":   None,
        "funding_rate_bucket":       None,
    }

    # 30-day limit check
    thirty_days_ago_ms = None
    try:
        today = datetime.strptime(research_day, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        thirty_days_ago_ms = int((today - timedelta(days=30)).timestamp() * 1000)
    except Exception:
        pass

    p3_ms = None
    if p3_ts_ms:
        try:
            p3_ms = int(p3_ts_ms)
        except (TypeError, ValueError):
            pass

    if p3_ms is None:
        result = dict(_EMPTY)
        result["oi_context_note"] = "no_p3_timestamp"
        return result

    oi_unavailable = (
        thirty_days_ago_ms is not None and p3_ms < thirty_days_ago_ms
    )

    if oi_unavailable:
        result = dict(_EMPTY)
        result["oi_data_unavailable"] = True
        result["oi_regime_label"]     = "OI_UNCLEAR_OR_MISSING"
        result["oi_context_note"]     = "oi_history_beyond_30day_limit"
        result.update(_compute_funding_fields(_safe_fetch_funding(symbol, p3_ms, client)))
        return result

    # Fetch OI changes (1 API call covers 1h and 4h)
    try:
        oi_data = client.fetch_oi_changes(symbol, end_ts_ms=p3_ms)
        oi_1h = oi_data.get("oi_change_1h_pct")
        oi_4h = oi_data.get("oi_change_4h_pct")
    except Exception:
        oi_1h = None
        oi_4h = None

    # Fetch funding rate (1 API call)
    funding_raw = _safe_fetch_funding(symbol, p3_ms, client)

    # OI regime classification using P2→P3 price direction as proxy
    price_direction_negative = False
    if p3_price is not None and p2_price is not None and p2_price > 0:
        p2_to_p3_change = (float(p3_price) - float(p2_price)) / float(p2_price) * 100
        price_direction_negative = p2_to_p3_change < 0

    if oi_1h is None:
        oi_regime = "OI_UNCLEAR_OR_MISSING"
        oi_note = "oi_fetch_failed"
    elif abs(oi_1h) <= 2.0 and price_direction_negative:
        oi_regime = "OI_STABLE_PRICE_FAILS"
        oi_note = ""
    elif oi_1h > 0 and price_direction_negative:
        oi_regime = "OI_EXPANSION_PRICE_DOWN"
        oi_note = ""
    elif oi_1h < 0 and price_direction_negative:
        oi_regime = "OI_CONTRACTION_AFTER_DUMP"
        oi_note = ""
    else:
        oi_regime = "OI_UNCLEAR_OR_MISSING"
        oi_note = "price_not_falling_at_p3"

    return {
        "oi_regime_label":     oi_regime,
        "oi_change_1h_pct":    oi_1h,
        "oi_change_4h_pct":    oi_4h,
        "oi_data_unavailable": oi_unavailable,
        "oi_context_note":     oi_note,
        **_compute_funding_fields(funding_raw),
    }


def _safe_fetch_funding(symbol: str, p3_ms: int, client) -> Optional[float]:
    try:
        return client.fetch_funding_rate_at(symbol, end_ts_ms=p3_ms)
    except Exception:
        return None


def _compute_funding_fields(funding_raw: Optional[float]) -> Dict:
    """Convert raw funding decimal to pct and bucket. Both representations always persisted."""
    if funding_raw is None:
        return {
            "latest_funding_rate_raw": None,
            "latest_funding_rate_pct": None,
            "funding_rate_bucket":     None,
        }

    # Round raw first, then derive pct from rounded value so pct == raw_stored × 100
    funding_raw_stored = round(funding_raw, 6)
    funding_pct = round(funding_raw_stored * 100, 6)

    if funding_raw_stored < -0.0001:
        bucket = "negative"
    elif funding_raw_stored <= 0.0001:
        bucket = "neutral"
    elif funding_raw_stored <= 0.0005:
        bucket = "slightly_long"
    else:
        bucket = "crowded_long"

    return {
        "latest_funding_rate_raw": funding_raw_stored,
        "latest_funding_rate_pct": funding_pct,
        "funding_rate_bucket":     bucket,
    }


# ---------------------------------------------------------------------------
# V4-4 — Layer 5 P3 Tradability Fields
# ---------------------------------------------------------------------------

_LIQUIDITY_MIN_USDT  = 10_000
_STOP_BUFFER         = 0.01        # 1% above breakdown_level for short stop
_MAX_SIGNAL_AGE_MIN  = 15
_MIN_RR              = 1.5
_MAX_ENTRY_ABOVE_BREAK_PCT = 2.0   # entry must be <= breakdown_level * 1.02
_RETEST_TOL_PCT      = 2.0         # retest within ±2% of breakdown_level


def compute_p3_tradability(
    symbol: str,
    p3_ts_ms,
    retest_depth_vs_break_pct,
    retest_reject_wick_ratio,
    retest_close_back_above_break_flag,
    bars_to_retest,
    retest_pack_available,
    breakdown_level,
    client,
) -> Dict:
    """Compute 9 P3 tradability fields (Layer 5 V4-4).

    Makes 1 API call (5m klines around P3) when p3_ts_ms is available.
    Returns dict of all 9 fields. Returns EMPTY dict (all None) on no retest or fetch failure.
    """
    EMPTY = {
        "quote_vol_5m_median_at_p3_usdt":   None,
        "live_liquidity_gate_pass":          None,
        "p3_detectable_in_live_mode_flag":   None,
        "live_signal_age_min":               None,
        "live_entry_price_proxy":            None,
        "live_stop_price_proxy":             None,
        "live_rr_conservative":              None,
        "entry_feasible_flag":               None,
        "entry_feasible_reason":             "no_retest",
    }

    if not p3_ts_ms or not retest_pack_available or client is None:
        return EMPTY

    FIVE_MIN_MS = 300_000
    try:
        p3_ms = int(p3_ts_ms)
    except (TypeError, ValueError):
        return EMPTY

    # Step 1: fetch 5m klines around P3 (1 API call)
    try:
        bars_5m = client.get_klines(
            symbol=symbol,
            interval="5m",
            start_ms=p3_ms - 2 * FIVE_MIN_MS,
            end_ms=p3_ms + 3 * FIVE_MIN_MS,
            limit=6,
        )
        if not bars_5m:
            return EMPTY
    except Exception:
        return EMPTY

    # Step 2: quote_vol_5m_median_at_p3_usdt — median quote_volume across fetched bars
    quote_vols = []
    for bar in bars_5m:
        try:
            qv = float(bar["quote_volume"])
            if qv > 0:
                quote_vols.append(qv)
        except (KeyError, ValueError, TypeError):
            pass

    if quote_vols:
        sorted_qv = sorted(quote_vols)
        n = len(sorted_qv)
        median_qvol = sorted_qv[n // 2] if n % 2 == 1 else (sorted_qv[n // 2 - 1] + sorted_qv[n // 2]) / 2
        median_qvol = round(median_qvol, 2)
    else:
        median_qvol = None

    # Step 3: live_liquidity_gate_pass
    liq_gate = (median_qvol >= _LIQUIDITY_MIN_USDT) if median_qvol is not None else None

    # Step 4: live_entry_price_proxy — close of the 5m bar AFTER P3 bar
    entry_bar_open_ms = p3_ms + FIVE_MIN_MS
    live_entry_price = None
    for bar in bars_5m:
        try:
            if int(bar["open_time"]) == entry_bar_open_ms:
                live_entry_price = float(bar["close"])
                break
        except (KeyError, ValueError, TypeError):
            pass

    # Step 5: live_signal_age_min = bars_to_retest × 5
    signal_age_min = None
    if bars_to_retest is not None:
        try:
            signal_age_min = int(bars_to_retest) * 5
        except (TypeError, ValueError):
            pass

    # Step 6: live_stop_price_proxy — 1% above breakdown_level (SHORT: stop above entry)
    live_stop_price = None
    if breakdown_level and breakdown_level > 0:
        live_stop_price = round(float(breakdown_level) * (1 + _STOP_BUFFER), 8)

    # Step 7: live_rr_conservative — for SHORT: risk = stop − entry, target = entry − 1.5×risk
    live_rr = None
    if live_entry_price is not None and live_stop_price is not None:
        risk = live_stop_price - live_entry_price  # positive when stop > entry (correct for short)
        if risk > 0:
            live_rr = round(1.5 * risk / risk, 4)   # = 1.5 by construction; kept explicit for clarity

    # Step 8: p3_detectable_in_live_mode_flag — 4 conditions
    cond1_depth = (
        retest_depth_vs_break_pct is not None
        and abs(float(retest_depth_vs_break_pct)) <= _RETEST_TOL_PCT
    )
    cond2_rejection = (
        retest_reject_wick_ratio is not None
        and float(retest_reject_wick_ratio) > 0
    )
    cond3_no_reclaim = (
        retest_close_back_above_break_flag is not None
        and str(retest_close_back_above_break_flag).strip().upper() == "N"
    )
    cond4_liquidity = liq_gate is True

    p3_detectable = all([cond1_depth, cond2_rejection, cond3_no_reclaim, cond4_liquidity])

    # Step 9: entry_feasible_flag + entry_feasible_reason
    reasons = []
    if not p3_detectable:
        reasons.append("p3_not_detectable")
    if signal_age_min is None or signal_age_min > _MAX_SIGNAL_AGE_MIN:
        reasons.append("signal_too_old")
    if live_rr is None or live_rr < _MIN_RR:
        reasons.append("rr_too_low")
    if live_entry_price is not None and breakdown_level:
        if live_entry_price > float(breakdown_level) * (1 + _MAX_ENTRY_ABOVE_BREAK_PCT / 100):
            reasons.append("entry_price_too_high")
    if not cond4_liquidity:
        reasons.append("liquidity_insufficient")

    entry_feasible = (len(reasons) == 0)
    entry_feasible_reason = reasons[0] if reasons else "feasible"

    return {
        "quote_vol_5m_median_at_p3_usdt":   median_qvol,
        "live_liquidity_gate_pass":          liq_gate,
        "p3_detectable_in_live_mode_flag":   p3_detectable,
        "live_signal_age_min":               signal_age_min,
        "live_entry_price_proxy":            round(live_entry_price, 8) if live_entry_price is not None else None,
        "live_stop_price_proxy":             live_stop_price,
        "live_rr_conservative":              live_rr,
        "entry_feasible_flag":               entry_feasible,
        "entry_feasible_reason":             entry_feasible_reason,
    }
