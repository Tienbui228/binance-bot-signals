"""
research/top_movers/anchor_detector.py

Detects P0–P4 anchors for one token-day on the 5m timeline.

Algorithm (per spec):
  Step 1: Search backward 18 bars before break_candidate for best 6-bar compression window
          → defines range_high, range_low, P0
  Step 2: P1 = first bar after P0 window and before P2 meeting ignition criteria
  Step 3: P2 = first bar meeting break quality criteria
  Step 4: P3 = bar with max directional extension in 12 bars after P2
  Step 5: P4 = first 15m bar meeting resolution criteria, or timeout at P3+60min

All anchor timestamps are on the 5m timeline.
P4 is resolved against the 15m bars.
"""

import statistics
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from research.top_movers.proxy_features import (
    compute_break_quality_score,
    compute_compression_score_for_window,
)
from research.top_movers.canonical_move import CanonicalMove

try:
    from scanner.market_math import upper_wick_ratio, lower_wick_ratio, candle_body_ratio
except ImportError:
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


_P3_LOOKFORWARD_BARS = 12         # 12 x 5m = 60 min after P2
_COMPRESSION_SEARCH_WINDOW = 18   # search 18 bars back from break candidate
_COMPRESSION_WINDOW_SIZE = 6      # compression window is 6 bars


@dataclass
class AnchorPoint:
    code: str            # P0 / P1 / P2 / P3 / P4
    bar_idx: int         # index in bars_5m
    ts_ms: int
    bar: Dict            # the OHLC bar dict at this anchor
    detect_method: str = "AUTO_V1"
    auto_confidence: float = 1.0
    fallback_used: bool = False
    # P0 specific
    compression_score: Optional[float] = None
    # P2 specific
    break_quality_score: Optional[float] = None
    break_quality_band: Optional[str] = None
    bq_components: Dict = field(default_factory=dict)
    # P3 specific
    directional_extension_pct: Optional[float] = None


@dataclass
class AnchorSet:
    symbol: str
    side: str
    range_high: float
    range_low: float
    compression_window_start_idx: int
    compression_window_end_idx: int
    p0: AnchorPoint
    p1: Optional[AnchorPoint]
    p2: AnchorPoint
    p3: AnchorPoint
    p4: Optional[AnchorPoint]
    detection_ok: bool = True
    detection_note: str = ""


# ---------------------------------------------------------------------------
# Wave 2 — L4 Breakdown pack
# ---------------------------------------------------------------------------

@dataclass
class BreakdownPack:
    """L4 fields measured at and immediately after the P2 break bar."""
    break_distance_pct: Optional[float]              # close_p2 vs break_level, pct
    break_close_strength: Optional[float]            # PROVISIONAL: body_quality from bq_components
    break_bar_body_ratio: Optional[float]            # PROVEN: candle_body_ratio(p2_bar)
    break_volume_ratio: Optional[float]              # PROVISIONAL: volume_ratio_3bar at p2_idx
    support_test_count: int                          # bars testing break_level in P0→P2 zone
    break_cleanliness_score: Optional[float]         # PROVEN: = break_quality_score
    immediate_followthrough_pct_1bar: Optional[float]
    immediate_followthrough_pct_3bar: Optional[float]
    false_break_reclaim_fast_flag: str               # Y/N


# ---------------------------------------------------------------------------
# Wave 2 — L5 Retest fail / reclaim pack
# ---------------------------------------------------------------------------

@dataclass
class RetestFailPack:
    """L5 fields measuring retest geometry in P3→P4 zone (dual-direction)."""
    pack_available: bool                              # False when P3 or P4 is None
    reclaim_pct: Optional[float]                     # pullback from P3 toward break_level, pct
    retest_depth_vs_break_pct: Optional[float]       # retest_extreme distance from break_level, pct
    retest_duration_bars: Optional[int]              # bars from P3 to retest_extreme
    retest_reject_wick_ratio: Optional[float]        # wick at retest_extreme bar (direction-aware)
    retest_rejection_quality: str                    # strong/clean/weak/none from wick ratio
    retest_close_back_above_break_flag: str          # Y/N: close at retest_extreme vs break_level
    fail_strength: Optional[float]                   # 0–1 normalized: adverse move after retest_extreme
    fail_confirmation_bar_count: Optional[int]       # bars closing through break_level after retest
    max_reclaim_before_resolution_pct: Optional[float]  # max recovery from retest_extreme to P4
    second_rejection_present_flag: str               # Y/N: second touch of break_level after retest
    bars_to_retest: Optional[int]                    # same as retest_duration_bars (L6 alias)
    bars_to_fail: Optional[int]                      # bars from retest_extreme to fail confirmation
    # internal: used by proxy_features.compute_retest_volume_decay_ratio
    retest_start_bar_idx: Optional[int]
    retest_extreme_bar_idx: Optional[int]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _find_best_compression_window(
    bars_5m: List[Dict],
    before_idx: int,
    search_window: int = _COMPRESSION_SEARCH_WINDOW,
    window_size: int = _COMPRESSION_WINDOW_SIZE,
) -> Optional[Dict]:
    """Find the best 6-bar compression window in the 18 bars before before_idx.

    Returns dict with: start_idx, end_idx, range_high, range_low,
                       compression_score, window_bars
    Returns None if not enough bars.
    """
    start_search = max(window_size, before_idx - search_window)
    # end of search is before_idx (exclusive)
    # windows: from (start_search) to (before_idx - window_size + 1)
    end_search = before_idx - window_size + 1

    if end_search <= start_search:
        # Fall back: use whatever bars are available
        start_search = max(0, before_idx - window_size)
        end_search = start_search + 1
        if start_search < 0:
            return None

    best_score = -1.0
    best_result = None

    for s in range(start_search, end_search + 1):
        e = s + window_size  # exclusive
        if e > before_idx:
            break
        window_bars = bars_5m[s:e]
        if len(window_bars) < window_size:
            continue
        # Context bars: bars before this window
        context_bars = bars_5m[max(0, s - 12):s]
        score = compute_compression_score_for_window(window_bars, context_bars)
        if score is None:
            score = 0.0
        if score > best_score:
            best_score = score
            best_result = {
                "start_idx": s,
                "end_idx": e - 1,  # inclusive
                "range_high": max(b["high"] for b in window_bars),
                "range_low": min(b["low"] for b in window_bars),
                "compression_score": score,
                "window_bars": window_bars,
            }

    return best_result


def _find_p1(
    bars_5m: List[Dict],
    p0_window_end_idx: int,
    before_idx: int,  # before P2
    side: str,
    range_high: float,
    range_low: float,
    oi_delta_series: List[Optional[float]],
    volume_ratio_series: List[Optional[float]],
) -> Optional[AnchorPoint]:
    """Find P1 (ignition) between P0 window end and P2.

    LONG criteria (all must hold):
      - oi_delta_3bar_pct > 0
      - volume_ratio_3bar >= 1.10
      - AND one of:
          close in top 30% of [range_low, range_high]
          OR close above prior 3-bar high

    SHORT criteria (mirror):
      - oi_delta_3bar_pct > 0
      - volume_ratio_3bar >= 1.10
      - AND one of:
          close in bottom 30% of [range_low, range_high]
          OR close below prior 3-bar low

    Fallback: one bar before P2.
    """
    range_span = range_high - range_low
    search_start = p0_window_end_idx + 1

    for i in range(search_start, before_idx):
        bar = bars_5m[i]

        oi_delta = oi_delta_series[i] if i < len(oi_delta_series) else None
        vol_ratio = volume_ratio_series[i] if i < len(volume_ratio_series) else None

        if oi_delta is None or oi_delta <= 0:
            continue
        if vol_ratio is None or vol_ratio < 1.10:
            continue

        if side == "LONG":
            # Top 30% of range
            threshold = range_low + 0.70 * range_span
            in_top_30 = bar["close"] >= threshold

            # Above prior 3-bar high
            if i >= 3:
                prior_high = max(bars_5m[j]["high"] for j in range(i - 3, i))
                above_prior = bar["close"] > prior_high
            else:
                above_prior = False

            if in_top_30 or above_prior:
                return AnchorPoint(
                    code="P1",
                    bar_idx=i,
                    ts_ms=bar["open_time"],
                    bar=bar,
                    auto_confidence=0.8,
                )
        else:
            # Bottom 30% of range
            threshold = range_low + 0.30 * range_span
            in_bottom_30 = bar["close"] <= threshold

            # Below prior 3-bar low
            if i >= 3:
                prior_low = min(bars_5m[j]["low"] for j in range(i - 3, i))
                below_prior = bar["close"] < prior_low
            else:
                below_prior = False

            if in_bottom_30 or below_prior:
                return AnchorPoint(
                    code="P1",
                    bar_idx=i,
                    ts_ms=bar["open_time"],
                    bar=bar,
                    auto_confidence=0.8,
                )

    # Fallback: one bar before P2
    fallback_idx = max(0, before_idx - 1)
    bar = bars_5m[fallback_idx]
    return AnchorPoint(
        code="P1",
        bar_idx=fallback_idx,
        ts_ms=bar["open_time"],
        bar=bar,
        auto_confidence=0.3,
        fallback_used=True,
    )


def _find_p2(
    bars_5m: List[Dict],
    search_from_idx: int,
    side: str,
    range_high: float,
    range_low: float,
    oi_delta_series: List[Optional[float]],
    volume_ratio_series: List[Optional[float]],
    break_candidate_bar_idx: int,
) -> AnchorPoint:
    """Find P2 (structural break).

    LONG:
      close > range_high + 0.10 * atr_proxy
      AND break_quality_band in {clean, strong}
      Fallback: first close above range_high

    SHORT:
      close < range_low - 0.10 * atr_proxy
      AND break_quality_band in {clean, strong}
      Fallback: first close below range_low

    Searches forward from search_from_idx.
    Also considers the break_candidate_bar_idx as a hint.
    """
    # Search up to break_candidate + 12 bars forward
    search_end = min(break_candidate_bar_idx + 12, len(bars_5m))
    first_break_idx = None  # fallback

    for i in range(search_from_idx, search_end):
        bar = bars_5m[i]
        bars_before = bars_5m[max(0, i - 20):i]

        oi_delta = oi_delta_series[i] if i < len(oi_delta_series) else None
        vol_ratio = volume_ratio_series[i] if i < len(volume_ratio_series) else None

        # Compute ATR proxy
        if bars_before:
            ranges = [b["high"] - b["low"] for b in bars_before[-20:]]
            atr_proxy = statistics.median(ranges) if ranges else 0.0
        else:
            atr_proxy = (range_high - range_low) * 0.5

        bq = compute_break_quality_score(
            bar=bar,
            break_level=range_high if side == "LONG" else range_low,
            side=side,
            bars_before=bars_before,
            oi_delta_3bar=oi_delta,
            volume_ratio_3bar=vol_ratio,
        )
        score = bq["break_quality_score"]
        band = bq["break_quality_band"]

        if side == "LONG":
            threshold = range_high + 0.10 * atr_proxy
            if bar["close"] > range_high and first_break_idx is None:
                first_break_idx = i
            if bar["close"] > threshold and band in ("clean", "strong"):
                ap = AnchorPoint(
                    code="P2",
                    bar_idx=i,
                    ts_ms=bar["open_time"],
                    bar=bar,
                    auto_confidence=score or 0.5,
                    break_quality_score=score,
                    break_quality_band=band,
                    bq_components=bq.get("bq_components", {}),
                )
                return ap
        else:
            threshold = range_low - 0.10 * atr_proxy
            if bar["close"] < range_low and first_break_idx is None:
                first_break_idx = i
            if bar["close"] < threshold and band in ("clean", "strong"):
                ap = AnchorPoint(
                    code="P2",
                    bar_idx=i,
                    ts_ms=bar["open_time"],
                    bar=bar,
                    auto_confidence=score or 0.5,
                    break_quality_score=score,
                    break_quality_band=band,
                    bq_components=bq.get("bq_components", {}),
                )
                return ap

    # Fallback: use break_candidate itself
    fallback_idx = (
        first_break_idx
        if first_break_idx is not None
        else min(break_candidate_bar_idx, len(bars_5m) - 1)
    )
    bar = bars_5m[fallback_idx]
    bars_before = bars_5m[max(0, fallback_idx - 20):fallback_idx]
    oi_delta = oi_delta_series[fallback_idx] if fallback_idx < len(oi_delta_series) else None
    vol_ratio = volume_ratio_series[fallback_idx] if fallback_idx < len(volume_ratio_series) else None
    bq = compute_break_quality_score(
        bar=bar,
        break_level=range_high if side == "LONG" else range_low,
        side=side,
        bars_before=bars_before,
        oi_delta_3bar=oi_delta,
        volume_ratio_3bar=vol_ratio,
    )
    return AnchorPoint(
        code="P2",
        bar_idx=fallback_idx,
        ts_ms=bar["open_time"],
        bar=bar,
        auto_confidence=0.3,
        fallback_used=True,
        break_quality_score=bq["break_quality_score"],
        break_quality_band=bq["break_quality_band"],
        bq_components=bq.get("bq_components", {}),
    )


def _find_p3(
    bars_5m: List[Dict],
    p2_idx: int,
    side: str,
    range_high: float,
    range_low: float,
    lookahead: int = _P3_LOOKFORWARD_BARS,
) -> AnchorPoint:
    """Find P3 (max directional extension from range_high/low in next 12 bars)."""
    search_end = min(p2_idx + 1 + lookahead, len(bars_5m))
    search_bars = bars_5m[p2_idx + 1: search_end]

    if not search_bars:
        # P2 is at the end of bars
        bar = bars_5m[p2_idx]
        return AnchorPoint(
            code="P3",
            bar_idx=p2_idx,
            ts_ms=bar["open_time"],
            bar=bar,
            auto_confidence=0.2,
            fallback_used=True,
            directional_extension_pct=0.0,
        )

    if side == "LONG":
        # directional_extension = high_t - range_high
        extensions = [b["high"] - range_high for b in search_bars]
    else:
        # directional_extension = range_low - low_t
        extensions = [range_low - b["low"] for b in search_bars]

    best_offset = max(range(len(extensions)), key=lambda j: extensions[j])
    best_ext = extensions[best_offset]
    best_bar = search_bars[best_offset]
    best_idx = p2_idx + 1 + best_offset

    # extension as pct of range_level
    ref = range_high if side == "LONG" else range_low
    ext_pct = (best_ext / max(abs(ref), 1e-12)) * 100.0

    confidence = min(1.0, max(0.3, best_ext / max(abs(ref) * 0.02, 1e-12) * 0.5))

    return AnchorPoint(
        code="P3",
        bar_idx=best_idx,
        ts_ms=best_bar["open_time"],
        bar=best_bar,
        auto_confidence=round(confidence, 3),
        directional_extension_pct=round(ext_pct, 4),
    )


def _find_p4(
    bars_5m: List[Dict],
    bars_15m: List[Dict],
    p3: AnchorPoint,
    side: str,
    range_high: float,
    range_low: float,
) -> Optional[AnchorPoint]:
    """Find P4 (resolution) on the 15m timeline.

    Condition 1: price loses structure
      LONG: 15m close < range_high
      SHORT: 15m close > range_low

    Condition 2: expansion cools (3 consecutive 5m bars with median range
                 < 60% of expansion-phase median)
      Uses 5m bars post-P3

    Condition 3: timeout at P3 + 60 minutes

    Returns None if bars_15m is empty.
    """
    if not bars_15m or not bars_5m:
        return None

    p3_ts = p3.ts_ms
    timeout_ts = p3_ts + 60 * 60 * 1000  # P3 + 60 min

    # Compute expansion-phase 5m range median (P2 to P3)
    p2_ts_approx = p3_ts - _P3_LOOKFORWARD_BARS * 5 * 60 * 1000
    expansion_bars = [
        b for b in bars_5m
        if p2_ts_approx <= b["open_time"] <= p3_ts
    ]
    if expansion_bars:
        exp_ranges = [b["high"] - b["low"] for b in expansion_bars]
        exp_median = statistics.median(exp_ranges)
    else:
        exp_median = 0.0

    # 15m bars after P3
    post_p3_15m = [b for b in bars_15m if b["open_time"] > p3_ts]

    for bar_15m in post_p3_15m:
        # Check condition 1
        if side == "LONG" and bar_15m["close"] < range_high:
            return AnchorPoint(
                code="P4",
                bar_idx=0,
                ts_ms=bar_15m["open_time"],
                bar=bar_15m,
                auto_confidence=0.8,
            )
        if side == "SHORT" and bar_15m["close"] > range_low:
            return AnchorPoint(
                code="P4",
                bar_idx=0,
                ts_ms=bar_15m["open_time"],
                bar=bar_15m,
                auto_confidence=0.8,
            )

        # Check condition 2: 3 consecutive 5m bars with range < 60% of exp_median
        t_start = bar_15m["open_time"]
        t_end = t_start + 15 * 60 * 1000
        local_5m = [
            b for b in bars_5m
            if t_start <= b["open_time"] < t_end
        ]
        if len(local_5m) >= 3 and exp_median > 0:
            last3_ranges = [b["high"] - b["low"] for b in local_5m[-3:]]
            if all(r < 0.60 * exp_median for r in last3_ranges):
                return AnchorPoint(
                    code="P4",
                    bar_idx=0,
                    ts_ms=bar_15m["open_time"],
                    bar=bar_15m,
                    auto_confidence=0.7,
                )

        # Check condition 3: timeout
        if bar_15m["open_time"] >= timeout_ts:
            return AnchorPoint(
                code="P4",
                bar_idx=0,
                ts_ms=bar_15m["open_time"],
                bar=bar_15m,
                auto_confidence=0.5,
                fallback_used=True,
            )

    # If no 15m bar found (P3 is near end of day)
    # Return None — report builder should handle not_reached_yet
    return None


# ---------------------------------------------------------------------------
# Wave 2 — L4: compute_support_test_count + compute_breakdown_pack
# ---------------------------------------------------------------------------

def compute_support_test_count(
    bars_5m: List[Dict],
    p0_start_idx: int,
    p2_idx: int,
    range_high: float,
    range_low: float,
    side: str,
    tol_pct: float = 0.003,
) -> int:
    """Count bars between P0 and P2 that touched the break level.

    LONG: bar.high >= range_high - tolerance (tested resistance)
    SHORT: bar.low <= range_low + tolerance (tested support)
    Tolerance defaults to 0.3% of the level price.
    """
    level = range_high if side == "LONG" else range_low
    tol = abs(level) * tol_pct
    count = 0
    for i in range(p0_start_idx, p2_idx):
        if i >= len(bars_5m):
            break
        bar = bars_5m[i]
        if side == "LONG" and bar["high"] >= level - tol:
            count += 1
        elif side == "SHORT" and bar["low"] <= level + tol:
            count += 1
    return count


def compute_breakdown_pack(
    bars_5m: List[Dict],
    anchors: "AnchorSet",
    volume_ratio_series: List[Optional[float]],
) -> BreakdownPack:
    """Compute L4 breakdown fields from bars and existing AnchorSet data.

    All bar-local computations live here, not in case_builder.
    Provisional aliases are documented inline.
    """
    p2 = anchors.p2
    p2_bar = p2.bar if p2 else None
    p2_idx = p2.bar_idx if p2 else 0
    side = anchors.side
    rh, rl = anchors.range_high, anchors.range_low
    break_level = rh if side == "LONG" else rl
    p2_close = p2_bar["close"] if p2_bar else None

    # break_distance_pct: how far past break_level the close landed
    dist_pct: Optional[float] = None
    if p2_close is not None and abs(break_level) > 1e-12:
        if side == "LONG":
            dist_pct = round((p2_close - rh) / rh * 100.0, 4)
        else:
            dist_pct = round((rl - p2_close) / rl * 100.0, 4)

    # bq_components — already computed in _find_p2, surfaced here
    bq_comps = (p2.bq_components or {}) if p2 else {}
    body_q = bq_comps.get("body_quality")  # PROVEN for break_bar_body_ratio

    # break_volume_ratio — PROVISIONAL: rolling 3-bar ratio ending at P2
    vol_ratio: Optional[float] = None
    if volume_ratio_series and 0 <= p2_idx < len(volume_ratio_series):
        v = volume_ratio_series[p2_idx]
        vol_ratio = round(v, 4) if v is not None else None

    # support_test_count_before_break
    test_count = compute_support_test_count(
        bars_5m,
        anchors.compression_window_start_idx,
        p2_idx,
        rh, rl, side,
    )

    # break_cleanliness_score — PROVEN: = break_quality_score
    cleanliness: Optional[float] = None
    if p2 and p2.break_quality_score is not None:
        cleanliness = round(p2.break_quality_score, 4)

    # immediate_followthrough_pct_1bar: favorable move in bar immediately after P2
    ft_1bar: Optional[float] = None
    if p2_close is not None and p2_idx + 1 < len(bars_5m):
        b1 = bars_5m[p2_idx + 1]
        if side == "LONG":
            ft_1bar = round((b1["high"] - p2_close) / p2_close * 100.0, 4)
        else:
            ft_1bar = round((p2_close - b1["low"]) / p2_close * 100.0, 4)

    # immediate_followthrough_pct_3bar: max favorable move in bars P2+1..+3
    ft_3bar: Optional[float] = None
    if p2_close is not None:
        favs = []
        for j in range(p2_idx + 1, min(p2_idx + 4, len(bars_5m))):
            b = bars_5m[j]
            if side == "LONG":
                favs.append((b["high"] - p2_close) / p2_close * 100.0)
            else:
                favs.append((p2_close - b["low"]) / p2_close * 100.0)
        ft_3bar = round(max(favs), 4) if favs else None

    # false_break_reclaim_fast_flag: Y if price closes back through break_level within 5 bars
    fbr_flag = "N"
    for j in range(p2_idx + 1, min(p2_idx + 6, len(bars_5m))):
        b = bars_5m[j]
        if side == "LONG" and b["close"] < break_level:
            fbr_flag = "Y"
            break
        elif side == "SHORT" and b["close"] > break_level:
            fbr_flag = "Y"
            break

    return BreakdownPack(
        break_distance_pct=dist_pct,
        break_close_strength=round(body_q, 4) if body_q is not None else None,  # PROVISIONAL
        break_bar_body_ratio=round(body_q, 4) if body_q is not None else None,  # PROVEN
        break_volume_ratio=vol_ratio,                                            # PROVISIONAL
        support_test_count=test_count,
        break_cleanliness_score=cleanliness,
        immediate_followthrough_pct_1bar=ft_1bar,
        immediate_followthrough_pct_3bar=ft_3bar,
        false_break_reclaim_fast_flag=fbr_flag,
    )


# ---------------------------------------------------------------------------
# Wave 2 — L5: compute_retest_fail_pack (dual-direction)
# ---------------------------------------------------------------------------

_RETEST_FAIL_PACK_UNAVAILABLE = RetestFailPack(
    pack_available=False,
    reclaim_pct=None,
    retest_depth_vs_break_pct=None,
    retest_duration_bars=None,
    retest_reject_wick_ratio=None,
    retest_rejection_quality="none",
    retest_close_back_above_break_flag="N",
    fail_strength=None,
    fail_confirmation_bar_count=None,
    max_reclaim_before_resolution_pct=None,
    second_rejection_present_flag="N",
    bars_to_retest=None,
    bars_to_fail=None,
    retest_start_bar_idx=None,
    retest_extreme_bar_idx=None,
)


def compute_retest_fail_pack(
    bars_5m: List[Dict],
    p2: Optional[AnchorPoint],
    p3: Optional[AnchorPoint],
    p4: Optional[AnchorPoint],
    side: str,
    range_high: float,
    range_low: float,
) -> RetestFailPack:
    """Compute L5 retest geometry in P3→P4 zone (dual-direction).

    LONG: P3 = price peak; retest = pullback DOWN; retest_extreme = lowest bar.
    SHORT: P3 = price trough; retest = bounce UP; retest_extreme = highest bar.

    Returns pack_available=False (all null) when P3 or P4 is missing.
    Searches bars_5m by timestamp range [P3.ts_ms, P4.ts_ms).
    """
    if p2 is None or p3 is None or p4 is None:
        return _RETEST_FAIL_PACK_UNAVAILABLE

    break_level = range_high if side == "LONG" else range_low
    p3_ts = p3.ts_ms
    p4_ts = p4.ts_ms

    # Collect bars strictly between P3 and P4 on the 5m timeline
    zone_bars: List[Dict] = []
    zone_indices: List[int] = []
    for i, bar in enumerate(bars_5m):
        ts = bar["open_time"]
        if ts > p3_ts and ts < p4_ts:
            zone_bars.append(bar)
            zone_indices.append(i)

    if not zone_bars:
        return _RETEST_FAIL_PACK_UNAVAILABLE

    # Find retest_extreme: worst adverse bar in zone (pullback for LONG, bounce for SHORT)
    if side == "LONG":
        ext_offset = min(range(len(zone_bars)), key=lambda j: zone_bars[j]["low"])
        ext_price = zone_bars[ext_offset]["low"]
    else:
        ext_offset = max(range(len(zone_bars)), key=lambda j: zone_bars[j]["high"])
        ext_price = zone_bars[ext_offset]["high"]

    ext_bar = zone_bars[ext_offset]
    ext_bar_idx = zone_indices[ext_offset]
    retest_duration = ext_offset + 1  # bars from P3 to retest_extreme (1-indexed)

    # retest_depth_vs_break_pct: how close to break_level the retest got
    if abs(break_level) > 1e-12:
        if side == "LONG":
            depth_pct = round((break_level - ext_price) / break_level * 100.0, 4)
        else:
            depth_pct = round((ext_price - break_level) / break_level * 100.0, 4)
    else:
        depth_pct = None

    # reclaim_pct: pullback from P3 as pct of P3 price
    p3_price = p3.bar["high"] if side == "LONG" else p3.bar["low"] if p3.bar else None
    if p3_price and abs(p3_price) > 1e-12:
        if side == "LONG":
            reclaim_pct = round((p3_price - ext_price) / p3_price * 100.0, 4)
        else:
            reclaim_pct = round((ext_price - p3_price) / p3_price * 100.0, 4)
    else:
        reclaim_pct = None

    # retest_reject_wick_ratio: direction-aware wick at retest_extreme bar
    if side == "LONG":
        rej_wick = round(lower_wick_ratio(ext_bar), 4)
    else:
        rej_wick = round(upper_wick_ratio(ext_bar), 4)

    # retest_rejection_quality: qualitative band from wick ratio
    if rej_wick >= 0.60:
        rej_quality = "strong"
    elif rej_wick >= 0.40:
        rej_quality = "clean"
    elif rej_wick >= 0.20:
        rej_quality = "weak"
    else:
        rej_quality = "none"

    # retest_close_back_above_break_flag
    ext_close = ext_bar.get("close")
    if ext_close is not None:
        if side == "LONG":
            # Close above break level = price didn't actually lose break
            rcb_flag = "Y" if ext_close > break_level else "N"
        else:
            # Close below break level = bounce failed to reclaim break
            rcb_flag = "Y" if ext_close < break_level else "N"
    else:
        rcb_flag = "N"

    # fail_strength: max adverse move in 3 bars after retest_extreme (normalized by break_level)
    post_ext_bars = zone_bars[ext_offset + 1: ext_offset + 4]
    fail_strength: Optional[float] = None
    if post_ext_bars and abs(break_level) > 1e-12:
        if side == "LONG":
            worst = max((break_level - b["low"]) / break_level for b in post_ext_bars)
        else:
            worst = max((b["high"] - break_level) / break_level for b in post_ext_bars)
        fail_strength = round(min(max(worst, 0.0), 1.0), 4)

    # fail_confirmation_bar_count: bars closing beyond break_level after retest_extreme
    fail_count = 0
    fail_bar_offset: Optional[int] = None
    for j in range(ext_offset + 1, len(zone_bars)):
        b = zone_bars[j]
        c = b.get("close")
        if c is None:
            continue
        if side == "LONG" and c < break_level:
            fail_count += 1
            if fail_bar_offset is None:
                fail_bar_offset = j
        elif side == "SHORT" and c < break_level:
            fail_count += 1
            if fail_bar_offset is None:
                fail_bar_offset = j

    bars_to_fail = (fail_bar_offset - ext_offset) if fail_bar_offset is not None else None

    # max_reclaim_before_resolution_pct: best recovery from retest_extreme back toward break_level
    post_ext = zone_bars[ext_offset + 1:]
    max_rec: Optional[float] = None
    if post_ext and abs(break_level) > 1e-12:
        if side == "LONG":
            best = max(b["high"] for b in post_ext)
            max_rec = round((best - ext_price) / break_level * 100.0, 4)
        else:
            best = min(b["low"] for b in post_ext)
            max_rec = round((ext_price - best) / break_level * 100.0, 4)

    # second_rejection_present_flag: >=2 bars touching break_level from wrong side after retest_extreme
    touch_count = 0
    tol = abs(break_level) * 0.003
    for b in zone_bars[ext_offset + 1:]:
        if side == "LONG" and b["high"] >= break_level - tol:
            touch_count += 1
        elif side == "SHORT" and b["low"] <= break_level + tol:
            touch_count += 1
    sec_rej_flag = "Y" if touch_count >= 2 else "N"

    return RetestFailPack(
        pack_available=True,
        reclaim_pct=reclaim_pct,
        retest_depth_vs_break_pct=depth_pct,
        retest_duration_bars=retest_duration,
        retest_reject_wick_ratio=rej_wick,
        retest_rejection_quality=rej_quality,
        retest_close_back_above_break_flag=rcb_flag,
        fail_strength=fail_strength,
        fail_confirmation_bar_count=fail_count if fail_count > 0 else None,
        max_reclaim_before_resolution_pct=max_rec,
        second_rejection_present_flag=sec_rej_flag,
        bars_to_retest=retest_duration,
        bars_to_fail=bars_to_fail,
        retest_start_bar_idx=zone_indices[0] if zone_indices else None,
        retest_extreme_bar_idx=ext_bar_idx,
    )


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def detect_anchors(
    bars_5m: List[Dict],
    bars_15m: List[Dict],
    move: CanonicalMove,
    oi_delta_series: List[Optional[float]],
    volume_ratio_series: List[Optional[float]],
) -> AnchorSet:
    """Detect P0–P4 anchors for one canonical move.

    bars_5m: full series including pre-lookback (96 bars) + UTC day
    bars_15m: full series including pre-lookback + UTC day
    move: CanonicalMove from canonical_move.py
    oi_delta_series / volume_ratio_series: aligned to bars_5m
    """
    bc_idx = move.break_candidate_bar_idx
    side = move.side

    # ---- Step 1: Find compression window ----
    comp = _find_best_compression_window(bars_5m, before_idx=bc_idx)

    if comp is None:
        # Last resort: use the 6 bars ending right before break candidate
        s = max(0, bc_idx - _COMPRESSION_WINDOW_SIZE)
        window_bars = bars_5m[s:bc_idx]
        comp = {
            "start_idx": s,
            "end_idx": bc_idx - 1,
            "range_high": max(b["high"] for b in window_bars) if window_bars else 0,
            "range_low": min(b["low"] for b in window_bars) if window_bars else 0,
            "compression_score": 0.0,
            "window_bars": window_bars,
        }

    range_high = comp["range_high"]
    range_low = comp["range_low"]
    comp_start = comp["start_idx"]
    comp_end = comp["end_idx"]
    comp_score = comp["compression_score"]

    # ---- P0: first bar of compression window ----
    p0_bar = bars_5m[comp_start]
    p0 = AnchorPoint(
        code="P0",
        bar_idx=comp_start,
        ts_ms=p0_bar["open_time"],
        bar=p0_bar,
        auto_confidence=round(comp_score, 3),
        compression_score=round(comp_score, 4),
    )

    # ---- P2: structural break (search from end of compression window) ----
    p2_search_from = comp_end + 1
    p2 = _find_p2(
        bars_5m=bars_5m,
        search_from_idx=p2_search_from,
        side=side,
        range_high=range_high,
        range_low=range_low,
        oi_delta_series=oi_delta_series,
        volume_ratio_series=volume_ratio_series,
        break_candidate_bar_idx=bc_idx,
    )

    # ---- P1: ignition between P0 window end and P2 ----
    p1 = _find_p1(
        bars_5m=bars_5m,
        p0_window_end_idx=comp_end,
        before_idx=p2.bar_idx,
        side=side,
        range_high=range_high,
        range_low=range_low,
        oi_delta_series=oi_delta_series,
        volume_ratio_series=volume_ratio_series,
    )

    # ---- P3: max extension ----
    p3 = _find_p3(
        bars_5m=bars_5m,
        p2_idx=p2.bar_idx,
        side=side,
        range_high=range_high,
        range_low=range_low,
    )

    # ---- P4: resolution on 15m ----
    p4 = _find_p4(
        bars_5m=bars_5m,
        bars_15m=bars_15m,
        p3=p3,
        side=side,
        range_high=range_high,
        range_low=range_low,
    )

    return AnchorSet(
        symbol=move.symbol,
        side=side,
        range_high=range_high,
        range_low=range_low,
        compression_window_start_idx=comp_start,
        compression_window_end_idx=comp_end,
        p0=p0,
        p1=p1,
        p2=p2,
        p3=p3,
        p4=p4,
        detection_ok=True,
    )
