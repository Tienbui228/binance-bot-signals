"""
research/top_movers/image_renderer.py

Renders 5 chart images per token using matplotlib.

Required images (v2 standard):
  1. P0_context_1h
  2. P0_P1_setup_15m
  3. P1_ignition_5m
  4. P2_P3_break_expansion_5m
  5. P4_resolution_15m

Legacy images (P2_break_15m, P3_expansion_5m) are deprecated.
Default generate_legacy_images = False.
"""

import os
from typing import Dict, List, Optional

from research.top_movers.anchor_detector import AnchorSet
from research.top_movers.io import image_path, normalize_ts_1h, normalize_ts_15m

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.patches as mpatches
    _MPL_AVAILABLE = True
except ImportError:
    _MPL_AVAILABLE = False

REQUIRED_IMAGE_KEYS = [
    "P0_context_1h",
    "P0_P1_setup_15m",
    "P1_ignition_5m",
    "P2_P3_break_expansion_5m",
    "P4_resolution_15m",
]

generate_legacy_images = False


def _draw_ohlc(ax, bars):
    for i, bar in enumerate(bars):
        op, hi, lo, cl = bar["open"], bar["high"], bar["low"], bar["close"]
        color = "#2ca02c" if cl >= op else "#d62728"
        ax.plot([i, i], [lo, hi], color=color, linewidth=0.8, zorder=1)
        body_lo = min(op, cl)
        body_hi = max(op, cl)
        if body_hi - body_lo < (hi - lo) * 0.01:
            body_hi = body_lo + max((hi - lo) * 0.01, 1e-8)
        ax.add_patch(mpatches.Rectangle(
            (i - 0.3, body_lo), 0.6, body_hi - body_lo, color=color, zorder=2))


def _nearest(bars, ts_ms):
    if not bars:
        return None
    return min(range(len(bars)), key=lambda i: abs(bars[i]["open_time"] - ts_ms))


def _vline(ax, idx, label, color="#ff7f0e"):
    ax.axvline(x=idx, color=color, linestyle="--", linewidth=1.0, alpha=0.85, zorder=3)
    ax.text(idx + 0.1, ax.get_ylim()[1], label, color=color, fontsize=7, va="top", zorder=4)


def _hlines(ax, high, low):
    ax.axhline(y=high, color="#2ca02c", linestyle=":", linewidth=1.0, alpha=0.7, label=f"RH {high:.4f}")
    ax.axhline(y=low,  color="#d62728", linestyle=":", linewidth=1.0, alpha=0.7, label=f"RL {low:.4f}")


def _ts(ts_ms):
    from datetime import datetime
    return datetime.utcfromtimestamp(ts_ms / 1000).strftime("%m-%d %H:%M")


def _save(ax, fig, title, bars, fp):
    n = len(bars)
    step = max(1, n // 6)
    ticks = list(range(0, n, step))
    ax.set_xticks(ticks)
    ax.set_xticklabels([_ts(bars[i]["open_time"]) for i in ticks], fontsize=6, rotation=30, ha="right")
    ax.set_title(title, fontsize=9, pad=4)
    ax.set_xlim(-0.5, n - 0.5)
    ax.legend(fontsize=6, loc="upper left")
    fig.tight_layout()
    os.makedirs(os.path.dirname(fp), exist_ok=True)
    fig.savefig(fp, dpi=100, bbox_inches="tight")
    plt.close(fig)


def _render_p0_context_1h(bars_1h, anchors, fp, symbol, side):
    try:
        p0_1h_ts = normalize_ts_1h(anchors.p0.ts_ms)
        idx = _nearest(bars_1h, p0_1h_ts)
        if idx is None:
            return {"created": False, "reason": "no_1h_bar_near_p0"}
        chart = bars_1h[max(0, idx - 12): min(len(bars_1h), idx + 9)]
        if len(chart) < 3:
            return {"created": False, "reason": "insufficient_1h_bars"}
        fig, ax = plt.subplots(figsize=(12, 5))
        _draw_ohlc(ax, chart)
        _hlines(ax, anchors.range_high, anchors.range_low)
        local = _nearest(chart, anchors.p0.ts_ms)
        if local is not None:
            _vline(ax, local, "P0", "#9467bd")
        _save(ax, fig, f"{symbol} {side} | P0 Context (1h)", chart, fp)
        return {"created": True, "reason": ""}
    except Exception as e:
        return {"created": False, "reason": f"{type(e).__name__}: {str(e)[:80]}"}


def _render_p0_p1_setup_15m(bars_15m, anchors, fp, symbol, side):
    try:
        p0_15m_ts = normalize_ts_15m(anchors.p0.ts_ms)
        p1_ts = anchors.p1.ts_ms if anchors.p1 else anchors.p2.ts_ms
        p1_15m_ts = normalize_ts_15m(p1_ts)
        p0_idx = _nearest(bars_15m, p0_15m_ts)
        p1_idx = _nearest(bars_15m, p1_15m_ts)
        if p0_idx is None or p1_idx is None:
            return {"created": False, "reason": "no_15m_bars_p0_p1"}
        chart = bars_15m[max(0, p0_idx - 3): min(len(bars_15m), p1_idx + 4)]
        if len(chart) < 3:
            return {"created": False, "reason": "insufficient_15m_p0_p1"}
        fig, ax = plt.subplots(figsize=(12, 5))
        _draw_ohlc(ax, chart)
        _hlines(ax, anchors.range_high, anchors.range_low)
        for ts, lbl, col in [(anchors.p0.ts_ms, "P0", "#9467bd"), (p1_ts, "P1", "#17becf")]:
            i = _nearest(chart, ts)
            if i is not None:
                _vline(ax, i, lbl, col)
        _save(ax, fig, f"{symbol} {side} | P0→P1 Setup (15m)", chart, fp)
        return {"created": True, "reason": ""}
    except Exception as e:
        return {"created": False, "reason": f"{type(e).__name__}: {str(e)[:80]}"}


def _render_p1_ignition_5m(bars_5m, anchors, fp, symbol, side):
    try:
        p1_ts = anchors.p1.ts_ms if anchors.p1 else anchors.p2.ts_ms
        idx = _nearest(bars_5m, p1_ts)
        if idx is None:
            return {"created": False, "reason": "no_5m_bar_near_p1"}
        chart = bars_5m[max(0, idx - 6): min(len(bars_5m), idx + 7)]
        if len(chart) < 3:
            return {"created": False, "reason": "insufficient_5m_p1"}
        fig, ax = plt.subplots(figsize=(12, 5))
        _draw_ohlc(ax, chart)
        _hlines(ax, anchors.range_high, anchors.range_low)
        local = _nearest(chart, p1_ts)
        if local is not None:
            _vline(ax, local, "P1 Ignition", "#17becf")
        _save(ax, fig, f"{symbol} {side} | P1 Ignition (5m)", chart, fp)
        return {"created": True, "reason": ""}
    except Exception as e:
        return {"created": False, "reason": f"{type(e).__name__}: {str(e)[:80]}"}


def _render_p2_p3_break_expansion_5m(bars_5m, anchors, fp, symbol, side):
    try:
        p2_idx = _nearest(bars_5m, anchors.p2.ts_ms)
        p3_idx = _nearest(bars_5m, anchors.p3.ts_ms)
        if p2_idx is None or p3_idx is None:
            return {"created": False, "reason": "no_5m_bars_p2_p3"}
        chart = bars_5m[max(0, p2_idx - 3): min(len(bars_5m), p3_idx + 6)]
        if len(chart) < 3:
            return {"created": False, "reason": "insufficient_5m_p2_p3"}
        fig, ax = plt.subplots(figsize=(12, 5))
        _draw_ohlc(ax, chart)
        _hlines(ax, anchors.range_high, anchors.range_low)
        for ts, lbl, col in [
            (anchors.p2.ts_ms, "P2 Break", "#e377c2"),
            (anchors.p3.ts_ms, "P3 Peak", "#bcbd22"),
        ]:
            i = _nearest(chart, ts)
            if i is not None:
                _vline(ax, i, lbl, col)
        bq = anchors.p2.break_quality_band or "?"
        sc = anchors.p2.break_quality_score or 0
        ext = anchors.p3.directional_extension_pct or 0
        _save(ax, fig,
              f"{symbol} {side} | P2→P3 Break+Expansion (5m) | bq={bq}({sc:.2f}) ext={ext:.2f}%",
              chart, fp)
        return {"created": True, "reason": ""}
    except Exception as e:
        return {"created": False, "reason": f"{type(e).__name__}: {str(e)[:80]}"}


def _render_p4_resolution_15m(bars_15m, anchors, fp, symbol, side):
    try:
        if anchors.p4 is None:
            return {"created": False, "reason": "p4_not_reached_yet"}
        p2_15m_ts = normalize_ts_15m(anchors.p2.ts_ms)
        p4_15m_ts = normalize_ts_15m(anchors.p4.ts_ms)
        p2_idx = _nearest(bars_15m, p2_15m_ts)
        p4_idx = _nearest(bars_15m, p4_15m_ts)
        if p2_idx is None or p4_idx is None:
            return {"created": False, "reason": "no_15m_bars_p4"}
        chart = bars_15m[max(0, p2_idx - 2): min(len(bars_15m), p4_idx + 5)]
        if len(chart) < 3:
            return {"created": False, "reason": "insufficient_15m_p4"}
        fig, ax = plt.subplots(figsize=(12, 5))
        _draw_ohlc(ax, chart)
        _hlines(ax, anchors.range_high, anchors.range_low)
        for ts, lbl, col in [
            (anchors.p2.ts_ms, "P2", "#e377c2"),
            (anchors.p3.ts_ms, "P3", "#bcbd22"),
            (anchors.p4.ts_ms, "P4 Res", "#8c564b"),
        ]:
            i = _nearest(chart, ts)
            if i is not None:
                _vline(ax, i, lbl, col)
        _save(ax, fig, f"{symbol} {side} | P4 Resolution (15m)", chart, fp)
        return {"created": True, "reason": ""}
    except Exception as e:
        return {"created": False, "reason": f"{type(e).__name__}: {str(e)[:80]}"}


def render_images(case_id, research_day, symbol, side, anchors, bars_5m, bars_15m, bars_1h):
    """Render 5 required images. Never raises. Returns {img_key: {created, reason}}."""
    if not _MPL_AVAILABLE:
        return {k: {"created": False, "reason": "matplotlib_not_installed"} for k in REQUIRED_IMAGE_KEYS}

    results = {}
    renderers = [
        ("P0_context_1h",              lambda fp: _render_p0_context_1h(bars_1h, anchors, fp, symbol, side)),
        ("P0_P1_setup_15m",            lambda fp: _render_p0_p1_setup_15m(bars_15m, anchors, fp, symbol, side)),
        ("P1_ignition_5m",             lambda fp: _render_p1_ignition_5m(bars_5m, anchors, fp, symbol, side)),
        ("P2_P3_break_expansion_5m",   lambda fp: _render_p2_p3_break_expansion_5m(bars_5m, anchors, fp, symbol, side)),
        ("P4_resolution_15m",          lambda fp: _render_p4_resolution_15m(bars_15m, anchors, fp, symbol, side)),
    ]
    for img_key, fn in renderers:
        fp = image_path(research_day, case_id, img_key)
        try:
            r = fn(fp)
        except Exception as e:
            r = {"created": False, "reason": f"unexpected: {type(e).__name__}: {str(e)[:80]}"}
        results[img_key] = r
        print(f"  [{symbol}] {img_key}: {'✓' if r['created'] else '✗ '+r['reason']}")
    return results
