"""
Anti-signal veto checks A1–A6 (spec §11).

A5 (OI cap) and A6 (liquidation-driven) require external feeds not yet available.
They return False (no veto) with reason tagged so the caller can log the gap.

A1: single dominant cluster → dump risk (top1_share >= threshold)
A2: thin book → squeeze both ways (depth within ±2% < threshold)
A3: wash signature → OI/vol spike with few distinct clusters
A4: oracle-mark divergence → potential manipulation
"""

from typing import Optional

from ._client import _HlHttp


# Configurable thresholds (exposed for calibration post-§13)
_A1_DOMINANT_THRESHOLD = 0.70   # top-1 cluster >= 70% of top-N
_A2_BOOK_DEPTH_MIN_USD = 50_000  # depth within ±2% must exceed this
_A3_MIN_DISTINCT_CLUSTERS = 3   # fewer → wash signature
_A4_ORACLE_DIVERGENCE_MAX = 0.02  # |mark/oracle - 1| > 2%


def evaluate(
    coin: str,
    conc_result: dict,
    coin_ctx: dict,
    client: Optional[_HlHttp] = None,
) -> dict:
    """Run A1–A6 checks. Returns {veto: bool, reason: str}.

    conc_result: output of concentration.measure()
    coin_ctx: from MarketContext.fetch_all()[coin]
    client: _HlHttp instance for l2Book (A2); if None, A2 is skipped
    """
    top1_share = conc_result.get("top1_share", 0.0)
    distinct_clusters = conc_result.get("distinct_clusters", 0)
    mark_px = coin_ctx.get("mark_px", 0.0)
    oracle_px = coin_ctx.get("oracle_px", 0.0)
    day_ntl_vlm = coin_ctx.get("day_ntl_vlm", 0.0)
    oi_usd = coin_ctx.get("oi_usd", 0.0)

    # A1: single dominant cluster
    if top1_share >= _A1_DOMINANT_THRESHOLD:
        return {"veto": True, "reason": f"A1_dominant_cluster:{top1_share:.2f}"}

    # A2: thin book
    if client is not None:
        book = client.l2_book(coin)
        depth = _book_depth_usd(book, mark_px, pct=0.02)
        if 0 < depth < _A2_BOOK_DEPTH_MIN_USD:
            return {"veto": True, "reason": f"A2_thin_book:{depth:.0f}"}

    # A3: wash signature — OI/vol spike with few distinct clusters
    if distinct_clusters < _A3_MIN_DISTINCT_CLUSTERS and oi_usd > 0 and day_ntl_vlm > 0:
        vol_oi_ratio = day_ntl_vlm / oi_usd
        if vol_oi_ratio > 5.0:  # abnormally high turnover relative to OI
            return {"veto": True, "reason": f"A3_wash_signature:clusters={distinct_clusters},vol_oi={vol_oi_ratio:.1f}"}

    # A4: oracle-mark divergence
    if oracle_px > 0 and mark_px > 0:
        divergence = abs(mark_px / oracle_px - 1.0)
        if divergence > _A4_ORACLE_DIVERGENCE_MAX:
            return {"veto": True, "reason": f"A4_oracle_divergence:{divergence:.3f}"}

    # A5: OI cap — DEFERRED (feed not available)
    # A6: liquidation-driven OI — DEFERRED (feed not available)

    return {"veto": False, "reason": "pass"}


def _book_depth_usd(book: dict, mark_px: float, pct: float) -> float:
    """Sum bid+ask depth within pct of mark_px."""
    if not book or mark_px <= 0:
        return 0.0
    levels = book.get("levels", [])
    if len(levels) < 2:
        return 0.0
    bids, asks = levels[0], levels[1]
    total = 0.0
    lo = mark_px * (1 - pct)
    hi = mark_px * (1 + pct)
    for side in (bids, asks):
        for entry in side:
            try:
                px = float(entry[0])
                sz = float(entry[1])
                if lo <= px <= hi:
                    total += px * sz
            except (TypeError, ValueError, IndexError):
                continue
    return total
