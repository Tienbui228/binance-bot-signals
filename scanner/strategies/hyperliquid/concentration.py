"""
Real concentration measurement (spec §8.3).

Measures top-N cluster share of total long OI.
All computations are cluster-keyed (canonical cluster_id from clustering.py).
Returns None if OI denominator is zero — treat as coverage failure upstream.
"""

from typing import Dict, List, Optional


def measure(
    coin: str,
    registry: Dict[str, dict],
    coin_ctx: dict,
    top_n: int = 10,
) -> Optional[dict]:
    """Compute concentration metrics for one coin.

    Args:
        coin: HL coin name (for logging only)
        registry: addr→entry dict from AddressRegistry (entries must have cluster_id set)
        coin_ctx: from MarketContext.fetch_all()[coin]
        top_n: number of top clusters to consider

    Returns dict with:
        cluster_notionals   : {cluster_id: long_notional_usd}
        top_n_clusters      : [cluster_id, ...] sorted desc by notional
        top_n_long_notional : float — sum of top-N cluster notionals
        top1_share          : float — top-1 / top-N (for anti-signal A1)
        real_concentration  : float — top_n_long_notional / oi_usd (APPROXIMATION)
        tracked_long_notional: float — all tracked long notional
        distinct_clusters   : int — number of clusters with long position
    Or None if OI is zero or no long positions tracked.
    """
    oi_usd = coin_ctx.get("oi_usd", 0.0)
    mark_px = coin_ctx.get("mark_px", 0.0)
    if oi_usd <= 0 or mark_px <= 0:
        return None

    # Aggregate long notional per cluster (cluster-keyed, canonical id)
    cluster_notionals: Dict[str, float] = {}
    for addr, entry in registry.items():
        szi = entry.get("last_szi", 0.0)
        if not isinstance(szi, (int, float)):
            try:
                szi = float(szi)
            except (TypeError, ValueError):
                continue
        if szi <= 0:
            continue  # long only
        notional = szi * mark_px
        cluster_id = entry.get("cluster_id", addr)
        cluster_notionals[cluster_id] = cluster_notionals.get(cluster_id, 0.0) + notional

    if not cluster_notionals:
        return None

    tracked_long_notional = sum(cluster_notionals.values())
    distinct_clusters = len(cluster_notionals)

    # Top-N by notional
    sorted_clusters = sorted(cluster_notionals.items(), key=lambda x: x[1], reverse=True)
    top_clusters = sorted_clusters[:top_n]
    top_n_clusters = [c for c, _ in top_clusters]
    top_n_long_notional = sum(n for _, n in top_clusters)

    top1_share = (top_clusters[0][1] / top_n_long_notional) if top_n_long_notional > 0 else 0.0
    real_concentration = top_n_long_notional / oi_usd  # 0..1, APPROXIMATION (long-only / total-one-sided)

    return {
        "cluster_notionals": dict(cluster_notionals),
        "top_n_clusters": top_n_clusters,
        "top_n_long_notional": top_n_long_notional,
        "top1_share": top1_share,
        "real_concentration": real_concentration,
        "tracked_long_notional": tracked_long_notional,
        "distinct_clusters": distinct_clusters,
    }
