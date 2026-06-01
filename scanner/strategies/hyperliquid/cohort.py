"""
Cohort-churn metrics — decompose top-N change into accumulation vs rotation (spec §9).

ALL comparisons use cluster_id substrate (canonical, stable across scans).
This is the spec §0 invariant: comparing the SAME identity at t and t-1.

same_cohort_delta > 0 means clusters present in BOTH top_n sets are adding notional.
topn_jaccard measures cohort stability (not just total concentration level).
"""

from typing import Dict, Set, Tuple


def compute_churn(
    prev_top_n: Set[str],
    curr_top_n: Set[str],
    prev_notionals: Dict[str, float],
    curr_notionals: Dict[str, float],
) -> dict:
    """Compute cohort-churn metrics between two consecutive scan snapshots.

    All inputs are keyed by canonical cluster_id (spec §0 invariant).

    Returns:
        same_cohort_delta     : float — notional change within clusters present in both sets
        new_entrant_notional  : float — notional of clusters newly entering top_n
        exit_notional         : float — notional of clusters that left top_n
        topn_jaccard          : float in [0,1] — |intersection| / |union|
        same_cohort_rising    : bool — same_cohort_delta > 0
    """
    if not prev_top_n and not curr_top_n:
        return _empty_churn()

    same = prev_top_n & curr_top_n
    entered = curr_top_n - prev_top_n
    exited = prev_top_n - curr_top_n
    union = prev_top_n | curr_top_n

    same_cohort_delta = sum(
        curr_notionals.get(c, 0.0) - prev_notionals.get(c, 0.0)
        for c in same
    )
    new_entrant_notional = sum(curr_notionals.get(c, 0.0) for c in entered)
    exit_notional = sum(prev_notionals.get(c, 0.0) for c in exited)
    jaccard = len(same) / len(union) if union else 0.0

    return {
        "same_cohort_delta": same_cohort_delta,
        "new_entrant_notional": new_entrant_notional,
        "exit_notional": exit_notional,
        "topn_jaccard": jaccard,
        "same_cohort_rising": same_cohort_delta > 0,
    }


def _empty_churn() -> dict:
    return {
        "same_cohort_delta": 0.0,
        "new_entrant_notional": 0.0,
        "exit_notional": 0.0,
        "topn_jaccard": 0.0,
        "same_cohort_rising": False,
    }
