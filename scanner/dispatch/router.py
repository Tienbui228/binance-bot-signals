from __future__ import annotations

from scanner.domain import DispatchDecision, Signal


def _band_for_main(score: float) -> str:
    if score >= 90.0:
        return "HIGH"
    if score >= 80.0:
        return "MEDIUM"
    return "LOW"


def route_dispatch_v1(signal: Signal, rank: int, top_n: int, dispatch_floor_score: float = 70.0) -> DispatchDecision:
    if float(signal.score) < float(dispatch_floor_score):
        return DispatchDecision(
            dispatch_action="NO_SEND",
            dispatch_confidence_band="LOW",
            dispatch_reason=f"score_below_dispatch_floor_{dispatch_floor_score:.0f}",
            publish_priority=0,
        )

    if rank < max(int(top_n), 0):
        return DispatchDecision(
            dispatch_action="MAIN_SIGNAL",
            dispatch_confidence_band=_band_for_main(float(signal.score)),
            dispatch_reason="top_n_confirmed",
            publish_priority=max(1, 1000 - rank),
        )

    return DispatchDecision(
        dispatch_action="WATCHLIST",
        dispatch_confidence_band="MEDIUM" if float(signal.score) >= 80.0 else "LOW",
        dispatch_reason="confirmed_but_not_top_n",
        publish_priority=max(1, 100 - rank),
    )
