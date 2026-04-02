"""
contracts/regime_result.py
Canonical typed contract for regime classifier output.
All new code imports from here. scanner/domain.py::RegimeVerdict is a shim.
"""
from dataclasses import dataclass


@dataclass
class RegimeResult:
    """Output of regime/regime_classifier.py + regime/regime_normalizer.py.

    regime_label is always one of the three approved 3A labels:
        trend_continuation_friendly
        broad_weakness_sell_pressure
        unclear_mixed

    regime_fit_for_strategy is always HIGH, MEDIUM, or LOW once a
    strategy_family is known.  not_evaluated is only acceptable when the
    strategy family cannot be identified.
    """
    regime_label: str = "unclear_mixed"
    regime_fit_for_strategy: str = "not_evaluated"
    regime_confidence: str = "not_evaluated"
    regime_note: str = ""
