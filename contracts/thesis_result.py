"""
contracts/thesis_result.py
Canonical typed contract for strategy thesis output.
One instance per strategy family evaluation.
"""
from dataclasses import dataclass, field
from typing import List


@dataclass
class ThesisResult:
    """Output of a strategy detection pass.

    setup_detected=True means the family found a qualifying setup.
    strategy_family must be one of:
        legacy_5m_retest
        long_breakout_retest
        short_exhaustion_retest
    """
    setup_detected: bool = False
    strategy_family: str = ""
    side: str = ""
    score: float = 0.0
    confidence: float = 0.0
    setup_quality_band: str = "not_evaluated"
    reason: str = ""
    reason_tags: List[str] = field(default_factory=list)
