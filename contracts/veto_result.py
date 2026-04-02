"""
contracts/veto_result.py
Canonical typed contract for veto engine output.
Only veto may force NO_SEND for an otherwise detected setup.
"""
from dataclasses import dataclass


@dataclass
class VetoResult:
    """Output of veto/veto_engine.py.

    veto_flag=True means the veto engine issued a hard NO_SEND.
    veto_reason_code must always be set when veto_flag is True.
    """
    veto_flag: bool = False
    veto_reason_code: str = "not_evaluated"
    veto_layer: str = "not_evaluated"
    veto_note: str = ""
