from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from .enums import CaseCloseType, PendingStatus
from .models import CaseRecord


@dataclass(frozen=True)
class CaseEvent:
    name: str
    ts_local: Optional[str] = None
    close_reason: str = ""


class InvalidTransitionError(ValueError):
    pass


class CaseStateMachine:
    """Single place for lifecycle transitions.

    This is intentionally small in sprint 1. The goal is to stop scattered
    free-form state mutation before adding more semantics.
    """

    def apply(self, case: CaseRecord, event: CaseEvent) -> CaseRecord:
        if event.name == "pending_created":
            case.status_final = PendingStatus.PENDING.value
            case.created_time_local = event.ts_local or case.created_time_local
            return case

        if event.name == "pending_confirmed":
            if case.status_final not in {"", PendingStatus.PENDING.value, PendingStatus.CONFIRMED.value}:
                raise InvalidTransitionError(f"cannot confirm from {case.status_final}")
            case.status_final = PendingStatus.CONFIRMED.value
            case.is_confirmed = True
            case.confirmed_time_local = event.ts_local or case.confirmed_time_local
            return case

        if event.name == "signal_sent":
            if not case.is_confirmed:
                raise InvalidTransitionError("cannot send before confirm")
            case.is_sent_signal = True
            case.sent_time_local = event.ts_local or case.sent_time_local
            return case

        if event.name == "pending_invalidated":
            if case.status_final not in {PendingStatus.PENDING.value, PendingStatus.CONFIRMED.value}:
                raise InvalidTransitionError(f"cannot invalidate from {case.status_final}")
            case.status_final = PendingStatus.INVALIDATED.value
            case.close_reason = event.close_reason
            case.close_time_local = event.ts_local or case.close_time_local
            case.case_close_type = CaseCloseType.TRUE_CLOSE
            case.lifecycle_complete = True
            return case

        if event.name == "pending_expired":
            if case.status_final not in {PendingStatus.PENDING.value, PendingStatus.CONFIRMED.value}:
                raise InvalidTransitionError(f"cannot expire from {case.status_final}")
            case.status_final = PendingStatus.EXPIRED_WAIT.value
            case.close_reason = event.close_reason
            case.close_time_local = event.ts_local or case.close_time_local
            case.case_close_type = CaseCloseType.TRUE_CLOSE
            case.lifecycle_complete = True
            return case

        if event.name == "fallback_close_snapshot_captured":
            case.case_close_type = CaseCloseType.FALLBACK_4H_SNAPSHOT
            case.lifecycle_complete = True
            return case

        raise InvalidTransitionError(f"unknown event {event.name}")
