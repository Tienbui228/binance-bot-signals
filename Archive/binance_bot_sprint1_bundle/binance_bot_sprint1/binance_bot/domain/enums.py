from __future__ import annotations

from enum import Enum


class Side(str, Enum):
    LONG = "LONG"
    SHORT = "SHORT"


class PendingStatus(str, Enum):
    PENDING = "PENDING"
    CONFIRMED = "CONFIRMED"
    INVALIDATED = "INVALIDATED"
    EXPIRED_WAIT = "EXPIRED_WAIT"
    REJECTED_SCORE = "REJECTED_SCORE"
    REJECTED_RULE = "REJECTED_RULE"
    SKIPPED_SEND = "SKIPPED_SEND"


class SignalStatus(str, Enum):
    OPEN = "OPEN"
    CLOSED = "CLOSED"


class StageName(str, Enum):
    PRE_PENDING = "pre_pending"
    PENDING_OPEN = "pending_open"
    ENTRY_OR_CONFIRM = "entry_or_confirm"
    CASE_CLOSE = "case_close"


class StageStatus(str, Enum):
    CAPTURED = "captured"
    NOT_APPLICABLE = "not_applicable"
    NOT_REACHED_YET = "not_reached_yet"
    CAPTURE_FAILED = "capture_failed"
    MISSING_UNEXPECTED = "missing_unexpected"


class StageContentType(str, Enum):
    CHART_SNAPSHOT = "chart_snapshot"
    PLACEHOLDER = "placeholder"
    NONE = "none"


class CaseCloseType(str, Enum):
    TRUE_CLOSE = "true_close"
    FALLBACK_4H_SNAPSHOT = "fallback_4h_snapshot"
    NOT_DUE_YET = "not_due_yet"


class EvidenceReadiness(str, Enum):
    FULL = "full"
    PARTIAL = "partial"
    NONE = "none"


class CaseLifecycle(str, Enum):
    PENDING_ONLY = "pending_only"
    CONFIRMED_INTERNAL = "confirmed_internal"
    SENT_SIGNAL = "sent_signal"
    CLOSED_TRADE = "closed_trade"
