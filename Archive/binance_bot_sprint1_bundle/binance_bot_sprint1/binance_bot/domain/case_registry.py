from __future__ import annotations

from dataclasses import asdict
from typing import Dict

from .enums import (
    CaseCloseType,
    EvidenceReadiness,
    Side,
    StageContentType,
    StageName,
    StageStatus,
)
from .models import CaseRecord, StageSlot


class CaseRegistry:
    def __init__(self) -> None:
        self._cases: Dict[str, CaseRecord] = {}

    def create_case(
        self,
        *,
        case_id: str,
        case_day: str,
        symbol: str,
        side: str,
        strategy: str,
        created_time_local: str | None = None,
    ) -> CaseRecord:
        case = CaseRecord(
            case_id=case_id,
            case_day=case_day,
            symbol=symbol,
            side=Side(side),
            strategy=strategy,
            created_time_local=created_time_local,
            pre_pending=StageSlot(StageName.PRE_PENDING, StageStatus.NOT_REACHED_YET, StageContentType.NONE),
            pending_open=StageSlot(StageName.PENDING_OPEN, StageStatus.NOT_REACHED_YET, StageContentType.NONE),
            entry_or_confirm=StageSlot(StageName.ENTRY_OR_CONFIRM, StageStatus.NOT_REACHED_YET, StageContentType.NONE),
            case_close=StageSlot(StageName.CASE_CLOSE, StageStatus.NOT_REACHED_YET, StageContentType.NONE),
        )
        self._cases[case_id] = case
        self.recompute(case)
        return case

    def get(self, case_id: str) -> CaseRecord:
        return self._cases[case_id]

    def upsert_stage(
        self,
        case_id: str,
        stage_name: StageName,
        stage_status: StageStatus,
        content_type: StageContentType,
        image_path: str | None = None,
        capture_time_local: str | None = None,
    ) -> CaseRecord:
        case = self.get(case_id)
        slot = StageSlot(
            stage_name=stage_name,
            stage_status=stage_status,
            stage_content_type=content_type,
            image_path=image_path,
            capture_time_local=capture_time_local,
        )
        if stage_name == StageName.PRE_PENDING:
            case.pre_pending = slot
        elif stage_name == StageName.PENDING_OPEN:
            case.pending_open = slot
        elif stage_name == StageName.ENTRY_OR_CONFIRM:
            case.entry_or_confirm = slot
        elif stage_name == StageName.CASE_CLOSE:
            case.case_close = slot
        else:
            raise ValueError(stage_name)
        self.recompute(case)
        return case

    def recompute(self, case: CaseRecord) -> CaseRecord:
        slots = [case.pre_pending, case.pending_open, case.entry_or_confirm, case.case_close]
        case.slot_bundle_complete = len(slots) == 4
        captured = [
            slot
            for slot in slots
            if slot.stage_status == StageStatus.CAPTURED
            and slot.stage_content_type == StageContentType.CHART_SNAPSHOT
        ]
        if len(captured) == 0:
            case.evidence_ready_for_review = EvidenceReadiness.NONE
        elif len(captured) == 4:
            case.evidence_ready_for_review = EvidenceReadiness.FULL
        else:
            case.evidence_ready_for_review = EvidenceReadiness.PARTIAL
        case.lifecycle_complete = case.case_close_type in {
            CaseCloseType.TRUE_CLOSE,
            CaseCloseType.FALLBACK_4H_SNAPSHOT,
        }
        return case

    def as_flat_dict(self, case_id: str) -> Dict[str, object]:
        case = self.get(case_id)
        data = asdict(case)
        for slot_name in ("pre_pending", "pending_open", "entry_or_confirm", "case_close"):
            slot = data.pop(slot_name)
            for key, value in slot.items():
                data[f"{slot_name}_{key}"] = value
        return data
