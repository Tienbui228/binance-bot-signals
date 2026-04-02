import csv
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, Optional

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None

CANONICAL_STAGES = ["pre_pending", "pending_open", "entry_or_confirm", "case_close"]
STAGE_STATUS = {"captured", "not_applicable", "not_reached_yet", "capture_failed", "missing_unexpected"}
STAGE_CONTENT = {"chart_snapshot", "placeholder", "none"}


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        if v in (None, ""):
            return default
        return int(float(v))
    except Exception:
        return default


def _utc_ms_to_local_str(ts_ms: int, tz) -> str:
    if not ts_ms:
        return ""
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).astimezone(tz).strftime("%Y-%m-%d %H:%M:%S")


class CaseReviewRuntime:
    def __init__(self, workspace_dir: str | Path, tz_name: str = "Asia/Ho_Chi_Minh", fallback_close_hours: int = 4):
        self.workspace_dir = Path(workspace_dir)
        self.workspace_dir.mkdir(parents=True, exist_ok=True)
        self.cases_root = self.workspace_dir / "cases"
        self.cases_root.mkdir(parents=True, exist_ok=True)
        self.fallback_close_hours = int(fallback_close_hours)
        try:
            self.tz = ZoneInfo(tz_name) if ZoneInfo else timezone.utc
        except Exception:
            self.tz = timezone.utc

    def _case_dir(self, case_day: str, case_id: str) -> Path:
        d = self.cases_root / str(case_day) / str(case_id)
        d.mkdir(parents=True, exist_ok=True)
        return d

    def _case_path(self, case_day: str, case_id: str) -> Path:
        return self._case_dir(case_day, case_id) / "case_meta.json"

    def _default_stage(self, stage: str) -> Dict[str, Any]:
        if stage == "case_close":
            return {"stage_status": "not_reached_yet", "stage_content_type": "none", "image_path": "", "note": "", "case_close_type": "not_due_yet"}
        if stage == "entry_or_confirm":
            return {"stage_status": "not_reached_yet", "stage_content_type": "none", "image_path": "", "note": ""}
        return {"stage_status": "missing_unexpected", "stage_content_type": "none", "image_path": "", "note": ""}

    def _base_case(self, pending_row: Dict[str, Any]) -> Dict[str, Any]:
        created_ms = _safe_int(pending_row.get("created_ts_ms"))
        signal_ms = _safe_int(pending_row.get("signal_open_time"))
        case_id = pending_row.get("pending_id") or pending_row.get("setup_id") or ""
        case_day = datetime.fromtimestamp(max(created_ms, 0) / 1000.0, tz=timezone.utc).astimezone(self.tz).strftime("%Y-%m-%d") if created_ms else ""
        data = {
            "case_id": case_id,
            "symbol": pending_row.get("symbol", ""),
            "side": pending_row.get("side", ""),
            "strategy": pending_row.get("strategy", ""),
            "case_day": case_day,
            "signal_time_local": _utc_ms_to_local_str(signal_ms, self.tz),
            "created_time_local": _utc_ms_to_local_str(created_ms, self.tz),
            "confirmed_time_local": "",
            "sent_time_local": "",
            "close_time_local": "",
            "fallback_close_due_time_local": _utc_ms_to_local_str(created_ms + self.fallback_close_hours * 3600 * 1000, self.tz) if created_ms else "",
            "status_final": str(pending_row.get("status") or "PENDING"),
            "close_reason": str(pending_row.get("close_reason") or ""),
            "is_confirmed": "N",
            "is_sent_signal": "N",
            "case_close_type": "not_due_yet",
            "slot_bundle_complete": "Y",
            "evidence_ready_for_review": "none",
            "lifecycle_complete": "N",
            "human_review_status": "PENDING",
            "verdict_code": "",
            "root_cause_code": "",
            "action_candidate_code": "",
            "review_notes_short": "",
            "has_case_close_image": "N",
            "stages": {stage: self._default_stage(stage) for stage in CANONICAL_STAGES},
        }
        return data

    def _coerce_case(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        raw.setdefault("stages", {})
        for stage in CANONICAL_STAGES:
            raw["stages"].setdefault(stage, self._default_stage(stage))
        raw.setdefault("case_close_type", raw["stages"]["case_close"].get("case_close_type", "not_due_yet"))
        raw.setdefault("slot_bundle_complete", "Y")
        raw.setdefault("evidence_ready_for_review", "none")
        raw.setdefault("lifecycle_complete", "N")
        raw.setdefault("has_case_close_image", "N")
        return raw

    def _derive_semantics(self, case: Dict[str, Any]) -> Dict[str, Any]:
        case = self._coerce_case(case)
        stages = case["stages"]
        # normalize first two stages if still unexplained
        for stage in ("pre_pending", "pending_open"):
            s = stages[stage]
            if s["stage_status"] == "missing_unexpected" and s.get("image_path"):
                s["stage_status"] = "captured"
                s["stage_content_type"] = "chart_snapshot"

        status_final = str(case.get("status_final") or "PENDING").upper()
        terminal_without_confirm = status_final not in ("", "PENDING", "CONFIRMED") and case.get("is_confirmed") != "Y"
        if stages["entry_or_confirm"]["stage_status"] == "not_reached_yet" and terminal_without_confirm:
            stages["entry_or_confirm"]["stage_status"] = "not_applicable"
            stages["entry_or_confirm"]["stage_content_type"] = "none"
        if case.get("is_confirmed") == "Y" and stages["entry_or_confirm"]["stage_status"] == "not_reached_yet":
            stages["entry_or_confirm"]["stage_status"] = "missing_unexpected"

        if case.get("case_close_type") == "not_due_yet" and status_final not in ("", "PENDING", "CONFIRMED"):
            case["case_close_type"] = "true_close"
            stages["case_close"]["case_close_type"] = "true_close"

        close_stage = stages["case_close"]
        close_stage["case_close_type"] = case.get("case_close_type", close_stage.get("case_close_type", "not_due_yet"))
        if close_stage["stage_status"] == "not_reached_yet" and case.get("case_close_type") in ("true_close", "fallback_4h_snapshot"):
            close_stage["stage_status"] = "missing_unexpected"
        if case.get("case_close_type") == "not_due_yet":
            close_stage["stage_status"] = "not_reached_yet"
            close_stage["stage_content_type"] = "none"

        # completeness
        required = ["pre_pending", "pending_open"]
        if case.get("is_confirmed") == "Y":
            required.append("entry_or_confirm")
        if case.get("case_close_type") in ("true_close", "fallback_4h_snapshot"):
            required.append("case_close")
        captured = 0
        for stage in required:
            s = stages[stage]
            if s["stage_status"] == "captured" and s["stage_content_type"] == "chart_snapshot":
                captured += 1
        if not required:
            evidence = "none"
        elif captured == len(required):
            evidence = "full"
        elif captured > 0:
            evidence = "partial"
        else:
            evidence = "none"
        case["slot_bundle_complete"] = "Y"
        case["evidence_ready_for_review"] = evidence
        case["lifecycle_complete"] = "Y" if (case.get("case_close_type") in ("true_close", "fallback_4h_snapshot") and close_stage["stage_status"] == "captured") else "N"
        case["has_case_close_image"] = "Y" if (close_stage["stage_status"] == "captured" and close_stage["stage_content_type"] == "chart_snapshot") else "N"
        return case

    def _save_case(self, case_day: str, case_id: str, case: Dict[str, Any]) -> Dict[str, Any]:
        case = self._derive_semantics(case)
        path = self._case_path(case_day, case_id)
        path.write_text(json.dumps(case, ensure_ascii=False, indent=2), encoding="utf-8")
        return case

    def _load_case_dict(self, case_day: str, case_id: str) -> Optional[Dict[str, Any]]:
        path = self._case_path(case_day, case_id)
        if not path.exists():
            return None
        try:
            return self._coerce_case(json.loads(path.read_text(encoding="utf-8")))
        except Exception:
            return None

    def _load_case(self, case_day: str, case_id: str):
        case = self._load_case_dict(case_day, case_id)
        if not case:
            return None
        return SimpleNamespace(**case)

    def has_captured_stage(self, case_day: str, case_id: str, stage: str) -> bool:
        case = self._load_case_dict(case_day, case_id)
        if not case:
            return False
        s = case.get("stages", {}).get(stage, {})
        return s.get("stage_status") == "captured" and s.get("stage_content_type") == "chart_snapshot" and bool(s.get("image_path"))

    def ensure_case(self, pending_row: Dict[str, Any]) -> Dict[str, Any]:
        case_id = pending_row.get("pending_id") or pending_row.get("setup_id") or ""
        created_ms = _safe_int(pending_row.get("created_ts_ms"))
        case_day = datetime.fromtimestamp(max(created_ms, 0) / 1000.0, tz=timezone.utc).astimezone(self.tz).strftime("%Y-%m-%d") if created_ms else ""
        existing = self._load_case_dict(case_day, case_id)
        if existing:
            return existing
        return self._save_case(case_day, case_id, self._base_case(pending_row))

    def register_stage_image(self, case_day: str, case_id: str, stage: str, image_path: Optional[str], note: str = ""):
        case = self._load_case_dict(case_day, case_id)
        if not case:
            raise ValueError(f"Case not found: {case_day} {case_id}")
        stage_data = case["stages"][stage]
        stage_data["note"] = note or stage_data.get("note", "")
        if image_path and Path(image_path).exists():
            stage_data["stage_status"] = "captured"
            stage_data["stage_content_type"] = "chart_snapshot"
            stage_data["image_path"] = str(image_path)
        else:
            stage_data["stage_status"] = "capture_failed"
            stage_data["stage_content_type"] = "none"
            stage_data["image_path"] = ""
        if stage == "case_close":
            if "fallback_case_close_after" in (note or ""):
                case["case_close_type"] = "fallback_4h_snapshot"
            elif case.get("case_close_type") == "not_due_yet":
                case["case_close_type"] = "true_close"
            stage_data["case_close_type"] = case["case_close_type"]
        self._save_case(case_day, case_id, case)

    def register_confirmed(self, case_day: str, case_id: str, pending_row: Dict[str, Any]):
        case = self._load_case_dict(case_day, case_id)
        if not case:
            raise ValueError(f"Case not found: {case_day} {case_id}")
        ts = _safe_int(pending_row.get("closed_ts_ms") or pending_row.get("created_ts_ms"))
        case["is_confirmed"] = "Y"
        if not case.get("confirmed_time_local"):
            case["confirmed_time_local"] = _utc_ms_to_local_str(ts, self.tz)
        case["status_final"] = "CONFIRMED"
        self._save_case(case_day, case_id, case)

    def register_sent_signal(self, case_day: str, case_id: str, signal_row: Dict[str, Any]):
        case = self._load_case_dict(case_day, case_id)
        if not case:
            raise ValueError(f"Case not found: {case_day} {case_id}")
        ts = _safe_int(signal_row.get("timestamp_ms"))
        case["is_sent_signal"] = "Y"
        case["sent_time_local"] = _utc_ms_to_local_str(ts, self.tz)
        if case.get("is_confirmed") != "Y":
            case["is_confirmed"] = "Y"
            if not case.get("confirmed_time_local"):
                case["confirmed_time_local"] = _utc_ms_to_local_str(ts, self.tz)
        self._save_case(case_day, case_id, case)

    def register_close(self, case_day: str, case_id: str, close_row: Dict[str, Any]):
        case = self._load_case_dict(case_day, case_id)
        if not case:
            raise ValueError(f"Case not found: {case_day} {case_id}")
        status = str(close_row.get("status") or close_row.get("outcome") or case.get("status_final") or "")
        close_ts = _safe_int(close_row.get("close_time_ms") or close_row.get("closed_ts_ms"))
        close_reason = str(close_row.get("close_reason") or close_row.get("outcome") or "")
        if status:
            case["status_final"] = status
        if close_reason:
            case["close_reason"] = close_reason
        if close_ts:
            case["close_time_local"] = _utc_ms_to_local_str(close_ts, self.tz)
        case["case_close_type"] = "true_close"
        case["stages"]["case_close"]["case_close_type"] = "true_close"
        if case.get("is_confirmed") != "Y" and status.upper() not in ("", "PENDING", "CONFIRMED"):
            case["stages"]["entry_or_confirm"]["stage_status"] = "not_applicable"
            case["stages"]["entry_or_confirm"]["stage_content_type"] = "none"
            case["stages"]["entry_or_confirm"]["image_path"] = ""
        self._save_case(case_day, case_id, case)
