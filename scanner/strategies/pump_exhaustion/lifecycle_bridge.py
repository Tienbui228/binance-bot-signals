import time
from typing import Dict

try:
    from regime.regime_normalizer import enrich_row_with_regime as _enrich
except Exception:
    _enrich = None


def write_to_lifecycle(app, case: Dict, score_result: Dict) -> None:
    """Append a pump_exhaustion signal row to app.pending_file (daily report source).

    Writes raw dict through app._normalize_row_for_fields — extra fields dropped,
    missing fields default to "". Fails silently, must never crash the pump scanner.
    """
    try:
        _write_pending(app, case, score_result)
    except Exception as e:
        import traceback
        print(f"[pump_lifecycle] write_pending failed {case.get('symbol')}: {e}")
        traceback.print_exc()


def _write_pending(app, case: Dict, score_result: Dict) -> None:
    now_ms = int(time.time() * 1000)
    p3_ts = _safe_int(case.get("p3_ts"), now_ms)
    conf_float, conf_label = _confidence(score_result)
    score = score_result.get("score_total", 0)
    score_max = score_result.get("score_max", 12)
    entry = _safe_float(case.get("entry_price"))
    stop = _safe_float(case.get("stop_price"))
    tp1 = _safe_float(case.get("target_conservative"))
    tp2 = _safe_float(case.get("target_extreme"))

    row = {
        "pending_id": case.get("case_id", ""),
        "setup_id": case.get("case_id", ""),
        "created_ts_ms": p3_ts,
        "signal_open_time": p3_ts,
        "symbol": case.get("symbol", ""),
        "side": "SHORT",
        "score": score,
        "confidence": conf_float,
        "reason": f"pump_exhaustion_short {score}/{score_max} {conf_label}",
        "reason_tags": _reason_tags(case, score_result),
        "breakout_level": _safe_float(case.get("breakdown_level")),
        "signal_price": entry,
        "signal_high": _safe_float(case.get("retest_high") or entry),
        "signal_low": entry,
        "oi_jump_pct": _safe_float(case.get("oi_change_from_peak_pct")),
        "funding_pct": _safe_float(case.get("latest_funding_rate")),
        "vol_ratio": 0.0,
        "strategy": "pump_exhaustion_short",
        "market_regime": case.get("regime_label") or "unknown",
        "regime_label": case.get("regime_label") or "unknown",
        "btc_regime": case.get("regime_label") or "unknown",
        "score_oi": _safe_float(score_result.get("score_pump_context")),
        "score_exhaustion": _safe_float(score_result.get("score_exhaustion")),
        "score_breakout": _safe_float(score_result.get("score_breakdown_quality")),
        "score_retest": _safe_float(score_result.get("score_delivery")),
        # Confirmed immediately — pump signal fires at scoring time
        "status": "CONFIRMED",
        "is_confirmed": "Y",
        "confirmed_ts_ms": str(now_ms),
        "is_sent_signal": "Y",
        "sent_ts_ms": str(now_ms),
        "send_decision": "SENT",
        "close_reason": "signal confirmed",
        "close_trigger_detail": "pump_exhaustion_score_threshold",
        "entry_price": entry,
        "stop_loss": stop,
        "tp1": tp1,
        "tp2": tp2,
        "semantic_consistency": "Y",
        "semantic_issue": "",
        "review_eligible": "Y",
        "review_exclusion_reason": "",
        "close_anchor_time_ms": "",
        "close_capture_basis": "",
        "confirm_fail_detail": "",
        "invalidation_detail": "",
        "setup_quality_band": "not_evaluated",
        "delivery_band": "not_evaluated",
        "veto_reason_code": "not_evaluated",
        "dispatch_action": "not_evaluated",
        "dispatch_confidence_band": "not_evaluated",
        "dispatch_reason": "not_evaluated",
    }

    if _enrich is not None:
        try:
            row = _enrich(row)
        except Exception:
            pass

    app.append_csv(
        app.pending_file,
        app._normalize_row_for_fields(row, app.pending_fields),
        fieldnames=app.pending_fields,
    )


def _confidence(score_result: Dict):
    label = score_result.get("confidence_label") or "LOW"
    return {"HIGH": 0.9, "MEDIUM": 0.6, "LOW": 0.3}.get(label, 0.3), label


def _reason_tags(case: Dict, score_result: Dict) -> str:
    pump_pct = case.get("pump_pct") or 0
    return (
        f"oi_regime={case.get('oi_regime_label', '?')}|"
        f"cvd_1h={case.get('cvd_proxy_1h_trend', '?')}|"
        f"pump_pct={pump_pct:.0%}|"
        f"score_regime={score_result.get('score_regime', 0)}|"
        f"score_market={score_result.get('score_market_pressure', 0)}"
    )


def _safe_float(v) -> float:
    try:
        return float(v) if v not in (None, "", "not_reached_yet") else 0.0
    except (TypeError, ValueError):
        return 0.0


def _safe_int(v, fallback: int = 0) -> int:
    try:
        return int(v) if v not in (None, "", "not_reached_yet") else fallback
    except (TypeError, ValueError):
        return fallback
