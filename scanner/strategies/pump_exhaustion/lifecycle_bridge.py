import time
from typing import Dict

try:
    from regime.regime_normalizer import enrich_row_with_regime as _enrich
except Exception:
    _enrich = None


def write_to_lifecycle(app, case: Dict, score_result: Dict) -> None:
    """Append a pump_exhaustion signal to app.pending_file and app.signals_file.

    Bypasses PendingSetup/Signal dataclasses — writes raw dict through
    app._normalize_row_for_fields so unknown fields are silently dropped
    and missing fields default to "".

    Fails silently — must never crash the pump scanner.
    """
    try:
        _write_pending(app, case, score_result)
    except Exception as e:
        print(f"[pump_lifecycle] write_pending failed {case.get('symbol')}: {e}")
    try:
        _write_signal(app, case, score_result)
    except Exception as e:
        print(f"[pump_lifecycle] write_signal failed {case.get('symbol')}: {e}")


# ── Internal helpers ─────────────────────────────────────────────────────────

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
        # Score components — mapped to nearest PendingSetup counterparts
        "score_oi": _safe_float(score_result.get("score_pump_context")),
        "score_exhaustion": _safe_float(score_result.get("score_exhaustion")),
        "score_breakout": _safe_float(score_result.get("score_breakdown_quality")),
        "score_retest": _safe_float(score_result.get("score_delivery")),
        # Status — pump signal fires immediately at confirmation
        "status": "CONFIRMED",
        "is_confirmed": "Y",
        "confirmed_ts_ms": str(now_ms),
        "is_sent_signal": "Y",
        "sent_ts_ms": str(now_ms),
        "send_decision": "SENT",
        "close_reason": "signal confirmed",
        "close_trigger_detail": "pump_exhaustion_score_threshold",
        # Entry / exit
        "entry_price": entry,
        "stop_loss": stop,
        "tp1": tp1,
        "tp2": tp2,
        # Semantic housekeeping
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


def _write_signal(app, case: Dict, score_result: Dict) -> None:
    now_ms = int(time.time() * 1000)
    p3_ts = _safe_int(case.get("p3_ts"), now_ms)
    conf_float, _ = _confidence(score_result)
    score = score_result.get("score_total", 0)
    entry = _safe_float(case.get("entry_price"))
    stop = _safe_float(case.get("stop_price"))
    tp1 = _safe_float(case.get("target_conservative"))
    tp2 = _safe_float(case.get("target_extreme"))
    retest_high = _safe_float(case.get("retest_high") or entry)

    sl_dist = (stop - entry) / entry * 100 if entry > 0 and stop > entry else 0.0
    tp1_dist = (entry - tp1) / entry * 100 if entry > 0 and 0 < tp1 < entry else 0.0
    tp2_dist = (entry - tp2) / entry * 100 if entry > 0 and 0 < tp2 < entry else 0.0

    row = {
        "signal_id": f"{case.get('case_id', '')}_sig",
        "setup_id": case.get("case_id", ""),
        "timestamp_ms": p3_ts,
        "symbol": case.get("symbol", ""),
        "side": "SHORT",
        "score": score,
        "confidence": conf_float,
        "reason": "pump_exhaustion_short",
        "breakout_level": _safe_float(case.get("breakdown_level")),
        "entry_low": entry,
        "entry_high": retest_high,
        "entry_ref": entry,
        "stop": stop,
        "tp1": tp1,
        "tp2": tp2,
        "price": _safe_float(case.get("current_price_at_scan")),
        "oi_jump_pct": _safe_float(case.get("oi_change_from_peak_pct")),
        "funding_pct": _safe_float(case.get("latest_funding_rate")),
        "vol_ratio": 0.0,
        "retest_bars_waited": int(case.get("retest_duration_bars") or 0),
        "strategy": "pump_exhaustion_short",
        "market_regime": case.get("regime_label") or "unknown",
        "btc_regime": case.get("regime_label") or "unknown",
        "regime_label": case.get("regime_label") or "unknown",
        "score_oi": _safe_float(score_result.get("score_pump_context")),
        "score_exhaustion": _safe_float(score_result.get("score_exhaustion")),
        "score_breakout": _safe_float(score_result.get("score_breakdown_quality")),
        "score_retest": _safe_float(score_result.get("score_delivery")),
        "reason_tags": _reason_tags(case, score_result),
        "risk_pct_real": sl_dist,
        "sl_distance_pct": sl_dist,
        "tp1_distance_pct": tp1_dist,
        "tp2_distance_pct": tp2_dist,
        "status": "OPEN",
        "dispatch_action": "SENT",
        "dispatch_confidence_band": score_result.get("confidence_label") or "LOW",
        "dispatch_reason": "pump_exhaustion_score_threshold",
    }

    app.append_csv(
        app.signals_file,
        app._normalize_row_for_fields(row, app.signal_fields),
        fieldnames=app.signal_fields,
    )


def _confidence(score_result: Dict):
    label = score_result.get("confidence_label") or "LOW"
    mapping = {"HIGH": 0.9, "MEDIUM": 0.6, "LOW": 0.3}
    return mapping.get(label, 0.3), label


def _reason_tags(case: Dict, score_result: Dict) -> str:
    pump_pct = case.get("pump_pct") or 0
    return (
        f"oi_regime={case.get('oi_regime_label','?')}|"
        f"cvd_1h={case.get('cvd_proxy_1h_trend','?')}|"
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
