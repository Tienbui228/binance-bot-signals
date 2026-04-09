"""
report_builder.py
Builds the 4 measurement boards and the markdown summary report (Section 14–16).

Board A — Rule Summary (coverage + quality)
Board B — Outcome Distribution
Board C — Feature Slice Board
Board D — Regime Slice Board

All boards are downstream-only — no truth is invented here.
Offline research only.
"""
from __future__ import annotations

import math
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from research.signature_measurement.contracts import F, OutcomeClass, RuleDecisionCode

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _pct(x: Any, digits: int = 1) -> str:
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return "—"
    return f"{x:.{digits}f}%"


def _num(x: Any, digits: int = 2) -> str:
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return "—"
    return f"{x:.{digits}f}"


def _cnt(x: Any) -> str:
    if x is None:
        return "—"
    return str(int(x))


def _eligible(df: pd.DataFrame) -> pd.DataFrame:
    return df[df[F.ELIGIBLE_FOR_MEASUREMENT_YN] == "Y"].copy()


def _outcome_eligible(df: pd.DataFrame) -> pd.DataFrame:
    """Rows that are eligible AND have 1h outcome available."""
    elig = _eligible(df)
    return elig[elig[F.OUTCOME_1H_AVAILABLE_YN] == "Y"].copy()


def _fnum(df: pd.DataFrame, col: str) -> pd.Series:
    return pd.to_numeric(df[col], errors="coerce")


def _rate(sub: pd.DataFrame, outcome: str) -> float:
    total = len(sub)
    if total == 0:
        return float("nan")
    return len(sub[sub[F.OUTCOME_CLASS] == outcome]) / total * 100


def _a_or_better_rate(sub: pd.DataFrame) -> float:
    total = len(sub)
    if total == 0:
        return float("nan")
    ab = len(sub[sub[F.OUTCOME_CLASS].isin([OutcomeClass.A_PLUS_MOVE, OutcomeClass.A_MOVE])])
    return ab / total * 100


def _fail_rate(sub: pd.DataFrame) -> float:
    return _rate(sub, OutcomeClass.FAIL)


# ---------------------------------------------------------------------------
# Board A — Rule Summary
# ---------------------------------------------------------------------------

def build_board_a(df: pd.DataFrame, stats: Dict) -> Dict:
    """
    Top-level quality + coverage board (Section 14 Board A).
    stats: output from overlap_filter (raw_fire_count, kept_fire_count, dropped_overlap_count)
    """
    elig = _eligible(df)
    meas = _outcome_eligible(df)
    excluded = len(elig) - len(meas)
    unique_syms = df[F.SYMBOL].nunique() if not df.empty else 0

    # Date span
    if not df.empty and F.TRIGGER_TS_MS in df.columns:
        ts_col = pd.to_numeric(df[F.TRIGGER_TS_MS], errors="coerce")
        span_days = (ts_col.max() - ts_col.min()) / (86400 * 1000)
        fires_per_day = stats["kept_fire_count"] / max(span_days, 1)
    else:
        span_days = 0
        fires_per_day = 0

    a_plus_r = _rate(meas, OutcomeClass.A_PLUS_MOVE)
    a_or_b_r = _a_or_better_rate(meas)
    fail_r = _fail_rate(meas)

    fav_1h = _fnum(meas, F.FUTURE_1H_MAX_FAVOR_PCT)
    adv_1h = _fnum(meas, F.FUTURE_1H_MAX_ADVERSE_PCT)
    fav_4h = _fnum(meas, F.FUTURE_4H_MAX_FAVOR_PCT)
    adv_4h = _fnum(meas, F.FUTURE_4H_MAX_ADVERSE_PCT)
    ttf_2pct = _fnum(meas, F.TIME_TO_2PCT_FAVOR_MIN)

    return {
        "raw_fire_count": stats.get("raw_fire_count", 0),
        "kept_fire_count": stats.get("kept_fire_count", 0),
        "dropped_overlap_count": stats.get("dropped_overlap_count", 0),
        "eligible_events": len(elig),
        "measurement_eligible_events": len(meas),
        "excluded_events": excluded,
        "unique_symbols_hit": unique_syms,
        "fires_per_day": round(fires_per_day, 2),
        "span_days": round(span_days, 1),
        "a_plus_rate_pct": a_plus_r,
        "a_or_better_rate_pct": a_or_b_r,
        "fail_rate_pct": fail_r,
        "median_future_1h_max_favor_pct": fav_1h.median(),
        "median_future_1h_max_adverse_pct": adv_1h.median(),
        "median_future_4h_max_favor_pct": fav_4h.median(),
        "median_future_4h_max_adverse_pct": adv_4h.median(),
        "median_time_to_2pct_favor_min": ttf_2pct.median(),
    }


# ---------------------------------------------------------------------------
# Board B — Outcome Distribution
# ---------------------------------------------------------------------------

def build_board_b(df: pd.DataFrame) -> List[Dict]:
    """Outcome bucket counts and shares (Section 14 Board B)."""
    meas = _outcome_eligible(df)
    total = len(meas)
    buckets = []
    for oc in [OutcomeClass.A_PLUS_MOVE, OutcomeClass.A_MOVE, OutcomeClass.B_MOVE,
               OutcomeClass.NOISE, OutcomeClass.FAIL, OutcomeClass.NOT_AVAILABLE_YET]:
        cnt = len(meas[meas[F.OUTCOME_CLASS] == oc])
        buckets.append({
            "outcome_class": oc,
            "count": cnt,
            "share_pct": round(cnt / max(total, 1) * 100, 1),
        })
    return buckets


# ---------------------------------------------------------------------------
# Board C — Feature Slice Board
# ---------------------------------------------------------------------------

def build_board_c(df: pd.DataFrame) -> Dict[str, List[Dict]]:
    """
    Slice the outcome by feature bands to identify where the signature is strongest.
    Slices: top_vs_global_divergence, oi_delta_3bar_pct, taker_imbalance,
            flow_phase_code, breakout_quality_band.
    """
    meas = _outcome_eligible(df)
    result = {}

    # Continuous slices: bin into low / mid / high
    for col, lo_thresh, hi_thresh in [
        (F.TOP_VS_GLOBAL_DIVERGENCE, 0.5, 1.5),
        (F.OI_DELTA_3BAR_PCT, 2.0, 5.0),
        (F.TAKER_IMBALANCE, 0.08, 0.20),
    ]:
        vals = _fnum(meas, col)
        slices = []
        for label, mask in [
            ("low", vals < lo_thresh),
            ("mid", (vals >= lo_thresh) & (vals < hi_thresh)),
            ("high", vals >= hi_thresh),
        ]:
            sub = meas[mask]
            slices.append(_slice_row(label, sub))
        result[col] = slices

    # Categorical slices
    for col in [F.FLOW_PHASE_CODE, F.BREAKOUT_QUALITY_BAND]:
        cats = meas[col].dropna().unique()
        slices = [_slice_row(cat, meas[meas[col] == cat]) for cat in sorted(cats)]
        result[col] = slices

    return result


def _slice_row(label: str, sub: pd.DataFrame) -> Dict:
    return {
        "slice": label,
        "events": len(sub),
        "a_or_better_rate_pct": _a_or_better_rate(sub),
        "fail_rate_pct": _fail_rate(sub),
        "median_1h_favor_pct": _fnum(sub, F.FUTURE_1H_MAX_FAVOR_PCT).median(),
        "median_1h_adverse_pct": _fnum(sub, F.FUTURE_1H_MAX_ADVERSE_PCT).median(),
    }


# ---------------------------------------------------------------------------
# Board D — Regime Slice Board
# ---------------------------------------------------------------------------

def build_board_d(df: pd.DataFrame) -> Dict[str, List[Dict]]:
    """
    Regime + market context slices:
    regime_label_candidate, btc_24h_band, alt_breadth_band, volatility_band.
    """
    meas = _outcome_eligible(df)
    result = {}

    for col in [F.REGIME_LABEL_CANDIDATE, F.BTC_24H_BAND, F.ALT_BREADTH_BAND, F.VOLATILITY_BAND]:
        if col not in meas.columns:
            result[col] = []
            continue
        cats = meas[col].dropna().unique()
        slices = []
        for cat in sorted(cats):
            sub = meas[meas[col] == cat]
            slices.append({
                "slice": cat,
                "events": len(sub),
                "a_plus_rate_pct": _rate(sub, OutcomeClass.A_PLUS_MOVE),
                "a_or_better_rate_pct": _a_or_better_rate(sub),
                "fail_rate_pct": _fail_rate(sub),
                "median_4h_favor_pct": _fnum(sub, F.FUTURE_4H_MAX_FAVOR_PCT).median(),
                "median_4h_adverse_pct": _fnum(sub, F.FUTURE_4H_MAX_ADVERSE_PCT).median(),
            })
        result[col] = slices

    return result


# ---------------------------------------------------------------------------
# Rule decision code (Section 15, 16)
# ---------------------------------------------------------------------------

def determine_decision_code(
    df: pd.DataFrame,
    board_a: Dict,
    baseline_df: Optional[pd.DataFrame] = None,
) -> Dict:
    """
    Emit rule_decision_code based on Section 16 acceptance criteria.
    """
    meas_cnt = board_a.get("measurement_eligible_events", 0)
    unique_syms = board_a.get("unique_symbols_hit", 0)
    a_or_b = board_a.get("a_or_better_rate_pct", 0.0)
    fail_r = board_a.get("fail_rate_pct", 0.0)
    a_plus_r = board_a.get("a_plus_rate_pct", 0.0)
    med_fav = board_a.get("median_future_1h_max_favor_pct")
    med_adv = board_a.get("median_future_1h_max_adverse_pct")

    reasons: List[str] = []
    risks: List[str] = []
    code = RuleDecisionCode.LOW_SIGNAL_REJECT

    # Coverage check
    if meas_cnt < 30:
        code = RuleDecisionCode.TOO_FEW_EVENTS
        reasons.append(f"Only {meas_cnt} eligible events (need >= 30)")
        risks.append("Sample too small for reliable statistics")
        return _decision_result(code, reasons, risks)

    if unique_syms < 10:
        code = RuleDecisionCode.TOO_FEW_EVENTS
        reasons.append(f"Only {unique_syms} unique symbols (need >= 10)")
        risks.append("Results may not generalize beyond a narrow cluster")

    # Outlier dependence: check if mean >> median for favor
    meas = _outcome_eligible(df)
    fav_1h = _fnum(meas, F.FUTURE_1H_MAX_FAVOR_PCT)
    if not fav_1h.empty:
        ratio = fav_1h.mean() / max(fav_1h.median(), 0.01)
        if ratio > 3.0:
            code = RuleDecisionCode.OUTLIER_DEPENDENT
            reasons.append(f"Mean/median favor ratio = {ratio:.1f}x (outlier sensitivity)")
            risks.append("Remove top 5% events and re-check a_or_better_rate")

    # Regime specificity: check if one regime dominates (> 80% of events)
    if F.REGIME_LABEL_CANDIDATE in meas.columns:
        regime_counts = meas[F.REGIME_LABEL_CANDIDATE].value_counts(normalize=True)
        if not regime_counts.empty and regime_counts.iloc[0] > 0.80:
            top_regime = regime_counts.index[0]
            code = RuleDecisionCode.REGIME_SPECIFIC_ONLY
            reasons.append(f"80%+ of events in regime: {top_regime}")
            risks.append("Signal may not work across all regimes")

    # Quality gates
    if math.isnan(a_or_b) or a_or_b < 20.0:
        reasons.append(f"a_or_better_rate = {_pct(a_or_b)} (weak)")
        risks.append("Low quality rate — rule fires too often without follow-through")
    elif a_or_b >= 35.0 and fail_r < 25.0 and meas_cnt >= 30 and unique_syms >= 10:
        if a_plus_r >= 15.0:
            code = RuleDecisionCode.PROMISING_KEEP_TESTING
            reasons.append(f"a_or_better_rate = {_pct(a_or_b)}, a_plus_rate = {_pct(a_plus_r)}")
        else:
            code = RuleDecisionCode.PROMISING_NEEDS_REFINEMENT
            reasons.append(f"a_or_better_rate = {_pct(a_or_b)} but a_plus_rate = {_pct(a_plus_r)}")
    else:
        code = RuleDecisionCode.PROMISING_NEEDS_REFINEMENT if a_or_b >= 25.0 else RuleDecisionCode.LOW_SIGNAL_REJECT
        reasons.append(f"a_or_better_rate = {_pct(a_or_b)}, fail_rate = {_pct(fail_r)}")

    if med_fav is not None and med_adv is not None and not math.isnan(med_fav) and not math.isnan(med_adv):
        if med_fav > med_adv * 1.5:
            reasons.append(f"Median favor {_pct(med_fav)} > {_pct(med_adv)} adverse (favorable payoff shape)")
        else:
            risks.append(f"Median favor {_pct(med_fav)} not materially above adverse {_pct(med_adv)}")

    risks.append("Historical 30-day window only — validate against longer history")
    risks.append("Proxy data missing for some symbols may inflate quality rates")

    return _decision_result(code, reasons[:3], risks[:3])


def _decision_result(code: RuleDecisionCode, reasons: List[str], risks: List[str]) -> Dict:
    return {
        "rule_decision_code": code,
        "decision_reason": reasons[0] if reasons else "",
        "top_3_supporting_findings": reasons[:3],
        "top_3_risks": risks[:3],
    }


# ---------------------------------------------------------------------------
# Markdown renderer
# ---------------------------------------------------------------------------

def render_markdown_report(
    board_a: Dict,
    board_b: List[Dict],
    board_c: Dict[str, List[Dict]],
    board_d: Dict[str, List[Dict]],
    decision: Dict,
    run_meta: Dict,
) -> str:
    lines: List[str] = []

    lines += [
        f"# Rule Measurement Summary",
        f"",
        f"**Rule family:** {run_meta.get('rule_family', '—')}  ",
        f"**Rule version:** {run_meta.get('rule_version', '—')}  ",
        f"**Measurement version:** {run_meta.get('measurement_version', '—')}  ",
        f"**Universe version:** {run_meta.get('universe_version', '—')}  ",
        f"**Side:** {run_meta.get('side', '—')}  ",
        f"**Window:** {run_meta.get('window_start', '—')} → {run_meta.get('window_end', '—')}  ",
        f"**Run timestamp:** {run_meta.get('run_ts', '—')}",
        f"",
    ]

    # --- Decision banner ---
    code = decision.get("rule_decision_code", "—")
    lines += [
        f"---",
        f"## Decision",
        f"",
        f"**`{code}`**",
        f"",
        f"**Reason:** {decision.get('decision_reason', '—')}",
        f"",
        f"**Supporting findings:**",
    ]
    for f_ in decision.get("top_3_supporting_findings", []):
        lines.append(f"- {f_}")
    lines += ["", "**Risks:**"]
    for r in decision.get("top_3_risks", []):
        lines.append(f"- {r}")
    lines.append("")

    # --- Board A ---
    a = board_a
    lines += [
        "---",
        "## Board A — Rule Summary",
        "",
        f"| Metric | Value |",
        f"|--------|-------|",
        f"| Raw fires | {_cnt(a.get('raw_fire_count'))} |",
        f"| Kept fires (after dedup) | {_cnt(a.get('kept_fire_count'))} |",
        f"| Dropped (overlap) | {_cnt(a.get('dropped_overlap_count'))} |",
        f"| Eligible events | {_cnt(a.get('eligible_events'))} |",
        f"| Measurement-eligible events | {_cnt(a.get('measurement_eligible_events'))} |",
        f"| Excluded events | {_cnt(a.get('excluded_events'))} |",
        f"| Unique symbols hit | {_cnt(a.get('unique_symbols_hit'))} |",
        f"| Fires per day | {_num(a.get('fires_per_day'))} |",
        f"| Window span (days) | {_num(a.get('span_days'))} |",
        f"| A+ rate | {_pct(a.get('a_plus_rate_pct'))} |",
        f"| A-or-better rate | {_pct(a.get('a_or_better_rate_pct'))} |",
        f"| Fail rate | {_pct(a.get('fail_rate_pct'))} |",
        f"| Median 1h max favor | {_pct(a.get('median_future_1h_max_favor_pct'))} |",
        f"| Median 1h max adverse | {_pct(a.get('median_future_1h_max_adverse_pct'))} |",
        f"| Median 4h max favor | {_pct(a.get('median_future_4h_max_favor_pct'))} |",
        f"| Median 4h max adverse | {_pct(a.get('median_future_4h_max_adverse_pct'))} |",
        f"| Median time to 2% favor (min) | {_num(a.get('median_time_to_2pct_favor_min'))} |",
        "",
    ]

    # --- Board B ---
    lines += [
        "---",
        "## Board B — Outcome Distribution",
        "",
        "| Outcome Class | Count | Share |",
        "|---------------|-------|-------|",
    ]
    for row in board_b:
        lines.append(f"| {row['outcome_class']} | {row['count']} | {row['share_pct']}% |")
    lines.append("")

    # --- Board C ---
    lines += ["---", "## Board C — Feature Slice Board", ""]
    for col, slices in board_c.items():
        lines.append(f"### {col}")
        lines.append("| Slice | Events | A-or-better | Fail | Med 1h Favor | Med 1h Adverse |")
        lines.append("|-------|--------|-------------|------|-------------|----------------|")
        for s in slices:
            lines.append(
                f"| {s['slice']} | {_cnt(s['events'])} | {_pct(s['a_or_better_rate_pct'])} "
                f"| {_pct(s['fail_rate_pct'])} | {_pct(s['median_1h_favor_pct'])} "
                f"| {_pct(s['median_1h_adverse_pct'])} |"
            )
        lines.append("")

    # --- Board D ---
    lines += ["---", "## Board D — Regime Slice Board", ""]
    for col, slices in board_d.items():
        lines.append(f"### {col}")
        lines.append("| Slice | Events | A+ | A-or-better | Fail | Med 4h Favor | Med 4h Adverse |")
        lines.append("|-------|--------|----|-------------|------|-------------|----------------|")
        for s in slices:
            lines.append(
                f"| {s['slice']} | {_cnt(s['events'])} | {_pct(s['a_plus_rate_pct'])} "
                f"| {_pct(s['a_or_better_rate_pct'])} | {_pct(s['fail_rate_pct'])} "
                f"| {_pct(s['median_4h_favor_pct'])} | {_pct(s['median_4h_adverse_pct'])} |"
            )
        lines.append("")

    lines += ["---", "*Report generated by R2-lite measurement pipeline. Offline research only.*"]
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# CSV output helpers
# ---------------------------------------------------------------------------

def write_event_log_csv(events: List[Dict], path: Path) -> None:
    from research.signature_measurement.contracts import EVENT_LOG_FIELDS
    import csv
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=EVENT_LOG_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(events)


def write_measurement_details_csv(events: List[Dict], path: Path) -> None:
    from research.signature_measurement.contracts import MEASUREMENT_DETAIL_FIELDS
    import csv
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=MEASUREMENT_DETAIL_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(events)
