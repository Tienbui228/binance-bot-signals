"""
research/top_movers/signature_ledger.py

Handles:
  - signature_key / code generation
  - daily_signature_candidates.csv  (37 fields, always written with header)
  - signature_evidence_ledger.csv   (daily replace/upsert by research_day)
  - daily_research_summary.csv      (33 fields, CLEAN/PARTIAL/WEAK health only)

Key fix (pipeline consistency):
  upsert_ledger() now does daily replace:
    1. Purge ALL existing ledger rows where research_day == D
    2. Insert current run's candidates for D (may be zero)
  This ensures ledger always reflects the latest run for each day.
"""

import csv
import os
import shutil
import statistics
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from typing import Dict, List, Optional

LEDGER_PATH = "data/research_output/top_movers/rollups/signature_evidence_ledger.csv"
SIGNATURE_THRESHOLD = 2


# ---------------------------------------------------------------------------
# signature_key / code
# ---------------------------------------------------------------------------

def make_signature_key(side, pre_move_signature, participation_pattern, structural_quality) -> str:
    return f"{side}__{pre_move_signature}__{participation_pattern}__{structural_quality}"


def make_signature_candidate_code(signature_key) -> str:
    parts = signature_key.split("__")
    if len(parts) != 4:
        return f"SIG_{signature_key[:30].upper().replace(' ','_')}"
    side, pre, part, sq = parts
    pre_a  = {"oi_led":"OI","volume_led":"VOL","top_cohort_led":"TOP","crowd_led":"CRW",
               "mixed_led":"MIX","low_signal_premove":"LOW"}.get(pre, pre[:3].upper())
    part_a = {"top_leads_crowd_late":"TOPLEA","top_and_crowd_align":"ALIGN",
               "crowd_chase_dominant":"CCHASE","divergence_warning":"DIVWRN",
               "low_participation_move":"LOPRT","unclear_participation":"UNCL"}.get(part, part[:6].upper())
    sq_a   = {"clean_base_break":"CLEAN","repeated_test_then_break":"REPT",
               "shallow_pullback_continuation":"SHAL","runaway_no_base":"RUN",
               "dirty_break":"DIRTY","exhaustion_spike":"EXHSP"}.get(sq, sq[:4].upper())
    return f"SIG_{side}_{pre_a}_{part_a}_{sq_a}"


# ---------------------------------------------------------------------------
# Candidates CSV schema — always write this header even when 0 rows (fix 1)
# ---------------------------------------------------------------------------

CANDIDATE_SCHEMA = [
    "research_day", "signature_key", "signature_candidate_code", "signature_description",
    "support_count", "support_share_pct", "supporting_case_ids", "supporting_symbols",
    "dominant_side", "dominant_move_class", "dominant_pre_move_signature",
    "dominant_participation_pattern", "dominant_structural_quality",
    "representative_case_id", "representative_symbol",
    "median_30m_favor", "median_30m_adverse",
    "median_1h_favor", "median_1h_adverse",
    "median_4h_favor", "median_4h_adverse",
    "median_time_to_2pct_favor", "median_time_to_3pct_favor",
    "median_large_participant_proxy", "median_crowd_participation_proxy",
    "median_flow_composite_signal",
    "maps_to_existing_strategy_family", "improvement_target_layer_mode",
    "new_strategy_candidate_flag", "candidate_strategy_family_name",
    "candidate_trigger_description", "decision_grade", "confidence",
    "validation_status", "outlier_dependency_flag", "caution_flag",
    "next_action", "notes",
]


def write_candidates_csv(path: str, candidates: List[Dict]) -> None:
    """
    Always write daily_signature_candidates.csv with correct schema headers.
    If candidates is empty → file has header only, 0 data rows.
    This is valid: it means no repeated pattern met threshold today.
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CANDIDATE_SCHEMA, extrasaction="ignore")
        writer.writeheader()
        if candidates:
            writer.writerows(candidates)


# ---------------------------------------------------------------------------
# Extract signature candidates
# ---------------------------------------------------------------------------

def extract_signature_candidates(cases: List[Dict], research_day: str,
                                   threshold: int = SIGNATURE_THRESHOLD) -> List[Dict]:
    """
    Candidate = ≥ threshold eligible cases sharing (side, pre_sig, participation, structural_quality).
    Returns empty list if no pattern meets threshold — caller must still write the CSV.
    """
    eligible = [c for c in cases if c.get("research_eligible_YN") == "Y"
                or c.get("data_quality_ok") == "Y"]
    if not eligible:
        return []

    # Load historical ledger to derive validation_status from true history
    _hist_days_by_sig: Dict[str, set] = {}
    if os.path.exists(LEDGER_PATH):
        with open(LEDGER_PATH, "r", newline="", encoding="utf-8") as _f:
            for _row in csv.DictReader(_f):
                _sk = _row.get("signature_key", "")
                _rd = _row.get("research_day", "")
                if _sk and _rd and _rd != research_day:
                    _hist_days_by_sig.setdefault(_sk, set()).add(_rd)

    groups: Dict[str, List[Dict]] = {}
    for c in eligible:
        key = make_signature_key(
            c.get("side",""), c.get("pre_move_signature",""),
            c.get("participation_pattern",""), c.get("structural_quality",""),
        )
        groups.setdefault(key, []).append(c)

    total = len(eligible)
    results = []

    for sig_key, gc in groups.items():
        if len(gc) < threshold:
            continue

        code  = make_signature_candidate_code(sig_key)
        n     = len(gc)
        share = round(100.0 * n / max(total, 1), 1)

        def dom(f):
            vs = [c.get(f,"") for c in gc if c.get(f)]
            return max(set(vs), key=vs.count) if vs else ""

        def med(f):
            vs = [float(c[f]) for c in gc if c.get(f) not in (None,"","None")]
            return round(statistics.median(vs), 4) if vs else None

        case_ids = [c.get("case_id","") for c in gc]
        symbols  = [c.get("symbol","") for c in gc]
        rep_case = gc[0].get("case_id",""); rep_sym = gc[0].get("symbol","")

        new_yn   = "Y" if any(c.get("new_strategy_candidate_flag")=="Y" for c in gc) else "N"
        cand_fam = dom("candidate_strategy_family_name")
        cand_trig= gc[0].get("candidate_trigger_description","") if new_yn=="Y" else ""

        avg_px   = _safe_avg(gc, "proxy_completeness_score")
        sig_conf = "HIGH" if n>=4 and (avg_px or 0)>=60 else "LOW" if n<=2 or (avg_px or 0)<40 else "MEDIUM"
        outlier_dep = "Y" if n<=2 and any(abs(float(c.get("daily_return_pct",0) or 0))>15 for c in gc) else "N"
        caution  = "Y" if any(c.get("caution_flag")=="Y" for c in gc) else "N"

        results.append({
            "research_day": research_day,
            "signature_key": sig_key,
            "signature_candidate_code": code,
            "signature_description": _describe(sig_key),
            "support_count": n,
            "support_share_pct": share,
            "supporting_case_ids": ",".join(case_ids),
            "supporting_symbols": ",".join(symbols),
            "dominant_side": dom("side"),
            "dominant_move_class": dom("move_class"),
            "dominant_pre_move_signature": dom("pre_move_signature"),
            "dominant_participation_pattern": dom("participation_pattern"),
            "dominant_structural_quality": dom("structural_quality"),
            "representative_case_id": rep_case,
            "representative_symbol": rep_sym,
            "median_30m_favor": med("future_30m_max_favor_pct"),
            "median_30m_adverse": med("future_30m_max_adverse_pct"),
            "median_1h_favor": med("future_1h_max_favor_pct"),
            "median_1h_adverse": med("future_1h_max_adverse_pct"),
            "median_4h_favor": med("future_4h_max_favor_pct"),
            "median_4h_adverse": med("future_4h_max_adverse_pct"),
            "median_time_to_2pct_favor": med("time_to_2pct_favor_min"),
            "median_time_to_3pct_favor": med("time_to_3pct_favor_min"),
            "median_large_participant_proxy": med("large_participant_proxy"),
            "median_crowd_participation_proxy": med("crowd_participation_proxy"),
            "median_flow_composite_signal": med("flow_composite_signal"),
            "maps_to_existing_strategy_family": dom("maps_to_existing_strategy_family"),
            "improvement_target_layer_mode": dom("improvement_target_layer"),
            "new_strategy_candidate_flag": new_yn,
            "candidate_strategy_family_name": cand_fam,
            "candidate_trigger_description": cand_trig,
            "decision_grade": dom("decision_grade"),
            "confidence": sig_conf,
            "validation_status": "tracking" if (sig_conf in ("HIGH","MEDIUM") or bool(_hist_days_by_sig.get(sig_key))) else "first_observation",
            "outlier_dependency_flag": outlier_dep,
            "caution_flag": caution,
            "next_action": _next_action(dom("strategy_action_type"), sig_conf, n),
            "notes": "",
        })

    results.sort(key=lambda x: -x["support_count"])
    return results


# ---------------------------------------------------------------------------
# Ledger upsert — daily replace/upsert by research_day (fix 2)
# ---------------------------------------------------------------------------

_DAY_FIELDS = [
    "support_count", "support_share_pct", "supporting_symbols",
    "representative_case_id", "representative_symbol",
    "dominant_side", "dominant_move_class", "dominant_pre_move_signature",
    "dominant_participation_pattern", "dominant_structural_quality",
    "median_30m_favor", "median_30m_adverse",
    "median_1h_favor", "median_1h_adverse",
    "median_4h_favor", "median_4h_adverse",
    "median_time_to_2pct_favor", "median_time_to_3pct_favor",
    "median_large_participant_proxy", "median_crowd_participation_proxy",
    "median_flow_composite_signal",
    "maps_to_existing_strategy_family", "improvement_target_layer_mode",
    "new_strategy_candidate_flag", "candidate_strategy_family_name",
    "candidate_trigger_description", "decision_grade",
    "confidence", "validation_status",
    "outlier_dependency_flag", "caution_flag", "notes",
]

LEDGER_SCHEMA = (
    ["research_day", "signature_key", "signature_candidate_code", "signature_description"]
    + [f + "_day" for f in _DAY_FIELDS]
    + ["first_seen_date", "last_seen_date"]
)


def _candidate_to_ledger_row(cand: Dict) -> Dict:
    row = {
        "research_day":             cand.get("research_day",""),
        "signature_key":            cand.get("signature_key",""),
        "signature_candidate_code": cand.get("signature_candidate_code",""),
        "signature_description":    cand.get("signature_description",""),
    }
    for f in _DAY_FIELDS:
        row[f + "_day"] = cand.get(f, "")
    row["first_seen_date"] = cand.get("research_day","")
    row["last_seen_date"]  = cand.get("research_day","")
    return row


def upsert_ledger(new_candidates: List[Dict], research_day: str) -> None:
    """
    Daily replace/upsert for signature_evidence_ledger.csv.

    first_seen_date / last_seen_date are derived from raw research_day facts only.
    Never trust stored field values — they may be stale from prior buggy writes.
    All retained historical rows are corrected on each write.
    """
    os.makedirs(os.path.dirname(LEDGER_PATH), exist_ok=True)

    kept: Dict[tuple, Dict] = {}
    actual_days_by_sig: Dict[str, set] = {}

    if os.path.exists(LEDGER_PATH):
        with open(LEDGER_PATH, "r", newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                day = row.get("research_day","")
                sig = row.get("signature_key","")
                if day == research_day:
                    continue  # PURGE stale rows for this day
                kept[(day, sig)] = row
                if sig and day:
                    actual_days_by_sig.setdefault(sig, set()).add(day)

    # Include today's new candidates in day tracking
    for cand in new_candidates:
        sk = cand.get("signature_key","")
        if sk:
            actual_days_by_sig.setdefault(sk, set()).add(research_day)

    # Canonical first/last seen from raw research_day facts — never from stored fields
    first_seen_by_sig = {sig: min(days) for sig, days in actual_days_by_sig.items()}
    last_seen_by_sig  = {sig: max(days) for sig, days in actual_days_by_sig.items()}

    # Correct first/last seen on all retained historical rows
    for (day, sig), row in kept.items():
        if sig in first_seen_by_sig:
            row["first_seen_date"] = first_seen_by_sig[sig]
            row["last_seen_date"]  = last_seen_by_sig[sig]

    # Insert new candidates for today
    for cand in new_candidates:
        sig_key = cand.get("signature_key","")
        key     = (research_day, sig_key)
        new_row = _candidate_to_ledger_row(cand)
        new_row["first_seen_date"] = first_seen_by_sig.get(sig_key, research_day)
        new_row["last_seen_date"]  = last_seen_by_sig.get(sig_key, research_day)
        kept[key] = new_row

    all_rows = sorted(kept.values(), key=lambda r: (r.get("research_day",""), r.get("signature_key","")))
    all_keys = list(dict.fromkeys(
        LEDGER_SCHEMA + [k for k in (all_rows[0].keys() if all_rows else []) if k not in LEDGER_SCHEMA]
    ))

    with open(LEDGER_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=all_keys, extrasaction="ignore")
        writer.writeheader()
        for row in all_rows:
            writer.writerow({k: row.get(k,"") for k in all_keys})

# ---------------------------------------------------------------------------
# Daily research summary — 33 fields, CLEAN/PARTIAL/WEAK only
# ---------------------------------------------------------------------------

def build_daily_research_summary(research_day, selection_context, cases, signature_candidates) -> Dict:
    eligible    = [c for c in cases if c.get("research_eligible_YN")=="Y"]
    excluded    = [c for c in cases if c.get("research_eligible_YN")!="Y"]
    missing_prx = [c for c in cases if c.get("proxy_complete_YN")!="Y"]
    missing_img = [c for c in cases if c.get("full_visual_complete_YN")!="Y"]
    gainers = [c for c in cases if c.get("side")=="LONG"]
    losers  = [c for c in cases if c.get("side")=="SHORT"]

    n = len(cases)
    er = len(eligible)/n if n else 0
    pr = (n-len(missing_prx))/n if n else 0
    ir = (n-len(missing_img))/n if n else 0
    _has_visual_gaps = len(missing_img) > 0
    health = (
        ("CLEAN_WITH_VISUAL_GAPS" if _has_visual_gaps else "CLEAN")
        if er>=0.75 and pr>=0.60
        else ("PARTIAL" if er>=0.40 or pr>=0.40 else "WEAK")
    )

    def dom(f):
        vs = [c.get(f,"") for c in eligible if c.get(f)]
        return max(set(vs), key=vs.count) if vs else "—"

    def sig_code(i): return signature_candidates[i].get("signature_candidate_code","") if i<len(signature_candidates) else ""

    improve  = sum(1 for c in eligible if c.get("strategy_action_type")=="improve_existing")
    new_t    = sum(1 for c in eligible if c.get("strategy_action_type")=="create_new")
    reviews  = [c.get("case_id","") for c in eligible if c.get("decision_grade") in
                ("OLD_STRATEGY_IMPROVEMENT_CANDIDATE","NEW_STRATEGY_THESIS_CANDIDATE")]
    traps    = _detect_traps_brief(eligible)
    dom_cls  = dom("move_class")
    learning = f"dominant_class={dom_cls}" + (f" | top_sig={sig_code(0)}" if sig_code(0) else " | no_repeated_sig_today")

    _short_cands = [
        c for c in eligible
        if c.get("side") == "SHORT"
        and c.get("decision_grade") in (
            "OLD_STRATEGY_IMPROVEMENT_CANDIDATE", "NEW_STRATEGY_THESIS_CANDIDATE",
        )
        and c.get("intervention_confidence") in ("HIGH", "MEDIUM")
    ]
    _short_today  = "Y" if _short_cands else "N"
    _short_reason = (
        "; ".join(
            c.get("symbol", "?") + "(" + c.get("improvement_target_layer", "?") + ")"
            for c in _short_cands[:3]
        ) if _short_cands else "none"
    )
    return {
        "research_day": research_day,
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "total_selected_movers": n,
        "gainers_count": len(gainers), "losers_count": len(losers),
        "built_cases_count": n,
        "research_eligible_cases": len(eligible),
        "excluded_cases_count": len(excluded),
        "missing_proxy_cases_count": len(missing_prx),
        "missing_image_cases_count": len(missing_img),
        "overall_research_health": health,
        "dominant_move_class": dom("move_class"),
        "dominant_pre_move_signature": dom("pre_move_signature"),
        "dominant_participation_pattern": dom("participation_pattern"),
        "dominant_structural_quality": dom("structural_quality"),
        "top_repeated_clue_1": dom("flow_phase_code"),
        "top_repeated_clue_2": dom("resolution_label"),
        "top_repeated_clue_3": dom("move_class"),
        "top_candidate_signature_1": sig_code(0),
        "top_candidate_signature_2": sig_code(1),
        "top_candidate_signature_3": sig_code(2),
        "main_learning_of_day": learning,
        "main_trap_of_day": traps[0] if traps else "none",
        "top_review_case_1": reviews[0] if len(reviews)>0 else "",
        "top_review_case_2": reviews[1] if len(reviews)>1 else "",
        "top_review_case_3": reviews[2] if len(reviews)>2 else "",
        "old_strategy_improvement_cases_count": improve,
        "new_strategy_thesis_cases_count": new_t,
        "next_research_action": _derive_day_action(eligible, signature_candidates),
        "notes": f"sig_candidates_count={len(signature_candidates)}" + (
            " | reason=no_repeated_pattern_met_threshold" if not signature_candidates else ""),
        "research_regime": selection_context.get("research_regime",""),
        "btc_24h_change_pct": selection_context.get("btc_24h_change_pct",""),
        "alt_breadth_pct": selection_context.get("alt_breadth_pct",""),
        "short_intervention_candidate_today": _short_today,
        "short_intervention_reason":          _short_reason,
    }


# ---------------------------------------------------------------------------
# Daily journal
# ---------------------------------------------------------------------------

def build_daily_journal_row(research_day, cases, signature_candidates) -> Dict:
    eligible = [c for c in cases if c.get("research_eligible_YN")=="Y"]
    repeated = ",".join(s.get("signature_candidate_code","") for s in signature_candidates[:3])
    new_hyps = list(set(c.get("candidate_strategy_family_name","") for c in eligible
                        if c.get("new_strategy_candidate_flag")=="Y" and c.get("candidate_strategy_family_name")))
    traps    = _detect_traps_brief(eligible)
    priority = ",".join(c.get("case_id","") for c in eligible if c.get("decision_grade") in
                        ("OLD_STRATEGY_IMPROVEMENT_CANDIDATE","NEW_STRATEGY_THESIS_CANDIDATE"))
    return {
        "research_day": research_day,
        "what_was_new_today": ",".join(new_hyps) or "no_new_thesis_today",
        "what_repeated_today": repeated or "no_repeated_signature",
        "main_hypothesis_added": ",".join(new_hyps),
        "main_trap_noted": ",".join(traps) or "none",
        "priority_case_ids": priority or "none",
        "recommended_follow_up": _derive_day_action(eligible, signature_candidates),
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _describe(sig_key):
    p = sig_key.split("__")
    return f"{p[0]} | {p[1]} | {p[2]} | {p[3]}" if len(p)==4 else sig_key


def _safe_avg(cases, f):
    vs = []
    for c in cases:
        v = c.get(f)
        if v not in (None,"","None"):
            try: vs.append(float(v))
            except: pass
    return sum(vs)/len(vs) if vs else None


def _next_action(action, conf, n):
    if action=="improve_existing" and conf in ("HIGH","MEDIUM"): return "create_improvement_workstream"
    if action=="create_new" and conf=="MEDIUM":                   return "track_for_5_more_days"
    if n>=5:                                                       return "consider_validation_phase"
    return "continue_daily_tracking"


def _detect_traps_brief(eligible):
    traps = []
    if any(c.get("structural_quality")=="runaway_no_base" for c in eligible): traps.append("RUNAWAY_NO_BASE")
    if any(c.get("participation_pattern")=="crowd_chase_dominant" for c in eligible): traps.append("CROWD_CHASE_DOMINANT")
    if any(c.get("participation_pattern")=="low_participation_move" for c in eligible): traps.append("LOW_PARTICIPATION_MOVE")
    if sum(1 for c in eligible if (c.get("proxy_completeness_score") or 0)<40)>=3: traps.append("WEAK_PROXY_COMPLETENESS")
    return traps


def _derive_day_action(eligible, sigs):
    improve = sum(1 for c in eligible if c.get("strategy_action_type")=="improve_existing")
    if improve>=3: return "review_improvement_candidates_for_existing_strategy"
    if sigs:       return "continue_tracking_signature_candidates"
    return "observe_further_before_action"


# ---------------------------------------------------------------------------
# New helper 1 — Data layer: load + normalize ledger rows
# ---------------------------------------------------------------------------

def load_and_normalize_ledger_rows(research_day: str, window_days: int = 7, as_of_day: Optional[str] = None) -> List[Dict]:
    """
    Data layer: read signature_evidence_ledger.csv and attach derived fields to every row.
    Returns ALL rows — no presentation filtering.

    Derived fields added per row:
      support_days_count        — distinct research_day values for this signature_key (full ledger)
      recent_support_days_count — distinct research_day values within last window_days calendar days
      latest_validation_status  — validation_status_day from the row with max last_seen_date
      current_role              — "stale" / "first_observation" / "repeated_candidate" / "tracking"
    """
    if not os.path.exists(LEDGER_PATH):
        return []

    all_rows: List[Dict] = []
    with open(LEDGER_PATH, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if as_of_day and row.get("research_day", "") > as_of_day:
                continue  # as-of-day filter: exclude future rows
            all_rows.append(dict(row))

    if not all_rows:
        return []

    try:
        ref_date = datetime.strptime(research_day, "%Y-%m-%d")
    except ValueError:
        ref_date = datetime.utcnow()
    window_start = ref_date - timedelta(days=window_days - 1)
    stale_cutoff = ref_date - timedelta(days=window_days)

    sig_all_days: Dict[str, set] = defaultdict(set)
    sig_recent_days: Dict[str, set] = defaultdict(set)
    sig_latest_seen: Dict[str, str] = {}
    sig_latest_status: Dict[str, str] = {}

    for row in all_rows:
        sk = row.get("signature_key", "")
        day_str = row.get("research_day", "")
        if sk and day_str:
            sig_all_days[sk].add(day_str)
            try:
                if datetime.strptime(day_str, "%Y-%m-%d") >= window_start:
                    sig_recent_days[sk].add(day_str)
            except ValueError:
                pass
        if sk and day_str:
            # Use research_day, NOT stored last_seen_date, to avoid future contamination
            # (upsert_ledger writes future last_seen_date onto historical rows)
            if day_str >= sig_latest_seen.get(sk, ""):
                sig_latest_seen[sk] = day_str
                sig_latest_status[sk] = row.get("validation_status_day", "")

    # Derive first/last_seen from raw research_day facts within as-of-filtered set
    _sig_first_seen = {sk: min(days) for sk, days in sig_all_days.items()}
    _sig_last_seen  = sig_latest_seen  # already = max(research_day) per sig

    for row in all_rows:
        sk = row.get("signature_key", "")
        # Override stored first/last_seen with correctly derived values
        row["first_seen_date"] = _sig_first_seen.get(sk, row.get("research_day", ""))
        row["last_seen_date"]  = _sig_last_seen.get(sk, row.get("research_day", ""))
        derived_last_seen = row["last_seen_date"]
        support_days = len(sig_all_days.get(sk, set()))
        recent_days = len(sig_recent_days.get(sk, set()))
        latest_status = sig_latest_status.get(sk, "")
        row["support_days_count"] = support_days
        row["recent_support_days_count"] = recent_days
        # Data-layer reconcile: status stuck at first_observation despite multi-day support
        if latest_status == "first_observation" and support_days >= 2:
            latest_status = "tracking"
        row["latest_validation_status"] = latest_status
        try:
            is_stale = datetime.strptime(derived_last_seen, "%Y-%m-%d") < stale_cutoff
        except ValueError:
            is_stale = False
        if is_stale:
            row["current_role"] = "stale"
        elif support_days == 1:
            row["current_role"] = "first_observation"
        elif recent_days >= 2 and latest_status not in ("stale", "broken", ""):
            row["current_role"] = "repeated_candidate"
        elif support_days >= 2:
            row["current_role"] = "tracking"
        else:
            row["current_role"] = "first_observation"
        # Sync: latest_validation_status must not contradict current_role
        if row["current_role"] in ("first_observation", "stale"):
            row["latest_validation_status"] = row["current_role"]

    return all_rows


# ---------------------------------------------------------------------------
# New helper 2 — View layer: filter + deduplicate for report display
# ---------------------------------------------------------------------------

def build_ledger_snapshot_for_report(normalized_rows: List[Dict], research_day: str) -> List[Dict]:
    """
    Presentation layer: one deduplicated row per signature_key, filtered to recent window.
    Sorted: repeated_candidate first, then recent_support_days_count DESC.
    """
    if not normalized_rows:
        return []
    try:
        view_cutoff = datetime.strptime(research_day, "%Y-%m-%d") - timedelta(days=7)
    except ValueError:
        view_cutoff = None

    latest_by_sig: Dict[str, Dict] = {}
    for row in normalized_rows:
        sk = row.get("signature_key", "")
        if not sk:
            continue
        last_seen = row.get("last_seen_date", "")
        if view_cutoff:
            try:
                if datetime.strptime(last_seen, "%Y-%m-%d") < view_cutoff:
                    continue
            except ValueError:
                pass
        existing = latest_by_sig.get(sk)
        if not existing or last_seen >= existing.get("last_seen_date", ""):
            latest_by_sig[sk] = row

    # Compact snapshot: exclude rows with no activity in current rolling window
    snapshot = [r for r in latest_by_sig.values()
                if (r.get("recent_support_days_count") or 0) > 0]
    _role_order = {"repeated_candidate": 0, "tracking": 1, "first_observation": 2, "stale": 3}
    snapshot.sort(key=lambda r: (
        _role_order.get(r.get("current_role", ""), 4),
        -(r.get("recent_support_days_count") or 0),
    ))
    return snapshot


# ---------------------------------------------------------------------------
# New helper 3 — Ledger semantic validation
# ---------------------------------------------------------------------------

def validate_ledger_semantics(normalized_rows: List[Dict]) -> List[str]:
    """Returns warning strings (empty list = no issues)."""
    if not normalized_rows:
        return []
    latest_by_sig: Dict[str, Dict] = {}
    for row in normalized_rows:
        sk = row.get("signature_key", "")
        if not sk:
            continue
        if row.get("last_seen_date", "") >= latest_by_sig.get(sk, {}).get("last_seen_date", ""):
            latest_by_sig[sk] = row
    warnings = []
    for sk, row in latest_by_sig.items():
        code = row.get("signature_candidate_code", sk[:30])
        first_seen = row.get("first_seen_date", "")
        global_last = row.get("last_seen_date", "")
        support_days = row.get("support_days_count", 0) or 0
        recent_days  = row.get("recent_support_days_count", 0) or 0
        latest_status = row.get("latest_validation_status", "")
        if first_seen and global_last and first_seen > global_last:
            warnings.append(f"{code}: first_seen_date ({first_seen}) > last_seen_date ({global_last})")
        if support_days >= 3 and latest_status in ("first_observation", ""):
            warnings.append(f"{code}: support_days={support_days} but validation_status=\'{latest_status}\'")
        # Snapshot-membership inconsistency: has history but no recent window activity
        if support_days >= 2 and recent_days == 0:
            warnings.append(
                f"{code}: support_days={support_days} but recent_7d=0 — "
                "signature has history but no activity in current rolling window. "
                "Excluded from compact snapshot."
            )
    return warnings


# ---------------------------------------------------------------------------
# New helper 4 — Intervention shortlist (family/layer-driven)
# ---------------------------------------------------------------------------

def build_intervention_shortlist(
    eligible_cases: List[Dict],
    sig_candidates: List[Dict],
    normalized_ledger_rows: List[Dict],
) -> List[Dict]:
    """
    Groups eligible cases by (maps_to_existing_strategy_family, improvement_target_layer).
    Only cases with OLD_STRATEGY_IMPROVEMENT_CANDIDATE or NEW_STRATEGY_THESIS_CANDIDATE.
    Max 5 rows, sorted by repeated_support_today DESC then case_count_today DESC.
    """
    relevant = [
        c for c in eligible_cases
        if c.get("decision_grade") in (
            "OLD_STRATEGY_IMPROVEMENT_CANDIDATE",
            "NEW_STRATEGY_THESIS_CANDIDATE",
        ) and c.get("research_eligible_YN") == "Y"
    ]
    if not relevant:
        return []

    candidate_sig_keys = {s.get("signature_key", "") for s in sig_candidates if s.get("signature_key")}
    ledger_support_by_sig: Dict[str, int] = {}
    for row in normalized_ledger_rows:
        sk = row.get("signature_key", "")
        if sk:
            ledger_support_by_sig[sk] = max(
                ledger_support_by_sig.get(sk, 0), row.get("support_days_count", 0) or 0
            )

    groups: Dict[tuple, List[Dict]] = {}
    for c in relevant:
        key = (
            c.get("maps_to_existing_strategy_family", "") or "unclear",
            c.get("improvement_target_layer", "") or "not_applicable",
        )
        groups.setdefault(key, []).append(c)

    results = []
    for (family, layer), group_cases in groups.items():
        case_sig_keys = {
            make_signature_key(
                c.get("side", ""), c.get("pre_move_signature", ""),
                c.get("participation_pattern", ""), c.get("structural_quality", ""),
            )
            for c in group_cases
        }
        repeated_support_today = len(case_sig_keys & candidate_sig_keys)
        cross_day_support = max((ledger_support_by_sig.get(sk, 0) for sk in case_sig_keys), default=0)
        grades = [c.get("decision_grade", "") for c in group_cases]
        dom_grade = max(set(grades), key=grades.count) if grades else ""
        readiness = {
            "OLD_STRATEGY_IMPROVEMENT_CANDIDATE": "old_strategy_improvement_candidate",
            "NEW_STRATEGY_THESIS_CANDIDATE":       "new_strategy_thesis_candidate",
        }.get(dom_grade, "keep_tracking")
        actions = [c.get("strategy_action_type", "") for c in group_cases]
        # Display identity for thesis cases: preserve persisted family=none,
        # but surface candidate_strategy_family_name as display label
        def _thesis_display_family(cases_in_group, persisted_family):
            if persisted_family not in ("", "none", "unclear"):
                return persisted_family
            cands = [c.get("candidate_strategy_family_name","") for c in cases_in_group
                     if c.get("candidate_strategy_family_name","") not in ("","under_investigation")]
            if cands:
                return f"[new] {max(set(cands), key=cands.count)}"
            trigs = [c.get("candidate_trigger_description","") for c in cases_in_group
                     if c.get("candidate_trigger_description","") not in ("","under_investigation")]
            if trigs:
                return f"[new] {trigs[0][:40]}"
            return persisted_family
        display_family = _thesis_display_family(group_cases, family)
        results.append({
            "strategy_family":         display_family,
            "issue_layer":             layer,
            "case_count_today":        len(group_cases),
            "repeated_support_today":  repeated_support_today,
            "cross_day_support":       cross_day_support,
            "evidence_source":         "repeated_plus_case" if repeated_support_today > 0 else "case_level_only",
            "readiness":               readiness,
            "recommended_next_action": max(set(actions), key=actions.count) if actions else "",
            "next_action":             max(set(actions), key=actions.count) if actions else "",  # alias for DOCX renderer contract
        })

    results.sort(key=lambda r: (-r["repeated_support_today"], -r["case_count_today"]))
    return results[:5]


# ---------------------------------------------------------------------------
# Ledger repair and invariant validation (downstream-only)
# ---------------------------------------------------------------------------

def validate_ledger_invariants_from_rows(rows: List[Dict]) -> List[str]:
    """Check semantic invariants on ledger rows. Returns list of violation strings."""
    violations = []
    for row in rows:
        sk   = row.get("signature_key", "")
        rd   = row.get("research_day", "")
        fs   = row.get("first_seen_date", "")
        ls   = row.get("last_seen_date", "")
        code = row.get("signature_candidate_code", sk[:25] if sk else "?")
        if fs and ls and fs > ls:
            violations.append(f"{code} [{rd}]: first_seen ({fs}) > last_seen ({ls})")
        if rd and fs and rd < fs:
            violations.append(f"{code} [{rd}]: research_day ({rd}) < first_seen ({fs})")
        if rd and ls and rd > ls:
            violations.append(f"{code} [{rd}]: research_day ({rd}) > last_seen ({ls})")
    return violations


def validate_ledger_invariants() -> List[str]:
    """Load ledger from disk and run invariant checks. Returns violation strings."""
    if not os.path.exists(LEDGER_PATH):
        return []
    with open(LEDGER_PATH, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    return validate_ledger_invariants_from_rows(rows)


def repair_ledger_history(window_days: int = 7) -> Dict:
    """
    One-time idempotent repair of signature_evidence_ledger.csv.

    Rules:
      - first_seen_date = min(research_day) per signature_key across ALL rows
      - last_seen_date  = max(research_day) per signature_key across ALL rows
      - Does NOT delete any rows
      - Does NOT change strategy or scoring fields
      - Creates a timestamped backup before mutating
      - Idempotent: running twice gives the same result
      - Prints a concise repair summary

    Returns dict with: status, backup_path, total_rows, rows_repaired, examples,
                       post_repair_violations
    """
    if not os.path.exists(LEDGER_PATH):
        print("  repair_ledger_history: no ledger file found, nothing to repair")
        return {"status": "no_file", "rows_repaired": 0}

    # Backup
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    backup_path = LEDGER_PATH + f".bak_{ts}"
    shutil.copy(LEDGER_PATH, backup_path)
    print(f"  Backup: {backup_path}")

    # Load all rows
    with open(LEDGER_PATH, newline="", encoding="utf-8") as f:
        all_rows = list(csv.DictReader(f))

    if not all_rows:
        return {"status": "empty", "rows_repaired": 0, "backup_path": backup_path}

    # Group by signature_key: collect all raw research_days
    days_by_sig: Dict[str, set] = {}
    for row in all_rows:
        sk = row.get("signature_key", "")
        rd = row.get("research_day", "")
        if sk and rd:
            days_by_sig.setdefault(sk, set()).add(rd)

    first_by_sig = {sig: min(days) for sig, days in days_by_sig.items()}
    last_by_sig  = {sig: max(days) for sig, days in days_by_sig.items()}

    repaired = 0
    examples: List[Dict] = []

    for row in all_rows:
        sk = row.get("signature_key", "")
        if not sk:
            continue
        old_first = row.get("first_seen_date", "")
        old_last  = row.get("last_seen_date", "")
        new_first = first_by_sig.get(sk, "")
        new_last  = last_by_sig.get(sk, "")
        changed = (old_first != new_first) or (old_last != new_last)
        if changed:
            row["first_seen_date"] = new_first
            row["last_seen_date"]  = new_last
            repaired += 1
            if len(examples) < 6:
                examples.append({
                    "sig_key":    sk,
                    "code":       row.get("signature_candidate_code", sk[:25]),
                    "research_day": row.get("research_day", ""),
                    "old_first":  old_first,  "new_first": new_first,
                    "old_last":   old_last,   "new_last":  new_last,
                })

    # Write back
    all_keys = list(all_rows[0].keys())
    with open(LEDGER_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=all_keys, extrasaction="ignore")
        writer.writeheader()
        for row in all_rows:
            writer.writerow({k: row.get(k, "") for k in all_keys})

    violations = validate_ledger_invariants_from_rows(all_rows)

    summary = {
        "status":                 "repaired",
        "backup_path":            backup_path,
        "total_rows":             len(all_rows),
        "rows_repaired":          repaired,
        "unique_signatures":      len(days_by_sig),
        "examples":               examples,
        "post_repair_violations": violations,
    }

    print(f"  Total rows: {len(all_rows)} | Repaired: {repaired} | Sigs: {len(days_by_sig)}")
    if violations:
        print(f"  WARNING: {len(violations)} invariant violation(s) remain after repair")
        for v in violations[:5]:
            print(f"    {v}")
    else:
        print("  All invariants pass after repair.")

    return summary


# ---------------------------------------------------------------------------
# Canonical as-of-day ledger entry point
# ---------------------------------------------------------------------------

def ledger_rows_as_of(report_day: str, window_days: int = 7) -> List[Dict]:
    """
    Canonical entry point for all as-of-day ledger consumption.

    Rules:
      - Filters: only rows where research_day <= report_day
      - Derives first_seen_date, last_seen_date, support_days_count,
        recent_support_days_count from filtered raw research_day facts only
      - Does NOT trust stored denormalized first_seen_date / last_seen_date fields
        (those fields may have been written with future-date contamination)
      - Returns normalized rows ready for snapshot, raw appendix, and summary use

    All ledger-derived sections in any pack must use this function, not
    load from disk independently.
    """
    return load_and_normalize_ledger_rows(
        research_day=report_day,
        window_days=window_days,
        as_of_day=report_day,
    )


# ---------------------------------------------------------------------------
# Measurement Decision Card helper
# ---------------------------------------------------------------------------

def build_measurement_decision_card(
    cases: List[Dict],
    sig_candidates: List[Dict],
    normalized_ledger_rows: List[Dict],
) -> Dict:
    """
    Derive measurement decision state from existing downstream fields.
    Returns a dict with all Measurement Decision Card fields.

    Decision state ladder:
      NO_CHANGE           — no eligible improvement cases, no repeated sigs
      KEEP_TRACKING       — some improvement cases or sigs, not enough for hypothesis
      PREPARE_HYPOTHESIS  — >= 3 improvement cases AND >= 1 MEDIUM+ repeated sig
      (READY_FOR_CONTROLLED_VALIDATION — not reachable yet; deferred to future cycles)

    Does NOT promote strategy change. Conservative by design.
    """
    eligible = [c for c in cases if c.get("research_eligible_YN") == "Y"]
    # Constrain to SHORT side only — measurement universe (Phase 2B/2C/2D/2E) is SHORT-only.
    # Including LONG improvement candidates causes chosen_family mismatch
    # with the measured validation stack (chosen_family_sample_size = 0).
    improve   = [c for c in eligible
                 if c.get("decision_grade") == "OLD_STRATEGY_IMPROVEMENT_CANDIDATE"
                 and c.get("side") == "SHORT"]
    thesis    = [c for c in eligible
                 if c.get("decision_grade") == "NEW_STRATEGY_THESIS_CANDIDATE"]
    thesis_count = len(thesis)
    strong_sigs = [s for s in sig_candidates
                   if s.get("confidence") in ("HIGH", "MEDIUM")]

    # Repeated candidates from normalized ledger (deduped by signature_key)
    _seen_rpt: set = set()
    repeated_sig_count = 0
    for r in normalized_ledger_rows:
        sk = r.get("signature_key", "")
        if r.get("current_role") == "repeated_candidate" and sk and sk not in _seen_rpt:
            _seen_rpt.add(sk)
            repeated_sig_count += 1

    improve_count    = len(improve)
    strong_sig_count = len(strong_sigs)

    # Observed evidence counters — separate from promotable universe
    # all_today_sig_count: cases where >= threshold cases matched same pattern today.
    # Uses len(sig_candidates) directly — these are today's threshold-meeting patterns,
    # NOT the rolling ledger repeated_candidate role.
    all_today_sig_count = len(sig_candidates)

    # LONG-side improvement candidates: observed today but excluded from SHORT-only
    # measurement universe (Phase 2B/2C/2D/2E). Counted separately for narrative.
    long_improve = [
        c for c in eligible
        if c.get("decision_grade") == "OLD_STRATEGY_IMPROVEMENT_CANDIDATE"
        and c.get("side") != "SHORT"
    ]
    long_improve_count = len(long_improve)

    # Derive decision_state
    if improve_count >= 3 and strong_sig_count >= 1 and repeated_sig_count >= 1:
        decision_state = "PREPARE_HYPOTHESIS"
    elif improve_count >= 1 and (strong_sig_count >= 1 or repeated_sig_count >= 1):
        decision_state = "KEEP_TRACKING"
    elif improve_count >= 1 or strong_sig_count >= 1:
        decision_state = "KEEP_TRACKING"
    else:
        decision_state = "NO_CHANGE"

    # Chosen family and layer — from dominant improvement candidate
    def _dom(lst, field):
        vs = [c.get(field, "") for c in lst if c.get(field)]
        return max(set(vs), key=vs.count) if vs else "—"

    chosen_family = _dom(improve, "maps_to_existing_strategy_family") if improve else "—"
    chosen_layer  = _dom(improve, "improvement_target_layer") if improve else "—"

    # Evidence note
    # Split: A) total observed evidence today (any confidence / any side)
    #        B) promotable in-universe evidence (SHORT improve + MEDIUM+ sig + thesis)
    # When A > 0 but B = 0, card must acknowledge evidence without implying promotability.
    _any_evidence_today = (
        improve_count > 0           # SHORT in-universe improvement candidates
        or strong_sig_count > 0     # MEDIUM+ confidence repeated signatures
        or thesis_count > 0         # new thesis candidates
        or all_today_sig_count > 0  # any sig candidate today (incl LOW confidence)
        or long_improve_count > 0   # LONG-side improve (observed, out-of-universe)
    )
    if improve_count == 0 and strong_sig_count == 0 and thesis_count == 0:
        if not _any_evidence_today:
            # True no-evidence day — nothing observed at all
            evidence_note = "No improvement candidates, repeated signatures, or new thesis cases today."
        else:
            # Evidence observed today but none meets in-universe promotion threshold
            _obs_parts = []
            if all_today_sig_count > 0:
                _sig_codes = ", ".join(
                    s.get("signature_candidate_code", "?") for s in sig_candidates[:2]
                )
                _obs_parts.append(
                    f"{all_today_sig_count} signature candidate(s) repeated today "
                    f"at the case-pattern level ({_sig_codes}); "
                    f"confidence below MEDIUM+ threshold — not yet cross-day stable"
                )
            if long_improve_count > 0:
                _long_fams = list(dict.fromkeys(
                    c.get("maps_to_existing_strategy_family", "") or c.get("side", "")
                    for c in long_improve
                    if c.get("maps_to_existing_strategy_family", "") not in ("", "—", "none")
                ))
                _long_fam_str = ", ".join(_long_fams[:2]) if _long_fams else "LONG-side"
                _obs_parts.append(
                    f"{long_improve_count} LONG-side improvement candidate(s) observed "
                    f"({_long_fam_str}); excluded from measured universe "
                    f"(current stack is SHORT-only)"
                )
            evidence_note = (
                "Evidence observed today but none meets in-universe promotion threshold. "
                + "; ".join(_obs_parts) + ". "
                "Decision State = NO_CHANGE. "
                "Chosen Family = \u2014 (no SHORT in-universe action target today)."
            )
    elif improve_count == 0 and strong_sig_count == 0 and thesis_count > 0:
        # Isolated new thesis: visible but not action-ready
        _thesis_fams = list(set(
            c.get("candidate_strategy_family_name","") for c in thesis
            if c.get("candidate_strategy_family_name","") not in ("","under_investigation")
        ))
        _fam_str = ", ".join(_thesis_fams[:2]) if _thesis_fams else "unnamed"
        evidence_note = (f"{thesis_count} isolated new-thesis candidate(s) observed "
                         f"(family: {_fam_str}). Not action-ready — needs multi-day repetition "
                         "before hypothesis stage.")
    elif improve_count > 0 and strong_sig_count == 0:
        # Distinguish: no sigs at all vs sigs for other family vs sigs but all LOW confidence
        _chosen_sigs = [s for s in sig_candidates
                        if s.get("maps_to_existing_strategy_family", "") == chosen_family]
        _other_sigs  = [s for s in sig_candidates
                        if s.get("maps_to_existing_strategy_family", "") != chosen_family]
        if not sig_candidates:
            evidence_note = (f"{improve_count} case-level improvement candidate(s); "
                             f"no repeated cross-day signature yet.")
        elif _other_sigs and not _chosen_sigs:
            _codes = ", ".join(s.get("signature_candidate_code", "?") for s in _other_sigs[:2])
            evidence_note = (
                f"{improve_count} case-level improvement candidate(s) for {chosen_family}. "
                f"No MEDIUM+ repeated signature for {chosen_family} yet. "
                f"Note: {len(_other_sigs)} repeated signature(s) observed for other "
                f"families ({_codes}) — these are not evidence for {chosen_family}."
            )
        else:
            # sig_candidates exist (possibly for chosen family) but all LOW confidence
            evidence_note = (
                f"{improve_count} case-level improvement candidate(s); "
                f"{len(sig_candidates)} repeated signature(s) present but none MEDIUM+ "
                f"confidence — not yet cross-day stable."
            )
    elif improve_count == 0 and strong_sig_count > 0:
        evidence_note = (f"No case-level improvement candidates; "
                         f"{strong_sig_count} repeated signature(s) with MEDIUM+ confidence.")
    else:
        evidence_note = (f"{improve_count} improvement candidate(s) + "
                         f"{strong_sig_count} repeated signature(s). "
                         f"Cross-day support: {repeated_sig_count} signature(s) in repeated_candidate role.")

    # Why this family
    if chosen_family != "—":
        why_now = (f"{improve_count} eligible improvement case(s) map to {chosen_family} "
                   f"at layer {chosen_layer}.")
    else:
        why_now = "No dominant family identified today."

    # Expected upside
    upside = ("Better detection or delivery precision for chosen family "
              "if evidence holds across more days." if chosen_family != "—" else "—")

    # Risk
    risk = ("Overfitting risk if rule is changed before evidence repeats across >= 5 days "
            "with stable anchor quality.")

    # Why not others
    other_families = set(c.get("maps_to_existing_strategy_family", "")
                         for c in eligible
                         if c.get("maps_to_existing_strategy_family","") not in ("", "—", "none", chosen_family))
    _other_str = ', '.join(sorted(other_families)[:3]) if other_families else 'none'
    _sig_note = ""
    if sig_candidates:
        _sig_fams = list(set(s.get("maps_to_existing_strategy_family","") or
                             s.get("dominant_side","") for s in sig_candidates
                             if s.get("confidence") in ("HIGH","MEDIUM")))
        if _sig_fams:
            _sig_note = (f" Note: repeated signature(s) exist ({', '.join(_sig_fams[:2])}) "
                         "but no action-ready improvement candidates for that family today.")
    why_not = (f"No other action-ready families today (observed: {_other_str})."
               + _sig_note)

    # Validation next step
    next_step_map = {
        "NO_CHANGE":          "Continue daily tracking. No action until evidence builds.",
        "KEEP_TRACKING":      "Accumulate >= 3 improvement cases + 1 repeated MEDIUM+ sig before hypothesis.",
        "PREPARE_HYPOTHESIS": "Define one improvement rule, run controlled validation on unseen days.",
    }
    next_step = next_step_map.get(decision_state, "Continue tracking.")

    return {
        "decision_state":           decision_state,
        "chosen_family":            chosen_family,
        "chosen_issue_layer":       chosen_layer,
        "why_this_family_now":      why_now,
        "expected_upside":          upside,
        "main_risk_or_side_effect": risk,
        "evidence_strength_note":   evidence_note,
        "why_not_others":           why_not,
        "validation_next_step":     next_step,
        # raw counts for rendering
        "_improve_count":      improve_count,
        "_strong_sig_count":   strong_sig_count,
        "_repeated_sig_count": repeated_sig_count,
    }


# ---------------------------------------------------------------------------
# Trusted / Weak / Deferred Summary helper
# ---------------------------------------------------------------------------

def build_trusted_weak_deferred(
    cases: List[Dict],
    sig_candidates: List[Dict],
    selection_context: Dict,
) -> Dict:
    """
    Derive trusted / weak / deferred items from existing downstream fields.
    Returns dict with three lists of strings.

    Conservative by design: err toward weaker trust labels.
    """
    eligible     = [c for c in cases if c.get("research_eligible_YN") == "Y"]
    proxy_ok     = [c for c in cases if c.get("proxy_complete_YN") == "Y"]
    outcome_ok   = [c for c in cases if c.get("outcome_complete_YN") == "Y"]
    full_vis     = [c for c in cases if c.get("full_visual_complete_YN") == "Y"]
    n            = max(len(cases), 1)

    elig_pct    = round(100 * len(eligible) / n)
    proxy_pct   = round(100 * len(proxy_ok) / n)
    outcome_pct = round(100 * len(outcome_ok) / n)
    vis_pct     = round(100 * len(full_vis) / n)
    missing_vis = n - len(full_vis)

    strong_sigs = [s for s in sig_candidates if s.get("confidence") in ("HIGH","MEDIUM")]
    improve_cases = [c for c in eligible
                     if c.get("decision_grade") == "OLD_STRATEGY_IMPROVEMENT_CANDIDATE"]

    regime = selection_context.get("research_regime", "—")

    # --- Trusted ---
    trusted = []
    if elig_pct >= 75:
        trusted.append(f"Data completeness: {elig_pct}% eligible — sufficient for directional reading.")
    if proxy_pct >= 60:
        trusted.append(f"Proxy completeness: {proxy_pct}% — flow classification is usable.")
    if outcome_pct >= 67:
        trusted.append(f"Outcome completeness: {outcome_pct}% — 1h/4h horizons available for interpretation.")
    if not trusted:
        trusted.append("No fields meet trusted threshold today — treat all conclusions as provisional.")

    # --- Weak ---
    weak = []
    sig_count = len(sig_candidates)
    if sig_count == 0:
        weak.append("Repeated signature support: 0 today — no cross-case pattern confirmed.")
    elif not strong_sigs:
        weak.append(f"Repeated signature support: {sig_count} candidate(s) but all LOW confidence.")
    else:
        weak.append(f"Repeated signature support: {sig_count} candidate(s), {len(strong_sigs)} MEDIUM+. "
                    "Not yet multi-day stable.")

    # Family isolation
    active_families = set(c.get("maps_to_existing_strategy_family","")
                          for c in improve_cases
                          if c.get("maps_to_existing_strategy_family","") not in ("","—"))
    if len(active_families) > 1:
        weak.append(f"Family isolation: {len(active_families)} families present — "
                    "evidence is not yet concentrated in one family.")
    elif len(improve_cases) == 0:
        weak.append("Family isolation: no improvement candidates today.")

    if missing_vis > 0:
        weak.append(f"Visual completeness: {missing_vis}/{n} chart(s) missing — "
                    "manual review quality reduced for those cases.")

    if not weak:
        weak.append("No major weaknesses identified today — verify with multi-day accumulation.")

    # --- Deferred ---
    deferred = []
    deferred.append("Multi-day stability: need >= 5 days with consistent improvement candidates "
                    "before any rule change is proposed.")
    deferred.append("Anchor QA sample: P0–P4 manual spot-check not yet run for this dataset.")
    deferred.append("Controlled validation loop: fresh unseen-day validation required before "
                    "any promoted hypothesis is considered for live implementation.")
    short_improve_cases = [c for c in improve_cases if c.get("side") == "SHORT"]
    if len(short_improve_cases) < 3:
        deferred.append(
            f"More short-side improvement candidates needed: "
            f"currently {len(short_improve_cases)} short improvement candidate(s) "
            f"({len(improve_cases)} total improvement candidate(s) across all sides today); "
            f"target >= 3 short per day before hypothesis stage."
        )

    return {
        "trusted":  trusted,
        "weak":     weak,
        "deferred": deferred,
    }


# ===========================================================================
# Phase 2 additions — downstream measurement helpers
# ===========================================================================

# ---------------------------------------------------------------------------
# Phase 2 — shared primitives
# ---------------------------------------------------------------------------

def _dom_field_p2(group, field):
    vs = [c.get(field, "") for c in group if c.get(field)]
    return max(set(vs), key=vs.count) if vs else "—"


def _dom_resolution_p2(group, field):
    """
    Mode resolution for a bucket with tie-detection.

    Returns:
      - mode value when one resolution has strictly more cases than all others
      - 'MIXED' when two or more values tie at the top count
      - '—' when the group has no populated values for this field

    Fixes: _dom_field_p2 via max(set, key=count) picks an arbitrary winner
    on tie because Python set has non-deterministic hash order — caller
    cannot distinguish a real dominant resolution from a tie artifact.
    """
    vs = [c.get(field, "") for c in group if c.get(field)]
    if not vs:
        return "—"
    counts: dict = {}
    for v in vs:
        counts[v] = counts.get(v, 0) + 1
    max_count = max(counts.values())
    winners = [k for k, cnt in counts.items() if cnt == max_count]
    if len(winners) == 1:
        return winners[0]   # clear dominant resolution
    return "MIXED"          # tie — no single dominant resolution


def _med_field_p2(group, field):
    vs = []
    for c in group:
        v = c.get(field)
        if v not in (None, "", "None"):
            try:
                vs.append(float(v))
            except Exception:
                pass
    try:
        return round(statistics.median(vs), 3) if vs else None
    except Exception:
        return None


def _mean_field_p2(group, field):
    vs = []
    for c in group:
        v = c.get(field)
        if v not in (None, "", "None"):
            try:
                vs.append(float(v))
            except Exception:
                pass
    try:
        return round(sum(vs) / len(vs), 3) if vs else None
    except Exception:
        return None


def _readiness_note_p2(n):
    if n < 3:
        return "sample_too_small"
    if n < 10:
        return "directional_only"
    return "directional_usable"


def _display_family_p2(c):
    """Live strategy family first, then candidate, then unclassified.
    Matches Phase 1 shortlist fix semantics."""
    fam = (c.get("maps_to_existing_strategy_family") or "").strip()
    if fam and fam not in ("—", "none", "N/A"):
        return fam
    cand = (c.get("candidate_strategy_family_name") or "").strip()
    if cand and cand not in ("—", "under_investigation"):
        return cand
    return "unclassified"


def _timing_band_p2(c):
    t = c.get("time_to_2pct_favor_min")
    if t in (None, "", "None"):
        return "not_reached"
    try:
        t = float(t)
        if t < 30:
            return "fast_<30m"
        if t < 120:
            return "slow_30-120m"
        return "very_slow_>120m"
    except Exception:
        return "not_reached"


def _btc_bucket_p2(c):
    v = c.get("btc_24h_change_pct")
    if v in (None, "", "None"):
        return "unknown"
    try:
        v = float(v)
        if v <= -3:
            return "btc_weak"
        if v >= 3:
            return "btc_strong"
        return "btc_neutral"
    except Exception:
        return "unknown"


def _breadth_bucket_p2(c):
    v = c.get("alt_breadth_pct")
    if v in (None, "", "None"):
        return "unknown"
    try:
        v = float(v)
        if v < 30:
            return "weak_breadth"
        if v > 60:
            return "strong_breadth"
        return "neutral_breadth"
    except Exception:
        return "unknown"


def _board_rows_p2(items, key_fn):
    groups = defaultdict(list)
    for c in items:
        k = key_fn(c)
        groups[k].append(c)
    rows = []
    for key, group in sorted(groups.items(), key=lambda x: -len(x[1])):
        n = len(group)
        label = " | ".join(
            str(k) for k in (key if isinstance(key, tuple) else (key,))
        )
        rows.append({
            "group_label": label,
            "case_count": n,
            "median_f1h": _med_field_p2(group, "future_1h_max_favor_pct"),
            "median_f4h": _med_field_p2(group, "future_4h_max_favor_pct"),
            "median_a4h": _med_field_p2(group, "future_4h_max_adverse_pct"),
            "dominant_resolution": _dom_resolution_p2(group, "resolution_label"),
            "readiness_note": _readiness_note_p2(n),
        })
    return rows


# ---------------------------------------------------------------------------
# Phase 2 — A. Anchor QA / Anchor Audit Summary
# ---------------------------------------------------------------------------

def build_anchor_qa_summary(cases, anchor_rows):
    """
    Aggregates anchor render and conflict metadata from downstream case + anchor rows.
    anchor_measurement_readiness is NOT_RUN until manual spot-check is provided.
    anchor_conflict_rows_count uses case-level proxy — per-anchor fallback_used
    is not exposed in downstream anchor CSV rows.
    """
    n_cases = len(cases)
    expected = n_cases * 5
    rendered = len(anchor_rows)
    images_rendered = sum(1 for r in anchor_rows if r.get("image_created_YN") == "Y")
    conflict_cases = sum(1 for c in cases if c.get("anchor_conflict_flag") == "Y")
    fallback_cases = conflict_cases
    methods = [c.get("anchor_detect_method", "") for c in cases if c.get("anchor_detect_method", "")]
    detect_method = max(set(methods), key=methods.count) if methods else "AUTO_V1"

    audit_note = (
        f"Manual anchor spot-check has not yet run for this dataset. "
        f"anchor_measurement_readiness = NOT_RUN. "
        f"Render completeness: {rendered}/{expected} anchor rows, "
        f"{images_rendered} images rendered. "
        f"Cases using fallback detection (anchor_conflict_flag=Y): "
        f"{conflict_cases}/{n_cases}. "
        f"anchor_conflict_rows_count is a case-level proxy "
        f"(per-anchor fallback_used is not exposed in downstream anchor CSV). "
        f"anchor_measurement_readiness cannot exceed DIRECTIONAL_ONLY "
        f"without a manual audit sample."
    )
    return {
        "anchor_rows_expected": expected,
        "anchor_rows_rendered": rendered,
        "anchor_images_rendered": images_rendered,
        "anchor_detect_method": detect_method,
        "anchor_conflict_cases_count": conflict_cases,
        "anchor_conflict_rows_count": f"case_level_proxy:{conflict_cases}",
        "anchor_fallback_cases_count": fallback_cases,
        "manual_anchor_audit_sample_size": 0,
        "manual_anchor_audit_pass_rate": "not_run",
        "anchor_measurement_readiness": "NOT_RUN",
        "anchor_audit_note": audit_note,
    }


# ---------------------------------------------------------------------------
# Phase 2 — B. Short Family Measurement Boards
# ---------------------------------------------------------------------------

def build_short_family_boards(cases):
    """
    Five compact boards for short-side family measurement.
    B1: Breakdown Quality    — move_class x structural_quality x break_quality_band x resolution
    B2: Retest/Reclaim       — move_class x structural_quality x reclaim_4h x resolution
    B3: Exhaustion/Top       — pre_move_signature x participation_pattern x move_class x resolution
    B4: Timing/Staleness     — candidate_family x timing_band x resolution
    B5: Context/Regime       — research_regime x btc_bucket x breadth_bucket x resolution
    """
    shorts = [
        c for c in cases
        if c.get("side") == "SHORT" and c.get("research_eligible_YN") == "Y"
    ]
    if not shorts:
        return {
            "short_eligible_count": 0,
            "note": "No eligible SHORT cases today. Boards not rendered.",
            "b1_breakdown_quality": [],
            "b2_retest_reclaim": [],
            "b3_exhaustion_top": [],
            "b4_timing_staleness": [],
            "b5_context_regime": [],
        }

    b1 = _board_rows_p2(shorts, lambda c: (
        c.get("move_class", "—"), c.get("structural_quality", "—"),
        c.get("break_quality_band", "—"), c.get("resolution_label", "—"),
    ))
    b2 = _board_rows_p2(shorts, lambda c: (
        c.get("move_class", "—"), c.get("structural_quality", "—"),
        c.get("retest_close_back_above_break_flag", "—"), c.get("resolution_label", "—"),
    ))
    b3 = _board_rows_p2(shorts, lambda c: (
        c.get("pre_move_signature", "—"), c.get("participation_pattern", "—"),
        c.get("move_class", "—"), c.get("resolution_label", "—"),
    ))
    b4 = _board_rows_p2(shorts, lambda c: (
        _display_family_p2(c), c.get("time_of_day_bucket", "—"), c.get("resolution_label", "—"),
    ))
    b5 = _board_rows_p2(shorts, lambda c: (
        c.get("research_regime", "—"), _btc_bucket_p2(c),
        _breadth_bucket_p2(c), c.get("resolution_label", "—"),
    ))
    return {
        "short_eligible_count": len(shorts),
        "note": f"{len(shorts)} eligible SHORT case(s) today.",
        "b1_breakdown_quality": b1,
        "b2_retest_reclaim": b2,
        "b3_exhaustion_top": b3,
        "b4_timing_staleness": b4,
        "b5_context_regime": b5,
    }


# ---------------------------------------------------------------------------
# Phase 2 — C. Interaction Board
# ---------------------------------------------------------------------------

def build_interaction_board(cases):
    """
    Four interaction pairs. Top 5 combinations per pair by case_count.
    1. structural_quality x participation_pattern
    2. move_class x research_regime
    3. break_quality_band x participation_pattern
    4. candidate_family x caution_flag
    Directional only — not action-ready at current sample sizes.
    """
    eligible = [c for c in cases if c.get("research_eligible_YN") == "Y"]
    if not eligible:
        return []

    PAIRS = [
        ("structural_quality x participation_pattern",
         lambda c: (c.get("structural_quality", "—"), c.get("participation_pattern", "—"))),
        ("move_class x research_regime",
         lambda c: (c.get("move_class", "—"), c.get("research_regime", "—"))),
        ("break_quality_band x participation_pattern",
         lambda c: (c.get("break_quality_band", "—"), c.get("participation_pattern", "—"))),
        ("candidate_family x caution_flag",
         lambda c: (_display_family_p2(c), c.get("caution_flag", "—"))),
    ]

    results = []
    for pair_name, key_fn in PAIRS:
        groups = defaultdict(list)
        for c in eligible:
            groups[key_fn(c)].append(c)
        top_combos = sorted(groups.items(), key=lambda x: -len(x[1]))[:5]
        combo_rows = []
        for combo_key, group in top_combos:
            n = len(group)
            combo_rows.append({
                "combination_key": " | ".join(str(k) for k in combo_key),
                "case_count": n,
                "median_f1h": _med_field_p2(group, "future_1h_max_favor_pct"),
                "median_f4h": _med_field_p2(group, "future_4h_max_favor_pct"),
                "median_a4h": _med_field_p2(group, "future_4h_max_adverse_pct"),
                "dominant_resolution": _dom_resolution_p2(group, "resolution_label"),
                "sample_note": "sample_too_small" if n < 3 else "directional",
            })
        all_small = all(r["case_count"] < 3 for r in combo_rows)
        results.append({
            "interaction_pair": pair_name,
            "top_combinations": combo_rows,
            "total_eligible": len(eligible),
            "analyst_note": (
                "All combinations too small for stable conclusions — directional reading only."
                if all_small else
                "Directional only at current sample size. Not action-ready."
            ),
        })
    return results


# ---------------------------------------------------------------------------
# Phase 2 — D. Measurement Confidence Summary
# ---------------------------------------------------------------------------

def build_measurement_confidence_summary(cases, sig_candidates, normalized_ledger_rows):
    """
    Compact confidence frame. Conservative by design.
    confidence_band capped at MEDIUM — HIGH not achievable at Phase 2 stage.
    Stability derived from canonical signature_key cross-day repetition in ledger.
    """
    eligible = [c for c in cases if c.get("research_eligible_YN") == "Y"]
    # chosen_family_sample_size aligned with Decision Card logic:
    # Decision Card picks chosen_family only from strategy_action_type == improve_existing.
    # When Chosen Family = -, count is 0. Do not count thesis candidates here.
    short_improve = [
        c for c in eligible
        if c.get("side") == "SHORT"
        and c.get("strategy_action_type") == "improve_existing"
    ]
    decision_sample = len(eligible)
    chosen_sample   = len(short_improve)

    short_elig = [c for c in eligible if c.get("side") == "SHORT"]
    fam_counts = {}
    for c in short_elig:
        fam = _display_family_p2(c)
        fam_counts[fam] = fam_counts.get(fam, 0) + 1
    largest_family_n = max(fam_counts.values()) if fam_counts else 0

    sample_gate = "MET" if decision_sample >= 50 else ("PARTIAL" if decision_sample >= 10 else "NOT_MET")
    bucket_gate = "MET" if largest_family_n >= 10 else ("PARTIAL" if largest_family_n >= 3 else "NOT_MET")

    # Stability — active rolling-window rows only (current_role != stale).
    # recent_support_days_count is already computed by load_and_normalize_ledger_rows.
    # Stale rows have recent_support_days_count = 0 and are excluded from this signal.
    _active_rows = [
        r for r in normalized_ledger_rows
        if r.get("current_role") != "stale"
    ]
    max_recent_days = max(
        (int(r.get("recent_support_days_count", 0)) for r in _active_rows),
        default=0,
    )
    # Hard downgrade: 0 repeated sig today means no current evidence.
    # Do not let stale ledger history inflate stability.
    _today_sig_count = len(sig_candidates)
    if _today_sig_count == 0:
        stability_flag = "UNSTABLE"
    elif max_recent_days >= 5:
        stability_flag = "STABLE"
    elif max_recent_days >= 2:
        stability_flag = "EARLY_SIGNAL"
    else:
        stability_flag = "UNSTABLE"

    regimes = [c.get("research_regime", "") for c in eligible if c.get("research_regime", "")]
    if not regimes:
        regime_consistency = "UNKNOWN"
    else:
        dominant = max(set(regimes), key=regimes.count)
        regime_consistency = "CONSISTENT" if regimes.count(dominant) / len(regimes) >= 0.70 else "MIXED"

    # Hard downgrade: 0 repeated sig today → cap confidence at LOW regardless of gates.
    # Gates alone cannot justify MEDIUM when no repeated evidence exists today.
    if _today_sig_count == 0:
        confidence_band = "LOW"
    elif (
        sample_gate in ("MET", "PARTIAL")
        and stability_flag in ("STABLE", "EARLY_SIGNAL")
        and bucket_gate in ("MET", "PARTIAL")
    ):
        confidence_band = "MEDIUM"
    else:
        confidence_band = "LOW"

    return {
        "decision_sample_size": decision_sample,
        "chosen_family_sample_size": chosen_sample,
        "largest_short_family_sample_size": largest_family_n,
        "sample_gate_status": sample_gate,
        "bucket_gate_status": bucket_gate,
        "stability_flag": stability_flag,
        "regime_consistency_flag": regime_consistency,
        "confidence_band": confidence_band,
        "confidence_note": (
            f"Eligible sample: {decision_sample} case(s) (sample_gate: {sample_gate}). "
            f"Largest short family: {largest_family_n} case(s) (bucket_gate: {bucket_gate}). "
            f"Active cross-day repetition (non-stale): {max_recent_days} day(s) ({stability_flag}). "
            f"Regime consistency: {regime_consistency}. "
            f"confidence_band capped at {confidence_band} — not action-ready until "
            f">= 50 clean cases and >= 5 stable cross-day repetitions."
        ),
        "why_not_promoted": (
            "Phase 2 measurement scaffolding only. Not promoted to action-ready until: "
            "(1) >= 50 clean eligible cases, "
            "(2) signature_key stability >= 5 cross-day days, "
            "(3) controlled validation on unseen days."
        ),
        "next_validation_requirement": (
            f"Continue daily tracking. "
            f"Current active cross-day repetition (non-stale): {max_recent_days} day(s); "
            f"target >= 5 before hypothesis stage."
        ),
    }


# ---------------------------------------------------------------------------
# Phase 2 — E. Appendix Stats (compact, no statistical tests)
# ---------------------------------------------------------------------------

def build_appendix_stats(cases):
    """
    Compact grouped stats for appendix. No p-values / bootstrap / KS / t-test.
    All confidence bands conservatively LOW at Phase 2 stage.
    """
    eligible = [c for c in cases if c.get("research_eligible_YN") == "Y"]

    side_fam_groups = defaultdict(list)
    for c in eligible:
        key = (c.get("side", "—"), _display_family_p2(c))
        side_fam_groups[key].append(c)

    family_stats = []
    for (side, fam), group in sorted(side_fam_groups.items(), key=lambda x: -len(x[1])):
        n = len(group)
        family_stats.append({
            "side": side, "family": fam, "case_count": n,
            "median_f1h": _med_field_p2(group, "future_1h_max_favor_pct"),
            "median_f4h": _med_field_p2(group, "future_4h_max_favor_pct"),
            "median_a4h": _med_field_p2(group, "future_4h_max_adverse_pct"),
            "mean_f1h": _mean_field_p2(group, "future_1h_max_favor_pct"),
            "mean_f4h": _mean_field_p2(group, "future_4h_max_favor_pct"),
            "mean_a4h": _mean_field_p2(group, "future_4h_max_adverse_pct"),
            "stability_note": "sample_too_small" if n < 3 else "directional_only",
            "confidence_band": "LOW",
        })

    short_elig = [c for c in eligible if c.get("side") == "SHORT"]
    res_counts = {}
    for c in short_elig:
        rl = c.get("resolution_label", "—")
        res_counts[rl] = res_counts.get(rl, 0) + 1
    resolution_dist = [
        {"resolution_label": rl, "count": cnt,
         "share_pct": round(100 * cnt / max(len(short_elig), 1))}
        for rl, cnt in sorted(res_counts.items(), key=lambda x: -x[1])
    ]

    return {
        "family_stats": family_stats,
        "short_resolution_distribution": resolution_dist,
        "short_eligible_count": len(short_elig),
        "total_eligible_count": len(eligible),
        "note": (
            "All confidence bands are LOW at Phase 2 stage. "
            "No statistical tests applied. Directional reading only. "
            "Do not draw optimization conclusions from this appendix."
        ),
    }




# ===========================================================================
# Phase 2B — multi-day measurement accumulation + statistical validation
# ===========================================================================
#
# Scope: SHORT-side eligible cases only (side == "SHORT" and research_eligible_YN == "Y")
# Family-level cross-day case history: NOT AVAILABLE in current data model.
#   → family_days_count = "not_available_yet"
#   → family_stability_flag = "NOT_AVAILABLE_YET"
# Global ledger context (distinct research_days in ledger) is available but
#   must be labeled clearly and must NOT be used as family support evidence.
#
# Threshold constants — single source of truth for all Phase 2B gate logic:

_P2B_BUCKET_READY_THRESHOLD       = 10   # n >= 10: bucket_ready
_P2B_TRACKING_GRADE_THRESHOLD     = 20   # n >= 20: tracking_grade
_P2B_RECOMMENDATION_GRADE_THRESHOLD = 50 # n >= 50: recommendation_grade


# ---------------------------------------------------------------------------
# Phase 2B — private statistical helpers
# ---------------------------------------------------------------------------

def _sample_gate_status_2b(n: int) -> str:
    """
    Conservative per-family sample gate.
    Uses _P2B_* threshold constants — must not be changed locally.
    """
    if n >= _P2B_RECOMMENDATION_GRADE_THRESHOLD:
        return "RECOMMENDATION_GRADE"
    if n >= _P2B_TRACKING_GRADE_THRESHOLD:
        return "TRACKING_GRADE"
    if n >= _P2B_BUCKET_READY_THRESHOLD:
        return "LOW_SAMPLE"
    return "NOT_ENOUGH_SAMPLE"


def _bucket_gate_status_2b(n: int) -> str:
    """Per-bucket readiness using _P2B_BUCKET_READY_THRESHOLD."""
    if n >= _P2B_RECOMMENDATION_GRADE_THRESHOLD:
        return "BUCKET_READY"
    if n >= _P2B_TRACKING_GRADE_THRESHOLD:
        return "BUCKET_READY"
    if n >= _P2B_BUCKET_READY_THRESHOLD:
        return "BUCKET_READY"
    if n >= 5:
        return "BUCKET_THIN"
    return "BUCKET_INSUFFICIENT"


def _mean_ci_2b(values: list):
    """
    95% CI for the mean using normal approximation.
    Returns (ci_low, ci_high) or (None, None) if n < 5.
    Uses statistics module only — no numpy dependency.
    """
    n = len(values)
    if n < 5:
        return None, None
    try:
        mu = sum(values) / n
        if n < 2:
            return round(mu, 3), round(mu, 3)
        sd = statistics.stdev(values)
        se = sd / (n ** 0.5)
        margin = 1.96 * se
        return round(mu - margin, 3), round(mu + margin, 3)
    except Exception:
        return None, None


def _regime_consistency_flag_2b(rows: list) -> str:
    """
    Regime consistency from research_regime across case rows.
    Returns CONSISTENT / MIXED / FRAGMENTED.
    """
    regimes = [r.get("research_regime", "") for r in rows if r.get("research_regime", "")]
    if not regimes:
        return "FRAGMENTED"
    dominant = max(set(regimes), key=regimes.count)
    ratio = regimes.count(dominant) / len(regimes)
    if ratio >= 0.60:
        return "CONSISTENT"
    if ratio >= 0.40:
        return "MIXED"
    return "FRAGMENTED"


def _confidence_band_2b(sample_gate: str, family_stability_flag: str, regime_flag: str) -> str:
    """
    Confidence band from combined gates.
    MODERATE is the cap — no HIGH at Phase 2B stage.
    family_stability_flag = NOT_AVAILABLE_YET → always LOW or DESCRIPTIVE_ONLY.
    Returns MODERATE / LOW / DESCRIPTIVE_ONLY.
    """
    if sample_gate == "NOT_ENOUGH_SAMPLE":
        return "DESCRIPTIVE_ONLY"
    if family_stability_flag == "NOT_AVAILABLE_YET":
        return "LOW"
    if (sample_gate in ("TRACKING_GRADE", "RECOMMENDATION_GRADE")
            and family_stability_flag in ("STABLE", "EARLY_SIGNAL")
            and regime_flag in ("CONSISTENT", "MIXED")):
        return "MODERATE"
    return "LOW"


def _safe_win_like_rate_proxy_2b(cases: list) -> Optional[float]:
    """
    Share of SHORT cases where future_1h_max_favor_pct > 0.
    Returns None if n < _P2B_BUCKET_READY_THRESHOLD (not enough for meaningful rate).
    Directional proxy only — not a true win-rate.
    """
    vals = []
    for c in cases:
        v = c.get("future_1h_max_favor_pct")
        if v not in (None, "", "None"):
            try:
                vals.append(float(v))
            except Exception:
                pass
    if len(vals) < _P2B_BUCKET_READY_THRESHOLD:
        return None
    return round(sum(1 for v in vals if v > 0) / len(vals), 3)


def _sample_note_2b(n: int) -> str:
    """Human-readable note using threshold constants."""
    if n < _P2B_BUCKET_READY_THRESHOLD:
        return "too_small_for_any_conclusion"
    if n < _P2B_TRACKING_GRADE_THRESHOLD:
        return "directional_usable_not_action_ready"
    if n < _P2B_RECOMMENDATION_GRADE_THRESHOLD:
        return "tracking_grade_not_yet_action_ready"
    return "recommendation_grade"


# ---------------------------------------------------------------------------
# Phase 2B — A. Multi-day family stats (SHORT-only)
# ---------------------------------------------------------------------------

def build_multiday_family_stats(
    cases: List[Dict],
    normalized_ledger_rows: Optional[List[Dict]] = None,
) -> List[Dict]:
    """
    Grouped stats per display family for SHORT-side intervention analysis.

    Scope: side == "SHORT" AND research_eligible_YN == "Y" only.
    Do NOT include LONG cases — this is short-intervention specific.

    family_days_count = "not_available_yet"
      True family-level cross-day case history is not available in the current
      data model. Do NOT derive this from signature_evidence_ledger.csv, which is
      canonical for signature_key tracking, not family-level case counts.

    family_stability_flag = "NOT_AVAILABLE_YET"
      Stability requires cross-day family evidence that does not yet exist.
      This flag must NOT be used to claim stability or instability — only that
      the evidence layer is absent.

    normalized_ledger_rows param is accepted but only used for global context
    computation by the caller — not consumed inside this function.
    """
    # SHORT-only eligible filter — critical for short intervention framing
    short_eligible = [
        c for c in cases
        if c.get("research_eligible_YN") == "Y" and c.get("side") == "SHORT"
    ]

    fam_groups: Dict[str, List[Dict]] = defaultdict(list)
    for c in short_eligible:
        fam = _display_family_p2(c)
        fam_groups[fam].append(c)

    results = []
    for display_family, group in sorted(fam_groups.items(), key=lambda x: -len(x[1])):
        n = len(group)

        # Outcome fields — safe float extraction
        f1h_vals = []
        f4h_vals = []
        a4h_vals = []
        for c in group:
            for field, target in [
                ("future_1h_max_favor_pct", f1h_vals),
                ("future_4h_max_favor_pct", f4h_vals),
                ("future_4h_max_adverse_pct", a4h_vals),
            ]:
                v = c.get(field)
                if v not in (None, "", "None"):
                    try:
                        target.append(float(v))
                    except Exception:
                        pass

        # Gates using shared threshold constants
        sample_gate = _sample_gate_status_2b(n)
        bucket_gate = _bucket_gate_status_2b(n)
        regime_flag = _regime_consistency_flag_2b(group)

        # family_stability_flag is NOT_AVAILABLE_YET — no cross-day family case history
        family_stability_flag = "NOT_AVAILABLE_YET"

        # CI — 95% normal approx, graceful at small n
        ci_lo, ci_hi = _mean_ci_2b(f1h_vals) if f1h_vals else (None, None)

        confidence_band = _confidence_band_2b(sample_gate, family_stability_flag, regime_flag)
        win_proxy = _safe_win_like_rate_proxy_2b(group)

        results.append({
            "display_family":          display_family,
            "case_count":              n,              # today's SHORT eligible cases in this family
            "case_count_scope":        "today_only",
            "family_days_count":       "not_available_yet",   # replaces old days_count
            "family_stability_flag":   family_stability_flag,
            "median_f1h":              _med_field_p2(group, "future_1h_max_favor_pct"),
            "median_f4h":              _med_field_p2(group, "future_4h_max_favor_pct"),
            "median_a4h":              _med_field_p2(group, "future_4h_max_adverse_pct"),
            "mean_f1h":                _mean_field_p2(group, "future_1h_max_favor_pct"),
            "mean_f4h":                _mean_field_p2(group, "future_4h_max_favor_pct"),
            "mean_a4h":                _mean_field_p2(group, "future_4h_max_adverse_pct"),
            "win_like_rate_proxy":     win_proxy,
            "sample_gate_status":      sample_gate,
            "bucket_gate_status":      bucket_gate,
            "regime_consistency_flag": regime_flag,
            "bootstrap_ci_low":        ci_lo if ci_lo is not None else "not_enough_sample",
            "bootstrap_ci_high":       ci_hi if ci_hi is not None else "not_enough_sample",
            "confidence_band":         confidence_band,
            "sample_note":             _sample_note_2b(n),
        })

    return results


# ---------------------------------------------------------------------------
# Phase 2B — B. Multi-day interaction stats (SHORT-only)
# ---------------------------------------------------------------------------

def build_multiday_interaction_stats(cases: List[Dict]) -> List[Dict]:
    """
    Four compact interaction pairs for multi-day validation context.

    Scope: side == "SHORT" AND research_eligible_YN == "Y" only.
    days_count = 1 per cell — honest: today's cases only.

    Pairs:
      1. candidate_short_family x research_regime
      2. candidate_short_family x structural_quality
      3. break_quality_band x participation_pattern
      4. candidate_short_family x caution_flag
    """
    short_eligible = [
        c for c in cases
        if c.get("research_eligible_YN") == "Y" and c.get("side") == "SHORT"
    ]
    if not short_eligible:
        return []

    PAIRS = [
        (
            "candidate_short_family x research_regime",
            lambda c: (_display_family_p2(c), c.get("research_regime", "—")),
        ),
        (
            "candidate_short_family x structural_quality",
            lambda c: (_display_family_p2(c), c.get("structural_quality", "—")),
        ),
        (
            "break_quality_band x participation_pattern",
            lambda c: (c.get("break_quality_band", "—"), c.get("participation_pattern", "—")),
        ),
        (
            "candidate_short_family x caution_flag",
            lambda c: (_display_family_p2(c), c.get("caution_flag", "—")),
        ),
    ]

    results = []
    for pair_name, key_fn in PAIRS:
        groups: Dict[tuple, List[Dict]] = defaultdict(list)
        for c in short_eligible:
            groups[key_fn(c)].append(c)

        combo_rows = []
        for combo_key, group in sorted(groups.items(), key=lambda x: -len(x[1]))[:5]:
            n = len(group)
            combo_rows.append({
                "interaction_pair":   pair_name,
                "combination_key":    " | ".join(str(k) for k in combo_key),
                "case_count":         n,
                "days_count":         1,
                "median_f1h":         _med_field_p2(group, "future_1h_max_favor_pct"),
                "median_f4h":         _med_field_p2(group, "future_4h_max_favor_pct"),
                "median_a4h":         _med_field_p2(group, "future_4h_max_adverse_pct"),
                "sample_gate_status": _sample_gate_status_2b(n),
                "stability_flag":     "UNSTABLE",       # days_count = 1 always unstable
                "confidence_band":    "DESCRIPTIVE_ONLY",
                "sample_note":        "sample_too_small" if n < 3 else _sample_note_2b(n),
            })

        results.append({
            "interaction_pair":  pair_name,
            "top_combinations":  combo_rows,
            "total_short_eligible": len(short_eligible),
            "analyst_note": (
                "All combinations too small — directional reading only."
                if all(r["case_count"] < 3 for r in combo_rows) else
                "Directional only at current sample. Not action-ready."
            ),
        })

    return results


# ---------------------------------------------------------------------------
# Phase 2B — C. Controlled validation summary
# ---------------------------------------------------------------------------

def build_controlled_validation_summary(
    multiday_family_stats: List[Dict],
    multiday_interaction_stats: List[Dict],
    ledger_days_context: int = 0,
) -> Dict:
    """
    Compact promotion-state summary for SHORT-side intervention.

    ledger_days_context:
      Number of distinct research_day values in the normalized ledger (global context only).
      Computed by the caller from normalized_ledger_rows.
      Must NOT be used as family support, family stability, or promotion evidence.
      Labeled as ledger_research_days_global_context in output.

    Promotion ladder (conservative):
      DESCRIPTIVE_ONLY   — no SHORT family at bucket-ready threshold (n < 10)
      KEEP_TRACKING      — at least one SHORT family at bucket-ready or above (n >= 10)
      PREPARE_HYPOTHESIS — at least one SHORT family at tracking-grade (n >= 20),
                           AND family_stability_flag != NOT_AVAILABLE_YET,
                           AND family_days_count != not_available_yet.
                           EXPLICITLY BLOCKED when family history unavailable.
      NOT_READY_FOR_CONTROLLED_VALIDATION — not reachable while family history absent

    PREPARE_HYPOTHESIS is NOT reachable while family_days_count = not_available_yet
    or family_stability_flag = NOT_AVAILABLE_YET.
    This check is explicit — it does NOT rely only on confidence scoring.
    """
    _empty = {
        "promotion_state":                        "DESCRIPTIVE_ONLY",
        "top_candidate_family":                   "—",
        "promotion_reason":                       "No SHORT family stats available.",
        "blocking_reason":                        "No eligible SHORT cases to analyze.",
        "validation_next_step":                   "Continue daily tracking.",
        "family_multiday_history":                "not_available_yet",
        "ledger_research_days_global_context":    ledger_days_context,
        "families_at_bucket_ready_or_above":      0,
        "families_at_tracking_grade_or_above":    0,
        "families_at_recommendation_grade_or_above": 0,
    }
    if not multiday_family_stats:
        return _empty

    # Top candidate = highest case_count, preferring named families.
    # "unclassified" is valid data but not an actionable intervention target.
    _named = [r for r in multiday_family_stats if r.get("display_family") != "unclassified"]
    sorted_fams = sorted(_named or multiday_family_stats, key=lambda r: -r.get("case_count", 0))
    top = sorted_fams[0]
    top_family = top.get("display_family", "—")

    # Explicit counters using shared threshold constants
    families_at_bucket_ready_or_above = sum(
        1 for r in multiday_family_stats
        if r.get("case_count", 0) >= _P2B_BUCKET_READY_THRESHOLD
    )
    families_at_tracking_grade_or_above = sum(
        1 for r in multiday_family_stats
        if r.get("case_count", 0) >= _P2B_TRACKING_GRADE_THRESHOLD
    )
    families_at_recommendation_grade_or_above = sum(
        1 for r in multiday_family_stats
        if r.get("case_count", 0) >= _P2B_RECOMMENDATION_GRADE_THRESHOLD
    )

    # Check if family-level multiday history is available
    # If ANY family has not_available_yet, treat as globally unavailable
    family_history_unavailable = any(
        r.get("family_days_count") == "not_available_yet"
        or r.get("family_stability_flag") == "NOT_AVAILABLE_YET"
        for r in multiday_family_stats
    )

    # Candidates for each state
    prepare_candidates = [
        r for r in multiday_family_stats
        if r.get("case_count", 0) >= _P2B_TRACKING_GRADE_THRESHOLD
        and r.get("family_stability_flag") not in ("NOT_AVAILABLE_YET",)
        and r.get("family_days_count") != "not_available_yet"
        and r.get("regime_consistency_flag") in ("CONSISTENT", "MIXED")
    ]
    keep_tracking_candidates = [
        r for r in multiday_family_stats
        if r.get("case_count", 0) >= _P2B_BUCKET_READY_THRESHOLD
    ]

    # Determine promotion state
    # PREPARE_HYPOTHESIS: explicit block if family history unavailable
    if prepare_candidates and not family_history_unavailable:
        top_prepare = sorted(prepare_candidates, key=lambda r: -r.get("case_count", 0))[0]
        promotion_state = "PREPARE_HYPOTHESIS"
        promotion_reason = (
            f"{top_prepare.get('display_family','?')} has "
            f"{top_prepare.get('case_count', 0)} SHORT eligible cases today, "
            f"sample_gate={top_prepare.get('sample_gate_status','?')}, "
            f"regime={top_prepare.get('regime_consistency_flag','?')}."
        )
        blocking_reason = "—"
        next_step = (
            "Define one improvement rule. Run controlled validation on unseen days. "
            "Requires >= 5 stable cross-day days before promotion."
        )
    elif keep_tracking_candidates:
        top_kt = sorted(keep_tracking_candidates, key=lambda r: -r.get("case_count", 0))[0]
        promotion_state = "KEEP_TRACKING"
        promotion_reason = (
            f"{top_kt.get('display_family','?')} is the leading SHORT family with "
            f"{top_kt.get('case_count', 0)} eligible case(s) today."
        )
        if family_history_unavailable:
            blocking_reason = (
                f"Family multiday history is not_available_yet — "
                f"PREPARE_HYPOTHESIS is blocked until cross-day family case evidence exists. "
                f"Top family '{top_family}' has {top.get('case_count',0)} case(s) today "
                f"(need >= {_P2B_TRACKING_GRADE_THRESHOLD} for TRACKING_GRADE)."
            )
        else:
            blocking_reason = (
                f"Top family '{top_family}' has {top.get('case_count',0)} SHORT case(s) today. "
                f"Need >= {_P2B_TRACKING_GRADE_THRESHOLD} for TRACKING_GRADE, "
                f">= {_P2B_RECOMMENDATION_GRADE_THRESHOLD} for RECOMMENDATION_GRADE."
            )
        next_step = (
            f"Continue daily tracking for SHORT cases. "
            f"Target: >= {_P2B_TRACKING_GRADE_THRESHOLD} SHORT eligible cases "
            f"in top family per day before hypothesis stage."
        )
    else:
        promotion_state = "DESCRIPTIVE_ONLY"
        promotion_reason = (
            f"No SHORT family has reached bucket-ready threshold "
            f"(>= {_P2B_BUCKET_READY_THRESHOLD} cases)."
        )
        blocking_reason = (
            f"Top SHORT family '{top_family}' has {top.get('case_count',0)} case(s) today. "
            f"Need >= {_P2B_BUCKET_READY_THRESHOLD} for bucket-ready. "
            f"Need >= {_P2B_TRACKING_GRADE_THRESHOLD} for TRACKING_GRADE. "
            f"Need >= {_P2B_RECOMMENDATION_GRADE_THRESHOLD} for RECOMMENDATION_GRADE."
        )
        next_step = "Continue daily tracking. No action until evidence builds."

    return {
        "promotion_state":                        promotion_state,
        "top_candidate_family":                   top_family,
        "promotion_reason":                       promotion_reason,
        "blocking_reason":                        blocking_reason,
        "validation_next_step":                   next_step,
        "family_multiday_history":                "not_available_yet",
        "ledger_research_days_global_context":    ledger_days_context,
        "families_at_bucket_ready_or_above":      families_at_bucket_ready_or_above,
        "families_at_tracking_grade_or_above":    families_at_tracking_grade_or_above,
        "families_at_recommendation_grade_or_above": families_at_recommendation_grade_or_above,
    }




# ===========================================================================
# Phase 2B — multi-day measurement accumulation + statistical validation
# ===========================================================================
#
# Scope: SHORT-side eligible cases only (side == "SHORT" and research_eligible_YN == "Y")
# Family-level cross-day case history: NOT AVAILABLE in current data model.
#   → family_days_count = "not_available_yet"
#   → family_stability_flag = "NOT_AVAILABLE_YET"
# Global ledger context (distinct research_days in ledger) is available but
#   must be labeled clearly and must NOT be used as family support evidence.
#
# Threshold constants — single source of truth for all Phase 2B gate logic:

_P2B_BUCKET_READY_THRESHOLD       = 10   # n >= 10: bucket_ready
_P2B_TRACKING_GRADE_THRESHOLD     = 20   # n >= 20: tracking_grade
_P2B_RECOMMENDATION_GRADE_THRESHOLD = 50 # n >= 50: recommendation_grade


# ---------------------------------------------------------------------------
# Phase 2B — private statistical helpers
# ---------------------------------------------------------------------------

def _sample_gate_status_2b(n: int) -> str:
    """
    Conservative per-family sample gate.
    Uses _P2B_* threshold constants — must not be changed locally.
    """
    if n >= _P2B_RECOMMENDATION_GRADE_THRESHOLD:
        return "RECOMMENDATION_GRADE"
    if n >= _P2B_TRACKING_GRADE_THRESHOLD:
        return "TRACKING_GRADE"
    if n >= _P2B_BUCKET_READY_THRESHOLD:
        return "LOW_SAMPLE"
    return "NOT_ENOUGH_SAMPLE"


def _bucket_gate_status_2b(n: int) -> str:
    """Per-bucket readiness using _P2B_BUCKET_READY_THRESHOLD."""
    if n >= _P2B_RECOMMENDATION_GRADE_THRESHOLD:
        return "BUCKET_READY"
    if n >= _P2B_TRACKING_GRADE_THRESHOLD:
        return "BUCKET_READY"
    if n >= _P2B_BUCKET_READY_THRESHOLD:
        return "BUCKET_READY"
    if n >= 5:
        return "BUCKET_THIN"
    return "BUCKET_INSUFFICIENT"


def _mean_ci_2b(values: list):
    """
    95% CI for the mean using normal approximation.
    Returns (ci_low, ci_high) or (None, None) if n < 5.
    Uses statistics module only — no numpy dependency.
    """
    n = len(values)
    if n < 5:
        return None, None
    try:
        mu = sum(values) / n
        if n < 2:
            return round(mu, 3), round(mu, 3)
        sd = statistics.stdev(values)
        se = sd / (n ** 0.5)
        margin = 1.96 * se
        return round(mu - margin, 3), round(mu + margin, 3)
    except Exception:
        return None, None


def _regime_consistency_flag_2b(rows: list) -> str:
    """
    Regime consistency from research_regime across case rows.
    Returns CONSISTENT / MIXED / FRAGMENTED.
    """
    regimes = [r.get("research_regime", "") for r in rows if r.get("research_regime", "")]
    if not regimes:
        return "FRAGMENTED"
    dominant = max(set(regimes), key=regimes.count)
    ratio = regimes.count(dominant) / len(regimes)
    if ratio >= 0.60:
        return "CONSISTENT"
    if ratio >= 0.40:
        return "MIXED"
    return "FRAGMENTED"


def _confidence_band_2b(sample_gate: str, family_stability_flag: str, regime_flag: str) -> str:
    """
    Confidence band from combined gates.
    MODERATE is the cap — no HIGH at Phase 2B stage.
    family_stability_flag = NOT_AVAILABLE_YET → always LOW or DESCRIPTIVE_ONLY.
    Returns MODERATE / LOW / DESCRIPTIVE_ONLY.
    """
    if sample_gate == "NOT_ENOUGH_SAMPLE":
        return "DESCRIPTIVE_ONLY"
    if family_stability_flag == "NOT_AVAILABLE_YET":
        return "LOW"
    if (sample_gate in ("TRACKING_GRADE", "RECOMMENDATION_GRADE")
            and family_stability_flag in ("STABLE", "EARLY_SIGNAL")
            and regime_flag in ("CONSISTENT", "MIXED")):
        return "MODERATE"
    return "LOW"


def _safe_win_like_rate_proxy_2b(cases: list) -> Optional[float]:
    """
    Share of SHORT cases where future_1h_max_favor_pct > 0.
    Returns None if n < _P2B_BUCKET_READY_THRESHOLD (not enough for meaningful rate).
    Directional proxy only — not a true win-rate.
    """
    vals = []
    for c in cases:
        v = c.get("future_1h_max_favor_pct")
        if v not in (None, "", "None"):
            try:
                vals.append(float(v))
            except Exception:
                pass
    if len(vals) < _P2B_BUCKET_READY_THRESHOLD:
        return None
    return round(sum(1 for v in vals if v > 0) / len(vals), 3)


def _sample_note_2b(n: int) -> str:
    """Human-readable note using threshold constants."""
    if n < _P2B_BUCKET_READY_THRESHOLD:
        return "too_small_for_any_conclusion"
    if n < _P2B_TRACKING_GRADE_THRESHOLD:
        return "directional_usable_not_action_ready"
    if n < _P2B_RECOMMENDATION_GRADE_THRESHOLD:
        return "tracking_grade_not_yet_action_ready"
    return "recommendation_grade"


# ---------------------------------------------------------------------------
# Phase 2B — A. Multi-day family stats (SHORT-only)
# ---------------------------------------------------------------------------

def build_multiday_family_stats(
    cases: List[Dict],
    normalized_ledger_rows: Optional[List[Dict]] = None,
) -> List[Dict]:
    """
    Grouped stats per display family for SHORT-side intervention analysis.

    Scope: side == "SHORT" AND research_eligible_YN == "Y" only.
    Do NOT include LONG cases — this is short-intervention specific.

    family_days_count = "not_available_yet"
      True family-level cross-day case history is not available in the current
      data model. Do NOT derive this from signature_evidence_ledger.csv, which is
      canonical for signature_key tracking, not family-level case counts.

    family_stability_flag = "NOT_AVAILABLE_YET"
      Stability requires cross-day family evidence that does not yet exist.
      This flag must NOT be used to claim stability or instability — only that
      the evidence layer is absent.

    normalized_ledger_rows param is accepted but only used for global context
    computation by the caller — not consumed inside this function.
    """
    # SHORT-only eligible filter — critical for short intervention framing
    short_eligible = [
        c for c in cases
        if c.get("research_eligible_YN") == "Y" and c.get("side") == "SHORT"
    ]

    fam_groups: Dict[str, List[Dict]] = defaultdict(list)
    for c in short_eligible:
        fam = _display_family_p2(c)
        fam_groups[fam].append(c)

    results = []
    for display_family, group in sorted(fam_groups.items(), key=lambda x: -len(x[1])):
        n = len(group)

        # Outcome fields — safe float extraction
        f1h_vals = []
        f4h_vals = []
        a4h_vals = []
        for c in group:
            for field, target in [
                ("future_1h_max_favor_pct", f1h_vals),
                ("future_4h_max_favor_pct", f4h_vals),
                ("future_4h_max_adverse_pct", a4h_vals),
            ]:
                v = c.get(field)
                if v not in (None, "", "None"):
                    try:
                        target.append(float(v))
                    except Exception:
                        pass

        # Gates using shared threshold constants
        sample_gate = _sample_gate_status_2b(n)
        bucket_gate = _bucket_gate_status_2b(n)
        regime_flag = _regime_consistency_flag_2b(group)

        # family_stability_flag is NOT_AVAILABLE_YET — no cross-day family case history
        family_stability_flag = "NOT_AVAILABLE_YET"

        # CI — 95% normal approx, graceful at small n
        ci_lo, ci_hi = _mean_ci_2b(f1h_vals) if f1h_vals else (None, None)

        confidence_band = _confidence_band_2b(sample_gate, family_stability_flag, regime_flag)
        win_proxy = _safe_win_like_rate_proxy_2b(group)

        results.append({
            "display_family":          display_family,
            "case_count":              n,              # today's SHORT eligible cases in this family
            "case_count_scope":        "today_only",
            "family_days_count":       "not_available_yet",   # replaces old days_count
            "family_stability_flag":   family_stability_flag,
            "median_f1h":              _med_field_p2(group, "future_1h_max_favor_pct"),
            "median_f4h":              _med_field_p2(group, "future_4h_max_favor_pct"),
            "median_a4h":              _med_field_p2(group, "future_4h_max_adverse_pct"),
            "mean_f1h":                _mean_field_p2(group, "future_1h_max_favor_pct"),
            "mean_f4h":                _mean_field_p2(group, "future_4h_max_favor_pct"),
            "mean_a4h":                _mean_field_p2(group, "future_4h_max_adverse_pct"),
            "win_like_rate_proxy":     win_proxy,
            "sample_gate_status":      sample_gate,
            "bucket_gate_status":      bucket_gate,
            "regime_consistency_flag": regime_flag,
            "bootstrap_ci_low":        ci_lo if ci_lo is not None else "not_enough_sample",
            "bootstrap_ci_high":       ci_hi if ci_hi is not None else "not_enough_sample",
            "confidence_band":         confidence_band,
            "sample_note":             _sample_note_2b(n),
        })

    return results


# ---------------------------------------------------------------------------
# Phase 2B — B. Multi-day interaction stats (SHORT-only)
# ---------------------------------------------------------------------------

def build_multiday_interaction_stats(cases: List[Dict]) -> List[Dict]:
    """
    Four compact interaction pairs for multi-day validation context.

    Scope: side == "SHORT" AND research_eligible_YN == "Y" only.
    days_count = 1 per cell — honest: today's cases only.

    Pairs:
      1. candidate_short_family x research_regime
      2. candidate_short_family x structural_quality
      3. break_quality_band x participation_pattern
      4. candidate_short_family x caution_flag
    """
    short_eligible = [
        c for c in cases
        if c.get("research_eligible_YN") == "Y" and c.get("side") == "SHORT"
    ]
    if not short_eligible:
        return []

    PAIRS = [
        (
            "candidate_short_family x research_regime",
            lambda c: (_display_family_p2(c), c.get("research_regime", "—")),
        ),
        (
            "candidate_short_family x structural_quality",
            lambda c: (_display_family_p2(c), c.get("structural_quality", "—")),
        ),
        (
            "break_quality_band x participation_pattern",
            lambda c: (c.get("break_quality_band", "—"), c.get("participation_pattern", "—")),
        ),
        (
            "candidate_short_family x caution_flag",
            lambda c: (_display_family_p2(c), c.get("caution_flag", "—")),
        ),
    ]

    results = []
    for pair_name, key_fn in PAIRS:
        groups: Dict[tuple, List[Dict]] = defaultdict(list)
        for c in short_eligible:
            groups[key_fn(c)].append(c)

        combo_rows = []
        for combo_key, group in sorted(groups.items(), key=lambda x: -len(x[1]))[:5]:
            n = len(group)
            combo_rows.append({
                "interaction_pair":   pair_name,
                "combination_key":    " | ".join(str(k) for k in combo_key),
                "case_count":         n,
                "days_count":         1,
                "median_f1h":         _med_field_p2(group, "future_1h_max_favor_pct"),
                "median_f4h":         _med_field_p2(group, "future_4h_max_favor_pct"),
                "median_a4h":         _med_field_p2(group, "future_4h_max_adverse_pct"),
                "sample_gate_status": _sample_gate_status_2b(n),
                "stability_flag":     "UNSTABLE",       # days_count = 1 always unstable
                "confidence_band":    "DESCRIPTIVE_ONLY",
                "sample_note":        "sample_too_small" if n < 3 else _sample_note_2b(n),
            })

        results.append({
            "interaction_pair":  pair_name,
            "top_combinations":  combo_rows,
            "total_short_eligible": len(short_eligible),
            "analyst_note": (
                "All combinations too small — directional reading only."
                if all(r["case_count"] < 3 for r in combo_rows) else
                "Directional only at current sample. Not action-ready."
            ),
        })

    return results


# ---------------------------------------------------------------------------
# Phase 2B — C. Controlled validation summary
# ---------------------------------------------------------------------------

def build_controlled_validation_summary(
    multiday_family_stats: List[Dict],
    multiday_interaction_stats: List[Dict],
    ledger_days_context: int = 0,
) -> Dict:
    """
    Compact promotion-state summary for SHORT-side intervention.

    ledger_days_context:
      Number of distinct research_day values in the normalized ledger (global context only).
      Computed by the caller from normalized_ledger_rows.
      Must NOT be used as family support, family stability, or promotion evidence.
      Labeled as ledger_research_days_global_context in output.

    Promotion ladder (conservative):
      DESCRIPTIVE_ONLY   — no SHORT family at bucket-ready threshold (n < 10)
      KEEP_TRACKING      — at least one SHORT family at bucket-ready or above (n >= 10)
      PREPARE_HYPOTHESIS — at least one SHORT family at tracking-grade (n >= 20),
                           AND family_stability_flag != NOT_AVAILABLE_YET,
                           AND family_days_count != not_available_yet.
                           EXPLICITLY BLOCKED when family history unavailable.
      NOT_READY_FOR_CONTROLLED_VALIDATION — not reachable while family history absent

    PREPARE_HYPOTHESIS is NOT reachable while family_days_count = not_available_yet
    or family_stability_flag = NOT_AVAILABLE_YET.
    This check is explicit — it does NOT rely only on confidence scoring.
    """
    _empty = {
        "promotion_state":                        "DESCRIPTIVE_ONLY",
        "top_candidate_family":                   "—",
        "promotion_reason":                       "No SHORT family stats available.",
        "blocking_reason":                        "No eligible SHORT cases to analyze.",
        "validation_next_step":                   "Continue daily tracking.",
        "family_multiday_history":                "not_available_yet",
        "ledger_research_days_global_context":    ledger_days_context,
        "families_at_bucket_ready_or_above":      0,
        "families_at_tracking_grade_or_above":    0,
        "families_at_recommendation_grade_or_above": 0,
    }
    if not multiday_family_stats:
        return _empty

    # Top candidate = highest case_count among SHORT eligible
    sorted_fams = sorted(multiday_family_stats, key=lambda r: -r.get("case_count", 0))
    top = sorted_fams[0]
    top_family = top.get("display_family", "—")

    # Explicit counters using shared threshold constants
    families_at_bucket_ready_or_above = sum(
        1 for r in multiday_family_stats
        if r.get("case_count", 0) >= _P2B_BUCKET_READY_THRESHOLD
    )
    families_at_tracking_grade_or_above = sum(
        1 for r in multiday_family_stats
        if r.get("case_count", 0) >= _P2B_TRACKING_GRADE_THRESHOLD
    )
    families_at_recommendation_grade_or_above = sum(
        1 for r in multiday_family_stats
        if r.get("case_count", 0) >= _P2B_RECOMMENDATION_GRADE_THRESHOLD
    )

    # Check if family-level multiday history is available
    # If ANY family has not_available_yet, treat as globally unavailable
    family_history_unavailable = any(
        r.get("family_days_count") == "not_available_yet"
        or r.get("family_stability_flag") == "NOT_AVAILABLE_YET"
        for r in multiday_family_stats
    )

    # Candidates for each state
    prepare_candidates = [
        r for r in multiday_family_stats
        if r.get("case_count", 0) >= _P2B_TRACKING_GRADE_THRESHOLD
        and r.get("family_stability_flag") not in ("NOT_AVAILABLE_YET",)
        and r.get("family_days_count") != "not_available_yet"
        and r.get("regime_consistency_flag") in ("CONSISTENT", "MIXED")
    ]
    keep_tracking_candidates = [
        r for r in multiday_family_stats
        if r.get("case_count", 0) >= _P2B_BUCKET_READY_THRESHOLD
    ]

    # Determine promotion state
    # PREPARE_HYPOTHESIS: explicit block if family history unavailable
    if prepare_candidates and not family_history_unavailable:
        top_prepare = sorted(prepare_candidates, key=lambda r: -r.get("case_count", 0))[0]
        promotion_state = "PREPARE_HYPOTHESIS"
        promotion_reason = (
            f"{top_prepare.get('display_family','?')} has "
            f"{top_prepare.get('case_count', 0)} SHORT eligible cases today, "
            f"sample_gate={top_prepare.get('sample_gate_status','?')}, "
            f"regime={top_prepare.get('regime_consistency_flag','?')}."
        )
        blocking_reason = "—"
        next_step = (
            "Define one improvement rule. Run controlled validation on unseen days. "
            "Requires >= 5 stable cross-day days before promotion."
        )
    elif keep_tracking_candidates:
        top_kt = sorted(keep_tracking_candidates, key=lambda r: -r.get("case_count", 0))[0]
        promotion_state = "KEEP_TRACKING"
        promotion_reason = (
            f"{top_kt.get('display_family','?')} is the leading SHORT family with "
            f"{top_kt.get('case_count', 0)} eligible case(s) today."
        )
        if family_history_unavailable:
            blocking_reason = (
                f"Family multiday history is not_available_yet — "
                f"PREPARE_HYPOTHESIS is blocked until cross-day family case evidence exists. "
                f"Top family '{top_family}' has {top.get('case_count',0)} case(s) today "
                f"(need >= {_P2B_TRACKING_GRADE_THRESHOLD} for TRACKING_GRADE)."
            )
        else:
            blocking_reason = (
                f"Top family '{top_family}' has {top.get('case_count',0)} SHORT case(s) today. "
                f"Need >= {_P2B_TRACKING_GRADE_THRESHOLD} for TRACKING_GRADE, "
                f">= {_P2B_RECOMMENDATION_GRADE_THRESHOLD} for RECOMMENDATION_GRADE."
            )
        next_step = (
            f"Continue daily tracking for SHORT cases. "
            f"Target: >= {_P2B_TRACKING_GRADE_THRESHOLD} SHORT eligible cases "
            f"in top family per day before hypothesis stage."
        )
    else:
        promotion_state = "DESCRIPTIVE_ONLY"
        promotion_reason = (
            f"No SHORT family has reached bucket-ready threshold "
            f"(>= {_P2B_BUCKET_READY_THRESHOLD} cases)."
        )
        blocking_reason = (
            f"Top SHORT family '{top_family}' has {top.get('case_count',0)} case(s) today. "
            f"Need >= {_P2B_BUCKET_READY_THRESHOLD} for bucket-ready. "
            f"Need >= {_P2B_TRACKING_GRADE_THRESHOLD} for TRACKING_GRADE. "
            f"Need >= {_P2B_RECOMMENDATION_GRADE_THRESHOLD} for RECOMMENDATION_GRADE."
        )
        next_step = "Continue daily tracking. No action until evidence builds."

    return {
        "promotion_state":                        promotion_state,
        "top_candidate_family":                   top_family,
        "promotion_reason":                       promotion_reason,
        "blocking_reason":                        blocking_reason,
        "validation_next_step":                   next_step,
        "family_multiday_history":                "not_available_yet",
        "ledger_research_days_global_context":    ledger_days_context,
        "families_at_bucket_ready_or_above":      families_at_bucket_ready_or_above,
        "families_at_tracking_grade_or_above":    families_at_tracking_grade_or_above,
        "families_at_recommendation_grade_or_above": families_at_recommendation_grade_or_above,
    }


# ===========================================================================
# Phase 2C — Family History Foundation + Layer Field Coverage Audit
# ===========================================================================

# ---------------------------------------------------------------------------
# Layer 0–8 required field contract (aligned to master spec)
# ---------------------------------------------------------------------------
# Field names use actual case_builder names where they exist.
# Spec-aspirational names are used for fields not yet in case schema.
# The audit will classify them as MISSING — this is expected Phase 2C output.

_LAYER_CONTRACT: Dict[int, Dict] = {
    0: {
        "name": "Research Frame / Unit of Analysis",
        "required_fields": [
            # Present in case_builder:
            "case_id", "research_day", "symbol", "side",
            "top_mover_rank", "anchor_quality_flag", "anchor_conflict_flag",
            # Not yet in case schema (spec aspirational):
            "case_inclusion_reason", "semantic_clean_flag",
            "exclusion_reason", "dataset_batch",
        ],
    },
    1: {
        "name": "Universe / Selection Layer",
        "required_fields": [
            # Present:
            "daily_return_pct",
            # Not yet in case schema:
            "day_range_pct", "intraday_expansion_pct",
            "rank_volume_24h", "notional_volume_usd", "rank_abs_change_24h",
        ],
    },
    2: {
        "name": "Canonical Move / Anchor Detection Layer",
        "required_fields": [
            # Present:
            "p0_ts_ms", "p1_ts_ms", "p2_ts_ms", "p3_ts_ms", "p4_ts_ms",
            "p0_price", "p2_price", "p4_price",
            "anchor_quality_flag", "anchor_conflict_flag",
            # Not yet in case schema:
            "p1_price", "p3_price",
            "bars_p0_to_p1", "bars_p1_to_p2", "bars_p2_to_p3", "bars_p3_to_p4",
            "anchor_reason_code",
        ],
    },
    3: {
        "name": "Exhaustion Measurement Layer",
        "required_fields": [
            # All not yet in case schema:
            "pre_break_extension_pct_from_local_base",
            "pre_break_extension_pct_from_vwap",
            "pre_break_extension_pct_from_ema",
            "peak_upper_wick_ratio",
            "wick_cluster_count_last_n_bars",
            "blowoff_volume_ratio",
            "close_location_near_low_score",
            "failed_continuation_count",
            "exhaustion_strength_raw",
            "exhaustion_strength_bucket",
        ],
    },
    4: {
        "name": "Breakdown Quality Layer",
        "required_fields": [
            # Present (partial):
            "break_quality_score", "break_quality_band",
            # Not yet in case schema:
            "break_distance_pct", "break_close_strength",
            "break_bar_body_ratio", "break_volume_ratio", "support_test_count",
        ],
    },
    5: {
        "name": "Retest Fail / Reclaim Layer",
        "required_fields": [
            # Present (proxy only):
            "reclaim_break_4h_YN",
            # Not yet in case schema:
            "reclaim_pct", "retest_depth_vs_break_pct",
            "retest_duration_bars", "retest_volume_decay_ratio",
            "retest_rejection_quality",
        ],
    },
    6: {
        "name": "Timing / Staleness Layer",
        "required_fields": [
            # Present (partial):
            "time_to_2pct_favor_min", "time_to_3pct_favor_min",
            # Not yet in case schema:
            "bars_to_retest", "bars_to_fail", "bars_fail_to_acceleration",
            "late_retest_flag", "stale_setup_flag", "time_of_day_bucket",
        ],
    },
    7: {
        "name": "Context / Regime / Crowding Layer",
        "required_fields": [
            # Present:
            "research_regime", "btc_24h_change_pct", "alt_breadth_pct",
            "taker_imbalance_at_p2",
            # Not yet in case schema:
            "btc_change_15m_pct", "btc_change_1h_pct", "market_volatility_proxy",
        ],
    },
    8: {
        "name": "Outcome / Label Layer",
        "required_fields": [
            # All present:
            "future_1h_max_favor_pct", "future_1h_max_adverse_pct",
            "future_4h_max_favor_pct", "future_4h_max_adverse_pct",
            "resolution_label",
        ],
    },
}


# ---------------------------------------------------------------------------
# Phase 2C — A. Layer field coverage audit
# ---------------------------------------------------------------------------

def build_layer_field_coverage_audit(cases: List[Dict]) -> Dict:
    """
    Audit mandatory field coverage against the Layer 0–8 master spec contract.

    Coverage status per layer:
      OK       — all required fields present and non-null in at least one case
      PARTIAL  — all required fields present in schema but >= 1 always null/empty
      BLOCKING — >= 1 required field missing entirely from the case row schema

    Expected Phase 2C result: Layers 3–6 will show BLOCKING.
    This is correct, not a bug — it surfaces the field gap for Phase 2D planning.
    """
    if not cases:
        total_required = sum(len(v["required_fields"]) for v in _LAYER_CONTRACT.values())
        return {
            "layers_checked": len(_LAYER_CONTRACT),
            "total_required_fields": total_required,
            "fields_present_count": 0,
            "fields_missing_count": total_required,
            "fields_all_null_count": 0,
            "blocking_layers": list(_LAYER_CONTRACT.keys()),
            "blocking_layers_count": len(_LAYER_CONTRACT),
            "layer_rows": [],
        }

    # All keys present in the case schema (from first row — CSV DictReader is consistent)
    case_keys = set(cases[0].keys())

    def _is_all_null(field: str) -> bool:
        """True if field is in schema but always empty/null across all cases."""
        return all(
            c.get(field) in (None, "", "None", "N/A", "nan")
            for c in cases
        )

    layer_rows = []
    total_present = 0
    total_missing = 0
    total_all_null = 0
    blocking_layers = []

    for layer_id in sorted(_LAYER_CONTRACT.keys()):
        layer_def = _LAYER_CONTRACT[layer_id]
        required = layer_def["required_fields"]

        present  = [f for f in required if f in case_keys]
        missing  = [f for f in required if f not in case_keys]
        all_null = [f for f in present if _is_all_null(f)]

        n_req     = len(required)
        n_present = len(present)
        n_missing = len(missing)
        n_null    = len(all_null)

        if n_missing > 0:
            coverage_status = "BLOCKING"
            blocking_layers.append(layer_id)
        elif n_null > 0:
            coverage_status = "PARTIAL"
        else:
            coverage_status = "OK"

        total_present  += n_present
        total_missing  += n_missing
        total_all_null += n_null

        layer_rows.append({
            "layer_id":               layer_id,
            "layer_name":             layer_def["name"],
            "required_fields_count":  n_req,
            "present_fields_count":   n_present,
            "missing_fields_count":   n_missing,
            "all_null_fields_count":  n_null,
            "coverage_status":        coverage_status,
            "missing_fields":         ", ".join(missing) if missing else "—",
            "all_null_fields":        ", ".join(all_null) if all_null else "—",
        })

    total_required = sum(len(v["required_fields"]) for v in _LAYER_CONTRACT.values())

    return {
        "layers_checked":         len(_LAYER_CONTRACT),
        "total_required_fields":  total_required,
        "fields_present_count":   total_present,
        "fields_missing_count":   total_missing,
        "fields_all_null_count":  total_all_null,
        "blocking_layers":        blocking_layers,
        "blocking_layers_count":  len(blocking_layers),
        "layer_rows":             layer_rows,
    }


# ---------------------------------------------------------------------------
# Phase 2C — B. Load historical case rows from daily case datasets
# ---------------------------------------------------------------------------

def load_historical_case_rows(
    report_day: str,
    window_days: int = 30,
    output_root: Optional[str] = None,
) -> List[Dict]:
    """
    Load historical daily_case_dataset_{day}.csv files for days <= report_day.

    Rules:
    - Only loads days where research_day <= report_day (as-of-day safe)
    - Tolerates missing days silently (not every day may have been run)
    - Does NOT use signature_evidence_ledger.csv — family history comes from cases only
    - output_root defaults to io.OUTPUT_BASE if not provided

    Returns flat list of all case rows across all loaded days.
    """
    if output_root is None:
        try:
            from research.top_movers.io import OUTPUT_BASE
            output_root = OUTPUT_BASE
        except ImportError:
            output_root = "data/research_output/top_movers"

    from datetime import datetime as _dt, timedelta as _td
    try:
        end_date = _dt.strptime(report_day, "%Y-%m-%d")
    except ValueError:
        return []

    all_rows: List[Dict] = []

    for i in range(window_days):
        day = (end_date - _td(days=i)).strftime("%Y-%m-%d")
        path = os.path.join(output_root, day, "csv", f"daily_case_dataset_{day}.csv")
        if not os.path.exists(path):
            continue
        try:
            with open(path, "r", newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                rows = [dict(r) for r in reader]
                all_rows.extend(rows)
        except Exception:
            continue  # tolerate malformed files silently

    return all_rows


# ---------------------------------------------------------------------------
# Phase 2C — C. Build SHORT family cross-day case history
# ---------------------------------------------------------------------------

def build_short_family_case_history(
    historical_case_rows: List[Dict],
    report_day: str,
) -> List[Dict]:
    """
    Build per-family cross-day case history from historical daily case datasets.

    Filter: side == SHORT AND research_eligible_YN == Y
    Family grouping: same _display_family_p2 logic as Phase 2A/2B.
    family_days_count = distinct research_day values in case rows — NEVER from ledger.

    family_history_availability_status:
      not_available_yet  — 0 days of history
      early_tracking     — 1–2 days of history
      available          — 3+ days of history (Phase 2C foundation exists)
      Note: 'available' does NOT unlock PREPARE_HYPOTHESIS in Phase 2C.
    """
    short_eligible = [
        r for r in historical_case_rows
        if r.get("side") == "SHORT" and r.get("research_eligible_YN") == "Y"
    ]

    fam_days:  Dict[str, set]        = defaultdict(set)
    fam_cases: Dict[str, List[Dict]] = defaultdict(list)

    for c in short_eligible:
        fam = _display_family_p2(c)
        day = c.get("research_day", "")
        if day:
            fam_days[fam].add(day)
        fam_cases[fam].append(c)

    results = []
    for fam in sorted(fam_cases.keys(), key=lambda f: -len(fam_cases[f])):
        cases_list = fam_cases[fam]
        days       = fam_days[fam]
        n_days     = len(days)

        today_cases = [c for c in cases_list if c.get("research_day") == report_day]

        # Regime mix
        regimes = [c.get("research_regime", "") for c in cases_list if c.get("research_regime")]
        regime_counts: Dict[str, int] = {}
        for r in regimes:
            regime_counts[r] = regime_counts.get(r, 0) + 1
        regime_mix_summary = (
            ", ".join(f"{k}:{v}" for k, v in sorted(regime_counts.items(), key=lambda x: -x[1])[:3])
            or "—"
        )

        # Resolution mix
        resolutions = [c.get("resolution_label", "") for c in cases_list if c.get("resolution_label")]
        res_counts: Dict[str, int] = {}
        for r in resolutions:
            res_counts[r] = res_counts.get(r, 0) + 1
        resolution_mix_summary = (
            ", ".join(f"{k}:{v}" for k, v in sorted(res_counts.items(), key=lambda x: -x[1])[:3])
            or "—"
        )

        # Caution mix
        caution_y = sum(1 for c in cases_list if c.get("caution_flag") == "Y")
        caution_mix_summary = f"Y:{caution_y}, N:{len(cases_list) - caution_y}"

        if n_days == 0:
            availability = "not_available_yet"
        elif n_days < 3:
            availability = "early_tracking"
        else:
            availability = "available"

        results.append({
            "display_family":                  fam,
            "first_case_day":                  min(days) if days else "—",
            "last_case_day":                   max(days) if days else "—",
            "family_days_count":               n_days,
            "family_case_count_multiday":      len(cases_list),
            "family_case_count_today":         len(today_cases),
            "family_history_availability_status": availability,
            "regime_mix_summary":              regime_mix_summary,
            "resolution_mix_summary":          resolution_mix_summary,
            "caution_mix_summary":             caution_mix_summary,
        })

    return results


# ---------------------------------------------------------------------------
# Phase 2C — D. Compact family history snapshot (for render layers)
# ---------------------------------------------------------------------------

def build_family_history_snapshot(
    cases: List[Dict],
    report_day: str,
    family_case_history: Optional[List[Dict]] = None,
    historical_case_rows: Optional[List[Dict]] = None,
    window_days: int = 30,
) -> Dict:
    """
    Compact summary for render layers (DOCX, unified, markdown).

    Uses precomputed family_case_history if provided (preferred — avoids recomputation).
    Falls back to historical_case_rows if family_case_history is not provided.

    history_window_days: the window used when loading historical data.
    historical_case_days_loaded: distinct research_days actually found in history.

    IMPORTANT:
    family_history_status = 'available' means the Phase 2C foundation exists.
    It does NOT unlock PREPARE_HYPOTHESIS — that belongs to Phase 2D.
    """
    # Resolve family_case_history
    if family_case_history is None:
        if historical_case_rows is not None:
            family_case_history = build_short_family_case_history(historical_case_rows, report_day)
        else:
            family_case_history = []

    # Compute historical_case_days_loaded from whatever source we have
    if historical_case_rows is not None:
        hist_days_loaded = len(set(
            r.get("research_day", "") for r in historical_case_rows
            if r.get("research_day")
        ))
    elif family_case_history:
        # Approximate from first/last day range — not perfect but honest
        all_first = [f.get("first_case_day", "") for f in family_case_history if f.get("first_case_day") not in ("", "—")]
        all_last  = [f.get("last_case_day",  "") for f in family_case_history if f.get("last_case_day")  not in ("", "—")]
        if all_first and all_last:
            from datetime import datetime as _dt2
            try:
                first = _dt2.strptime(min(all_first), "%Y-%m-%d")
                last  = _dt2.strptime(max(all_last),  "%Y-%m-%d")
                hist_days_loaded = (last - first).days + 1
            except Exception:
                hist_days_loaded = 0
        else:
            hist_days_loaded = 0
    else:
        hist_days_loaded = 0

    _empty = {
        "top_candidate_family":          "—",
        "top_family_case_count_today":   0,
        "top_family_case_count_multiday": 0,
        "top_family_days_count":         0,
        "family_history_status":         "not_available_yet",
        "families_with_history_count":   0,
        "history_window_days":           window_days,
        "historical_case_days_loaded":   hist_days_loaded,
        "history_note": (
            "No SHORT eligible family history available yet. "
            "Run historical days to build foundation."
        ),
    }

    if not family_case_history:
        return _empty

    # Top family = highest family_case_count_multiday among NAMED families.
    # "unclassified" is valid data but not an intervention target — exclude from top selection.
    # If all families are unclassified, fall back to the most common one with a note.
    named_families = [f for f in family_case_history if f.get("display_family") != "unclassified"]
    if named_families:
        top = sorted(named_families, key=lambda r: -(r.get("family_case_count_multiday") or 0))[0]
    else:
        top = sorted(family_case_history, key=lambda r: -(r.get("family_case_count_multiday") or 0))[0]
    top_days = top.get("family_days_count", 0)

    if top_days >= 3:
        history_status = "available"
    elif top_days >= 1:
        history_status = "early_tracking"
    else:
        history_status = "not_available_yet"

    families_with_history = sum(1 for f in family_case_history if f.get("family_days_count", 0) >= 1)

    history_note = (
        f"Phase 2C foundation: '{top.get('display_family','?')}' has "
        f"{top.get('family_case_count_multiday', 0)} SHORT eligible cases "
        f"across {top_days} day(s). "
        f"history_status={history_status}. "
        f"PREPARE_HYPOTHESIS remains blocked — Phase 2E gates required "
        f"(anchor QA, unseen-day validation, sample thresholds)."
    )

    return {
        "top_candidate_family":           top.get("display_family", "—"),
        "top_family_case_count_today":    top.get("family_case_count_today", 0),
        "top_family_case_count_multiday": top.get("family_case_count_multiday", 0),
        "top_family_days_count":          top_days,
        "family_history_status":          history_status,
        "families_with_history_count":    families_with_history,
        "history_window_days":            window_days,
        "historical_case_days_loaded":    hist_days_loaded,
        "history_note":                   history_note,
    }


# ===========================================================================
# Phase 2D — Statistical Validation + Family Answer Contracts
# ===========================================================================


# ---------------------------------------------------------------------------
# Phase 2D — Private statistical helpers
# ---------------------------------------------------------------------------

def _bootstrap_ci_p2d(
    values: list,
    n_iter: int = 500,
    ci: float = 0.95,
) -> tuple:
    """
    Bootstrap percentile CI for the mean.  Uses stdlib random only — no scipy needed.

    Conservative: requires n >= 3; returns (None, None) otherwise.
    Fixed seed 42 for reproducibility.
    """
    if len(values) < 3:
        return None, None
    import random
    rng = random.Random(42)
    n = len(values)
    boot_means = []
    for _ in range(n_iter):
        sample = [values[rng.randint(0, n - 1)] for _ in range(n)]
        boot_means.append(sum(sample) / n)
    boot_means.sort()
    lo_idx = int((1.0 - ci) / 2.0 * n_iter)
    hi_idx = int((1.0 + ci) / 2.0 * n_iter)
    return round(boot_means[lo_idx], 3), round(boot_means[min(hi_idx, n_iter - 1)], 3)


def _ks_test_p2d(values: list) -> str:
    """
    One-sample KS test (compare sample distribution vs fitted normal).
    Returns a compact status string.

    Fallback rules:
      na_low_sample    — n < 10
      na_no_scipy      — scipy not installed
      na_zero_variance — all values identical
      na_error         — unexpected exception
    """
    if len(values) < 10:
        return "na_low_sample"
    try:
        from scipy import stats as _sps  # optional dependency
        mu = statistics.mean(values)
        sigma = statistics.stdev(values) if len(values) >= 2 else 0
        if sigma == 0:
            return "na_zero_variance"
        _, pval = _sps.kstest(values, "norm", args=(mu, sigma))
        return f"ks_pval_{round(pval, 3)}_n{len(values)}"
    except ImportError:
        return "na_no_scipy"
    except Exception:
        return "na_error"


def _ttest_p2d(values: list, popmean: float = 0.0) -> str:
    """
    One-sample t-test (testing if mean differs from popmean, default 0).
    Returns a compact status string.

    Fallback rules:
      na_low_sample — n < 10
      na_no_scipy   — scipy not installed
      na_error      — unexpected exception
    """
    if len(values) < 10:
        return "na_low_sample"
    try:
        from scipy import stats as _sps  # optional dependency
        stat, pval = _sps.ttest_1samp(values, popmean)
        direction = "pos" if stat > 0 else "neg"
        return f"ttest_{direction}_pval_{round(pval, 3)}_n{len(values)}"
    except ImportError:
        return "na_no_scipy"
    except Exception:
        return "na_error"


# ---------------------------------------------------------------------------
# Phase 2D — A. Family validation stats (statistical layer)
# ---------------------------------------------------------------------------

def build_family_validation_stats(
    case_rows: List[Dict],
    family_history_rows: List[Dict],
) -> List[Dict]:
    """
    Phase 2D producer: statistical validation stats per SHORT display_family.

    Parameters
    ----------
    case_rows:
        Historical individual case rows from load_historical_case_rows().
        Used to compute outcome stats (mean/median/CI) and regime consistency.
        Filtered to: side == SHORT AND research_eligible_YN == Y.

    family_history_rows:
        Per-family summary list from build_short_family_case_history().
        Provides case_count_multiday, family_days_count, and defines iteration order.
        Family history NEVER comes from signature_evidence_ledger.

    Sample grade thresholds (same constants as Phase 2B):
        bucket-ready   : case_count_multiday >= 10
        tracking-grade : case_count_multiday >= 20
        recommendation : case_count_multiday >= 50

    Low sample -> explicit conservative outputs (not_enough_sample / na_low_sample).
    Do NOT invent statistical precision.

    Returns one dict per display_family from family_history_rows.
    """
    if not family_history_rows:
        return []

    # Build lookup: display_family -> [case rows] from historical pool
    short_eligible = [
        c for c in case_rows
        if c.get("side") == "SHORT" and c.get("research_eligible_YN") == "Y"
    ]
    family_cases: Dict[str, List[Dict]] = defaultdict(list)
    for c in short_eligible:
        fam = _display_family_p2(c)
        family_cases[fam].append(c)

    results = []
    for fam_row in family_history_rows:
        display_family      = fam_row.get("display_family", "—")
        case_count_multiday = fam_row.get("family_case_count_multiday", 0)
        case_count_today    = fam_row.get("family_case_count_today", 0)
        family_days_count   = fam_row.get("family_days_count", 0)

        group = family_cases.get(display_family, [])

        # Safe float extraction for outcome fields
        def _safe_floats(field: str) -> list:
            vals = []
            for c in group:
                v = c.get(field)
                if v not in (None, "", "None"):
                    try:
                        vals.append(float(v))
                    except Exception:
                        pass
            return vals

        f1h_vals = _safe_floats("future_1h_max_favor_pct")
        f4h_vals = _safe_floats("future_4h_max_favor_pct")
        a4h_vals = _safe_floats("future_4h_max_adverse_pct")

        def _smean(vals: list):
            return round(statistics.mean(vals), 3) if vals else "not_enough_sample"

        def _smedian(vals: list):
            return round(statistics.median(vals), 3) if vals else "not_enough_sample"

        # Sample grade gates — based on multiday count from family_history_rows
        n = case_count_multiday
        if n >= _P2B_RECOMMENDATION_GRADE_THRESHOLD:
            bucket_ready_status         = "bucket_ready"
            tracking_grade_status       = "tracking_grade"
            recommendation_grade_status = "recommendation_grade"
        elif n >= _P2B_TRACKING_GRADE_THRESHOLD:
            bucket_ready_status         = "bucket_ready"
            tracking_grade_status       = "tracking_grade"
            recommendation_grade_status = "not_enough_sample"
        elif n >= _P2B_BUCKET_READY_THRESHOLD:
            bucket_ready_status         = "bucket_ready"
            tracking_grade_status       = "not_enough_sample"
            recommendation_grade_status = "not_enough_sample"
        else:
            bucket_ready_status         = "not_enough_sample"
            tracking_grade_status       = "not_enough_sample"
            recommendation_grade_status = "not_enough_sample"

        # Stability derived from family_days_count (cross-day breadth)
        if family_days_count == 0:
            stability_flag = "INSUFFICIENT"
        elif family_days_count < 3:
            stability_flag = "UNSTABLE"
        elif family_days_count < 6:
            stability_flag = "EARLY_SIGNAL"
        else:
            stability_flag = "STABLE"

        # Regime consistency — reuse Phase 2B helper on multiday case group
        regime_consistency_flag = (
            _regime_consistency_flag_2b(group) if group else "UNKNOWN"
        )

        # Bootstrap CI on f1h (primary outcome field)
        ci_low, ci_high = _bootstrap_ci_p2d(f1h_vals)

        # Statistical tests on f1h (one-sample: is mean > 0?)
        ks_status    = _ks_test_p2d(f1h_vals)
        ttest_status = _ttest_p2d(f1h_vals, popmean=0.0)

        # Semantic role: unclassified = generic population bucket, not an intervention target
        is_actionable_family = (display_family != "unclassified")
        family_bucket_role   = (
            "actionable_family" if is_actionable_family else "generic_population_bucket"
        )

        # Conservative statistical readiness note — wording differs by role
        if not is_actionable_family:
            stat_note = (
                f"Generic population bucket ({n} multiday cases, "
                f"{family_days_count} day(s)). "
                f"Useful as population context only. "
                f"Not an actionable rule-family target — "
                f"do not read sample grade as intervention readiness."
            )
        elif n >= _P2B_RECOMMENDATION_GRADE_THRESHOLD:
            stat_note = (
                f"Recommendation-grade sample ({n} multiday cases, "
                f"{family_days_count} day(s)). "
                f"Statistical validation feasible. CI and tests meaningful."
            )
        elif n >= _P2B_TRACKING_GRADE_THRESHOLD:
            stat_note = (
                f"Tracking-grade sample ({n} multiday cases, "
                f"{family_days_count} day(s)). "
                f"Pattern observable. CI and tests meaningful but wide."
            )
        elif n >= _P2B_BUCKET_READY_THRESHOLD:
            stat_note = (
                f"Bucket-ready sample ({n} multiday cases, "
                f"{family_days_count} day(s)). "
                f"Direction observable — not statistically defensible."
            )
        else:
            stat_note = (
                f"Low sample ({n} multiday cases — "
                f"need >= {_P2B_BUCKET_READY_THRESHOLD} for bucket-ready). "
                f"Continue tracking. No statistical conclusion possible."
            )

        results.append({
            "display_family":              display_family,
            "is_actionable_family":        is_actionable_family,
            "family_bucket_role":          family_bucket_role,
            "case_count_today":            case_count_today,
            "case_count_multiday":         case_count_multiday,
            "family_days_count":           family_days_count,
            "mean_f1h":                    _smean(f1h_vals),
            "mean_f4h":                    _smean(f4h_vals),
            "mean_a4h":                    _smean(a4h_vals),
            "median_f1h":                  _smedian(f1h_vals),
            "median_f4h":                  _smedian(f4h_vals),
            "median_a4h":                  _smedian(a4h_vals),
            "bucket_ready_status":         bucket_ready_status,
            "tracking_grade_status":       tracking_grade_status,
            "recommendation_grade_status": recommendation_grade_status,
            "stability_flag":              stability_flag,
            "regime_consistency_flag":     regime_consistency_flag,
            "bootstrap_ci_low":            ci_low if ci_low is not None else "na_low_sample",
            "bootstrap_ci_high":           ci_high if ci_high is not None else "na_low_sample",
            "ks_test_status_or_na":        ks_status,
            "ttest_status_or_na":          ttest_status,
            "statistical_readiness_note":  stat_note,
        })

    return results


# ---------------------------------------------------------------------------
# Phase 2D — B. Answer contract static definitions
# ---------------------------------------------------------------------------

_P2D_ANSWER_CONTRACT_DEFS: Dict[str, Dict] = {
    "Exhaustion": {
        "strategy_question": (
            "Does pre-break exhaustion level (extension from base/VWAP, wick clustering, "
            "blowoff volume, failed continuation count) predict short resolution quality?"
        ),
        "threshold_direction":  "higher_exhaustion_strength_expected_better_for_short",
        "layer_id":             3,
        "required_fields_spec": [
            "pre_break_extension_pct_from_local_base", "pre_break_extension_pct_from_vwap",
            "peak_upper_wick_ratio", "wick_cluster_count_last_n_bars",
            "blowoff_volume_ratio", "exhaustion_strength_bucket",
        ],
        "likely_side_effect": (
            "May over-filter clean breakdowns lacking visible prior extension. "
            "Regime-dependent: exhaustion patterns differ in trending vs ranging markets."
        ),
        "validation_next_step": (
            "Source owner: canonical_move.py (pre-break price extension from P0 base) "
            "and anchor_detector.py (wick clustering, exhaustion_strength_bucket at P1). "
            "Propagate via case_builder.py after source fields are computed."
        ),
    },
    "Breakdown": {
        "strategy_question": (
            "Does break quality (distance from support, bar body strength, "
            "volume confirmation, support test count) predict successful short resolution?"
        ),
        "threshold_direction":  "higher_break_quality_score_band_expected_better",
        "layer_id":             4,
        "required_fields_spec": [
            "break_quality_score", "break_quality_band",
            "break_distance_pct", "break_close_strength",
            "break_bar_body_ratio", "break_volume_ratio", "support_test_count",
        ],
        "likely_side_effect": (
            "Requiring high break quality may miss early-stage moves. "
            "break_quality_score and break_quality_band are partial proxies — "
            "insufficient alone for threshold-setting."
        ),
        "validation_next_step": (
            "Source owner: anchor_detector.py for break_distance_pct, break_close_strength, "
            "break_bar_body_ratio (extend existing bq_components at P2). "
            "Source owner: proxy_features.py for break_volume_ratio. "
            "Source owner: canonical_move.py for support_test_count. "
            "Propagate all via case_builder.py."
        ),
    },
    "Retest fail": {
        "strategy_question": (
            "Does retest behavior (depth relative to break, duration, volume decay, "
            "rejection quality) reliably predict reclaim failure and sustained downside?"
        ),
        "threshold_direction":  "deeper_clean_retest_rejection_expected_better_for_short",
        "layer_id":             5,
        "required_fields_spec": [
            "reclaim_break_4h_YN",
            "reclaim_pct", "retest_depth_vs_break_pct",
            "retest_duration_bars", "retest_volume_decay_ratio", "retest_rejection_quality",
        ],
        "likely_side_effect": (
            "Strict retest fail filters may significantly reduce eligible case volume. "
            "reclaim_break_4h_YN is a coarse 4h proxy — not granular enough for gating."
        ),
        "validation_next_step": (
            "Source owner: anchor_detector.py for retest_depth_vs_break_pct, "
            "retest_duration_bars, retest_rejection_quality (P3→P4 analysis). "
            "Source owner: proxy_features.py for retest_volume_decay_ratio. "
            "Propagate via case_builder.py."
        ),
    },
    "Timing": {
        "strategy_question": (
            "Does setup timing (time to initial move, bars to retest, "
            "stale setup flag, time-of-day bucket) affect short resolution quality?"
        ),
        "threshold_direction":  "faster_initial_move_lower_staleness_flag_expected_better",
        "layer_id":             6,
        "required_fields_spec": [
            "time_to_2pct_favor_min", "time_to_3pct_favor_min",
            "bars_to_retest", "bars_to_fail",
            "bars_fail_to_acceleration", "late_retest_flag", "stale_setup_flag",
        ],
        "likely_side_effect": (
            "Timing filters may reduce eligible signal window in fast-moving markets. "
            "time_to_2pct_favor_min and time_to_3pct_favor_min exist but are "
            "insufficient alone for staleness gating."
        ),
        "validation_next_step": (
            "Source owner: anchor_detector.py for bars_to_retest, bars_to_fail, "
            "bars_fail_to_acceleration (derived from P0–P4 bar indices). "
            "Source owner: decision_mapping.py for late_retest_flag, stale_setup_flag. "
            "Source owner: canonical_move.py for time_of_day_bucket (from P0 timestamp). "
            "Propagate via case_builder.py."
        ),
    },
    "Invalidation": {
        "strategy_question": (
            "What conditions (BTC reversal, reclaim of break level, "
            "crowd participation reversal, regime shift) should invalidate "
            "the short setup before close?"
        ),
        "threshold_direction":  "not_applicable_define_invalidation_criteria_first",
        "layer_id":             None,
        "required_fields_spec": [
            "reclaim_break_4h_YN", "research_regime", "taker_imbalance_at_p2",
            "retest_rejection_quality", "btc_change_15m_pct",
        ],
        "likely_side_effect": (
            "Strict invalidation rules reduce false-hold risk but may exit winners early. "
            "Cannot define specific thresholds without Layer 5 retest quality fields."
        ),
        "validation_next_step": (
            "Source owner: anchor_detector.py for retest_rejection_quality (Layer 5 first). "
            "Source owner: research_binance_client.py (API fetch) + proxy_features.py "
            "(compute) for btc_change_15m_pct. "
            "Define invalidation thresholds only after Layer 5 retest fields are complete. "
            "Propagate via case_builder.py."
        ),
    },
    "Context": {
        "strategy_question": (
            "Does research regime, BTC trend direction, or alt breadth context "
            "predict short setup resolution quality?"
        ),
        "threshold_direction":  "bear_or_mixed_regime_expected_higher_resolution_rate_for_short",
        "layer_id":             7,
        "required_fields_spec": [
            "research_regime", "btc_24h_change_pct", "alt_breadth_pct", "taker_imbalance_at_p2",
            "btc_change_15m_pct", "btc_change_1h_pct", "market_volatility_proxy",
        ],
        "likely_side_effect": (
            "Context filters reduce signal on neutral-regime days. "
            "btc_change_15m_pct, btc_change_1h_pct, market_volatility_proxy "
            "are not yet in case schema."
        ),
        "validation_next_step": (
            "Source owner: research_binance_client.py (API fetch) for btc_change_15m_pct, "
            "btc_change_1h_pct, market_volatility_proxy. "
            "Compute and normalize in proxy_features.py. "
            "Propagate via case_builder.py. "
            "Then rerun regime x resolution cross-tabulation."
        ),
    },
}


# ---------------------------------------------------------------------------
# Phase 2D — C. Family answer contracts
# ---------------------------------------------------------------------------

def build_family_answer_contracts(
    case_rows: List[Dict],
    family_history_rows: List[Dict],
    family_validation_stats: List[Dict],
) -> List[Dict]:
    """
    Phase 2D — one answer contract row per question family (6 total).

    Question families are defined in _P2D_ANSWER_CONTRACT_DEFS:
      Exhaustion, Breakdown, Retest fail, Timing, Invalidation, Context

    Rules:
    - Does NOT map display_family -> question_family via string heuristics.
    - Builds each contract from field-pack evidence + layer coverage status.
    - Returns explicit blocking_reason when required fields are missing at source.
    - recommendation_state stays conservative — no live-rule instructions.

    Allowed recommendation_state values:
      blocked          — required fields missing at source
      descriptive_only — partial fields, unreliable directional reading
      keep_tracking    — directional evidence exists, not action-ready
      no_change        — evidence does not support any intervention

    Parameters
    ----------
    case_rows:
        Historical case rows (from load_historical_case_rows).
        Used for Context family evidence computation.
    family_history_rows:
        Per-family summary from build_short_family_case_history.
    family_validation_stats:
        Output of build_family_validation_stats — used for aggregate sample context.
    """
    # Case schema key set (from first available case)
    case_keys: set = set(case_rows[0].keys()) if case_rows else set()

    # Aggregate sample context from validation stats
    named_fam_stats = [
        r for r in family_validation_stats
        if r.get("is_actionable_family", True)
    ]
    best_n_multiday = max(
        (r.get("case_count_multiday", 0) for r in named_fam_stats), default=0
    )

    # SHORT eligible pool for Context evidence computation
    short_eligible = [
        c for c in case_rows
        if c.get("side") == "SHORT" and c.get("research_eligible_YN") == "Y"
    ]
    n_short_total = len(short_eligible)

    results = []

    for family_name, defn in _P2D_ANSWER_CONTRACT_DEFS.items():
        req_fields     = defn["required_fields_spec"]
        present_fields = [f for f in req_fields if f in case_keys]
        missing_fields = [f for f in req_fields if f not in case_keys]
        n_req     = len(req_fields)
        n_missing = len(missing_fields)
        n_present = len(present_fields)

        strategy_question    = defn["strategy_question"]
        threshold_direction  = defn["threshold_direction"]
        likely_side_effect   = defn["likely_side_effect"]
        validation_next_step = defn["validation_next_step"]

        # --- Determine state based on field presence + sample context ---
        if n_missing == n_req:
            # Zero required fields present — fully blocked
            layer_ref = (
                f"Layer {defn['layer_id']}" if defn["layer_id"] is not None
                else "Cross-layer"
            )
            blocking_reason = (
                f"Required {layer_ref} field pack is not yet available in downstream case rows "
                f"({n_req} required fields all absent)."
            )
            recommendation_state = "blocked"
            evidence_summary     = f"0/{n_req} required fields present. No evidence available."
            confidence_summary   = "not_assessable"

        elif n_missing > 0:
            # Partial fields present
            missing_preview = ", ".join(missing_fields[:5])
            if n_missing > 5:
                missing_preview += f" (+{n_missing - 5} more)"

            if family_name == "Context" and n_short_total >= _P2B_BUCKET_READY_THRESHOLD:
                # Context (Layer 7) has 4/7 fields present and enough sample
                # — compute directional regime x resolution evidence
                regime_counts: Dict[str, int] = {}
                for c in short_eligible:
                    r = c.get("research_regime") or "unknown"
                    regime_counts[r] = regime_counts.get(r, 0) + 1

                top_regime   = max(regime_counts, key=lambda k: regime_counts[k])
                top_regime_n = regime_counts[top_regime]

                res_in_top: Dict[str, int] = {}
                for c in short_eligible:
                    if (c.get("research_regime") or "unknown") == top_regime:
                        res = c.get("resolution_label") or "unknown"
                        res_in_top[res] = res_in_top.get(res, 0) + 1
                dom_res = (
                    max(res_in_top, key=lambda k: res_in_top[k])
                    if res_in_top else "unknown"
                )

                evidence_summary = (
                    f"{n_present}/{n_req} required fields present. "
                    f"n={n_short_total} SHORT eligible (multiday). "
                    f"Top regime: '{top_regime}' ({top_regime_n} cases, "
                    f"{round(100 * top_regime_n / max(n_short_total, 1))}%). "
                    f"Dominant resolution in top regime: '{dom_res}'. "
                    f"Directional only — {n_missing} fields still missing."
                )
                confidence_summary   = (
                    f"directional_only — n={n_short_total}, "
                    f"partial field pack ({n_present}/{n_req})"
                )
                blocking_reason      = (
                    f"Partial Layer 7 field pack: {n_missing}/{n_req} required fields "
                    f"still absent from downstream case rows ({missing_preview})."
                )
                recommendation_state = "keep_tracking"

            else:
                evidence_summary = (
                    f"{n_present}/{n_req} required fields present (proxy-level only). "
                    f"n_short_multiday={n_short_total}, best_family_n={best_n_multiday}. "
                    f"Missing: {missing_preview}. "
                    "Directional reading not reliable at current field completeness."
                )
                confidence_summary   = (
                    f"not_assessable — partial field pack ({n_present}/{n_req})"
                )
                blocking_reason      = (
                    f"Partial field pack: {n_missing}/{n_req} required fields "
                    f"still absent from downstream case rows: {missing_preview}."
                )
                recommendation_state = "descriptive_only"

        else:
            # All required fields present in schema — but check if they are usable (non-null)
            # A field present in headers but all-null in case rows is not usable for measurement.
            _all_null_present = [
                f for f in present_fields
                if all(c.get(f) in (None, "", "None") for c in case_rows)
            ]

            if _all_null_present:
                # Headers exist but source values are all-null — PARTIAL usability
                _null_preview = ", ".join(_all_null_present[:4])
                if len(_all_null_present) > 4:
                    _null_preview += f" (+{len(_all_null_present) - 4} more)"
                blocking_reason = (
                    f"{len(_all_null_present)}/{n_req} required fields are present in schema "
                    f"but all-null in current case rows: {_null_preview}. "
                    f"Source population fix required before statistical validation."
                )
                evidence_summary = (
                    f"All {n_req} required fields present in schema. "
                    f"{len(_all_null_present)} field(s) have no populated values yet. "
                    f"n_short_multiday={n_short_total}. "
                    f"Not assessable until source values are non-null."
                )
                confidence_summary   = "present_but_unusable — mandatory fields all-null in case rows"
                recommendation_state = "descriptive_only"
            else:
                # All present and all usable
                blocking_reason  = "—"
                evidence_summary = (
                    f"All {n_req} required fields present and populated. "
                    f"n_short_multiday={n_short_total}, best_family_n={best_n_multiday}. "
                    f"Full statistical validation feasible."
                )
                confidence_summary   = "assessable — full field pack available"
                recommendation_state = (
                    "keep_tracking"
                    if best_n_multiday >= _P2B_BUCKET_READY_THRESHOLD
                    else "descriptive_only"
                )
                # Override static dict wording for families whose field pack has
                # now landed — static _P2D_ANSWER_CONTRACT_DEFS retains the
                # "not yet in schema" wording written before source fill completed.
                if family_name == "Context":
                    likely_side_effect = (
                        "Context filters reduce signal on neutral-regime days. "
                        "Separating by BTC / breadth / volatility bucket reduces "
                        "per-cell sample — interpret directional patterns conservatively."
                    )
                    validation_next_step = (
                        "All Layer 7 fields are now present and populated. "
                        "Next: assess btc_change_15m_pct / btc_change_1h_pct / "
                        "market_volatility_proxy separation by resolution_label "
                        "across SHORT eligible cases. "
                        "If directional evidence is not yet strong, keep tracking. "
                        "Do not issue live-rule instructions from this analysis alone."
                    )

        results.append({
            "family_name":           family_name,
            "strategy_question":     strategy_question,
            "threshold_direction":   threshold_direction,
            "evidence_summary":      evidence_summary,
            "confidence_summary":    confidence_summary,
            "likely_side_effect":    likely_side_effect,
            "blocking_reason":       blocking_reason,
            "validation_next_step":  validation_next_step,
            "recommendation_state":  recommendation_state,
        })

    return results


# ===========================================================================
# Phase 2E — Controlled Validation State Machine
# ===========================================================================

# Anchor readiness values that do NOT block PREPARE_HYPOTHESIS
_P2E_ANCHOR_PROMOTABLE = {"PASSED", "DIRECTIONAL_ONLY"}


def build_controlled_validation_state(
    family_history_rows: List[Dict],
    family_validation_stats: List[Dict],
    family_answer_contracts: List[Dict],
    anchor_qa_summary: Optional[Dict] = None,
) -> List[Dict]:
    """
    Phase 2E producer: controlled validation state machine.

    One row per display_family (iterates over family_validation_stats).
    Promotion is structurally impossible when gates fail.

    Allowed states
    --------------
    DESCRIPTIVE_ONLY
    KEEP_TRACKING
    NOT_READY_FOR_CONTROLLED_VALIDATION
    PREPARE_HYPOTHESIS
    READY_FOR_CONTROLLED_VALIDATION

    Hard gates for PREPARE_HYPOTHESIS (any failure blocks):
      1. family history must be available (not "not_available_yet")
      2. case_count_multiday >= 20
      3. bucket_ready_status != "not_enough_sample"  (n >= 10 in a key bucket)
      4. regime_consistency_flag not in {INSUFFICIENT, UNSTABLE, UNKNOWN}
      5. anchor_qa_summary["anchor_measurement_readiness"] in _P2E_ANCHOR_PROMOTABLE

    Additional gates for READY_FOR_CONTROLLED_VALIDATION:
      6. tracking_grade_status == "tracking_grade"
      7. at least one statistical test is not na_low_sample
      8. unseen_day_validation_plan exists (always False first pass — always blocks)

    unclassified / is_actionable_family=False -> always DESCRIPTIVE_ONLY.
    """
    if not family_validation_stats:
        return []

    # History lookup: display_family -> family_history_availability_status
    history_by_fam: Dict[str, str] = {}
    for h in (family_history_rows or []):
        fam = h.get("display_family", "")
        if fam:
            history_by_fam[fam] = h.get("family_history_availability_status", "not_available_yet")

    # Anchor QA gate — reuse build_anchor_qa_summary output
    if anchor_qa_summary is None:
        anchor_readiness = "NOT_RUN"
    else:
        anchor_readiness = anchor_qa_summary.get("anchor_measurement_readiness", "NOT_RUN")
    anchor_gate_passed = anchor_readiness in _P2E_ANCHOR_PROMOTABLE
    anchor_gate_label  = "PASSED" if anchor_gate_passed else f"BLOCKED_{anchor_readiness}"

    # Answer contracts: supplementary context only — not the primary gate namespace
    contract_states: List[str] = [
        c.get("recommendation_state", "blocked")
        for c in (family_answer_contracts or [])
    ]
    _rank = {"blocked": 0, "descriptive_only": 1, "keep_tracking": 2, "no_change": 3}
    worst_contract_state = (
        min(contract_states, key=lambda s: _rank.get(s, 0))
        if contract_states else "no_contracts_available"
    )

    results = []

    for stat_row in family_validation_stats:
        display_family  = stat_row.get("display_family", "—")
        is_actionable   = stat_row.get("is_actionable_family", True)
        n_multiday      = int(stat_row.get("case_count_multiday", 0) or 0)
        family_days     = int(stat_row.get("family_days_count", 0) or 0)
        regime_flag     = stat_row.get("regime_consistency_flag", "UNKNOWN")
        bucket_status   = stat_row.get("bucket_ready_status", "not_enough_sample")
        tracking_status = stat_row.get("tracking_grade_status", "not_enough_sample")
        ks_status       = str(stat_row.get("ks_test_status_or_na", "na_low_sample"))
        ttest_status    = str(stat_row.get("ttest_status_or_na", "na_low_sample"))
        history_avail   = history_by_fam.get(display_family, "not_available_yet")

        # Non-actionable families (e.g. unclassified) — hard block, no promotion path
        if not is_actionable:
            results.append({
                "display_family":               display_family,
                "controlled_validation_state":  "DESCRIPTIVE_ONLY",
                "gating_reasons":               [
                    "Generic population bucket — not an actionable rule-family target."
                ],
                "case_count_multiday":          n_multiday,
                "family_days_count":            family_days,
                "regime_consistency_flag":      regime_flag,
                "anchor_qa_gate":               "NOT_APPLICABLE",
                "promotion_blocked":            True,
                "promotion_blocker_summary":    "Non-actionable family. Not eligible for promotion path.",
                "answer_contract_supplementary": worst_contract_state,
            })
            continue

        # Collect gate failures
        gating_reasons: List[str] = []
        if history_avail == "not_available_yet":
            gating_reasons.append(
                "Family history unavailable — no historical case days found."
            )
        if n_multiday < _P2B_TRACKING_GRADE_THRESHOLD:
            gating_reasons.append(
                f"case_count_multiday={n_multiday} < {_P2B_TRACKING_GRADE_THRESHOLD} "
                f"(tracking-grade threshold). Continue tracking."
            )
        if bucket_status == "not_enough_sample":
            gating_reasons.append(
                f"Bucket-ready gate not met (need >= {_P2B_BUCKET_READY_THRESHOLD} "
                f"multiday cases in a key decision bucket)."
            )
        if regime_flag in ("INSUFFICIENT", "UNSTABLE", "UNKNOWN"):
            gating_reasons.append(
                f"Regime consistency gate failed: regime_consistency_flag={regime_flag}."
            )
        if not anchor_gate_passed:
            gating_reasons.append(
                f"Anchor QA gate not passed: anchor_measurement_readiness={anchor_readiness}. "
                f"Manual spot-check required before promotion."
            )

        # Evaluate hard-gate booleans (structurally enforced)
        prepare_gates_pass = (
            history_avail != "not_available_yet"
            and n_multiday >= _P2B_TRACKING_GRADE_THRESHOLD
            and bucket_status != "not_enough_sample"
            and regime_flag not in ("INSUFFICIENT", "UNSTABLE", "UNKNOWN")
            and anchor_gate_passed
        )

        # Extra gates for READY_FOR_CONTROLLED_VALIDATION
        stats_valid      = not (ks_status.startswith("na_") and ttest_status.startswith("na_"))
        unseen_day_ready = False  # first pass: holdout data not yet available

        ready_cv_gates_pass = (
            prepare_gates_pass
            and tracking_status == "tracking_grade"
            and stats_valid
            and unseen_day_ready
        )

        # Add READY_FOR_CV-specific gate reasons (only when PREPARE gates pass)
        if prepare_gates_pass:
            if not stats_valid:
                gating_reasons.append(
                    f"Statistical tests returned na results "
                    f"(ks={ks_status}, ttest={ttest_status}). "
                    f"READY_FOR_CONTROLLED_VALIDATION requires at least one valid test."
                )
            if not unseen_day_ready:
                gating_reasons.append(
                    "Unseen-day validation plan not yet available. "
                    "READY_FOR_CONTROLLED_VALIDATION requires holdout data."
                )

        # State determination — structurally impossible to skip gates
        if ready_cv_gates_pass:
            state = "READY_FOR_CONTROLLED_VALIDATION"
        elif prepare_gates_pass:
            state = "PREPARE_HYPOTHESIS"
        elif history_avail == "not_available_yet":
            state = "DESCRIPTIVE_ONLY"
        elif n_multiday < _P2B_TRACKING_GRADE_THRESHOLD:
            state = "KEEP_TRACKING"
        else:
            state = "NOT_READY_FOR_CONTROLLED_VALIDATION"

        # Safety guard: non-terminal state must always have explicit gating reasons
        if state != "READY_FOR_CONTROLLED_VALIDATION" and not gating_reasons:
            gating_reasons.append(
                f"State={state} but no explicit gate failures recorded — "
                f"review gate logic for this family."
            )

        promotion_blocked = state not in (
            "PREPARE_HYPOTHESIS", "READY_FOR_CONTROLLED_VALIDATION"
        )

        results.append({
            "display_family":               display_family,
            "controlled_validation_state":  state,
            "gating_reasons":               gating_reasons,
            "case_count_multiday":          n_multiday,
            "family_days_count":            family_days,
            "regime_consistency_flag":      regime_flag,
            "anchor_qa_gate":               anchor_gate_label,
            "promotion_blocked":            promotion_blocked,
            "promotion_blocker_summary": (
                gating_reasons[0]
                if gating_reasons
                else "No blockers — state allows progression."
            ),
            "answer_contract_supplementary": worst_contract_state,
        })

    return results


# ===========================================================================
# Phase 2E — Unseen-Day / Holdout Validation Summary
# ===========================================================================


def build_unseen_day_validation_summary(
    family_history_rows: List[Dict],
    holdout_case_rows: Optional[List[Dict]] = None,
) -> List[Dict]:
    """
    Phase 2E producer: unseen-day / holdout validation summary.

    First pass: holdout_case_rows is None -> explicit NOT_READY rows for all families.
    Never invents fake validation strength.

    Required output fields per row:
        display_family
        unseen_validation_status
        holdout_days_used
        holdout_case_count
        holdout_result_summary
        holdout_warning
    """
    families: List[str] = (
        [h.get("display_family", "—") for h in (family_history_rows or [])]
        if family_history_rows
        else ["all_families_placeholder"]
    )

    if not holdout_case_rows:
        return [
            {
                "display_family":           fam,
                "unseen_validation_status": "NOT_READY_no_holdout_data",
                "holdout_days_used":        0,
                "holdout_case_count":       0,
                "holdout_result_summary": (
                    "Holdout data not yet available. "
                    "Unseen-day validation cannot be run. "
                    "Continue tracking until a held-out date range is established."
                ),
                "holdout_warning": (
                    "No unseen-day validation has been run for this family. "
                    "READY_FOR_CONTROLLED_VALIDATION and PREPARE_HYPOTHESIS are blocked "
                    "until holdout data exists and is formally validated."
                ),
            }
            for fam in families
        ]

    # When holdout_case_rows provided — basic conservative summary per family
    holdout_eligible = [
        c for c in holdout_case_rows
        if c.get("side") == "SHORT" and c.get("research_eligible_YN") == "Y"
    ]
    holdout_days_set: set = set(
        c.get("research_day", "") for c in holdout_eligible if c.get("research_day")
    )
    by_fam: Dict[str, List[Dict]] = defaultdict(list)
    for c in holdout_eligible:
        by_fam[_display_family_p2(c)].append(c)

    results = []
    for fam in families:
        fam_rows = by_fam.get(fam, [])
        n        = len(fam_rows)
        n_days   = len(holdout_days_set)
        if n < 5:
            status  = "NOT_READY_insufficient_holdout_sample"
            summary = (
                f"Holdout sample too small: n={n} (need >= 5). "
                f"Days in holdout window: {n_days}. Continue tracking."
            )
            warning = (
                f"Holdout exists but n={n} is below minimum for directional use. "
                "Promotion remains blocked."
            )
        else:
            f1h_vals = [
                float(c["future_1h_max_favor_pct"])
                for c in fam_rows
                if c.get("future_1h_max_favor_pct") not in (None, "", "None")
            ]
            med_f1h = round(statistics.median(f1h_vals), 3) if f1h_vals else None
            status  = "PARTIAL_holdout_available"
            summary = (
                f"Holdout n={n}, days={n_days}. "
                f"Median F1h={med_f1h if med_f1h is not None else 'n/a'}. "
                "Directional reading only — not sufficient for formal promotion."
            )
            warning = (
                f"Holdout exists (n={n}) but has not been formally validated. "
                "READY_FOR_CONTROLLED_VALIDATION requires a formal holdout validation run."
            )
        results.append({
            "display_family":           fam,
            "unseen_validation_status": status,
            "holdout_days_used":        n_days,
            "holdout_case_count":       n,
            "holdout_result_summary":   summary,
            "holdout_warning":          warning,
        })
    return results
