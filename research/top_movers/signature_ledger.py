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
        last_seen_str = row.get("last_seen_date", day_str)
        if sk and day_str:
            sig_all_days[sk].add(day_str)
            try:
                if datetime.strptime(day_str, "%Y-%m-%d") >= window_start:
                    sig_recent_days[sk].add(day_str)
            except ValueError:
                pass
        if sk:
            if last_seen_str >= sig_latest_seen.get(sk, ""):
                sig_latest_seen[sk] = last_seen_str
                sig_latest_status[sk] = row.get("validation_status_day", "")

    for row in all_rows:
        sk = row.get("signature_key", "")
        last_seen_str = row.get("last_seen_date", row.get("research_day", ""))
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
            is_stale = datetime.strptime(last_seen_str, "%Y-%m-%d") < stale_cutoff
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

    snapshot = list(latest_by_sig.values())
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
        latest_status = row.get("latest_validation_status", "")
        if first_seen and global_last and first_seen > global_last:
            warnings.append(f"{code}: first_seen_date ({first_seen}) > last_seen_date ({global_last})")
        if support_days >= 3 and latest_status in ("first_observation", ""):
            warnings.append(f"{code}: support_days={support_days} but validation_status=\'{latest_status}\'")
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
        results.append({
            "strategy_family":         family,
            "issue_layer":             layer,
            "case_count_today":        len(group_cases),
            "repeated_support_today":  repeated_support_today,
            "cross_day_support":       cross_day_support,
            "evidence_source":         "repeated_plus_case" if repeated_support_today > 0 else "case_level_only",
            "readiness":               readiness,
            "recommended_next_action": max(set(actions), key=actions.count) if actions else "",
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
