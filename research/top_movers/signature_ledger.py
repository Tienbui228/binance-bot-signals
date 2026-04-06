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
import statistics
from datetime import datetime, timezone
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
            "validation_status": "tracking" if sig_conf in ("HIGH","MEDIUM") else "first_observation",
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

    Semantics:
      1. Load existing ledger.
      2. PURGE all rows where research_day == D (remove stale rows from prior runs of D).
      3. Insert current run's candidate rows for D (may be zero).
      4. Write back.

    This is NOT append-only. It is daily replace/upsert.
    If zero candidates for D: ledger ends with zero rows for D (no stale rows remain).
    first_seen_date is preserved from prior days for the same signature_key.
    """
    os.makedirs(os.path.dirname(LEDGER_PATH), exist_ok=True)

    # Load existing ledger, excluding rows for research_day (purge step)
    kept: Dict[tuple, Dict] = {}
    first_seen_by_sig: Dict[str, str] = {}

    if os.path.exists(LEDGER_PATH):
        with open(LEDGER_PATH, "r", newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                day = row.get("research_day","")
                sig = row.get("signature_key","")
                if day == research_day:
                    continue  # PURGE: remove all rows for this day
                kept[(day, sig)] = row
                # Track earliest first_seen across retained days
                first = row.get("first_seen_date", day)
                if sig and (sig not in first_seen_by_sig or first < first_seen_by_sig[sig]):
                    first_seen_by_sig[sig] = first

    # Insert new candidates for today
    for cand in new_candidates:
        sig_key = cand.get("signature_key","")
        key     = (research_day, sig_key)
        new_row = _candidate_to_ledger_row(cand)
        # Preserve first_seen from prior days if this signature was seen before
        new_row["first_seen_date"] = first_seen_by_sig.get(sig_key, research_day)
        new_row["last_seen_date"]  = research_day
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
    health = "CLEAN" if er>=0.75 and pr>=0.60 and ir>=0.60 else "PARTIAL" if er>=0.40 or pr>=0.40 else "WEAK"

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
