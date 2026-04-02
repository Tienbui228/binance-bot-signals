
#!/usr/bin/env python3
from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd


DEFAULT_STATUSS = {"OPEN", "PENDING", "CONFIRMED"}
CLOSED_OUTCOMES_WIN = {"WIN_TP1", "WIN_TP2"}
CLOSED_OUTCOMES_STOP = {"LOSS_STOP", "STOP"}
CLOSED_OUTCOMES_EXPIRED = {"EXPIRED"}


def ms_to_datetime(ms: float | int | None) -> Optional[datetime]:
    if ms is None or pd.isna(ms):
        return None
    try:
        return datetime.fromtimestamp(float(ms) / 1000.0, tz=timezone.utc)
    except Exception:
        return None


def ensure_datetime_col(df: pd.DataFrame, source_col: str, out_col: str) -> pd.DataFrame:
    if source_col in df.columns:
        df[out_col] = pd.to_datetime(df[source_col], unit="ms", utc=True, errors="coerce")
    else:
        df[out_col] = pd.NaT
    return df


def load_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    if path.is_dir():
        frames = []
        for fp in sorted(path.glob("*.csv")):
            if fp.name.endswith("_schema.csv") or fp.name == "_schema.csv":
                continue
            try:
                frames.append(pd.read_csv(fp))
            except pd.errors.EmptyDataError:
                continue
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    try:
        return pd.read_csv(path)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def pct(n: float, d: float) -> float:
    if not d:
        return 0.0
    return float(n) / float(d)


def safe_numeric(df: pd.DataFrame, cols: Iterable[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


@dataclass
class WeeklyPaths:
    signals: Path
    results: Path
    pending: Path
    review_candidates: Path
    review_snapshots: Path
    market_opportunity_review: Optional[Path]
    missed_opportunities: Optional[Path]
    output_dir: Path


def build_strategy_metrics(signals: pd.DataFrame, results: pd.DataFrame, start_ts: pd.Timestamp, end_ts: pd.Timestamp) -> pd.DataFrame:
    sig = signals.copy()
    res = results.copy()

    if not sig.empty:
        sig = ensure_datetime_col(sig, "timestamp_ms", "event_dt")
        sig = sig[(sig["event_dt"] >= start_ts) & (sig["event_dt"] < end_ts)].copy()
        sig = safe_numeric(sig, [
            "score", "confidence", "oi_jump_pct", "vol_ratio", "retest_bars_waited",
            "score_oi", "score_exhaustion", "score_breakout", "score_retest"
        ])

    if not res.empty:
        close_col = "close_time_ms" if "close_time_ms" in res.columns else "timestamp_ms"
        res = ensure_datetime_col(res, close_col, "event_dt")
        res = res[(res["event_dt"] >= start_ts) & (res["event_dt"] < end_ts)].copy()
        res = safe_numeric(res, [
            "r_multiple", "score_oi", "score_exhaustion", "score_breakout", "score_retest"
        ])

    strategy_side_pairs = set()
    if not sig.empty:
        strategy_side_pairs.update(zip(sig.get("strategy", pd.Series(dtype=str)), sig.get("side", pd.Series(dtype=str))))
    if not res.empty:
        strategy_side_pairs.update(zip(res.get("strategy", pd.Series(dtype=str)), res.get("side", pd.Series(dtype=str))))

    rows = []
    for strategy, side in sorted(strategy_side_pairs):
        sig_g = sig[(sig["strategy"] == strategy) & (sig["side"] == side)].copy() if not sig.empty else pd.DataFrame()
        res_g = res[(res["strategy"] == strategy) & (res["side"] == side)].copy() if not res.empty else pd.DataFrame()

        signals_count = len(sig_g)
        closed_count = len(res_g)
        win_tp1 = int((res_g.get("outcome", pd.Series(dtype=str)) == "WIN_TP1").sum()) if not res_g.empty else 0
        win_tp2 = int((res_g.get("outcome", pd.Series(dtype=str)) == "WIN_TP2").sum()) if not res_g.empty else 0
        win_count = win_tp1 + win_tp2
        stop_count = int(res_g.get("outcome", pd.Series(dtype=str)).isin(CLOSED_OUTCOMES_STOP).sum()) if not res_g.empty else 0
        expired_count = int((res_g.get("outcome", pd.Series(dtype=str)) == "EXPIRED").sum()) if not res_g.empty else 0

        manual_yes_sig = sig_g[sig_g.get("manual_tradable", pd.Series(dtype=str)).astype(str).str.lower() == "yes"] if not sig_g.empty and "manual_tradable" in sig_g.columns else pd.DataFrame()
        manual_no_sig = sig_g[sig_g.get("manual_tradable", pd.Series(dtype=str)).astype(str).str.lower() == "no"] if not sig_g.empty and "manual_tradable" in sig_g.columns else pd.DataFrame()
        manual_yes_res = res_g[res_g.get("manual_tradable", pd.Series(dtype=str)).astype(str).str.lower() == "yes"] if not res_g.empty and "manual_tradable" in res_g.columns else pd.DataFrame()
        manual_no_res = res_g[res_g.get("manual_tradable", pd.Series(dtype=str)).astype(str).str.lower() == "no"] if not res_g.empty and "manual_tradable" in res_g.columns else pd.DataFrame()

        rows.append({
            "period_start": start_ts.isoformat(),
            "period_end": end_ts.isoformat(),
            "strategy": strategy,
            "side": side,
            "signals_count": signals_count,
            "closed_count": closed_count,
            "win_count": win_count,
            "tp1_count": win_tp1,
            "tp2_count": win_tp2,
            "stop_count": stop_count,
            "expired_count": expired_count,
            "winrate": round(pct(win_count, closed_count), 4),
            "stop_rate": round(pct(stop_count, closed_count), 4),
            "expired_rate": round(pct(expired_count, closed_count), 4),
            "avg_r": round(float(res_g["r_multiple"].mean()), 4) if (not res_g.empty and "r_multiple" in res_g.columns) else None,
            "median_r": round(float(res_g["r_multiple"].median()), 4) if (not res_g.empty and "r_multiple" in res_g.columns) else None,
            "avg_confidence": round(float(sig_g["confidence"].mean()), 4) if (not sig_g.empty and "confidence" in sig_g.columns) else None,
            "avg_score": round(float(sig_g["score"].mean()), 4) if (not sig_g.empty and "score" in sig_g.columns) else None,
            "avg_oi_jump_pct": round(float(sig_g["oi_jump_pct"].mean()), 4) if (not sig_g.empty and "oi_jump_pct" in sig_g.columns) else None,
            "avg_vol_ratio": round(float(sig_g["vol_ratio"].mean()), 4) if (not sig_g.empty and "vol_ratio" in sig_g.columns) else None,
            "avg_retest_wait_bars": round(float(sig_g["retest_bars_waited"].mean()), 4) if (not sig_g.empty and "retest_bars_waited" in sig_g.columns) else None,
            "manual_yes_count": len(manual_yes_sig),
            "manual_yes_rate": round(pct(len(manual_yes_sig), signals_count), 4),
            "manual_yes_avg_r": round(float(manual_yes_res["r_multiple"].mean()), 4) if (not manual_yes_res.empty and "r_multiple" in manual_yes_res.columns) else None,
            "manual_no_count": len(manual_no_sig),
            "manual_no_avg_r": round(float(manual_no_res["r_multiple"].mean()), 4) if (not manual_no_res.empty and "r_multiple" in manual_no_res.columns) else None,
        })

    return pd.DataFrame(rows).sort_values(["strategy", "side"]).reset_index(drop=True) if rows else pd.DataFrame(columns=[
        "period_start","period_end","strategy","side","signals_count","closed_count","win_count","tp1_count","tp2_count",
        "stop_count","expired_count","winrate","stop_rate","expired_rate","avg_r","median_r","avg_confidence","avg_score",
        "avg_oi_jump_pct","avg_vol_ratio","avg_retest_wait_bars","manual_yes_count","manual_yes_rate","manual_yes_avg_r",
        "manual_no_count","manual_no_avg_r"
    ])


def build_funnel_metrics(signals: pd.DataFrame, pending: pd.DataFrame, results: pd.DataFrame, start_ts: pd.Timestamp, end_ts: pd.Timestamp) -> pd.DataFrame:
    sig = signals.copy()
    pen = pending.copy()
    res = results.copy()

    if not sig.empty:
        sig = ensure_datetime_col(sig, "timestamp_ms", "event_dt")
        sig = sig[(sig["event_dt"] >= start_ts) & (sig["event_dt"] < end_ts)].copy()

    if not pen.empty:
        pen = ensure_datetime_col(pen, "created_ts_ms", "event_dt")
        pen = pen[(pen["event_dt"] >= start_ts) & (pen["event_dt"] < end_ts)].copy()

    if not res.empty:
        close_col = "close_time_ms" if "close_time_ms" in res.columns else "timestamp_ms"
        res = ensure_datetime_col(res, close_col, "event_dt")
        res = res[(res["event_dt"] >= start_ts) & (res["event_dt"] < end_ts)].copy()

    strategy_side_pairs = set()
    if not pen.empty:
        strategy_side_pairs.update(zip(pen.get("strategy", pd.Series(dtype=str)), pen.get("side", pd.Series(dtype=str))))
    if not sig.empty:
        strategy_side_pairs.update(zip(sig.get("strategy", pd.Series(dtype=str)), sig.get("side", pd.Series(dtype=str))))
    if not res.empty:
        strategy_side_pairs.update(zip(res.get("strategy", pd.Series(dtype=str)), res.get("side", pd.Series(dtype=str))))

    rows = []
    for strategy, side in sorted(strategy_side_pairs):
        pen_g = pen[(pen["strategy"] == strategy) & (pen["side"] == side)].copy() if not pen.empty else pd.DataFrame()
        sig_g = sig[(sig["strategy"] == strategy) & (sig["side"] == side)].copy() if not sig.empty else pd.DataFrame()
        res_g = res[(res["strategy"] == strategy) & (res["side"] == side)].copy() if not res.empty else pd.DataFrame()

        detected_count = len(pen_g)
        pending_count = int((pen_g.get("status", pd.Series(dtype=str)) == "PENDING").sum()) if not pen_g.empty else 0
        confirmed_count = int((pen_g.get("status", pd.Series(dtype=str)) == "CONFIRMED").sum()) if not pen_g.empty else 0
        invalidated_count = int((pen_g.get("status", pd.Series(dtype=str)) == "INVALIDATED").sum()) if not pen_g.empty else 0
        expired_wait_count = int((pen_g.get("status", pd.Series(dtype=str)) == "EXPIRED_WAIT").sum()) if not pen_g.empty else 0
        rejected_score_count = int((pen_g.get("status", pd.Series(dtype=str)) == "REJECTED_SCORE").sum()) if not pen_g.empty else 0
        sent_count = len(sig_g)
        closed_count = len(res_g)

        rows.append({
            "period_start": start_ts.isoformat(),
            "period_end": end_ts.isoformat(),
            "strategy": strategy,
            "side": side,
            "detected_count": detected_count,
            "pending_count": pending_count,
            "confirmed_count": confirmed_count,
            "invalidated_count": invalidated_count,
            "expired_wait_count": expired_wait_count,
            "rejected_score_count": rejected_score_count,
            "sent_count": sent_count,
            "closed_count": closed_count,
            "detect_to_pending_rate": round(pct(pending_count, detected_count), 4),
            "pending_to_confirm_rate": round(pct(confirmed_count, detected_count), 4),
            "confirm_to_sent_rate": round(pct(sent_count, confirmed_count), 4),
            "sent_to_closed_rate": round(pct(closed_count, sent_count), 4),
        })

    return pd.DataFrame(rows).sort_values(["strategy", "side"]).reset_index(drop=True) if rows else pd.DataFrame(columns=[
        "period_start","period_end","strategy","side","detected_count","pending_count","confirmed_count","invalidated_count",
        "expired_wait_count","rejected_score_count","sent_count","closed_count","detect_to_pending_rate","pending_to_confirm_rate",
        "confirm_to_sent_rate","sent_to_closed_rate"
    ])


def build_missed_review(missed: pd.DataFrame, market_opp: pd.DataFrame, start_ts: pd.Timestamp, end_ts: pd.Timestamp) -> pd.DataFrame:
    if not missed.empty:
        date_col = "review_date" if "review_date" in missed.columns else None
        if date_col:
            missed["review_dt"] = pd.to_datetime(missed[date_col], utc=True, errors="coerce")
            missed = missed[(missed["review_dt"] >= start_ts.normalize()) & (missed["review_dt"] < end_ts.normalize())].copy()
        return missed.reset_index(drop=True)

    if not market_opp.empty:
        date_col = "review_date" if "review_date" in market_opp.columns else None
        if date_col:
            market_opp["review_dt"] = pd.to_datetime(market_opp[date_col], utc=True, errors="coerce")
            market_opp = market_opp[(market_opp["review_dt"] >= start_ts.normalize()) & (market_opp["review_dt"] < end_ts.normalize())].copy()
        if "bot_status" in market_opp.columns:
            market_opp = market_opp[market_opp["bot_status"].isin(["PENDING", "REJECTED", "NOT_DETECTED"])].copy()
        return market_opp.reset_index(drop=True)

    return pd.DataFrame()


def label_strategy_health(row: pd.Series) -> str:
    signals = row.get("signals_count", 0) or 0
    closed = row.get("closed_count", 0) or 0
    winrate = row.get("winrate", 0.0) or 0.0
    avg_r = row.get("avg_r", None)
    expired_rate = row.get("expired_rate", 0.0) or 0.0

    if closed < 5:
        return "SMALL_SAMPLE"
    if avg_r is not None and avg_r < 0 and winrate < 0.45:
        return "TOO_NOISY"
    if signals <= 1:
        return "UNDERFIRE"
    if expired_rate > 0.25:
        return "UNDERFIRE"
    if avg_r is not None and avg_r > 0 and winrate >= 0.5:
        return "HEALTHY"
    return "REFINE"


def render_weekly_summary(
    strategy_metrics: pd.DataFrame,
    funnel_metrics: pd.DataFrame,
    missed_review: pd.DataFrame,
    start_ts: pd.Timestamp,
    end_ts: pd.Timestamp,
) -> str:
    total_signals = int(strategy_metrics["signals_count"].sum()) if not strategy_metrics.empty else 0
    total_closed = int(strategy_metrics["closed_count"].sum()) if not strategy_metrics.empty else 0
    total_wins = int(strategy_metrics["win_count"].sum()) if not strategy_metrics.empty else 0
    total_stops = int(strategy_metrics["stop_count"].sum()) if not strategy_metrics.empty else 0
    total_expired = int(strategy_metrics["expired_count"].sum()) if not strategy_metrics.empty else 0
    weighted_avg_r = None
    if not strategy_metrics.empty and strategy_metrics["closed_count"].sum() > 0:
        tmp = strategy_metrics.copy()
        tmp["avg_r_weighted"] = tmp["avg_r"].fillna(0) * tmp["closed_count"]
        weighted_avg_r = tmp["avg_r_weighted"].sum() / max(tmp["closed_count"].sum(), 1)

    long_rows = strategy_metrics[strategy_metrics["side"] == "LONG"].copy() if not strategy_metrics.empty else pd.DataFrame()
    short_rows = strategy_metrics[strategy_metrics["side"] == "SHORT"].copy() if not strategy_metrics.empty else pd.DataFrame()

    missed_count = len(missed_review)
    high_missed = 0
    if not missed_review.empty and "human_review_priority" in missed_review.columns:
        high_missed = int((missed_review["human_review_priority"].astype(str).str.upper() == "HIGH").sum())

    lines: list[str] = []
    lines.append(f"=== WEEKLY SUMMARY ===")
    lines.append(f"period_start={start_ts.isoformat()}")
    lines.append(f"period_end={end_ts.isoformat()}")
    lines.append("")
    lines.append("Executive summary:")
    if total_closed == 0:
        lines.append("- No closed trades in this weekly window, so this is a behavior-only read.")
    else:
        lines.append(
            f"- Closed trades: {total_closed}; wins={total_wins} ({pct(total_wins, total_closed)*100:.2f}%), "
            f"stops={total_stops}, expired={total_expired}, avg_r={(weighted_avg_r or 0):.4f}"
        )
    lines.append(f"- Signals sent in window: {total_signals}.")
    lines.append(f"- Missed-opportunity rows in window: {missed_count} (HIGH priority: {high_missed}).")

    if not strategy_metrics.empty:
        top = strategy_metrics.copy()
        top["health_label"] = top.apply(label_strategy_health, axis=1)
        if total_closed > 0:
            top2 = top[top["closed_count"] > 0].sort_values(["avg_r", "winrate"], ascending=False)
            if not top2.empty:
                best = top2.iloc[0]
                lines.append(
                    f"- Best active bucket this week: {best['strategy']} {best['side']} "
                    f"(closed={int(best['closed_count'])}, winrate={best['winrate']*100:.2f}%, avg_r={best['avg_r']:.4f})."
                )
        issue = None
        if not top.empty:
            noisy = top[top["health_label"].isin(["TOO_NOISY", "UNDERFIRE"])]
            if not noisy.empty:
                issue = noisy.iloc[0]
        if issue is not None:
            lines.append(
                f"- Most urgent issue: {issue['strategy']} {issue['side']} labeled {issue['health_label']} "
                f"(signals={int(issue['signals_count'])}, closed={int(issue['closed_count'])}, avg_r={issue['avg_r']})."
            )
    lines.append("")
    lines.append("Top-level performance:")
    lines.append(f"- total_signals={total_signals}")
    lines.append(f"- total_closed={total_closed}")
    lines.append(f"- winrate={pct(total_wins, total_closed)*100:.2f}%")
    lines.append(f"- avg_r={(weighted_avg_r or 0):.4f}")
    lines.append(f"- stop_rate={pct(total_stops, total_closed)*100:.2f}%")
    lines.append(f"- expired_rate={pct(total_expired, total_closed)*100:.2f}%")

    def side_line(name: str, df: pd.DataFrame) -> str:
        if df.empty:
            return f"- {name}: no signals"
        sigs = int(df["signals_count"].sum())
        closed = int(df["closed_count"].sum())
        wins = int(df["win_count"].sum())
        avg_r = None
        if closed > 0:
            tmp = df.copy()
            tmp["avg_r_weighted"] = tmp["avg_r"].fillna(0) * tmp["closed_count"]
            avg_r = tmp["avg_r_weighted"].sum() / max(tmp["closed_count"].sum(), 1)
        return f"- {name}: signals={sigs}, closed={closed}, winrate={pct(wins, closed)*100:.2f}%, avg_r={(avg_r or 0):.4f}"

    lines.append(side_line("LONG", long_rows))
    lines.append(side_line("SHORT", short_rows))
    lines.append("")
    lines.append("Strategy breakdown:")
    if strategy_metrics.empty:
        lines.append("- No strategy rows in this window.")
    else:
        strat = strategy_metrics.copy()
        strat["health_label"] = strat.apply(label_strategy_health, axis=1)
        for _, r in strat.sort_values(["strategy", "side"]).iterrows():
            lines.append(
                f"- {r['strategy']} {r['side']}: signals={int(r['signals_count'])}, closed={int(r['closed_count'])}, "
                f"winrate={r['winrate']*100:.2f}%, avg_r={0 if pd.isna(r['avg_r']) else r['avg_r']:.4f}, "
                f"manual_yes_rate={r['manual_yes_rate']*100:.2f}%, label={r['health_label']}"
            )

    lines.append("")
    lines.append("Funnel diagnosis:")
    if funnel_metrics.empty:
        lines.append("- No funnel rows in this window.")
    else:
        for _, r in funnel_metrics.sort_values(["strategy", "side"]).iterrows():
            lines.append(
                f"- {r['strategy']} {r['side']}: detected={int(r['detected_count'])}, pending={int(r['pending_count'])}, "
                f"confirmed={int(r['confirmed_count'])}, invalidated={int(r['invalidated_count'])}, "
                f"expired_wait={int(r['expired_wait_count'])}, rejected_score={int(r['rejected_score_count'])}, "
                f"sent={int(r['sent_count'])}, closed={int(r['closed_count'])}"
            )

    lines.append("")
    lines.append("Missed opportunities:")
    if missed_review.empty:
        lines.append("- No missed-opportunity review rows in this window.")
    else:
        preview = missed_review.copy()
        sort_cols = []
        if "human_review_priority" in preview.columns:
            priority_order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
            preview["priority_rank"] = preview["human_review_priority"].astype(str).str.upper().map(priority_order).fillna(9)
            sort_cols.append("priority_rank")
        if "pct_change_24h" in preview.columns:
            sort_cols.append("pct_change_24h")
        if sort_cols:
            preview = preview.sort_values(sort_cols, ascending=[True] + [False] * (len(sort_cols)-1))
        for _, r in preview.head(5).iterrows():
            symbol = r.get("symbol", "")
            pattern = r.get("opportunity_pattern", "")
            status = r.get("bot_status", "")
            reason = r.get("bot_reason", "")
            prio = r.get("human_review_priority", "")
            lines.append(f"- {symbol}: pattern={pattern}, bot_status={status}, priority={prio}, reason={reason}")

    lines.append("")
    lines.append("Actions next week:")
    actions = []
    if not strategy_metrics.empty:
        underfire = strategy_metrics[(strategy_metrics["signals_count"] <= 1) | (strategy_metrics["expired_rate"] > 0.25)]
        if not underfire.empty:
            r = underfire.iloc[0]
            actions.append(f"Review underfire behavior in {r['strategy']} {r['side']} and compare against missed-opportunity rows.")
        stale_like = missed_review.copy()
        if not stale_like.empty and "bot_reason" in stale_like.columns:
            stale_ct = stale_like["bot_reason"].astype(str).str.contains("stale", case=False, na=False).sum()
            if stale_ct > 0:
                actions.append("Audit stale-breakout rejects this week to ensure the freshness filter is not too tight.")
        noisy = strategy_metrics[(strategy_metrics["closed_count"] >= 5) & (strategy_metrics["avg_r"].fillna(0) < 0)]
        if not noisy.empty:
            r = noisy.iloc[0]
            actions.append(f"Investigate false positives in {r['strategy']} {r['side']} because avg_r is negative on a usable sample.")
    if not actions:
        actions.append("Collect more clean weekly sample before changing thresholds.")
    for a in actions[:3]:
        lines.append(f"- {a}")

    return "\n".join(lines) + "\n"


def generate_period_report(data_dir: Path, days: int, label: str, output_dir: Path | None = None, end_date: str | None = None) -> None:
    data_dir = data_dir.resolve()
    output_dir = (output_dir or (data_dir / "reports" / label)).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    signals_path = (data_dir / "data" / "signals") if (data_dir / "data" / "signals").exists() else (data_dir / "signals.csv")
    results_path = (data_dir / "data" / "results") if (data_dir / "data" / "results").exists() else (data_dir / "results.csv")
    pending_path = (data_dir / "data" / "pending") if (data_dir / "data" / "pending").exists() else (data_dir / "pending_setups.csv")
    review_candidates_path = (data_dir / "data" / "review_candidates") if (data_dir / "data" / "review_candidates").exists() else (data_dir / "review_candidates.csv")
    review_snapshots_path = data_dir / "review_snapshots.csv"
    market_review_path = (data_dir / "data" / "market_review") if (data_dir / "data" / "market_review").exists() else ((data_dir / "market_opportunity_review.csv") if (data_dir / "market_opportunity_review.csv").exists() else None)
    missed_path = (data_dir / "missed_opportunities.csv") if (data_dir / "missed_opportunities.csv").exists() else None

    signals = load_csv(signals_path)
    results = load_csv(results_path)
    pending = load_csv(pending_path)
    review_candidates = load_csv(review_candidates_path)
    review_snapshots = load_csv(review_snapshots_path)
    market_opp = load_csv(market_review_path) if market_review_path else pd.DataFrame()
    missed = load_csv(missed_path) if missed_path else pd.DataFrame()

    candidate_max_times = []
    for df, col in [
        (signals, "timestamp_ms"),
        (results, "close_time_ms" if "close_time_ms" in results.columns else "timestamp_ms"),
        (pending, "created_ts_ms"),
        (review_snapshots, "snapshot_ts_ms"),
    ]:
        if not df.empty and col in df.columns:
            vals = pd.to_numeric(df[col], errors="coerce").dropna()
            if not vals.empty:
                candidate_max_times.append(vals.max())

    if end_date:
        end_ts = pd.Timestamp(end_date, tz="UTC") + pd.Timedelta(days=1)
    elif candidate_max_times:
        end_ts = pd.to_datetime(max(candidate_max_times), unit="ms", utc=True).ceil("D")
    else:
        end_ts = pd.Timestamp.now(tz="UTC").ceil("D")

    start_ts = end_ts - pd.Timedelta(days=days)

    strategy_metrics = build_strategy_metrics(signals, results, start_ts, end_ts)
    funnel_metrics = build_funnel_metrics(signals, pending, results, start_ts, end_ts)
    missed_review = build_missed_review(missed, market_opp, start_ts, end_ts)

    strategy_out = output_dir / f"{label}_strategy_metrics.csv"
    funnel_out = output_dir / f"{label}_funnel_metrics.csv"
    missed_out = output_dir / f"{label}_missed_review.csv"
    summary_out = output_dir / f"{label}_summary.txt"

    strategy_metrics.to_csv(strategy_out, index=False)
    funnel_metrics.to_csv(funnel_out, index=False)
    missed_review.to_csv(missed_out, index=False)

    summary = render_weekly_summary(strategy_metrics, funnel_metrics, missed_review, start_ts, end_ts)
    summary_out.write_text(summary, encoding="utf-8")

    print(f"Wrote: {strategy_out}")
    print(f"Wrote: {funnel_out}")
    print(f"Wrote: {missed_out}")
    print(f"Wrote: {summary_out}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate period summary outputs from bot CSV files.")
    parser.add_argument("--data-dir", type=Path, default=Path("."), help="Project root containing data/ and reports/ directories.")
    parser.add_argument("--output-dir", type=Path, default=None, help="Where to write outputs. Defaults to data dir/reports/<label>.")
    parser.add_argument("--end-date", type=str, default=None, help="Optional end date in YYYY-MM-DD. Defaults to latest timestamp found.")
    parser.add_argument("--days", type=int, default=7, help="Window size in days.")
    parser.add_argument("--label", type=str, default="weekly", help="Report label prefix in output filenames.")
    args = parser.parse_args()
    generate_period_report(data_dir=args.data_dir, days=args.days, label=args.label, output_dir=args.output_dir, end_date=args.end_date)


if __name__ == "__main__":
    main()
