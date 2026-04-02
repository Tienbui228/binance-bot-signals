import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional


CANONICAL_STAGE_TO_SNAPSHOT_STAGE = {
    "pre_pending": "pre_pending",
    "pending_open": "pending",
    "entry_or_confirm": "confirmed",
    "case_close": "closed",
}


SNAPSHOT_STAGE_TO_CANONICAL_STAGE = {
    "pre_pending": "pre_pending",
    "pending": "pending_open",
    "confirmed": "entry_or_confirm",
    "closed": "case_close",
}




CANONICAL_STAGE_TO_RUNTIME_STAGE = {
    "pre_pending": "pre_pending",
    "pending_open": "pending",
    "entry_or_confirm": "confirmed",
    "case_close": "closed",
}


def _stage_aliases(scanner, canonical_stage: str):
    runtime_stage = CANONICAL_STAGE_TO_RUNTIME_STAGE.get(canonical_stage, canonical_stage)
    aliases = []
    for stage_name in (canonical_stage, runtime_stage):
        if stage_name and stage_name not in aliases:
            aliases.append(stage_name)
    return aliases


def _append_snapshot_index_row(scanner, row: dict):
    scanner.append_csv(scanner.snapshot_index_file, row, fieldnames=scanner.snapshot_fields)


def _append_snapshot_index_bridge_rows(scanner, *, canonical_stage: str, snapshot_stage: str, base_row: dict):
    stages = []
    for s in (canonical_stage, snapshot_stage):
        if s and s not in stages:
            stages.append(s)
    for stage_name in stages:
        row = dict(base_row)
        row["stage"] = stage_name
        _append_snapshot_index_row(scanner, row)


def _sanitize_filename(scanner, value: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", value).strip("_") or "snapshot"


def _snapshot_base_name(scanner, *, symbol: str, side: str, strategy: str, stage: str, ts_ms: int, pending_id: str = "", signal_id: str = "") -> str:
    identity = pending_id or signal_id
    parts = [symbol, side, strategy]
    if identity:
        parts.append(identity)
    parts.extend([stage, str(ts_ms)])
    return _sanitize_filename(scanner, "_".join([p for p in parts if p]))


def _draw_candles(scanner, ax, bars: List[Dict], candle_width: float = 0.6):
    for idx, bar in enumerate(bars):
        op = float(bar["open"])
        cl = float(bar["close"])
        hi = float(bar["high"])
        lo = float(bar["low"])
        ax.vlines(idx, lo, hi, linewidth=1.0)
        lower = min(op, cl)
        height = max(abs(cl - op), max((hi - lo) * 0.003, 1e-12))
        try:
            import matplotlib.patches as mpatches
            rect = mpatches.Rectangle((idx - candle_width / 2, lower), candle_width, height, fill=False, linewidth=1.0)
            ax.add_patch(rect)
        except Exception:
            pass


def _mark_signal_bar(scanner, ax, bars: List[Dict], ts_ms: int):
    if not bars:
        return
    target_idx = None
    for idx, bar in enumerate(bars):
        if int(bar.get("open_time", 0)) == int(ts_ms):
            target_idx = idx
            break
    if target_idx is None:
        candidates = [i for i, bar in enumerate(bars) if int(bar.get("open_time", 0)) <= int(ts_ms)]
        if candidates:
            target_idx = candidates[-1]
    if target_idx is None:
        target_idx = len(bars) - 1
    ax.axvline(target_idx, linestyle="--", linewidth=1.0)


def _placeholder_snapshot_path(scanner, symbol: str, side: str, strategy: str, stage: str, ts_ms: int, pending_id: str = "", signal_id: str = "") -> Path:
    day_dir = scanner.snapshots_dir / datetime.now(timezone.utc).strftime("%Y%m%d")
    day_dir.mkdir(parents=True, exist_ok=True)
    base_name = _snapshot_base_name(scanner, symbol=symbol, side=side, strategy=strategy, stage=f"{stage}_placeholder", ts_ms=ts_ms, pending_id=pending_id, signal_id=signal_id)
    return day_dir / f"{base_name}.png"


def _create_placeholder_snapshot(scanner, *, symbol: str, side: str, strategy: str, stage: str, ts_ms: int,
                                 note: str, reason: str, signal_id: str = "", pending_id: str = "",
                                 outcome: str = "") -> str:
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        file_path = _placeholder_snapshot_path(scanner, symbol, side, strategy, stage, ts_ms, pending_id=pending_id, signal_id=signal_id)
        fig, ax = plt.subplots(1, 1, figsize=(14, 4))
        ax.axis("off")
        ax.text(0.01, 0.85, f"{symbol} {side} | {strategy}", fontsize=12, transform=ax.transAxes)
        ax.text(0.01, 0.65, f"stage={stage}", fontsize=11, transform=ax.transAxes)
        ax.text(0.01, 0.45, f"capture_failed: {reason}", fontsize=10, transform=ax.transAxes)
        if note:
            ax.text(0.01, 0.25, f"note={note[:180]}", fontsize=10, transform=ax.transAxes)
        fig.tight_layout()
        fig.savefig(file_path, dpi=int(scanner.cfg.get("review_snapshots", {}).get("dpi", 100)), bbox_inches="tight")
        plt.close(fig)
    except Exception:
        file_path = _placeholder_snapshot_path(scanner, symbol, side, strategy, stage, ts_ms, pending_id=pending_id, signal_id=signal_id)
        file_path.write_text(
            f"placeholder snapshot\nsymbol={symbol}\nside={side}\nstrategy={strategy}\nstage={stage}\nreason={reason}\nnote={note}\n",
            encoding="utf-8",
        )

    canonical_stage = SNAPSHOT_STAGE_TO_CANONICAL_STAGE.get(stage, stage)
    base_row = {
        "snapshot_ts_ms": int(time.time() * 1000),
        "symbol": symbol,
        "side": side,
        "strategy": strategy,
        "signal_id": signal_id,
        "pending_id": pending_id,
        "setup_id": pending_id or signal_id,
        "outcome": outcome,
        "image_path": str(file_path),
        "context_interval": "",
        "entry_interval": "",
        "breakout_level": "",
        "entry_ref": "",
        "stop": "",
        "tp1": "",
        "tp2": "",
        "note": f"capture_failed: {reason} | {note}".strip(" |"),
    }
    _append_snapshot_index_bridge_rows(scanner, canonical_stage=canonical_stage, snapshot_stage=stage, base_row=base_row)
    return str(file_path)


def save_review_snapshot(scanner, symbol: str, side: str, strategy: str, stage: str, ts_ms: int,
                         breakout_level: Optional[float] = None, entry_ref: Optional[float] = None,
                         stop: Optional[float] = None, tp1: Optional[float] = None, tp2: Optional[float] = None,
                         signal_id: str = "", pending_id: str = "", outcome: str = "", note: str = "") -> Optional[str]:
    cfg = scanner.cfg.get("review_snapshots", {})
    if not cfg.get("enabled", False):
        return None

    stage_toggle = {
        "pre_pending": cfg.get("save_pre_pending", True),
        "pending": cfg.get("save_pending", True),
        "confirmed": cfg.get("save_confirmed", True),
        "closed": cfg.get("save_closed", True),
    }
    stage_key = stage.split(":", 1)[0]
    if not stage_toggle.get(stage_key, True):
        return None

    # Simulation uses fake symbols by design. Do not call Binance for them.
    if symbol.startswith("SIM"):
        return _create_placeholder_snapshot(
            scanner,
            symbol=symbol,
            side=side,
            strategy=strategy,
            stage=stage,
            ts_ms=ts_ms,
            note=note,
            reason="simulation_symbol_no_live_klines",
            signal_id=signal_id,
            pending_id=pending_id,
            outcome=outcome,
        )

    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except Exception as e:
        print(f"[snapshot warn] matplotlib unavailable: {e}")
        return _create_placeholder_snapshot(
            scanner,
            symbol=symbol,
            side=side,
            strategy=strategy,
            stage=stage,
            ts_ms=ts_ms,
            note=note,
            reason=f"matplotlib_unavailable:{e}",
            signal_id=signal_id,
            pending_id=pending_id,
            outcome=outcome,
        )

    try:
        context_interval = scanner.cfg["scanner"].get("interval_1h", "1h")
        mid_interval = scanner.cfg["scanner"].get("interval_15m", "15m")
        entry_interval = scanner.cfg["scanner"].get("interval_5m", "5m")
        context_bars_n = int(cfg.get("context_1h_bars", 24))
        mid_bars_n = int(cfg.get("context_15m_bars", 32))
        entry_bars_n = int(cfg.get("entry_5m_bars", 48))

        context_bars = scanner.klines(symbol, context_interval, limit=context_bars_n + 2)[:-1]
        mid_bars = scanner.klines(symbol, mid_interval, limit=mid_bars_n + 2)[:-1]
        entry_bars = scanner.klines(symbol, entry_interval, limit=entry_bars_n + 2)[:-1]
        if len(context_bars) < 6 or len(mid_bars) < 8 or len(entry_bars) < 8:
            return _create_placeholder_snapshot(
                scanner,
                symbol=symbol,
                side=side,
                strategy=strategy,
                stage=stage,
                ts_ms=ts_ms,
                note=note,
                reason=f"insufficient_bars:1h={len(context_bars)}|15m={len(mid_bars)}|5m={len(entry_bars)}",
                signal_id=signal_id,
                pending_id=pending_id,
                outcome=outcome,
            )

        day_dir = scanner.snapshots_dir / datetime.now(timezone.utc).strftime("%Y%m%d")
        day_dir.mkdir(parents=True, exist_ok=True)
        base_name = _snapshot_base_name(scanner, symbol=symbol, side=side, strategy=strategy, stage=stage, ts_ms=ts_ms, pending_id=pending_id, signal_id=signal_id)
        file_path = day_dir / f"{base_name}.png"

        fig, axes = plt.subplots(3, 1, figsize=(15, 11), constrained_layout=False)
        ctx_ax, mid_ax, ent_ax = axes
        _draw_candles(scanner, ctx_ax, context_bars)
        _draw_candles(scanner, mid_ax, mid_bars)
        _draw_candles(scanner, ent_ax, entry_bars)
        _mark_signal_bar(scanner, ctx_ax, context_bars, ts_ms)
        _mark_signal_bar(scanner, mid_ax, mid_bars, ts_ms)
        _mark_signal_bar(scanner, ent_ax, entry_bars, ts_ms)

        for ax, bars, label in ((ctx_ax, context_bars, context_interval), (mid_ax, mid_bars, mid_interval), (ent_ax, entry_bars, entry_interval)):
            if breakout_level is not None:
                ax.axhline(float(breakout_level), linestyle='--', linewidth=1.0)
            if entry_ref is not None:
                ax.axhline(float(entry_ref), linestyle='-.', linewidth=1.0)
            if stop is not None:
                ax.axhline(float(stop), linestyle=':', linewidth=1.0)
            if tp1 is not None:
                ax.axhline(float(tp1), linestyle=':', linewidth=1.0)
            if tp2 is not None:
                ax.axhline(float(tp2), linestyle=':', linewidth=1.0)
            ax.set_title(f"{symbol} {side} | {strategy} | {label} | {stage}")
            ax.set_xlabel("bars")
            ax.set_ylabel("price")
            if bars:
                closes = [b["close"] for b in bars]
                lows = [b["low"] for b in bars]
                highs = [b["high"] for b in bars]
                lo = min(lows)
                hi = max(highs)
                pad = max((hi - lo) * 0.05, max(abs(c) for c in closes) * 0.002)
                ax.set_ylim(lo - pad, hi + pad)

        fig.suptitle(f"{symbol} {side} {stage} | {note[:180]}")
        try:
            fig.tight_layout(rect=[0, 0.02, 1, 0.965])
        except Exception:
            pass
        fig.savefig(file_path, dpi=int(cfg.get("dpi", 100)), bbox_inches="tight")
        plt.close(fig)

        canonical_stage = SNAPSHOT_STAGE_TO_CANONICAL_STAGE.get(stage, stage)
        base_row = {
            "snapshot_ts_ms": int(time.time() * 1000),
            "symbol": symbol,
            "side": side,
            "strategy": strategy,
            "signal_id": signal_id,
            "pending_id": pending_id,
            "setup_id": pending_id or signal_id,
            "outcome": outcome,
            "image_path": str(file_path),
            "context_interval": context_interval,
            "entry_interval": f"{mid_interval}|{entry_interval}",
            "breakout_level": "" if breakout_level is None else f"{breakout_level}",
            "entry_ref": "" if entry_ref is None else f"{entry_ref}",
            "stop": "" if stop is None else f"{stop}",
            "tp1": "" if tp1 is None else f"{tp1}",
            "tp2": "" if tp2 is None else f"{tp2}",
            "note": note,
        }
        _append_snapshot_index_bridge_rows(scanner, canonical_stage=canonical_stage, snapshot_stage=stage, base_row=base_row)
        return str(file_path)
    except Exception as e:
        print(f"[snapshot warn] {symbol} {side} {stage}: {e}")
        return _create_placeholder_snapshot(
            scanner,
            symbol=symbol,
            side=side,
            strategy=strategy,
            stage=stage,
            ts_ms=ts_ms,
            note=note,
            reason=f"snapshot_exception:{e}",
            signal_id=signal_id,
            pending_id=pending_id,
            outcome=outcome,
        )


def _local_day_from_ms(scanner, ts_ms: int) -> str:
    return datetime.fromtimestamp(max(int(ts_ms), 0) / 1000.0, tz=timezone.utc).astimezone().strftime("%Y-%m-%d")


def _bar_interval_ms_for_strategy(scanner, strategy: str) -> int:
    if strategy == "short_exhaustion_retest":
        return 15 * 60 * 1000
    return 5 * 60 * 1000


def _review_case_day(scanner, created_ts_ms: int) -> str:
    try:
        from zoneinfo import ZoneInfo
        tz = ZoneInfo(scanner.review_case_timezone)
    except Exception:
        tz = timezone.utc
    return datetime.fromtimestamp(max(int(created_ts_ms), 0) / 1000.0, tz=timezone.utc).astimezone(tz).strftime("%Y-%m-%d")


def _review_register_stage(scanner, case_day: str, case_id: str, stage: str, image_path: Optional[str], note: str = ""):
    if not scanner.review_runtime:
        return
    last_error = None
    for stage_name in _stage_aliases(scanner, stage):
        try:
            scanner.review_runtime.register_stage_image(case_day, case_id, stage_name, image_path, note=note)
        except Exception as e:
            last_error = e
    if last_error is not None:
        print(f"[review_case warn] register_stage {case_id} {stage}: {last_error}")


def _capture_and_register_case_stage(scanner, pending_row: Dict, stage: str, ts_ms: int, note: str = "", signal_row: Optional[Dict] = None):
    if not scanner.review_runtime:
        return None
    try:
        case_day = _review_case_day(scanner, int(pending_row.get("created_ts_ms") or 0))
        case_id = pending_row.get("pending_id") or pending_row.get("setup_id", "")
        strategy = pending_row.get("strategy", "")
        try:
            if case_id and not scanner.review_runtime._load_case(case_day, case_id):
                scanner.review_runtime.ensure_case(pending_row)
        except Exception:
            pass
        image_path = save_review_snapshot(
            scanner,
            symbol=pending_row.get("symbol", ""),
            side=pending_row.get("side", ""),
            strategy=strategy,
            stage=CANONICAL_STAGE_TO_SNAPSHOT_STAGE[stage],
            ts_ms=int(ts_ms),
            breakout_level=float(pending_row.get("breakout_level") or 0.0) if pending_row.get("breakout_level") not in (None, "") else None,
            entry_ref=float((signal_row or {}).get("entry_ref") or 0.0) if (signal_row or {}).get("entry_ref") not in (None, "") else None,
            stop=float((signal_row or {}).get("stop") or 0.0) if (signal_row or {}).get("stop") not in (None, "") else None,
            tp1=float((signal_row or {}).get("tp1") or 0.0) if (signal_row or {}).get("tp1") not in (None, "") else None,
            tp2=float((signal_row or {}).get("tp2") or 0.0) if (signal_row or {}).get("tp2") not in (None, "") else None,
            signal_id=(signal_row or {}).get("signal_id", ""),
            pending_id=case_id,
            outcome=(signal_row or {}).get("status", ""),
            note=note,
        )
        register_note = note
        if image_path and "placeholder" in Path(image_path).name:
            register_note = f"capture_failed | {note}".strip(" |")
        _review_register_stage(scanner, case_day, case_id, stage, image_path, note=register_note)
        return image_path
    except Exception as e:
        print(f"[review_case warn] capture_stage {stage} {pending_row.get('pending_id','')}: {e}")
        return None




def _find_signal_row_by_setup(scanner, setup_id: str, case_day: str) -> Optional[Dict]:
    if not setup_id:
        return None
    month_file = scanner.signals_dir / f"signals_{case_day[:7]}.csv"
    candidates = []
    if month_file.exists():
        candidates.append(month_file)
    else:
        try:
            candidates.extend(scanner._iter_table_files("signals"))
        except Exception:
            pass
    for path in candidates:
        try:
            rows = scanner.read_csv(path)
        except Exception:
            continue
        for row in reversed(rows):
            row_setup = row.get("setup_id") or row.get("signal_id", "")
            if row_setup == setup_id:
                return row
    return None




def _repair_confirmed_review_semantics(scanner, row: Dict) -> Dict:
    row = dict(row)
    status = str(row.get("status", "") or "").upper()
    if status != "CONFIRMED":
        return row
    confirmed_raw = str(row.get("confirmed_ts_ms", "") or "").strip()
    if not confirmed_raw:
        derived_ts = 0
        for key in ("sent_ts_ms", "closed_ts_ms", "timestamp_ms", "signal_open_time"):
            raw = str(row.get(key, "") or "").strip()
            if not raw:
                continue
            try:
                derived_ts = int(float(raw))
                if derived_ts > 0:
                    break
            except Exception:
                continue
        if derived_ts > 0:
            row["confirmed_ts_ms"] = str(derived_ts)
            confirmed_raw = row["confirmed_ts_ms"]
    if confirmed_raw:
        row["is_confirmed"] = "Y"
        if not str(row.get("close_reason", "") or "").strip():
            row["close_reason"] = "signal confirmed"
        if not str(row.get("send_decision", "") or "").strip():
            row["send_decision"] = "UNDECIDED"
        row["closed_ts_ms"] = "not_reached_yet"
        row["close_capture_basis"] = "not_due_yet"
        row["close_anchor_time_ms"] = "not_reached_yet"
        row["close_trigger_detail"] = row.get("close_trigger_detail") or "signal_confirmed"
        if hasattr(scanner, "_derive_review_integrity_fields"):
            try:
                row.update(scanner._derive_review_integrity_fields(row))
            except Exception:
                pass
    return row

def _backfill_review_stage_evidence_for_day(scanner, case_day: str):
    if not scanner.review_runtime:
        return
    now_ms = int(time.time() * 1000)
    pending_path = scanner.pending_dir / f"pending_{case_day}.csv"
    try:
        day_rows = scanner.read_csv(pending_path)
    except Exception as e:
        print(f"[review_case warn] backfill read pending failed: {e}")
        return

    rows = []
    changed_any = False
    for idx, row in enumerate(day_rows):
        try:
            created_ms = int(float(row.get("created_ts_ms") or 0)) if row.get("created_ts_ms") not in (None, "") else 0
        except Exception:
            created_ms = 0
        if created_ms and _review_case_day(scanner, created_ms) == case_day:
            repaired = _repair_confirmed_review_semantics(scanner, row)
            if repaired != row:
                day_rows[idx] = repaired
                changed_any = True
            rows.append(repaired)

    if changed_any:
        try:
            scanner.write_csv(pending_path, day_rows, fieldnames=scanner.pending_fields)
            print(f"[review_case] repaired confirmed semantics for {case_day}: rows={sum(1 for r in rows if str(r.get('status','')).upper() == 'CONFIRMED')}")
        except Exception as e:
            print(f"[review_case warn] persist confirmed repair failed: {e}")

    if not rows:
        return

    print(f"[review_case] backfill evidence for {case_day}: cases={len(rows)}")
    for row in rows:
        try:
            _review_register_pending_case(scanner, row)

            setup_id = row.get("setup_id") or row.get("pending_id", "")
            signal_row = _find_signal_row_by_setup(scanner, setup_id, case_day)

            confirmed_ts = int(float(row.get("confirmed_ts_ms") or 0)) if row.get("confirmed_ts_ms") not in (None, "") else 0
            if confirmed_ts > 0:
                entry_note = row.get("close_trigger_detail") or row.get("reason") or "confirmed"
                _capture_and_register_case_stage(scanner, row, "entry_or_confirm", confirmed_ts, note=entry_note, signal_row=signal_row)

            status = str(row.get("status", "") or "").upper()
            created_ms = int(float(row.get("created_ts_ms") or 0)) if row.get("created_ts_ms") not in (None, "") else 0
            due_ms = created_ms + scanner.review_case_fallback_close_hours * 3600 * 1000 if created_ms else 0
            close_ts = int(float(row.get("closed_ts_ms") or 0)) if row.get("closed_ts_ms") not in (None, "") else 0

            is_true_close = status not in {"PENDING", "CONFIRMED"} and str(row.get("close_capture_basis", "") or "") == "true_close"
            if close_ts > 0 and is_true_close:
                close_note = f"{(row.get('close_reason') or status)} | closed_ts_ms={close_ts}"
                _capture_and_register_case_stage(scanner, row, "case_close", close_ts, note=close_note, signal_row=signal_row)
            elif due_ms and now_ms >= due_ms:
                close_note = f"fallback_case_close_after_{scanner.review_case_fallback_close_hours}h"
                _capture_and_register_case_stage(scanner, row, "case_close", due_ms, note=close_note, signal_row=signal_row)
        except Exception as e:
            print(f"[review_case warn] backfill stage evidence {row.get('pending_id','')}: {e}")

def _review_register_pending_case(scanner, pending_row: Dict):
    if not scanner.review_runtime:
        return
    try:
        scanner.review_runtime.ensure_case(pending_row)
        bar_ms = _bar_interval_ms_for_strategy(scanner, pending_row.get("strategy", ""))
        signal_open_ms = int(pending_row.get("signal_open_time") or 0)
        created_ms = int(pending_row.get("created_ts_ms") or 0)
        pre_anchor = signal_open_ms or created_ms
        pre_ts = max(pre_anchor - bar_ms, 0)
        _capture_and_register_case_stage(scanner, pending_row, "pre_pending", pre_ts, note="pre_pending context")
        pending_open_ts = created_ms if created_ms > 0 else signal_open_ms
        _capture_and_register_case_stage(scanner, pending_row, "pending_open", pending_open_ts, note=f"pending_open | created_ts_ms={created_ms}")
    except Exception as e:
        print(f"[review_case warn] register_pending_case {pending_row.get('pending_id','')}: {e}")


def collect_due_case_close_fallbacks(scanner):
    if not scanner.review_runtime:
        return
    now_ms = int(time.time() * 1000)
    rows = scanner.read_csv(scanner.pending_file)
    processed = 0
    max_per_round = int(scanner.cfg.get("review_case_system", {}).get("fallback_max_per_round", 6))
    for row in rows:
        if processed >= max_per_round:
            break
        status = str(row.get("status", "") or "").upper()
        created_ms = int(float(row.get("created_ts_ms") or 0)) if row.get("created_ts_ms") not in (None, "") else 0
        if not created_ms:
            continue
        due_ms = created_ms + scanner.review_case_fallback_close_hours * 3600 * 1000
        case_day = _review_case_day(scanner, created_ms)
        case_id = row.get("pending_id") or row.get("setup_id", "")
        case_meta = None
        try:
            case_meta = scanner.review_runtime._load_case(case_day, case_id)
        except Exception:
            case_meta = None
        already_has = bool(case_meta and getattr(case_meta, "has_case_close_image", "N") == "Y")
        if already_has:
            continue
        if status == "PENDING" and now_ms >= due_ms:
            note = f"fallback_case_close_after_{scanner.review_case_fallback_close_hours}h"
            _capture_and_register_case_stage(scanner, row, "case_close", now_ms, note=note)
            processed += 1
        elif status not in {"PENDING", "CONFIRMED"} and str(row.get("close_capture_basis", "") or "") == "true_close":
            close_ts = int(float(row.get("closed_ts_ms") or now_ms)) if row.get("closed_ts_ms") not in (None, "") else now_ms
            note = f"{row.get('close_reason', status)} | closed_ts_ms={close_ts}"
            _capture_and_register_case_stage(scanner, row, "case_close", close_ts, note=note)
            processed += 1


def build_daily_review_pack(scanner, case_day: str, debug: bool = False):
    _backfill_review_stage_evidence_for_day(scanner, case_day)
    if not scanner.review_case_system_enabled:
        print("[review_case] disabled in config")
        return False
    if not scanner.review_builder_script.exists():
        print(f"[review_case warn] builder script not found: {scanner.review_builder_script}")
        return False

    pending_path = scanner.pending_dir / f"pending_{case_day}.csv"

    try:
        day_rows = scanner.read_csv(pending_path)
        if day_rows:
            enriched = [scanner._enrich_pending_row_for_daily_review(r) for r in day_rows]
            scanner.write_csv(pending_path, enriched, fieldnames=scanner.pending_fields)
    except Exception as e:
        print(f"[review_case warn] enrich pending rows before build failed: {e}")

    signals_path = scanner.signals_dir / f"signals_{case_day[:7]}.csv"
    results_path = scanner.results_dir / f"results_{case_day[:7]}.csv"

    out_dir = scanner.review_daily_exports_dir if not debug else scanner.review_daily_exports_dir / "debug"

    cmd = [
        sys.executable,
        str(scanner.review_builder_script),
        "--date", case_day,
        "--workspace", str(scanner.review_case_workspace),
        "--pending", str(pending_path),
        "--signals", str(signals_path),
        "--results", str(results_path),
        "--snapshot-index", str(scanner.snapshot_index_file),
        "--out-dir", str(out_dir),
        "--tz", scanner.review_case_timezone,
        "--fallback-hours", str(scanner.review_case_fallback_close_hours),
    ]
    try:
        print("[review_case] building daily pack:", " ".join(cmd))
        res = subprocess.run(cmd, capture_output=True, text=True, check=False)
        if res.stdout:
            print(res.stdout.strip())
        if res.stderr:
            print(res.stderr.strip())
        if res.returncode != 0:
            print(f"[review_case warn] builder returned code {res.returncode}")
            return False
        return True
    except Exception as e:
        print(f"[review_case warn] build daily pack failed: {e}")
        return False
