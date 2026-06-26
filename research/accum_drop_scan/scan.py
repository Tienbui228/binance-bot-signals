"""
Accumulation-after-drop pattern scan (forensic, READ-ONLY).

Build per spec from owner (2026-06-26). Do NOT import oi_scanner.py.
Do NOT write to bot data/. Outputs land in research/accum_drop_scan/.

Methodology refs:
  - docs/SHORT_SIGNAL_THESIS_v1.md  (§3 works def, §5 control, ±3h rule)
  - docs/STRATEGY_SPEC_long_accumulation_continuation_V1_3.md (§5 field shape)

Hindsight pattern screen — NOT an entry rule, NOT validated edge.
"""

import argparse
import csv
import json
import math
import os
import statistics
import sys
import time
from collections import OrderedDict
from typing import Dict, List, Optional, Tuple

import requests


# ── Config (locked) ─────────────────────────────────────────────────────────

MC_CAP_USD        = 1_000_000_000
LOOKBACK_HOURS    = 720
DROP_MIN_PCT      = 30
POS_RISE_MIN_PP   = 5        # 0.05 fraction
ACCT_RISE_MIN_PP  = 5
RISE_MIN_HOURS    = 48
FORWARD_MIN_DAYS  = 7
CONT_UP_PCT       = 15
CONT_DOWN_PCT     = 15
PREPUMP_WIN_HOURS = 168
PREPUMP_BUFFER_H  = 24
PREPUMP_MIN_H     = 48
PRICE_CROSS_MAX_DELTA_H = 3   # ±3h rule for point-lookups

FAPI = "https://fapi.binance.com"
HTTP_TIMEOUT = 20
HTTP_DELAY   = 0.15

ROOT       = os.path.dirname(os.path.abspath(__file__))
CACHE_DIR  = os.path.join(ROOT, "cache")
WATCHLIST  = os.path.join(ROOT, "watchlist_lowcap.json")
MATCHES_CSV= os.path.join(ROOT, "matches.csv")
CONTROL_CSV= os.path.join(ROOT, "control.csv")
UNIVERSE_LOG= os.path.join(ROOT, "universe_filtered.json")

MS_HOUR = 3600 * 1000
MS_DAY  = 24 * MS_HOUR


# ── Universe loader ─────────────────────────────────────────────────────────

def load_universe() -> Tuple[List[Dict], List[str]]:
    """Return (kept, dropped_nonascii). kept rows have keys: symbol, mc_unknown."""
    with open(WATCHLIST, "r", encoding="utf-8") as fh:
        raw = json.load(fh)

    kept: List[Dict] = []
    dropped_nonascii: List[str] = []
    for entry in raw:
        sym = entry.get("symbol", "")
        try:
            sym.encode("ascii")
        except UnicodeEncodeError:
            dropped_nonascii.append(sym)
            continue
        mc = entry.get("market_cap_usd")
        try:
            mc_f = float(mc) if mc is not None else 0.0
        except (TypeError, ValueError):
            mc_f = 0.0
        kept.append({
            "symbol": sym,
            "mc_unknown": (mc_f == 0.0),
        })
    return kept, dropped_nonascii


# ── HTTP + cache ────────────────────────────────────────────────────────────

_session = requests.Session()
_session.headers.update({"User-Agent": "accum-drop-scan/1.0"})


def _cache_path(endpoint: str, symbol: str) -> str:
    d = os.path.join(CACHE_DIR, endpoint)
    os.makedirs(d, exist_ok=True)
    return os.path.join(d, f"{symbol}.json")


def _cache_load(endpoint: str, symbol: str) -> Optional[List]:
    p = _cache_path(endpoint, symbol)
    if not os.path.exists(p):
        return None
    try:
        with open(p, "r", encoding="utf-8") as fh:
            return json.load(fh).get("data")
    except Exception:
        return None


def _cache_save(endpoint: str, symbol: str, data: List) -> None:
    p = _cache_path(endpoint, symbol)
    tmp = p + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump({"fetched_at_ms": int(time.time() * 1000), "data": data}, fh)
    os.replace(tmp, p)


def _http_get(path: str, params: Dict) -> Optional[List]:
    url = FAPI + path
    try:
        r = _session.get(url, params=params, timeout=HTTP_TIMEOUT)
        if r.status_code == 200:
            return r.json()
        # 400 = bad symbol / delisted; 4xx other = log and skip
        return None
    except Exception:
        return None
    finally:
        time.sleep(HTTP_DELAY)


def _fetch_paged(path: str, symbol: str, period_or_interval_key: str,
                 period_or_interval_val: str, total_hours: int,
                 endpoint_cache_name: str) -> List[Dict]:
    """Fetch hourly series via 2 paging calls; merge+dedup by timestamp.

    Returns list sorted ascending by timestamp (key 'timestamp' or 'open_time').
    Each item is the raw dict/list returned by Binance.
    """
    cached = _cache_load(endpoint_cache_name, symbol)
    if cached is not None:
        return cached

    now_ms = int(time.time() * 1000)
    start_ms = now_ms - total_hours * MS_HOUR
    mid_ms = start_ms + 500 * MS_HOUR

    merged: List[Dict] = []
    for chunk_start, chunk_end in [(start_ms, mid_ms - 1), (mid_ms, now_ms)]:
        params = {
            "symbol": symbol,
            period_or_interval_key: period_or_interval_val,
            "limit": 500,
            "startTime": chunk_start,
            "endTime":   chunk_end,
        }
        resp = _http_get(path, params)
        if resp is None:
            continue
        merged.extend(resp)

    # Dedup by ts key
    seen = set()
    deduped: List[Dict] = []
    for item in merged:
        if isinstance(item, list):
            ts = item[0]
        else:
            ts = item.get("timestamp") or item.get("openTime") or item.get("open_time")
        if ts is None:
            continue
        ts = int(ts)
        if ts in seen:
            continue
        seen.add(ts)
        deduped.append(item)

    # Sort ascending
    def _ts_of(it):
        if isinstance(it, list):
            return int(it[0])
        return int(it.get("timestamp") or it.get("openTime") or it.get("open_time"))
    deduped.sort(key=_ts_of)

    _cache_save(endpoint_cache_name, symbol, deduped)
    return deduped


# ── Endpoint wrappers ───────────────────────────────────────────────────────

def fetch_klines(symbol: str) -> List[Tuple[int, float, float, float]]:
    """Returns sorted list of (open_time_ms, high, low, close)."""
    raw = _fetch_paged("/fapi/v1/klines", symbol, "interval", "1h",
                       LOOKBACK_HOURS, "klines")
    out: List[Tuple[int, float, float, float]] = []
    for k in raw:
        try:
            out.append((int(k[0]), float(k[2]), float(k[3]), float(k[4])))
        except (TypeError, ValueError, IndexError):
            continue
    return out


def _fetch_ratio_series(symbol: str, path: str, cache_name: str,
                       field: str) -> List[Tuple[int, float]]:
    raw = _fetch_paged(path, symbol, "period", "1h",
                       LOOKBACK_HOURS, cache_name)
    out: List[Tuple[int, float]] = []
    for it in raw:
        try:
            ts = int(it.get("timestamp"))
            v  = float(it.get(field))
            out.append((ts, v))
        except (TypeError, ValueError):
            continue
    return out


def fetch_top_pos(symbol: str) -> List[Tuple[int, float]]:
    return _fetch_ratio_series(
        symbol, "/futures/data/topLongShortPositionRatio",
        "topPosRatio", "longAccount")


def fetch_top_acct(symbol: str) -> List[Tuple[int, float]]:
    return _fetch_ratio_series(
        symbol, "/futures/data/topLongShortAccountRatio",
        "topAcctRatio", "longAccount")


def fetch_retail(symbol: str) -> List[Tuple[int, float]]:
    return _fetch_ratio_series(
        symbol, "/futures/data/globalLongShortAccountRatio",
        "globalRetail", "longAccount")


def fetch_taker(symbol: str) -> List[Tuple[int, float]]:
    return _fetch_ratio_series(
        symbol, "/futures/data/takerlongshortRatio",
        "takerRatio", "buySellRatio")


def fetch_oi(symbol: str) -> List[Tuple[int, float]]:
    raw = _fetch_paged("/futures/data/openInterestHist", symbol, "period", "1h",
                       LOOKBACK_HOURS, "oiHist")
    out: List[Tuple[int, float]] = []
    for it in raw:
        try:
            ts = int(it.get("timestamp"))
            v  = float(it.get("sumOpenInterestValue"))
            out.append((ts, v))
        except (TypeError, ValueError):
            continue
    return out


def fetch_funding(symbol: str) -> List[Tuple[int, float]]:
    """Funding: 90 events × 8h ≈ 30d. 1 call, no paging."""
    cached = _cache_load("funding", symbol)
    if cached is None:
        params = {"symbol": symbol, "limit": 90}
        resp = _http_get("/fapi/v1/fundingRate", params)
        if resp is None:
            resp = []
        _cache_save("funding", symbol, resp)
        cached = resp
    out: List[Tuple[int, float]] = []
    for it in cached:
        try:
            ts = int(it.get("fundingTime"))
            v  = float(it.get("fundingRate"))
            out.append((ts, v))
        except (TypeError, ValueError):
            continue
    out.sort(key=lambda x: x[0])
    return out


def fetch_btc_daily() -> List[Tuple[int, float]]:
    """BTC daily close, ~40 bars."""
    cached = _cache_load("btc_daily", "BTCUSDT")
    if cached is None:
        params = {"symbol": "BTCUSDT", "interval": "1d", "limit": 40}
        resp = _http_get("/fapi/v1/klines", params)
        if resp is None:
            resp = []
        _cache_save("btc_daily", "BTCUSDT", resp)
        cached = resp
    out: List[Tuple[int, float]] = []
    for k in cached:
        try:
            out.append((int(k[0]), float(k[4])))
        except (TypeError, ValueError, IndexError):
            continue
    out.sort(key=lambda x: x[0])
    return out


# ── Point lookup (±3h rule, no nearest-fill across gap) ─────────────────────

def point_lookup(series: List[Tuple[int, float]], ts_target: int,
                 max_delta_h: int = PRICE_CROSS_MAX_DELTA_H) -> Optional[float]:
    """Return value if any series point is within ±max_delta_h of ts_target.
    Pick the CLOSEST point. Return None if nothing within window.

    'Closest' is well-defined: we never extrapolate across a gap because the
    candidate must be inside ±N hours. This is NOT nearest-fill across gap.
    """
    if not series:
        return None
    max_delta_ms = max_delta_h * MS_HOUR
    # binary search would be faster; series ≤ 720 so linear is fine
    best_val = None
    best_delta = None
    for ts, v in series:
        d = abs(ts - ts_target)
        if d > max_delta_ms:
            continue
        if best_delta is None or d < best_delta:
            best_delta = d
            best_val = v
    return best_val


# ── Pattern detection ───────────────────────────────────────────────────────

def detect_pattern(klines: List[Tuple[int, float, float, float]],
                   pos: List[Tuple[int, float]],
                   acct: List[Tuple[int, float]],
                   now_ms: int) -> Dict:
    """Return dict with keys:
       t_high, t_bottom, drop_pct, t_sig (Optional), reason_no_sig (str|None),
       price_at_sig (Optional), price_bottom, window_high
    """
    out = {
        "t_high": None, "t_bottom": None, "drop_pct": None,
        "t_sig": None, "reason_no_sig": None,
        "price_at_sig": None, "price_bottom": None, "window_high": None,
    }
    if not klines:
        out["reason_no_sig"] = "no_klines"
        return out

    # window_high = max(high)
    max_high = -1.0
    t_high = None
    for ts, hi, lo, cl in klines:
        if hi > max_high:
            max_high = hi
            t_high = ts
    out["window_high"] = max_high
    out["t_high"] = t_high

    # bottom = min(close) AFTER t_high
    after = [(ts, hi, lo, cl) for (ts, hi, lo, cl) in klines if ts > t_high]
    if not after:
        out["reason_no_sig"] = "no_bars_after_high"
        return out
    min_close = math.inf
    t_bottom = None
    for ts, hi, lo, cl in after:
        if cl < min_close:
            min_close = cl
            t_bottom = ts
    out["t_bottom"] = t_bottom
    out["price_bottom"] = min_close
    out["drop_pct"] = (min_close - max_high) / max_high * 100.0

    if out["drop_pct"] > -DROP_MIN_PCT:
        out["reason_no_sig"] = "drop_lt_30pct"
        return out

    # 5c: find earliest t_sig after t_bottom such that
    #     pos[t_sig]-pos[t_bottom] >= 0.05, acct[t_sig]-acct[t_bottom] >= 0.05,
    #     (t_sig - t_bottom) >= 48h, t_sig <= now - 7d.
    pos_base = point_lookup(pos, t_bottom)
    acct_base = point_lookup(acct, t_bottom)
    if pos_base is None or acct_base is None:
        out["reason_no_sig"] = "no_baseline_at_bottom"
        return out

    deadline_ms = now_ms - FORWARD_MIN_DAYS * MS_DAY
    earliest_after = t_bottom + RISE_MIN_HOURS * MS_HOUR

    # Iterate hourly bars from earliest_after onward
    for ts, hi, lo, cl in klines:
        if ts < earliest_after:
            continue
        if ts > deadline_ms:
            break
        p_now = point_lookup(pos, ts)
        a_now = point_lookup(acct, ts)
        if p_now is None or a_now is None:
            continue
        if (p_now - pos_base) >= (POS_RISE_MIN_PP / 100.0) and \
           (a_now - acct_base) >= (ACCT_RISE_MIN_PP / 100.0):
            out["t_sig"] = ts
            out["price_at_sig"] = cl
            return out

    out["reason_no_sig"] = "no_rise_satisfied"
    return out


# ── Stratification at t_sig ─────────────────────────────────────────────────

def compute_stratification(
    t_sig: int, t_bottom: int, t_high: int,
    pos: List[Tuple[int, float]],
    acct: List[Tuple[int, float]],
    retail: List[Tuple[int, float]],
    oi: List[Tuple[int, float]],
    funding: List[Tuple[int, float]],
    taker: List[Tuple[int, float]],
) -> Dict:
    pos_sig    = point_lookup(pos, t_sig)
    acct_sig   = point_lookup(acct, t_sig)
    retail_sig = point_lookup(retail, t_sig)
    oi_sig     = point_lookup(oi, t_sig)
    oi_bot     = point_lookup(oi, t_bottom)
    taker_sig  = point_lookup(taker, t_sig)
    # funding: closest within ±4h (funding cadence 8h, so widen)
    fund_sig   = point_lookup(funding, t_sig, max_delta_h=4)

    # gap requires same ±3h point-lookup; if either None → None
    gap = None
    if pos_sig is not None and acct_sig is not None:
        gap = pos_sig - acct_sig

    oi_vs_bottom = None
    if oi_sig is not None and oi_bot is not None and oi_bot > 0:
        oi_vs_bottom = (oi_sig - oi_bot) / oi_bot * 100.0

    # prepump = MEDIAN of OI in [t_high - 168h, t_high - 24h]; need ≥48 pts
    pp_lo = t_high - PREPUMP_WIN_HOURS * MS_HOUR
    pp_hi = t_high - PREPUMP_BUFFER_H  * MS_HOUR
    pp_vals = [v for ts, v in oi if pp_lo <= ts <= pp_hi]
    oi_vs_prepump = None
    prepump_note = None
    if len(pp_vals) < PREPUMP_MIN_H:
        prepump_note = "prepump_window_insufficient"
    else:
        pp_med = statistics.median(pp_vals)
        if pp_med > 0 and oi_sig is not None:
            oi_vs_prepump = (oi_sig - pp_med) / pp_med * 100.0

    return {
        "pos_at_sig": pos_sig,
        "acct_at_sig": acct_sig,
        "gap_at_sig": gap,
        "retail_at_sig": retail_sig,
        "oi_at_sig": oi_sig,
        "oi_vs_bottom_pct": oi_vs_bottom,
        "oi_vs_prepump_pct": oi_vs_prepump,
        "prepump_note": prepump_note,
        "funding_at_sig": fund_sig,
        "taker_at_sig": taker_sig,
    }


# ── Outcome (first-cross on CLOSE; wick logged separately) ──────────────────

def outcome(klines: List[Tuple[int, float, float, float]],
            t_sig: int, ref_price: float) -> Dict:
    fwd_end_ms = t_sig + FORWARD_MIN_DAYS * MS_DAY
    up_thr = ref_price * (1 + CONT_UP_PCT / 100.0)
    dn_thr = ref_price * (1 - CONT_DOWN_PCT / 100.0)

    first_cross = "GREY"
    hours_to_cross = None
    wick_up = False
    wick_dn = False
    fwd_3d = None
    fwd_7d = None

    for ts, hi, lo, cl in klines:
        if ts <= t_sig:
            continue
        if ts > fwd_end_ms:
            break
        # first cross on CLOSE
        if first_cross == "GREY":
            if cl >= up_thr:
                first_cross = "UP"
                hours_to_cross = (ts - t_sig) // MS_HOUR
            elif cl <= dn_thr:
                first_cross = "DOWN"
                hours_to_cross = (ts - t_sig) // MS_HOUR
        # wick touched
        if hi >= up_thr:
            wick_up = True
        if lo <= dn_thr:
            wick_dn = True
        # 3d / 7d returns by closest bar at offset
        if fwd_3d is None and ts >= t_sig + 3 * MS_DAY:
            fwd_3d = (cl - ref_price) / ref_price * 100.0
        if fwd_7d is None and ts >= t_sig + 7 * MS_DAY:
            fwd_7d = (cl - ref_price) / ref_price * 100.0

    return {
        "first_cross": first_cross,
        "hours_to_cross": hours_to_cross,
        "fwd_ret_3d": fwd_3d,
        "fwd_ret_7d": fwd_7d,
        "wick_touched_up": wick_up,
        "wick_touched_down": wick_dn,
    }


# ── BTC regime proxy ────────────────────────────────────────────────────────

def btc_regime_proxy(btc: List[Tuple[int, float]], ts_target: int) -> str:
    """Compute ma7/ma30 - 1 at ts_target (day containing it).
    Up > +2%, Down < -2%, else chop. Returns 'up'/'down'/'chop'/'unknown'."""
    if not btc:
        return "unknown"
    # Find daily bar that contains ts_target (open_time <= ts_target < open_time + 1d)
    idx_target = None
    for i, (ts, cl) in enumerate(btc):
        if ts <= ts_target < ts + MS_DAY:
            idx_target = i
            break
    if idx_target is None:
        # fall back to latest bar <= ts_target
        for i, (ts, cl) in enumerate(btc):
            if ts <= ts_target:
                idx_target = i
    if idx_target is None or idx_target < 30:
        return "unknown"
    closes = [cl for _, cl in btc[: idx_target + 1]]
    ma7 = sum(closes[-7:]) / 7.0
    ma30 = sum(closes[-30:]) / 30.0
    if ma30 == 0:
        return "unknown"
    r = ma7 / ma30 - 1.0
    if r > 0.02:
        return "up"
    if r < -0.02:
        return "down"
    return "chop"


# ── Per-token scan ──────────────────────────────────────────────────────────

def scan_token(symbol: str, mc_unknown: bool, btc: List[Tuple[int, float]],
               now_ms: int) -> Dict:
    """Return one of:
      {"kind":"signal", "row": {...}}
      {"kind":"control", "row": {...}}
      {"kind":"skip", "reason": ...}
    """
    klines = fetch_klines(symbol)
    if len(klines) < 24:
        return {"kind": "skip", "reason": "fetch_failed_klines"}

    pos    = fetch_top_pos(symbol)
    acct   = fetch_top_acct(symbol)
    retail = fetch_retail(symbol)
    oi     = fetch_oi(symbol)
    taker  = fetch_taker(symbol)
    funding= fetch_funding(symbol)

    pat = detect_pattern(klines, pos, acct, now_ms)

    if pat["t_high"] is None or pat["t_bottom"] is None:
        return {"kind": "skip", "reason": pat.get("reason_no_sig") or "no_pattern"}

    # Token didn't even meet drop ≥ 30% → not in signal OR control
    if pat["drop_pct"] is None or pat["drop_pct"] > -DROP_MIN_PCT:
        return {"kind": "skip", "reason": "drop_lt_30pct"}

    # Has drop ≥ 30%. Either signal (has t_sig) or control.
    if pat["t_sig"] is not None:
        t_sig = pat["t_sig"]
        strat = compute_stratification(
            t_sig, pat["t_bottom"], pat["t_high"],
            pos, acct, retail, oi, funding, taker,
        )
        outc = outcome(klines, t_sig, pat["price_at_sig"])
        regime = btc_regime_proxy(btc, t_sig)
        row = {
            "symbol": symbol,
            "mc_unknown": mc_unknown,
            "t_high":   _fmt_ms(pat["t_high"]),
            "t_bottom": _fmt_ms(pat["t_bottom"]),
            "drop_pct": round(pat["drop_pct"], 3),
            "t_sig":    _fmt_ms(t_sig),
            **{k: _round(v) for k, v in strat.items() if k != "prepump_note"},
            "prepump_note": strat["prepump_note"] or "",
            "btc_regime_proxy": regime,
            **{k: _round(v) for k, v in outc.items()},
        }
        return {"kind": "signal", "row": row}

    # No t_sig → control. t_sig_control = t_bottom + 48h. Need ≤ now - 7d.
    t_ctrl = pat["t_bottom"] + RISE_MIN_HOURS * MS_HOUR
    if t_ctrl > now_ms - FORWARD_MIN_DAYS * MS_DAY:
        return {"kind": "skip", "reason": "no_signal_no_fwd_window_for_control"}
    # Find ref_price = close at or near t_ctrl
    ref_price = None
    for ts, hi, lo, cl in klines:
        if ts >= t_ctrl:
            ref_price = cl
            break
    if ref_price is None:
        return {"kind": "skip", "reason": "control_no_ref_price"}

    strat = compute_stratification(
        t_ctrl, pat["t_bottom"], pat["t_high"],
        pos, acct, retail, oi, funding, taker,
    )
    outc = outcome(klines, t_ctrl, ref_price)
    regime = btc_regime_proxy(btc, t_ctrl)
    row = {
        "symbol": symbol,
        "mc_unknown": mc_unknown,
        "t_high":   _fmt_ms(pat["t_high"]),
        "t_bottom": _fmt_ms(pat["t_bottom"]),
        "drop_pct": round(pat["drop_pct"], 3),
        "t_sig":    _fmt_ms(t_ctrl),
        "reason_no_signal": pat.get("reason_no_sig") or "",
        **{k: _round(v) for k, v in strat.items() if k != "prepump_note"},
        "prepump_note": strat["prepump_note"] or "",
        "btc_regime_proxy": regime,
        **{k: _round(v) for k, v in outc.items()},
    }
    return {"kind": "control", "row": row}


# ── Formatting helpers ──────────────────────────────────────────────────────

def _fmt_ms(ts_ms: Optional[int]) -> str:
    if ts_ms is None:
        return ""
    import datetime as dt
    return dt.datetime.utcfromtimestamp(ts_ms / 1000).strftime("%Y-%m-%d %H:%M")


def _round(v):
    if v is None or isinstance(v, bool):
        return v
    if isinstance(v, str):
        return v
    try:
        return round(float(v), 4)
    except (TypeError, ValueError):
        return v


# ── CSV writers ─────────────────────────────────────────────────────────────

SIGNAL_FIELDS = [
    "symbol", "mc_unknown", "t_high", "t_bottom", "drop_pct", "t_sig",
    "pos_at_sig", "acct_at_sig", "gap_at_sig", "retail_at_sig",
    "oi_at_sig", "oi_vs_bottom_pct", "oi_vs_prepump_pct", "prepump_note",
    "funding_at_sig", "taker_at_sig", "btc_regime_proxy",
    "first_cross", "hours_to_cross", "fwd_ret_3d", "fwd_ret_7d",
    "wick_touched_up", "wick_touched_down",
]

CONTROL_FIELDS = [
    "symbol", "mc_unknown", "t_high", "t_bottom", "drop_pct", "t_sig",
    "reason_no_signal",
    "pos_at_sig", "acct_at_sig", "gap_at_sig", "retail_at_sig",
    "oi_at_sig", "oi_vs_bottom_pct", "oi_vs_prepump_pct", "prepump_note",
    "funding_at_sig", "taker_at_sig", "btc_regime_proxy",
    "first_cross", "hours_to_cross", "fwd_ret_3d", "fwd_ret_7d",
    "wick_touched_up", "wick_touched_down",
]


def write_csv(path: str, fields: List[str], rows: List[Dict]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)


# ── Sanity checks ───────────────────────────────────────────────────────────

def sanity_check_signal_rows(rows: List[Dict]) -> List[str]:
    """Return list of failure messages (empty if pass)."""
    errs: List[str] = []
    sample = rows[:3]
    for r in sample:
        for k in ("pos_at_sig", "acct_at_sig", "retail_at_sig"):
            v = r.get(k)
            if v is None:
                continue
            try:
                vf = float(v)
            except (TypeError, ValueError):
                continue
            if not (0.0 <= vf <= 1.0):
                errs.append(f"SANITY: {r.get('symbol')} {k}={vf} out of [0,1] — possible longShortRatio leak")

    for r in rows:
        if r.get("symbol") == "BLUAIUSDT":
            g = r.get("gap_at_sig")
            if g is not None:
                try:
                    gf = float(g)
                    if gf >= 0:
                        errs.append(f"SANITY: BLUAIUSDT gap_at_sig={gf} not negative — pos/acct may be swapped")
                except (TypeError, ValueError):
                    pass
    return errs


# ── Contrast table ──────────────────────────────────────────────────────────

def _safe_median(vals):
    cleaned = []
    for v in vals:
        if v is None or v == "":
            continue
        try:
            cleaned.append(float(v))
        except (TypeError, ValueError):
            continue
    if not cleaned:
        return None
    return statistics.median(cleaned)


def _rates(rows: List[Dict]):
    n = len(rows)
    if n == 0:
        return {"n": 0, "UP": 0.0, "DOWN": 0.0, "GREY": 0.0,
                "median_fwd_7d": None, "median_hours_to_cross": None}
    up = sum(1 for r in rows if r.get("first_cross") == "UP")
    dn = sum(1 for r in rows if r.get("first_cross") == "DOWN")
    gy = n - up - dn
    return {
        "n": n,
        "UP":   round(up / n * 100, 1),
        "DOWN": round(dn / n * 100, 1),
        "GREY": round(gy / n * 100, 1),
        "median_fwd_7d": _safe_median([r.get("fwd_ret_7d") for r in rows]),
        "median_hours_to_cross": _safe_median([r.get("hours_to_cross") for r in rows]),
    }


def print_contrast(signal_rows: List[Dict], control_rows: List[Dict]) -> None:
    print()
    print("=" * 78)
    print("CONTRAST TABLE — accumulation-after-drop pattern")
    print("=" * 78)

    s = _rates(signal_rows)
    c = _rates(control_rows)
    fmt = lambda v: ("None" if v is None else f"{v:.2f}")

    # Force stdout to handle utf-8 if possible (Windows cp1252 chokes on - arrow)
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    print(f"{'Group':<22} {'n':>4} {'UP%':>6} {'DOWN%':>7} {'GREY%':>7} "
          f"{'med7d%':>9} {'medHrs':>8}")
    print("-" * 78)
    print(f"{'SIGNAL':<22} {s['n']:>4} {s['UP']:>6.1f} {s['DOWN']:>7.1f} {s['GREY']:>7.1f} "
          f"{fmt(s['median_fwd_7d']):>9} {fmt(s['median_hours_to_cross']):>8}")
    print(f"{'CONTROL':<22} {c['n']:>4} {c['UP']:>6.1f} {c['DOWN']:>7.1f} {c['GREY']:>7.1f} "
          f"{fmt(c['median_fwd_7d']):>9} {fmt(c['median_hours_to_cross']):>8}")

    # Split by gap sign
    sig_gap_pos = []
    sig_gap_neg = []
    for r in signal_rows:
        g = r.get("gap_at_sig")
        try:
            gf = float(g)
        except (TypeError, ValueError):
            continue
        if gf > 0:
            sig_gap_pos.append(r)
        elif gf < 0:
            sig_gap_neg.append(r)

    sp = _rates(sig_gap_pos)
    sn = _rates(sig_gap_neg)
    print()
    print("WITHIN SIGNAL GROUP — split by gap_at_sig (pos - acct):")
    print(f"{'SIGNAL gap>0':<22} {sp['n']:>4} {sp['UP']:>6.1f} {sp['DOWN']:>7.1f} {sp['GREY']:>7.1f} "
          f"{fmt(sp['median_fwd_7d']):>9} {fmt(sp['median_hours_to_cross']):>8}")
    print(f"{'SIGNAL gap<0':<22} {sn['n']:>4} {sn['UP']:>6.1f} {sn['DOWN']:>7.1f} {sn['GREY']:>7.1f} "
          f"{fmt(sn['median_fwd_7d']):>9} {fmt(sn['median_hours_to_cross']):>8}")

    # BLUAIUSDT row(s)
    print()
    print("BLUAIUSDT direct row(s):")
    found = False
    for r in signal_rows + control_rows:
        if r.get("symbol") == "BLUAIUSDT":
            found = True
            print(f"  group={'signal' if r in signal_rows else 'control'}  "
                  f"t_sig={r.get('t_sig')}  drop={r.get('drop_pct')}  "
                  f"gap={r.get('gap_at_sig')}  first_cross={r.get('first_cross')}  "
                  f"fwd_7d={r.get('fwd_ret_7d')}")
    if not found:
        print("  (BLUAIUSDT did not qualify for either group)")


def print_limitations(meta: Dict) -> None:
    print()
    print("=" * 78)
    print("LIMITATIONS (mandatory)")
    print("=" * 78)
    print(f"Universe after pre-filter        : {meta['universe_n']}")
    print(f"  dropped non-ASCII              : {meta['dropped_nonascii_n']}")
    print(f"  mc_unknown kept                : {meta['mc_unknown_kept']}")
    print(f"  fetch_failed (delisted/400)    : {meta['fetch_failed']}")
    print(f"  short_history (<{LOOKBACK_HOURS}h klines) : {meta['short_history']}")
    print(f"Tokens with drop>=30%            : {meta['drop_pass']}")
    print(f"  -> signal group n              : {meta['signal_n']}")
    print(f"  -> control group n             : {meta['control_n']}")
    print(f"  -> has >=7d forward (built-in by t_sig rule)")
    if meta.get("tsig_range"):
        lo, hi = meta["tsig_range"]
        print(f"t_sig timestamp range            : {lo}  ->  {hi}")
    print()
    print("This is a HINDSIGHT pattern screen on a ~30d retention window during a")
    print("base-rate bear month. window_high / t_bottom use data AFTER t_sig, so the")
    print("rule CANNOT run live as-is. Scan answers 'does this shape precede UP',")
    print("NOT 'this is an entry rule'. Sample is small, single-regime. NOT a validated")
    print("edge.")


# ── Main ────────────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", help="Scan one token only (e.g. BLUAIUSDT)")
    ap.add_argument("--limit", type=int, default=None, help="Limit universe size for testing")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    kept, dropped_nonascii = load_universe()
    if args.symbol:
        kept = [k for k in kept if k["symbol"] == args.symbol]

    if args.limit:
        kept = kept[: args.limit]

    universe_n = len(kept)
    mc_unknown_n = sum(1 for k in kept if k["mc_unknown"])
    print(f"[universe] kept={universe_n}  mc_unknown_kept={mc_unknown_n}  "
          f"dropped_nonascii={len(dropped_nonascii)}")
    if dropped_nonascii:
        with open(UNIVERSE_LOG, "w", encoding="utf-8") as fh:
            json.dump({"dropped_nonascii": dropped_nonascii,
                       "kept_n": universe_n,
                       "mc_unknown_kept": mc_unknown_n}, fh, indent=2,
                      ensure_ascii=False)

    # BTC regime proxy data
    print("[btc] fetching daily klines...")
    btc = fetch_btc_daily()
    print(f"[btc] {len(btc)} daily bars")

    now_ms = int(time.time() * 1000)

    signal_rows: List[Dict] = []
    control_rows: List[Dict] = []
    meta = {
        "universe_n": universe_n,
        "dropped_nonascii_n": len(dropped_nonascii),
        "mc_unknown_kept": mc_unknown_n,
        "fetch_failed": 0,
        "short_history": 0,
        "drop_pass": 0,
        "signal_n": 0,
        "control_n": 0,
    }

    for i, k in enumerate(kept, 1):
        sym = k["symbol"]
        if args.verbose or i % 20 == 0 or i == universe_n:
            print(f"[{i}/{universe_n}] {sym}")
        try:
            res = scan_token(sym, k["mc_unknown"], btc, now_ms)
        except Exception as e:
            print(f"  ERROR {sym}: {e}")
            meta["fetch_failed"] += 1
            continue

        if res["kind"] == "skip":
            reason = res.get("reason", "")
            if reason == "fetch_failed_klines":
                meta["fetch_failed"] += 1
            elif reason == "drop_lt_30pct":
                # we wanted to count drop_pass separately; drop_lt_30pct is the fail side
                pass
            continue

        if res["kind"] == "signal":
            meta["drop_pass"] += 1
            meta["signal_n"] += 1
            signal_rows.append(res["row"])
        elif res["kind"] == "control":
            meta["drop_pass"] += 1
            meta["control_n"] += 1
            control_rows.append(res["row"])

    # Sanity check
    sanity = sanity_check_signal_rows(signal_rows)
    if sanity:
        print()
        print("!!! SANITY CHECK FAILED — halting before output !!!")
        for s in sanity:
            print(s)
        sys.exit(2)

    # t_sig range
    tsig_strs = [r["t_sig"] for r in signal_rows + control_rows if r.get("t_sig")]
    if tsig_strs:
        meta["tsig_range"] = (min(tsig_strs), max(tsig_strs))

    # Write CSVs
    write_csv(MATCHES_CSV, SIGNAL_FIELDS, signal_rows)
    write_csv(CONTROL_CSV, CONTROL_FIELDS, control_rows)
    print(f"[output] matches.csv  rows={len(signal_rows)}")
    print(f"[output] control.csv  rows={len(control_rows)}")

    print_contrast(signal_rows, control_rows)
    print_limitations(meta)


if __name__ == "__main__":
    main()
