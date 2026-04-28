"""
debug_pump.py — trace why a token does/doesn't enter pump_exhaustion_short.

Usage:
    python debug_pump.py              # uses SYMBOL below
    python debug_pump.py AEROUSDT     # override via CLI arg

Gates traced:
    [UNIVERSE]   market cap <= 500M (CoinGecko)
    [G_D1]       base detection valid (ignition found, not trending)
    [G_D2]       pump_pct >= 35%
    [G_D3]       pump_vol_ratio >= 3x
    [G_D4]       peak_age_h <= 72h
    [G_D5]       room_pct >= 15% (price still elevated above base)
    [G_D8]       anchor order valid (p0_ts < p1_ts)
    [WATCHLIST]  already in watchlist? → show current state
    [SCANNER]    room re-check, peak_age re-check, breakdown state
"""

import sys
import json
import os
import time
import requests
import yaml
from datetime import datetime, timezone

# ── CONFIG ────────────────────────────────────────────────────────────────────
SYMBOL = "AEROUSDT"        # change this or pass as CLI arg

if len(sys.argv) > 1:
    SYMBOL = sys.argv[1].upper()
    if not SYMBOL.endswith("USDT"):
        SYMBOL += "USDT"

CONFIG_PATH = "config.yaml"
# ─────────────────────────────────────────────────────────────────────────────

# ── helpers ──────────────────────────────────────────────────────────────────
def _pass(label, val_str=""):
    print(f"  [PASS] {label}" + (f"  ({val_str})" if val_str else ""))

def _fail(label, val_str=""):
    print(f"  [FAIL] {label}" + (f"  ({val_str})" if val_str else ""))

def _info(label, val_str=""):
    print(f"  [INFO] {label}" + (f"  — {val_str}" if val_str else ""))

def _section(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")

def _ts_to_str(ts_ms):
    if not ts_ms or ts_ms in ("not_reached_yet", "not_evaluated", ""):
        return str(ts_ms)
    try:
        return datetime.fromtimestamp(int(ts_ms)/1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    except Exception:
        return str(ts_ms)

# ── load config ───────────────────────────────────────────────────────────────
with open(CONFIG_PATH) as f:
    cfg = yaml.safe_load(f)

pump_cfg   = cfg.get("pump_exhaustion", {})
disc_cfg   = pump_cfg.get("discovery", {})
scan_cfg   = pump_cfg.get("scan", {})
uf_cfg     = cfg.get("universe_filter", {})

PUMP_PCT_MIN      = disc_cfg.get("pump_pct_min", 0.35)
PUMP_VOL_MIN      = disc_cfg.get("pump_vol_ratio_min", 3.0)
PEAK_AGE_MAX_H    = disc_cfg.get("peak_age_max_hours", 72)
ROOM_PCT_MIN      = disc_cfg.get("room_pct_min", 0.15)
PEAK_WINDOW       = disc_cfg.get("peak_window_candles", 200)
VOL_SPIKE_MULT    = disc_cfg.get("vol_spike_multiplier", 2.5)
MC_MAX            = uf_cfg.get("market_cap_max_usd", 500_000_000)
QUOTE_VOL_MIN_5M  = scan_cfg.get("quote_vol_5m_min_usdt", 10_000)
DATA_DIR          = pump_cfg.get("watchlist", {}).get("data_dir", "data/pump_exhaustion")

print(f"\ndebug_pump.py — token: {SYMBOL}")
print(f"Thresholds: pump>={PUMP_PCT_MIN:.0%}, vol>={PUMP_VOL_MIN}x, "
      f"room>={ROOM_PCT_MIN:.0%}, mc<=${MC_MAX/1e6:.0f}M")

# ── import strategy modules ───────────────────────────────────────────────────
try:
    from scanner.strategies.pump_exhaustion._client import _BinanceHttp
    from scanner.strategies.pump_exhaustion.detectors.base_detector import detect_base
    from scanner.strategies.pump_exhaustion.detectors.pump_detector import detect_pump
    from scanner.strategies.pump_exhaustion.volume_utils import quote_vol_5m_median
except ImportError as e:
    print(f"\n[ERROR] Cannot import pump_exhaustion modules: {e}")
    print("Run from repo root: python debug_pump.py")
    sys.exit(1)

http = _BinanceHttp(delay_sec=0.15, timeout_sec=10)

# ═══════════════════════════════════════════════════════════════════════
# GATE 0 — Universe filter (market cap via CoinGecko)
# ═══════════════════════════════════════════════════════════════════════
_section(f"GATE 0 — Universe filter  [{SYMBOL}]")

# Check eligible_universe.json cache — THIS is what the live bot actually uses
cache_path = uf_cfg.get("cache_path", "data/eligible_universe.json")
universe_eligible = None
universe_from_cache = False
if os.path.exists(cache_path):
    try:
        with open(cache_path) as f:
            uf_data = json.load(f)
        built_at  = uf_data.get("built_at", "?")
        n_eligible = len(uf_data.get("symbols", []))
        n_excluded = len(uf_data.get("excluded", {}))
        _info("eligible_universe.json", f"built_at={built_at}, eligible={n_eligible}, excluded={n_excluded}")
        if SYMBOL in uf_data.get("symbols", []):
            universe_eligible = True
            universe_from_cache = True
            _pass("In eligible_universe.json  (bot WILL scan this token)")
        elif SYMBOL in uf_data.get("excluded", {}):
            excl_reason = uf_data["excluded"][SYMBOL]
            universe_eligible = False
            universe_from_cache = True
            _fail("Excluded from eligible_universe.json  (bot SKIPS this token in discovery)",
                  f"reason={excl_reason}")
            print("  >>> FIX: rebuild universe cache (UniverseFilter.build_eligible_universe()) "
                  "or wait for next 4h refresh")
        else:
            _fail("NOT IN eligible_universe.json  (bot SKIPS this token — not checked at all)",
                  "not in symbols[] nor excluded{}")
            print("  >>> FIX: rebuild universe cache — token was not present when cache was built")
    except Exception as e:
        _info(f"Cache read error: {e}")
else:
    _info("eligible_universe.json NOT FOUND",
          f"path={cache_path} — bot has not built universe cache yet, or running locally")

if not universe_from_cache:
    # Live CoinGecko check — search for the base symbol
    base_sym = SYMBOL.replace("USDT", "").lower()
    try:
        r = requests.get(
            "https://api.coingecko.com/api/v3/search",
            params={"query": base_sym},
            timeout=10,
        )
        r.raise_for_status()
        results = r.json().get("coins", [])
        # find best match
        match = next(
            (c for c in results if c.get("symbol", "").lower() == base_sym),
            results[0] if results else None,
        )
        if not match:
            _info("CoinGecko: symbol not found — treating as market_cap_unknown")
            universe_eligible = False if uf_cfg.get("exclude_if_unknown", True) else True
        else:
            cg_id = match["id"]
            mc_r = requests.get(
                f"https://api.coingecko.com/api/v3/coins/{cg_id}",
                params={"localization": "false", "tickers": "false",
                        "market_data": "true", "community_data": "false"},
                timeout=10,
            )
            mc_r.raise_for_status()
            mc_data = mc_r.json()
            mc = mc_data.get("market_data", {}).get("market_cap", {}).get("usd", None)
            if mc is None:
                _info("CoinGecko: market cap unavailable")
                universe_eligible = False if uf_cfg.get("exclude_if_unknown", True) else True
            else:
                _info(f"Market cap", f"${mc/1e6:.1f}M  (max=${MC_MAX/1e6:.0f}M)")
                if mc <= MC_MAX:
                    universe_eligible = True
                    _pass(f"Market cap OK", f"${mc/1e6:.1f}M <= ${MC_MAX/1e6:.0f}M")
                else:
                    universe_eligible = False
                    _fail(f"Market cap too large", f"${mc/1e6:.1f}M > ${MC_MAX/1e6:.0f}M")
    except Exception as e:
        _info(f"CoinGecko error: {e} — cannot verify market cap")
        universe_eligible = None  # unknown

# ═══════════════════════════════════════════════════════════════════════
# GATE 0b — Already in watchlist?
# ═══════════════════════════════════════════════════════════════════════
_section(f"WATCHLIST STATUS  [{SYMBOL}]")

wl_path = os.path.join(DATA_DIR, "watchlist.json")
wl_case = None
if os.path.exists(wl_path):
    try:
        with open(wl_path) as f:
            wl = json.load(f)
        for case_id, case in wl.items():
            if case.get("symbol") == SYMBOL:
                wl_case = case
                break
    except Exception as e:
        _info(f"Watchlist read error: {e}")

if wl_case:
    state = wl_case.get("case_state", "?")
    excl  = wl_case.get("exclusion_reason", "")
    _info("Already in watchlist", f"state={state}" + (f", exclusion={excl}" if excl else ""))
    _info("Created",   _ts_to_str(wl_case.get("created_ts")))
    _info("Updated",   _ts_to_str(wl_case.get("updated_ts")))
    _info("Peak high", f"{wl_case.get('peak_high', '?')}")
    _info("Peak time", _ts_to_str(wl_case.get("peak_time")))
    _info("Pump pct",  f"{float(wl_case.get('pump_pct', 0) or 0):.1%}")
    _info("Room pct",  f"{float(wl_case.get('room_pct', 0) or 0):.1%}")
    _info("OI regime", wl_case.get("oi_regime_label", "?"))
    _info("Regime",    wl_case.get("regime_label", "?"))
    _info("p0",        _ts_to_str(wl_case.get("p0_ts")))
    _info("p1",        _ts_to_str(wl_case.get("p1_ts")))
    _info("p2",        _ts_to_str(wl_case.get("p2_ts")))
    _info("p3",        _ts_to_str(wl_case.get("p3_ts")))
    _info("Score",     f"{wl_case.get('score_total', '?')}/{wl_case.get('score_max', '?')}")
    print()
    print("  Token is already in watchlist — discovery gates below show CURRENT values.")
else:
    _info("Not in watchlist")

# ═══════════════════════════════════════════════════════════════════════
# Fetch klines
# ═══════════════════════════════════════════════════════════════════════
_section(f"FETCH DATA  [{SYMBOL}]")

print(f"  Fetching {PEAK_WINDOW} x 1h klines ...")
bars_1h = http.klines(SYMBOL, "1h", PEAK_WINDOW)
print(f"  Fetching 288 x 5m klines ...")
bars_5m = http.klines(SYMBOL, "5m", 288)

if not bars_1h:
    _fail("1h klines fetch failed — cannot trace gates")
    sys.exit(1)

_info(f"1h bars received", str(len(bars_1h)))
_info(f"5m bars received", str(len(bars_5m)))

current_price = bars_1h[-1]["close"]
_info("Current price", f"{current_price:.6g}")

# ═══════════════════════════════════════════════════════════════════════
# GATE D1 — Base detection
# ═══════════════════════════════════════════════════════════════════════
_section(f"GATE D1 — Base detection  [{SYMBOL}]")

base = detect_base(bars_1h, {"discovery": disc_cfg})
base_valid = base.get("base_validity_flag", False)

if base_valid:
    _pass("Base detection valid",
          f"method={base.get('base_detection_method','?')}, "
          f"median={base.get('base_price_median',0):.6g}, "
          f"trending={base.get('base_trending_flag')}")
else:
    reason = base.get("base_invalid_reason", "unknown")
    _fail(f"Base detection FAILED", f"reason={reason}")
    if base.get("peak_high"):
        _info("Peak high found", f"{base.get('peak_high'):.6g}  at {_ts_to_str(base.get('peak_time'))}")
    print("\n  Explanation of base_invalid reasons:")
    print("    ignition_not_found_before_peak  — no vol spike >= {:.1f}x baseline before peak".format(VOL_SPIKE_MULT))
    print("    insufficient_bars_before_ignition — ignition found but < 12 bars before it")
    print("    base_window_already_trending     — 12-bar base window has upward slope")
    print("    insufficient_data                — < 20 bars or baseline vol = 0")

# ═══════════════════════════════════════════════════════════════════════
# GATES D2-D8 — Pump detection
# ═══════════════════════════════════════════════════════════════════════
_section(f"GATES D2-D8 — Pump metrics  [{SYMBOL}]")

pump = detect_pump(bars_1h, base)

pump_pct       = pump.get("pump_pct", 0.0)
pump_vol_ratio = pump.get("pump_vol_ratio", 0.0)
peak_age_h     = pump.get("peak_age_h", 9999.0)
peak_high      = pump.get("peak_high") or base.get("peak_high") or 0.0
peak_time      = pump.get("peak_time") or base.get("peak_time") or 0
base_price_med = base.get("base_price_median", 0.0)
p0_ts          = base.get("base_window_end_ts") or 0
p1_ts          = peak_time or 0
anchor_valid   = bool(p0_ts and p1_ts and p0_ts < p1_ts)

room_pct = (current_price - base_price_med) / base_price_med if base_price_med > 0 else 0.0

_info("Peak high",       f"{peak_high:.6g}  at {_ts_to_str(peak_time)}")
_info("Base median",     f"{base_price_med:.6g}")
_info("Current price",   f"{current_price:.6g}")
print()

# D2 — pump pct
if pump_pct >= PUMP_PCT_MIN:
    _pass(f"D2 pump_pct", f"{pump_pct:.1%} >= {PUMP_PCT_MIN:.0%}")
else:
    _fail(f"D2 pump_pct", f"{pump_pct:.1%} < {PUMP_PCT_MIN:.0%}  need +{(PUMP_PCT_MIN-pump_pct)*100:.1f}%")

# D3 — pump vol ratio
if pump_vol_ratio >= PUMP_VOL_MIN:
    _pass(f"D3 pump_vol_ratio", f"{pump_vol_ratio:.2f}x >= {PUMP_VOL_MIN}x")
else:
    _fail(f"D3 pump_vol_ratio", f"{pump_vol_ratio:.2f}x < {PUMP_VOL_MIN}x")

# D4 — peak age (gate removed — token may re-pump, not necessarily linear dump)
_info(f"D4 peak_age_h (info only)", f"{peak_age_h:.1f}h  (gate removed)")

# D5 — room
if room_pct >= ROOM_PCT_MIN:
    _pass(f"D5 room_pct", f"{room_pct:.1%} >= {ROOM_PCT_MIN:.0%}  (price still {room_pct:.1%} above base)")
else:
    _fail(f"D5 room_pct", f"{room_pct:.1%} < {ROOM_PCT_MIN:.0%}  (already fell back to base)")

# D8 — anchor order
if anchor_valid:
    _pass(f"D8 anchor_order", f"p0={_ts_to_str(p0_ts)} < p1={_ts_to_str(p1_ts)}")
else:
    _fail(f"D8 anchor_order", f"p0={_ts_to_str(p0_ts)}, p1={_ts_to_str(p1_ts)}")

# ═══════════════════════════════════════════════════════════════════════
# Overall discovery verdict
# ═══════════════════════════════════════════════════════════════════════
_section(f"DISCOVERY VERDICT  [{SYMBOL}]")

gates = {
    "D1 base_valid": base_valid,
    "D2 pump_pct":   pump_pct >= PUMP_PCT_MIN,
    "D3 pump_vol":   pump_vol_ratio >= PUMP_VOL_MIN,
    "D5 room":       room_pct >= ROOM_PCT_MIN,
    "D8 anchor":     anchor_valid,
}

first_fail = next((k for k, v in gates.items() if not v), None)
all_pass   = first_fail is None

if universe_eligible is False:
    print(f"  BLOCKED at UNIVERSE filter — market cap too large or unknown")
elif not all_pass:
    print(f"  BLOCKED at gate {first_fail}")
    passing = [k for k, v in gates.items() if v]
    failing = [k for k, v in gates.items() if not v]
    if passing:
        print(f"  Passed:  {', '.join(passing)}")
    print(f"  Failed:  {', '.join(failing)}")
else:
    print(f"  ALL discovery gates PASS — token would enter watchlist")
    if wl_case:
        print(f"  (already in watchlist with state={wl_case.get('case_state')})")

# ═══════════════════════════════════════════════════════════════════════
# EXTRA — 5m liquidity check
# ═══════════════════════════════════════════════════════════════════════
if bars_5m:
    _section(f"SCANNER CHECK — 5m liquidity  [{SYMBOL}]")
    try:
        qvol = quote_vol_5m_median(bars_5m, last_n=20)
        if qvol >= QUOTE_VOL_MIN_5M:
            _pass(f"5m quote_vol_median", f"${qvol:,.0f} >= ${QUOTE_VOL_MIN_5M:,}")
        else:
            _fail(f"5m quote_vol_median", f"${qvol:,.0f} < ${QUOTE_VOL_MIN_5M:,}  (low liquidity)")
    except Exception as e:
        _info(f"5m vol check error", str(e))

# ═══════════════════════════════════════════════════════════════════════
# EXTRA — OI history snapshot
# ═══════════════════════════════════════════════════════════════════════
_section(f"OI SNAPSHOT (last 6h)  [{SYMBOL}]")
try:
    oi = http.oi_hist(SYMBOL, period="1h", limit=6)
    if oi:
        for bar in oi:
            print(f"  {_ts_to_str(bar['ts'])}  OI={bar['oi_value']/1e6:.2f}M USDT")
    else:
        _info("OI history not available")
except Exception as e:
    _info(f"OI fetch error: {e}")

print()
