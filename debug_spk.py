"""
Debug script: fetch live SPK data and trace through all ORB gate conditions.
Run from repo root: python debug_spk.py
"""
import math
import requests

SYMBOL = "ZKJUSDT"
BASE = "https://fapi.binance.com"

# -- Config thresholds (from config.yaml) --
CFG = {
    "min_quote_volume_usdt_24h": 10_000_000,
    "max_quote_volume_usdt_24h": 300_000_000,
    "max_symbols": 300,
    "oi_spike_min_pct": 5.0,
    "oi_delta_abs_min_usdt": 400_000,
    "max_market_cap_usd": 500_000_000,
    "max_range_width_pct": 60.0,
    "min_range_width_pct": 0.0,
    "range_atr_period": 14,
    "range_atr_ratio_max": 2.0,
    "range_lookback_bars_1h": 20,
    "min_risk_pct": 0.5,
    "max_risk_pct": 20.0,
    "stop_buffer_pct": 0.2,
    "score_min_send": 0,
}

S = "[PASS]"
F = "[FAIL]"
W = "[SKIP]"

def get(path, params=None):
    r = requests.get(BASE + path, params=params, timeout=10)
    r.raise_for_status()
    return r.json()

def calc_ema(series, period):
    if len(series) < period:
        return float("nan")
    mult = 2.0 / (period + 1)
    ema = sum(series[:period]) / period
    for v in series[period:]:
        ema = v * mult + ema * (1 - mult)
    return ema

def calc_atr(highs, lows, closes, period):
    if len(closes) < period + 1:
        return None, None
    trs = [max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1]))
           for i in range(1, len(closes))]
    if len(trs) < period:
        return None, None
    atr_val = sum(trs[:period]) / period
    series = [atr_val]
    for tr in trs[period:]:
        atr_val = (atr_val * (period - 1) + tr) / period
        series.append(atr_val)
    return series[-1], sum(series) / len(series)

def fetch_market_cap(symbol):
    base = symbol.lower().removesuffix("usdt")
    try:
        coins = requests.get("https://api.coingecko.com/api/v3/search",
                             params={"query": base}, timeout=5).json().get("coins", [])
        if not coins:
            return None
        exact = [c for c in coins if c.get("symbol","").lower() == base]
        cg_id = exact[0]["id"] if exact else coins[0]["id"]
        price = requests.get("https://api.coingecko.com/api/v3/simple/price",
                             params={"ids": cg_id, "vs_currencies": "usd", "include_market_cap": "true"},
                             timeout=5).json()
        mc = float(price.get(cg_id, {}).get("usd_market_cap", 0)) or None
        return mc, cg_id
    except Exception as e:
        return None, str(e)

# ----------------------------------------------------------------------------─
print(f"\n{'='*60}")
print(f"  ORB Gate Diagnostic - {SYMBOL}")
print(f"{'='*60}\n")

# -- Gate 1: Symbol in top 80 --
print("-- Gate 1: Symbol scan list (top 80 by volume) --")
tickers = {x["symbol"]: x for x in get("/fapi/v1/ticker/24hr")}
t = tickers.get(SYMBOL)
if not t:
    print(f"  {F} {SYMBOL} không có trong Binance USDT futures ticker -> BLOCKED\n")
    raise SystemExit

qv = float(t.get("quoteVolume", 0))
price = float(t.get("lastPrice", 0))
print(f"  24h volume  : ${qv:>15,.0f}  (min ${CFG['min_quote_volume_usdt_24h']:,} / max ${CFG['max_quote_volume_usdt_24h']:,})")
print(f"  Last price  : ${price}")

if qv < CFG["min_quote_volume_usdt_24h"]:
    print(f"  {F} Volume < ${CFG['min_quote_volume_usdt_24h']:,} -> BLOCKED")
elif qv > CFG["max_quote_volume_usdt_24h"]:
    print(f"  {F} Volume > ${CFG['max_quote_volume_usdt_24h']:,} (too liquid) -> BLOCKED")
else:
    print(f"  {S} Volume OK")

# Rank check
all_usdt = get("/fapi/v1/exchangeInfo")
usdt_perps = [s["symbol"] for s in all_usdt["symbols"]
              if s.get("contractType") == "PERPETUAL"
              and s.get("quoteAsset") == "USDT"
              and s.get("status") == "TRADING"
              and s["symbol"] not in {"BTCUSDT", "ETHUSDT"}]
ranked = sorted([s for s in usdt_perps if s in tickers],
                key=lambda s: float(tickers[s]["quoteVolume"]), reverse=True)
rank = ranked.index(SYMBOL) + 1 if SYMBOL in ranked else None
top80 = SYMBOL in ranked[:CFG["max_symbols"]]
if rank:
    print(f"  Volume rank : #{rank} of {len(ranked)} symbols")
    if top80:
        print(f"  {S} In top {CFG['max_symbols']}\n")
    else:
        print(f"  {F} NOT in top {CFG['max_symbols']} -> BLOCKED\n")
        raise SystemExit
else:
    print(f"  {F} Symbol not found in ranked list -> BLOCKED\n")
    raise SystemExit

# -- Gate 2: OI history --
print("-- Gate 2: OI history (13 bars × 5m = 1h window) --")
# Fetch more bars to compare multiple windows
oi_hist_raw = get("/futures/data/openInterestHist", {"symbol": SYMBOL, "period": "5m", "limit": 289})  # ~24h
oi_hist = get("/futures/data/openInterestHist", {"symbol": SYMBOL, "period": "5m", "limit": 13})
oi_bars = [{"ts": int(x["timestamp"]),
            "oi_value": float(x.get("sumOpenInterestValue", 0)),
            "oi_coins": float(x.get("sumOpenInterest", 0))} for x in oi_hist]
print(f"  Bars returned: {len(oi_bars)}  (need >= 13)")
if len(oi_bars) < 13:
    print(f"  {F} Insufficient OI history -> BLOCKED\n")
    raise SystemExit
print(f"  {S} OI history OK")
oi_now    = oi_bars[-1]["oi_value"]
oi_1h_ago = oi_bars[-13]["oi_value"]
oi_delta  = (oi_now - oi_1h_ago) / oi_1h_ago * 100.0 if oi_1h_ago > 0 else 0.0
oi_now_coins    = oi_bars[-1]["oi_coins"]
oi_1h_ago_coins = oi_bars[-13]["oi_coins"]
oi_delta_coins  = (oi_now_coins - oi_1h_ago_coins) / oi_1h_ago_coins * 100.0 if oi_1h_ago_coins > 0 else 0.0

print(f"  === Binance OI (USD value) ===")
print(f"  OI now      : ${oi_now:>15,.0f}")
print(f"  OI 1h ago   : ${oi_1h_ago:>15,.0f}")
print(f"  OI delta 1h : {oi_delta:+.2f}%  <- bot gate dung cai nay")
print(f"  === Binance OI (contracts/coins) ===")
print(f"  OI now      : {oi_now_coins:>18,.2f}")
print(f"  OI 1h ago   : {oi_1h_ago_coins:>18,.2f}")
print(f"  OI delta 1h : {oi_delta_coins:+.2f}%  <- Velo co the dung cai nay")

# Multi-window delta (using raw 289 bars)
all_bars = [{"ts": int(x["timestamp"]),
             "oi_value": float(x.get("sumOpenInterestValue", 0)),
             "oi_coins": float(x.get("sumOpenInterest", 0))} for x in oi_hist_raw]
def delta_window(bars, n_bars_back, label):
    if len(bars) < n_bars_back + 1:
        return
    now_v = bars[-1]["oi_value"]; ago_v = bars[-n_bars_back]["oi_value"]
    now_c = bars[-1]["oi_coins"]; ago_c = bars[-n_bars_back]["oi_coins"]
    d_v = (now_v - ago_v) / ago_v * 100 if ago_v else 0
    d_c = (now_c - ago_c) / ago_c * 100 if ago_c else 0
    print(f"  {label:8s}: USD {d_v:+.2f}%  |  coins {d_c:+.2f}%")

print(f"  === Multi-window delta (USD | coins) ===")
delta_window(all_bars, 6,   "30m")
delta_window(all_bars, 12,  "1h")
delta_window(all_bars, 24,  "2h")
delta_window(all_bars, 48,  "4h")
delta_window(all_bars, 288, "24h")

# Print last 15 bars (75 min) to see OI shape
import datetime as _dt
print(f"  === Raw 5m bars (last 15) ===")
print(f"  {'Time (UTC)':19s}  {'OI USD':>15s}  {'OI Coins':>18s}  {'Delta USD':>10s}")
ref_v = all_bars[-16]["oi_value"] if len(all_bars) >= 16 else all_bars[0]["oi_value"]
for b in all_bars[-15:]:
    t = _dt.datetime.utcfromtimestamp(b["ts"] / 1000).strftime("%Y-%m-%d %H:%M")
    dv = (b["oi_value"] - ref_v) / ref_v * 100 if ref_v else 0
    ref_v = b["oi_value"]
    print(f"  {t}  ${b['oi_value']:>15,.0f}  {b['oi_coins']:>18,.0f}  {dv:>+9.2f}%")

# Bybit OI for comparison
print(f"  === Bybit OI (coins) ===")
try:
    bybit_r = requests.get("https://api.bybit.com/v5/market/open-interest",
        params={"category":"linear","symbol":SYMBOL,"intervalTime":"5min","limit":"13"},
        timeout=8).json()
    bybit_bars = bybit_r["result"]["list"]
    if len(bybit_bars) >= 13:
        by_now   = float(bybit_bars[0]["openInterest"])   # newest-first
        by_1h    = float(bybit_bars[12]["openInterest"])
        by_delta = (by_now - by_1h) / by_1h * 100 if by_1h else 0
        print(f"  OI now      : {by_now:>18,.2f} coins")
        print(f"  OI 1h ago   : {by_1h:>18,.2f} coins")
        print(f"  OI delta 1h : {by_delta:+.2f}%")
    else:
        print(f"  Only {len(bybit_bars)} bars returned")
except Exception as e:
    print(f"  Bybit fetch failed: {e}")
print()

# -- Gate 3: OI spike --
print("-- Gate 3: OI spike >= 5% --")
print(f"  oi_delta_pct: {oi_delta:+.2f}%  (min {CFG['oi_spike_min_pct']}%)")
if oi_delta < CFG["oi_spike_min_pct"]:
    print(f"  {F} OI spike insufficient -> BLOCKED\n")
    # Don't raise SystemExit here — show remaining gates too
    oi_gate_pass = False
else:
    print(f"  {S} OI spike OK\n")
    oi_gate_pass = True

# -- Gate 3b: OI abs delta positive + >= $0.4M (live bot uses max of Binance vs Bybit) --
print("-- Gate 3b: OI abs delta > 0 and >= $0.4M --")
oi_delta_abs_usdt = oi_now - oi_1h_ago
print(f"  oi_delta_abs (Binance): ${oi_delta_abs_usdt/1e6:.3f}M  (min ${CFG['oi_delta_abs_min_usdt']/1e6:.3f}M)")
print(f"  Note: live bot uses max(Binance, Bybit) — Bybit may push value higher")
if oi_delta_abs_usdt <= 0:
    print(f"  {F} OI abs delta not positive -> BLOCKED\n")
elif oi_delta_abs_usdt < CFG["oi_delta_abs_min_usdt"]:
    print(f"  {F} OI abs delta below threshold (Binance-only; check Bybit section above for full picture)\n")
else:
    print(f"  {S} OI abs delta OK\n")

# -- Gate 4: Market cap --
print("-- Gate 4: Market cap < $150M --")
mc_result = fetch_market_cap(SYMBOL)
if isinstance(mc_result, tuple):
    mc, cg_id = mc_result
else:
    mc, cg_id = mc_result, "?"
if mc is None:
    print(f"  {W} CoinGecko: symbol not found -> gate SKIPPED (pass by default)\n")
else:
    print(f"  CoinGecko ID: {cg_id}")
    print(f"  Market cap  : ${mc:>15,.0f}  (max ${CFG['max_market_cap_usd']:,})")
    if mc >= CFG["max_market_cap_usd"]:
        print(f"  {F} Market cap too large -> BLOCKED\n")
    else:
        print(f"  {S} Market cap OK\n")

# -- Gate 5: Range detection (1h klines) --
print("-- Gate 5: Range detection (ATR ratio + range width) --")
klines_raw = get("/fapi/v1/klines", {"symbol": SYMBOL, "interval": "1h", "limit": 22})
klines = [{"open_time": int(k[0]), "high": float(k[2]), "low": float(k[3]),
            "close": float(k[4]), "volume": float(k[5])} for k in klines_raw]
print(f"  Klines 1h   : {len(klines)} bars  (need >= 22)")
if len(klines) < 22:
    print(f"  {F} Insufficient klines -> BLOCKED\n")
else:
    window = CFG["range_lookback_bars_1h"]
    atr_period = CFG["range_atr_period"]
    highs  = [k["high"]  for k in klines[-window-1:-1]]
    lows   = [k["low"]   for k in klines[-window-1:-1]]
    closes = [k["close"] for k in klines[-window-1:-1]]

    rh = max(highs); rl = min(lows); rheight = rh - rl
    rw_pct = (rheight / rl) * 100.0 if rl > 0 else 0
    atr_cur, atr_avg = calc_atr(highs, lows, closes, atr_period)
    atr_ratio = atr_cur / atr_avg if (atr_cur and atr_avg) else None

    print(f"  Range high  : {rh:.6g}")
    print(f"  Range low   : {rl:.6g}")
    print(f"  Range width : {rw_pct:.2f}%  (max {CFG['max_range_width_pct']}%, min {CFG['min_range_width_pct']}%)")
    if atr_ratio:
        print(f"  ATR ratio   : {atr_ratio:.3f}  (max {CFG['range_atr_ratio_max']})")
    else:
        print(f"  ATR ratio   : N/A")

    range_ok = (CFG["min_range_width_pct"] <= rw_pct <= CFG["max_range_width_pct"])
    atr_ok = (atr_ratio is not None and atr_ratio <= CFG["range_atr_ratio_max"])

    if not range_ok:
        print(f"  {F} Range width out of bounds -> BLOCKED")
    elif not atr_ok:
        print(f"  {F} ATR ratio too high ({atr_ratio:.3f} > {CFG['range_atr_ratio_max']}) -> BLOCKED")
    else:
        print(f"  {S} Range OK\n")

    # -- Gate 6: Risk check --
    print("-- Gate 6: Risk in [0.5%, 15%] --")
    entry = klines[-1]["close"]
    sl = rl * (1.0 - CFG["stop_buffer_pct"] / 100.0)
    sl_dist = entry - sl
    risk_pct = (sl_dist / entry) * 100.0 if entry > 0 else 0
    print(f"  Entry       : {entry:.6g}")
    print(f"  SL          : {sl:.6g}  (range_low - {CFG['stop_buffer_pct']}%)")
    print(f"  Risk        : {risk_pct:.2f}%  (range [{CFG['min_risk_pct']}, {CFG['max_risk_pct']}])")
    if sl_dist <= 0:
        print(f"  {F} Entry below range_low -> SL invalid -> BLOCKED\n")
    elif risk_pct < CFG["min_risk_pct"]:
        print(f"  {F} Risk too tight -> BLOCKED\n")
    elif risk_pct > CFG["max_risk_pct"]:
        print(f"  {F} Risk too wide -> BLOCKED\n")
    else:
        print(f"  {S} Risk OK\n")

# -- Volume info (not a gate, but informational) --
print("-- Volume info (vol_ema_multiplier=2.0 gate active) --")
vol_series = [k["volume"] for k in klines]
vol_ema20 = calc_ema(vol_series[:-1], 20)
vol_now = klines[-1]["volume"]
vol_ratio = vol_now / vol_ema20 if vol_ema20 > 0 else 0
print(f"  Vol current : {vol_now:,.0f}")
print(f"  Vol EMA20   : {vol_ema20:,.0f}")
print(f"  Vol ratio   : {vol_ratio:.2f}x  (gate disabled)\n")

print("-- Summary --")
print(f"  OI delta {oi_delta:+.2f}% {'[PASS]' if oi_gate_pass else '[FAIL < 5%]'}")
print()
