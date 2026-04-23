"""
Debug script: fetch live SPK data and trace through all ORB gate conditions.
Run from repo root: python debug_spk.py
"""
import math
import requests

SYMBOL = "AXLUSDT"
BASE = "https://fapi.binance.com"

# -- Config thresholds (from config.yaml) --
CFG = {
    "min_quote_volume_usdt_24h": 2_000_000,
    "max_symbols": 80,
    "oi_spike_min_pct": 5.0,
    "max_market_cap_usd": 150_000_000,
    "max_range_width_pct": 60.0,
    "min_range_width_pct": 0.0,
    "range_atr_period": 14,
    "range_atr_ratio_max": 5.0,
    "range_lookback_bars_1h": 20,
    "min_risk_pct": 0.5,
    "max_risk_pct": 15.0,
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
print(f"  24h volume  : ${qv:>15,.0f}  (min ${CFG['min_quote_volume_usdt_24h']:,})")
print(f"  Last price  : ${price}")

if qv < CFG["min_quote_volume_usdt_24h"]:
    print(f"  {F} Volume < ${CFG['min_quote_volume_usdt_24h']:,} -> BLOCKED")
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
oi_hist = get("/futures/data/openInterestHist", {"symbol": SYMBOL, "period": "5m", "limit": 13})
oi_bars = [{"ts": int(x["timestamp"]), "oi_value": float(x.get("sumOpenInterestValue", 0))} for x in oi_hist]
print(f"  Bars returned: {len(oi_bars)}  (need >= 13)")
if len(oi_bars) < 13:
    print(f"  {F} Insufficient OI history -> BLOCKED\n")
    raise SystemExit
print(f"  {S} OI history OK")
oi_now    = oi_bars[-1]["oi_value"]
oi_1h_ago = oi_bars[-13]["oi_value"] if len(oi_bars) >= 13 else oi_bars[0]["oi_value"]
oi_delta  = (oi_now - oi_1h_ago) / oi_1h_ago * 100.0 if oi_1h_ago > 0 else 0.0
print(f"  OI now      : ${oi_now:>15,.0f}")
print(f"  OI 1h ago   : ${oi_1h_ago:>15,.0f}")
print(f"  OI delta    : {oi_delta:+.2f}%\n")

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
print("-- Volume info (vol_ema_multiplier=0.0 -> disabled) --")
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
