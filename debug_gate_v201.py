"""
Diagnostic: check Gate T (price trend) and Gate F (funding) for specified tokens.
Usage: python debug_gate_v201.py [SYMBOL1 SYMBOL2 ...]
       python debug_gate_v201.py  # uses default list
"""

import sys
import statistics
import urllib.request
import json

BINANCE_BASE = "https://fapi.binance.com"

GATE_PRICE_VS_BASELINE_MIN = -0.02   # Gate T: 30d price floor
GATE_PRICE_TREND_7V30_MIN  = -0.02   # Gate T: 7d momentum floor
GATE_FUNDING_8H_PCT_MAX    =  0.03   # Gate F: crowded-long cap (per-8h %)


def fetch_json(url: str) -> object:
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read())


def get_all_usdt_perp_symbols() -> list[str]:
    data = fetch_json(f"{BINANCE_BASE}/fapi/v1/exchangeInfo")
    return [s["symbol"] for s in data["symbols"]
            if s["quoteAsset"] == "USDT" and s["status"] == "TRADING" and s["contractType"] == "PERPETUAL"]


def get_daily_klines(symbol: str, limit: int = 31) -> list[float]:
    """Fetch daily klines, return close prices. [:-1] to drop forming bar."""
    url = f"{BINANCE_BASE}/fapi/v1/klines?symbol={symbol}&interval=1d&limit={limit}"
    raw = fetch_json(url)
    if not raw or len(raw) < 2:
        return []
    # raw is list of lists; index 4 = close
    closes = [float(row[4]) for row in raw[:-1] if row[4]]  # drop last (forming)
    return closes


def get_funding_rate(symbol: str) -> float:
    """Returns current 8h funding rate as percent. Returns 0.0 on error."""
    url = f"{BINANCE_BASE}/fapi/v1/premiumIndex?symbol={symbol}"
    data = fetch_json(url)
    rate = float(data.get("lastFundingRate", 0.0))
    return rate * 100.0  # convert to percent (same as self.funding())


def mean(series: list[float]) -> float:
    return sum(series) / len(series) if series else 0.0


def compute_gates(symbol: str) -> dict:
    closes = get_daily_klines(symbol, limit=31)
    funding = get_funding_rate(symbol)

    if len(closes) >= 14:
        base = mean(closes)
        price_vs_baseline = (closes[-1] / base - 1.0) if base > 0 else 0.0
        n = len(closes)
        m7  = mean(closes[-min(7, n):])
        m30 = mean(closes[-min(30, n):])
        price_trend_7v30 = (m7 / m30 - 1.0) if m30 > 0 else 0.0
    else:
        price_vs_baseline = 0.0
        price_trend_7v30  = 0.0

    gate6 = price_vs_baseline >= GATE_PRICE_VS_BASELINE_MIN
    gate7 = price_trend_7v30  >= GATE_PRICE_TREND_7V30_MIN
    gate8 = funding            <  GATE_FUNDING_8H_PCT_MAX

    return {
        "symbol":             symbol,
        "price_now":          closes[-1] if closes else None,
        "bars":               len(closes),
        "price_vs_baseline":  price_vs_baseline,
        "price_trend_7v30":   price_trend_7v30,
        "funding_8h_pct":     funding,
        "gate6_pass":         gate6,
        "gate7_pass":         gate7,
        "gate8_pass":         gate8,
    }


def find_bas_candidates(all_symbols: list[str]) -> list[str]:
    return [s for s in all_symbols if s.upper().startswith("BAS")]


def print_result(r: dict):
    g6 = "[PASS]" if r["gate6_pass"] else "[FAIL]"
    g7 = "[PASS]" if r["gate7_pass"] else "[FAIL]"
    g8 = "[PASS]" if r["gate8_pass"] else "[FAIL]"
    all_pass = r["gate6_pass"] and r["gate7_pass"] and r["gate8_pass"]
    overall = "ALL_PASS" if all_pass else "HAS_FAIL"

    print(f"\n{'='*52}")
    print(f"  {r['symbol']}  [{overall}]")
    print(f"{'='*52}")
    print(f"  Daily bars fetched : {r['bars']}")
    print(f"  Price now          : {r['price_now']}")
    print(f"  price_vs_baseline  : {r['price_vs_baseline']:+.4f}  (floor >= {GATE_PRICE_VS_BASELINE_MIN:+.2f})  {g6}")
    print(f"  price_trend_7v30   : {r['price_trend_7v30']:+.4f}  (floor >= {GATE_PRICE_TREND_7V30_MIN:+.2f})  {g7}")
    print(f"  funding_8h_pct     : {r['funding_8h_pct']:+.4f}%  (cap <  {GATE_FUNDING_8H_PCT_MAX:.2f}%)  {g8}")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        symbols = [s.upper() if s.upper().endswith("USDT") else s.upper() + "USDT"
                   for s in sys.argv[1:]]
    else:
        # Default: FFUSDT + discover BAS* symbols
        symbols = ["FFUSDT"]

    # Discover BAS* candidates from live exchange
    print("Fetching exchange info to resolve symbols...")
    try:
        all_syms = get_all_usdt_perp_symbols()
        bas_candidates = find_bas_candidates(all_syms)
        if bas_candidates:
            print(f"BAS* perp symbols found: {bas_candidates}")
            symbols += [s for s in bas_candidates if s not in symbols]
    except Exception as e:
        print(f"Warning: could not fetch exchange info: {e}")
        all_syms = []

    print(f"\nChecking {len(symbols)} symbol(s): {symbols}")
    print(f"Thresholds: Gate T(30d)>={GATE_PRICE_VS_BASELINE_MIN}, Gate T(7d)>={GATE_PRICE_TREND_7V30_MIN}, Gate F<{GATE_FUNDING_8H_PCT_MAX}%")

    for sym in symbols:
        if sym not in all_syms and all_syms:
            print(f"\n  {sym}: NOT FOUND in Binance USDT perp universe — skip")
            continue
        try:
            r = compute_gates(sym)
            print_result(r)
        except Exception as e:
            print(f"\n  {sym}: ERROR — {e}")

    print()
