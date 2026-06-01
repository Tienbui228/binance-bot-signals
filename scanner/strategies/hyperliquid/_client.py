"""
Hyperliquid HTTP client — all calls are POST https://api.hyperliquid.xyz/info.

Verified response shapes (2026-06-01):
  clearinghouseState  → {assetPositions: [{type, position:{coin,szi,positionValue,...}}], ...}
  metaAndAssetCtxs    → [{universe:[{name,szDecimals,maxLeverage,...}],...}, [ctx...]]  (index-aligned, len=230)
  candleSnapshot      → [{t,T,s,i,o,c,h,l,v,n}, ...]  (short keys; v=base_volume, no quote_volume)
  recentTrades        → [{coin,side,px,sz,time,hash,tid,users:[addr,addr]}, ...]
  userFills           → [{coin,px,sz,side,time,startPosition,dir,...}, ...]  (no notional — compute px*sz)
  userNonFundingLedgerUpdates → [{time,hash,delta:{type,user,destination,token,amount,...}}, ...]
  fundingHistory      → [{coin,fundingRate,premium,time}, ...]  (hourly cadence confirmed)
  l2Book             → {levels: [[bid_list, ask_list]], time}  (each entry: [px, sz, n])
"""

import time
import threading
from typing import Dict, List, Optional

import requests


_HL_INFO_URL = "https://api.hyperliquid.xyz/info"


class _ThrottleCounter:
    """Module-level per-method throttle/error tracking for §13 metrics."""

    def __init__(self):
        self._lock = threading.Lock()
        self._counts: Dict[str, int] = {}

    def record(self, method: str) -> None:
        with self._lock:
            self._counts[method] = self._counts.get(method, 0) + 1

    def get_and_reset(self) -> Dict[str, int]:
        with self._lock:
            snap = dict(self._counts)
            self._counts.clear()
            return snap


throttle_counter = _ThrottleCounter()


class _HlHttp:
    """Thread-safe Hyperliquid REST client. Each instance owns its own Session."""

    def __init__(self, delay_sec: float = 0.3, timeout_sec: float = 10.0):
        self._delay = delay_sec
        self._timeout = timeout_sec
        self._session = requests.Session()
        self._session.headers.update({"Content-Type": "application/json"})
        self._last_call = 0.0
        self._lock = threading.Lock()

    def _post(self, payload: dict) -> Optional[object]:
        with self._lock:
            gap = time.monotonic() - self._last_call
            if gap < self._delay:
                time.sleep(self._delay - gap)
            try:
                r = self._session.post(_HL_INFO_URL, json=payload, timeout=self._timeout)
                self._last_call = time.monotonic()
                if r.status_code == 429:
                    throttle_counter.record("throttle_429")
                    print(f"[HL-THROTTLE] 429 on payload type={payload.get('type')}")
                    return None
                r.raise_for_status()
                result = r.json()
                if result == [] or result == {} or result is None:
                    throttle_counter.record(f"empty_{payload.get('type', 'unknown')}")
                return result
            except requests.exceptions.Timeout:
                throttle_counter.record("timeout")
                print(f"[HL-TIMEOUT] type={payload.get('type')}")
                return None
            except Exception as e:
                throttle_counter.record("error")
                print(f"[HL-ERROR] type={payload.get('type')}: {e}")
                return None

    def meta_and_asset_ctxs(self) -> Dict[str, dict]:
        """Returns coin_name → ctx dict. Enforces index-alignment internally."""
        raw = self._post({"type": "metaAndAssetCtxs"})
        if not raw or not isinstance(raw, list) or len(raw) != 2:
            return {}
        universe_arr = raw[0].get("universe", [])
        ctx_arr = raw[1]
        if len(universe_arr) != len(ctx_arr):
            print(f"[HL-WARN] metaAndAssetCtxs alignment broken: universe={len(universe_arr)} ctx={len(ctx_arr)}")
            return {}
        return {u["name"]: c for u, c in zip(universe_arr, ctx_arr)}

    def clearinghouse_state(self, address: str, coin: str) -> Optional[dict]:
        """Returns the position dict for the given coin, or None if not found.

        Shape: {coin, szi, leverage, entryPx, positionValue, unrealizedPnl, ...}
        szi is a string — positive = long, negative = short.
        """
        raw = self._post({"type": "clearinghouseState", "user": address})
        if not raw or not isinstance(raw, dict):
            return None
        for ap in raw.get("assetPositions", []):
            pos = ap.get("position", {})
            if pos.get("coin") == coin:
                return pos
        return None

    def clearinghouse_state_all(self, address: str) -> List[dict]:
        """Returns all position dicts for the address (all coins)."""
        raw = self._post({"type": "clearinghouseState", "user": address})
        if not raw or not isinstance(raw, dict):
            return []
        return [ap["position"] for ap in raw.get("assetPositions", []) if "position" in ap]

    def user_fills(self, address: str, start_ms: Optional[int] = None) -> List[dict]:
        """Returns fills list. Each fill: {coin, px, sz, side, time, ...}.
        Notional not included — compute as float(px) * float(sz).
        """
        payload: dict = {"type": "userFills", "user": address}
        if start_ms is not None:
            payload["startTime"] = start_ms
        raw = self._post(payload)
        return raw if isinstance(raw, list) else []

    def recent_trades(self, coin: str) -> List[dict]:
        """Returns [{coin, side, px, sz, time, hash, tid, users:[addr,addr]}, ...].
        Both maker and taker are in users[]. Use for address discovery.
        """
        raw = self._post({"type": "recentTrades", "coin": coin})
        return raw if isinstance(raw, list) else []

    def candle_snapshot(self, coin: str, interval: str, start_ms: int, end_ms: int) -> List[dict]:
        """Returns [{t, T, s, i, o, c, h, l, v, n}, ...].
        Keys: t=open_time_ms, c=close, h=high, l=low, v=base_volume (no quote_volume).
        """
        raw = self._post({
            "type": "candleSnapshot",
            "req": {"coin": coin, "interval": interval, "startTime": start_ms, "endTime": end_ms},
        })
        return raw if isinstance(raw, list) else []

    def candle_snapshot_n(self, coin: str, interval: str, n: int) -> List[dict]:
        """Fetch last n candles for the given interval."""
        interval_ms = _interval_to_ms(interval)
        now_ms = int(time.time() * 1000)
        start_ms = now_ms - (n + 2) * interval_ms
        return self.candle_snapshot(coin, interval, start_ms, now_ms)[-n:]

    def l2_book(self, coin: str) -> dict:
        """Returns {levels: [[bids], [asks]], time}. Each entry: [px, sz, n_orders]."""
        raw = self._post({"type": "l2Book", "coin": coin})
        return raw if isinstance(raw, dict) else {}

    def user_non_funding_ledger_updates(self, address: str, start_ms: Optional[int] = None) -> List[dict]:
        """Returns [{time, hash, delta:{type,user,destination,token,amount,...}}, ...].
        Used for Sybil clustering — look for delta.type=='send' between tracked addresses.
        """
        payload: dict = {"type": "userNonFundingLedgerUpdates", "user": address}
        if start_ms is not None:
            payload["startTime"] = start_ms
        raw = self._post(payload)
        return raw if isinstance(raw, list) else []

    def funding_history(self, coin: str, start_ms: int) -> List[dict]:
        """Returns [{coin, fundingRate, premium, time}, ...]. Hourly cadence confirmed."""
        raw = self._post({"type": "fundingHistory", "coin": coin, "startTime": start_ms})
        return raw if isinstance(raw, list) else []


def _interval_to_ms(interval: str) -> int:
    """Convert interval string to milliseconds."""
    units = {"m": 60_000, "h": 3_600_000, "d": 86_400_000}
    if len(interval) >= 2 and interval[-1] in units:
        try:
            return int(interval[:-1]) * units[interval[-1]]
        except ValueError:
            pass
    return 3_600_000
