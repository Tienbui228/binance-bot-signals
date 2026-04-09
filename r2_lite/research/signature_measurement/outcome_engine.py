"""
outcome_engine.py
Computes outcome metrics for each kept event (Section 10, 11, 12).

Outcome is measured from trigger_ts_ms + price_at_trigger over:
  30m (6 x 5m bars), 1h (12 bars), 4h (48 bars)

No entry-feasibility logic is applied (spec Section 10).
All outcome is measured from the trigger candle close price.

Offline research only.
"""
from __future__ import annotations

from typing import Any, Dict, List

import numpy as np
import pandas as pd

from research.signature_measurement.contracts import F, OutcomeClass, MovePersistenceCode

BAR_MS = 5 * 60 * 1000  # 5 minutes in ms

# Horizon definitions: (label, num_bars)
HORIZONS = [("30m", 6), ("1h", 12), ("4h", 48)]


# ---------------------------------------------------------------------------
# Future path metrics (Section 10.1)
# ---------------------------------------------------------------------------

def compute_future_path_metrics(
    events: List[Dict[str, Any]],
    candles_by_symbol: Dict[str, pd.DataFrame],
) -> List[Dict[str, Any]]:
    """
    For each event, look into future candles to compute:
      - max favorable move (high relative to entry for LONG)
      - max adverse move (low relative to entry for LONG)
      - close above breakout level at each horizon
      - reclaim breakout (dipped below then closed above)
      - speed metrics (bars to reach 1%/2%/3% favor or adverse)

    Updates events in-place with outcome fields.
    """
    for ev in events:
        sym = ev[F.SYMBOL]
        ts_ms = int(ev[F.TRIGGER_TS_MS])
        price = float(ev[F.PRICE_AT_TRIGGER]) if ev[F.PRICE_AT_TRIGGER] != "" else None
        bl = float(ev[F.BREAKOUT_LEVEL]) if ev[F.BREAKOUT_LEVEL] != "" else None

        if price is None or price <= 0:
            _mark_outcome_unavailable(ev, "price_at_trigger_missing")
            continue

        kdf = candles_by_symbol.get(sym)
        if kdf is None or kdf.empty:
            _mark_outcome_unavailable(ev, "symbol_candles_missing")
            continue

        # Locate trigger row in klines
        future = kdf[kdf["open_time"] > ts_ms].sort_values("open_time").reset_index(drop=True)

        if future.empty:
            _mark_outcome_unavailable(ev, "no_future_bars")
            continue

        # Compute per-bar returns vs trigger price
        future_high_ret = (future["high"] - price) / price * 100   # positive = favor (LONG)
        future_low_ret = (price - future["low"]) / price * 100     # positive = adverse (LONG)

        # Speed metrics
        ev[F.TIME_TO_1PCT_FAVOR_MIN] = _time_to_threshold(future, future_high_ret, 1.0)
        ev[F.TIME_TO_2PCT_FAVOR_MIN] = _time_to_threshold(future, future_high_ret, 2.0)
        ev[F.TIME_TO_3PCT_FAVOR_MIN] = _time_to_threshold(future, future_high_ret, 3.0)
        ev[F.TIME_TO_1PCT_ADVERSE_MIN] = _time_to_threshold(future, future_low_ret, 1.0)
        ev[F.TIME_TO_2PCT_ADVERSE_MIN] = _time_to_threshold(future, future_low_ret, 2.0)

        any_horizon_available = False

        for label, n_bars in HORIZONS:
            fut_slice = future.iloc[:n_bars]
            available = len(fut_slice) >= n_bars

            fav_field = f"future_{label}_max_favor_pct"
            adv_field = f"future_{label}_max_adverse_pct"
            avail_field = f"outcome_{label}_available_YN"

            ev[F.field(f"outcome_{label}_available_YN")] = "Y" if available else "N"

            if available:
                any_horizon_available = True
                ev[F.field(fav_field)] = round(float(fut_slice["high"].max() - price) / price * 100, 4)
                ev[F.field(adv_field)] = round(float(price - fut_slice["low"].min()) / price * 100, 4)

                # Close above breakout
                last_close = float(fut_slice["close"].iloc[-1])
                above = "Y" if (bl is not None and last_close > bl) else "N"

                if label == "30m":
                    ev[F.CLOSE_ABOVE_BREAKOUT_AFTER_30M_YN] = above
                elif label == "1h":
                    ev[F.CLOSE_ABOVE_BREAKOUT_AFTER_1H_YN] = above
                    ev[F.RECLAIM_BREAKOUT_1H_YN] = _reclaim_yn(fut_slice, price, bl)
                elif label == "4h":
                    ev[F.RECLAIM_BREAKOUT_4H_YN] = _reclaim_yn(fut_slice, price, bl)
            else:
                ev[F.field(fav_field)] = ""
                ev[F.field(adv_field)] = ""
                if label == "30m":
                    ev[F.CLOSE_ABOVE_BREAKOUT_AFTER_30M_YN] = ""
                elif label == "1h":
                    ev[F.CLOSE_ABOVE_BREAKOUT_AFTER_1H_YN] = ""
                    ev[F.RECLAIM_BREAKOUT_1H_YN] = ""
                elif label == "4h":
                    ev[F.RECLAIM_BREAKOUT_4H_YN] = ""

        if not any_horizon_available:
            ev[F.OUTCOME_NOT_AVAILABLE_REASON] = "insufficient_future_bars"
        else:
            ev[F.OUTCOME_NOT_AVAILABLE_REASON] = ""

    return events


def _time_to_threshold(future: pd.DataFrame, ret_series: pd.Series,
                        threshold: float) -> Any:
    """Return minutes to first bar where ret_series >= threshold, or empty if not reached."""
    hit = ret_series[ret_series >= threshold]
    if hit.empty:
        return ""
    first_idx = hit.index[0]
    return round((first_idx + 1) * 5.0, 1)  # each bar = 5 min


def _reclaim_yn(fut_slice: pd.DataFrame, price: float, bl: Any) -> str:
    """
    Y if: price dipped below breakout_level at some point, then closed above it at end.
    If breakout_level is unknown, return N.
    """
    if bl is None or bl <= 0:
        return "N"
    dipped = (fut_slice["low"] < bl).any()
    final_above = float(fut_slice["close"].iloc[-1]) > bl
    return "Y" if (dipped and final_above) else "N"


def _mark_outcome_unavailable(ev: Dict, reason: str) -> None:
    for label, _ in HORIZONS:
        ev[F.field(f"outcome_{label}_available_YN")] = "N"
        ev[F.field(f"future_{label}_max_favor_pct")] = ""
        ev[F.field(f"future_{label}_max_adverse_pct")] = ""
    ev[F.CLOSE_ABOVE_BREAKOUT_AFTER_30M_YN] = ""
    ev[F.CLOSE_ABOVE_BREAKOUT_AFTER_1H_YN] = ""
    ev[F.RECLAIM_BREAKOUT_1H_YN] = ""
    ev[F.RECLAIM_BREAKOUT_4H_YN] = ""
    ev[F.OUTCOME_NOT_AVAILABLE_REASON] = reason


# ---------------------------------------------------------------------------
# Payoff metrics (Section 11.1)
# ---------------------------------------------------------------------------

def compute_payoff_metrics(
    events: List[Dict[str, Any]],
    adverse_floor: float = 0.10,
) -> List[Dict[str, Any]]:
    """
    payoff_{horizon} = max_favor / max(max_adverse, adverse_floor)
    """
    for ev in events:
        for label in ["30m", "1h", "4h"]:
            fav_f = F.field(f"future_{label}_max_favor_pct")
            adv_f = F.field(f"future_{label}_max_adverse_pct")
            pay_f = F.field(f"payoff_{label}")
            if ev.get(fav_f, "") != "" and ev.get(adv_f, "") != "":
                fav = float(ev[fav_f])
                adv = float(ev[adv_f])
                ev[pay_f] = round(fav / max(adv, adverse_floor), 4)
            else:
                ev[pay_f] = ""
    return events


# ---------------------------------------------------------------------------
# Outcome class (Section 12)
# ---------------------------------------------------------------------------

def classify_outcome_class(
    events: List[Dict[str, Any]],
    thresholds: dict,
) -> List[Dict[str, Any]]:
    """
    Assigns outcome_class per event based on Section 12 logic.
    Uses configurable thresholds from research_config.yaml.
    """
    for ev in events:
        if ev.get(F.ELIGIBLE_FOR_MEASUREMENT_YN) != "Y":
            ev[F.OUTCOME_CLASS] = ""
            continue

        # Not available yet
        if ev.get(F.OUTCOME_1H_AVAILABLE_YN) != "Y":
            ev[F.OUTCOME_CLASS] = OutcomeClass.NOT_AVAILABLE_YET
            continue

        fav_1h = _ff(ev, F.FUTURE_1H_MAX_FAVOR_PCT)
        adv_1h = _ff(ev, F.FUTURE_1H_MAX_ADVERSE_PCT)
        fav_30m = _ff(ev, F.FUTURE_30M_MAX_FAVOR_PCT)
        adv_30m = _ff(ev, F.FUTURE_30M_MAX_ADVERSE_PCT)
        fav_4h = _ff(ev, F.FUTURE_4H_MAX_FAVOR_PCT)
        payoff_1h = _ff(ev, F.PAYOFF_1H)
        close_above_1h = ev.get(F.CLOSE_ABOVE_BREAKOUT_AFTER_1H_YN, "N")

        # FAIL: adverse develops early and materially
        fail_adv = thresholds.get("fail_early_adverse_pct", 2.0)
        if adv_30m is not None and adv_30m >= fail_adv:
            ev[F.OUTCOME_CLASS] = OutcomeClass.FAIL
            continue

        # A_PLUS (primary: 1h criteria)
        a_plus_fav_1h = thresholds.get("a_plus_1h_favor_pct", 4.0)
        a_plus_adv_1h = thresholds.get("a_plus_1h_adverse_pct_max", 1.5)
        a_plus_close = thresholds.get("a_plus_close_above_1h", True)
        if (fav_1h is not None and fav_1h >= a_plus_fav_1h
                and adv_1h is not None and adv_1h <= a_plus_adv_1h
                and (not a_plus_close or close_above_1h == "Y")):
            ev[F.OUTCOME_CLASS] = OutcomeClass.A_PLUS_MOVE
            continue

        # A_PLUS (alternate via 4h)
        if ev.get(F.OUTCOME_4H_AVAILABLE_YN) == "Y":
            a_plus_fav_4h = thresholds.get("a_plus_4h_favor_pct", 8.0)
            if fav_4h is not None and fav_4h >= a_plus_fav_4h and adv_1h is not None and adv_1h <= a_plus_adv_1h:
                ev[F.OUTCOME_CLASS] = OutcomeClass.A_PLUS_MOVE
                continue

        # A_MOVE
        a_fav = thresholds.get("a_move_1h_favor_pct", 2.5)
        a_pay = thresholds.get("a_move_payoff_1h_min", 2.0)
        if (fav_1h is not None and fav_1h >= a_fav
                and payoff_1h is not None and payoff_1h >= a_pay):
            ev[F.OUTCOME_CLASS] = OutcomeClass.A_MOVE
            continue

        # B_MOVE
        b_fav = thresholds.get("b_move_1h_favor_pct", 1.0)
        if fav_1h is not None and fav_1h >= b_fav:
            ev[F.OUTCOME_CLASS] = OutcomeClass.B_MOVE
            continue

        # NOISE
        noise_max = thresholds.get("noise_max_1h_favor_pct", 1.0)
        if fav_1h is not None and fav_1h < noise_max and (adv_1h is None or adv_1h < fail_adv):
            ev[F.OUTCOME_CLASS] = OutcomeClass.NOISE
            continue

        ev[F.OUTCOME_CLASS] = OutcomeClass.NOISE

    return events


# ---------------------------------------------------------------------------
# Move persistence code (Section 11.2)
# ---------------------------------------------------------------------------

def classify_move_persistence(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Classify each event's move structure:
      PERSISTENT_CONTINUATION: favor grows from 30m -> 1h -> 4h
      FAST_THEN_STALL: large 30m favor but 4h not significantly better
      FAST_THEN_REVERSE: large initial favor, then adverse exceeds favor
      SLOW_GRIND: modest 30m but growing 4h
      NO_FOLLOWTHROUGH: minimal favor at all horizons
      FAILED_EARLY: early adverse dominates
    """
    for ev in events:
        fav_30m = _ff(ev, F.FUTURE_30M_MAX_FAVOR_PCT)
        fav_1h = _ff(ev, F.FUTURE_1H_MAX_FAVOR_PCT)
        fav_4h = _ff(ev, F.FUTURE_4H_MAX_FAVOR_PCT)
        adv_1h = _ff(ev, F.FUTURE_1H_MAX_ADVERSE_PCT)
        adv_30m = _ff(ev, F.FUTURE_30M_MAX_ADVERSE_PCT)

        if fav_1h is None:
            ev[F.MOVE_PERSISTENCE_CODE] = ""
            continue

        # Failed early: adverse >= 2% in first 30m
        if adv_30m is not None and adv_30m >= 2.0:
            ev[F.MOVE_PERSISTENCE_CODE] = MovePersistenceCode.FAILED_EARLY
            continue

        if fav_1h < 0.5:
            ev[F.MOVE_PERSISTENCE_CODE] = MovePersistenceCode.NO_FOLLOWTHROUGH
            continue

        if fav_4h is not None:
            # Growing vs stalling
            if fav_4h >= fav_1h * 1.5 and fav_1h >= 1.5:
                ev[F.MOVE_PERSISTENCE_CODE] = MovePersistenceCode.PERSISTENT_CONTINUATION
            elif fav_30m is not None and fav_30m >= 1.5 and fav_4h < fav_30m * 1.2:
                # Fast start, didn't grow much
                if adv_1h is not None and adv_1h > fav_1h * 0.6:
                    ev[F.MOVE_PERSISTENCE_CODE] = MovePersistenceCode.FAST_THEN_REVERSE
                else:
                    ev[F.MOVE_PERSISTENCE_CODE] = MovePersistenceCode.FAST_THEN_STALL
            elif fav_30m is not None and fav_30m < 0.5 and fav_4h >= 1.5:
                ev[F.MOVE_PERSISTENCE_CODE] = MovePersistenceCode.SLOW_GRIND
            else:
                ev[F.MOVE_PERSISTENCE_CODE] = MovePersistenceCode.PERSISTENT_CONTINUATION
        else:
            # No 4h data
            if fav_1h >= 2.0:
                ev[F.MOVE_PERSISTENCE_CODE] = MovePersistenceCode.PERSISTENT_CONTINUATION
            else:
                ev[F.MOVE_PERSISTENCE_CODE] = MovePersistenceCode.SLOW_GRIND

    return events


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ff(ev: Dict, field: str) -> Any:
    """Get float value from event dict, return None if missing or empty."""
    v = ev.get(field, "")
    if v == "" or v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Patch F class with .field() convenience lookup
# ---------------------------------------------------------------------------

# Allow F.field("future_30m_max_favor_pct") -> the constant value
# This avoids building a reverse map separately.

class _FMeta:
    """Extends F with a static field lookup."""
    _reverse: Dict[str, str] = {}

    @classmethod
    def build_reverse(cls):
        from research.signature_measurement.contracts import F as _F
        for k, v in vars(_F).items():
            if isinstance(v, str) and not k.startswith("_"):
                cls._reverse[v] = v


_FMeta.build_reverse()

# Monkey-patch F to support field() lookup
from research.signature_measurement.contracts import F as _F_cls
_F_cls.field = staticmethod(lambda name: name)  # type: ignore
