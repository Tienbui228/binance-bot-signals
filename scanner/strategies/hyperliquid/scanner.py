"""
Main HlScanner — adaptive scan loop (spec §12).

Scan ordering per coin (churn BEFORE eviction — issue #10):
  1. market_context (batched, once per cycle)
  2. coverage_gate precondition
  3. refresh_positions
  4. clustering (cached, only new addresses query ledger)
  5. apply_cluster_assignments
  6. concentration.measure
  7. load prev scan state
  8. cohort.compute_churn  ← BEFORE eviction
  9. registry.evict_stale  ← AFTER churn
  10. signal.evaluate (with insufficient-history guard)
  11. anti_signal.evaluate (if gates_pass or shadow)
  12. watchlist_manager.log_scan
  13. alert (if detected or should_shadow_alert)

Rate budget (issue #6): log [CYCLE-OVERRUN] if actual duration > 90% of tier interval.
"""

import time
import traceback
from typing import Dict, List, Optional, Set, Tuple

from . import anti_signal, cohort, concentration, coverage, signal
from ._client import _HlHttp
from .alert import fire, fire_shadow
from .clustering import SybilClusterer, apply_cluster_assignments
from .market_context import MarketContext
from .registry import AddressRegistry
from .universe import HlUniverse
from .watchlist.watchlist_manager import WatchlistManager


class HlScanner:
    def __init__(self, cfg: dict, universe: HlUniverse, registry: AddressRegistry):
        self._cfg = cfg
        self._universe = universe
        self._registry = registry
        hl_cfg = cfg.get("hl_whale_accum", {})

        self._coverage_min = float(hl_cfg.get("coverage_min", 0.30))
        self._top_n = int(hl_cfg.get("top_n", 10))
        self._warmup_days = int(hl_cfg.get("warmup_days", 5))
        self._interval_a = int(hl_cfg.get("scan_interval_tier_a_min", 10)) * 60
        self._interval_b = int(hl_cfg.get("scan_interval_tier_b_min", 60)) * 60
        self._interval_c = int(hl_cfg.get("scan_interval_tier_c_min", 360)) * 60

        self._market_ctx = MarketContext(cfg)
        self._clusterer = SybilClusterer(cfg)
        self._wl = WatchlistManager(cfg)

        # Anti-signal needs its own client for l2Book
        self._anti_client = _HlHttp(
            delay_sec=float(hl_cfg.get("http_delay_sec", 0.3)),
            timeout_sec=float(hl_cfg.get("http_timeout_sec", 10.0)),
        )

        # Per-coin: next scheduled scan time
        self._next_scan: Dict[str, float] = {}
        # Per-coin: prev active address count for health metrics
        self._prev_active: Dict[str, int] = {}
        self._prev_reg_size: Dict[str, int] = {}

        # Scanner start time — for warmup elapsed check
        self._start_ts = time.time()

    # ── Public ───────────────────────────────────────────────────────────────

    def scan_once(self, watchlist_coins: List[str]) -> None:
        """Run one scan cycle. Called from run_forever() loop."""
        if not watchlist_coins:
            return

        cycle_start = time.time()

        # Batch market context — one call covers all coins
        try:
            ctx_map = self._market_ctx.fetch_all(watchlist_coins)
        except Exception:
            traceback.print_exc()
            ctx_map = {}

        now = time.time()
        coins_this_cycle = [c for c in watchlist_coins if now >= self._next_scan.get(c, 0)]

        for coin in coins_this_cycle:
            try:
                self._scan_coin(coin, ctx_map.get(coin, {}))
            except Exception as e:
                traceback.print_exc()
                print(f"[HlScanner] error scanning {coin}: {e}")

        elapsed = time.time() - cycle_start
        # Use tier-B interval as cycle budget reference
        if elapsed > self._interval_b * 0.9:
            print(f"[CYCLE-OVERRUN] scan_once took {elapsed:.1f}s (budget={self._interval_b:.0f}s)")

    def run_forever(self) -> None:
        print("[HlScanner] Starting scan loop")
        coins = self._universe.get_watchlist_coins()
        # Load registry for each coin from disk
        for coin in coins:
            self._registry.load_coin(coin)
        print(f"[HlScanner] Loaded {len(coins)} watchlist coins")

        while True:
            try:
                self._universe.refresh_if_stale()
                coins = self._universe.get_watchlist_coins()
                self.scan_once(coins)
            except Exception:
                traceback.print_exc()
            time.sleep(60)

    # ── Per-coin ─────────────────────────────────────────────────────────────

    def _scan_coin(self, coin: str, coin_ctx: dict) -> None:
        if not coin_ctx:
            return

        mark_px = coin_ctx.get("mark_px", 0.0)
        oi_usd = coin_ctx.get("oi_usd", 0.0)

        # Refresh positions BEFORE snapshot so concentration uses current szi data
        self._registry.refresh_positions(coin)
        reg = self._registry.get_all_entries(coin)

        # Clustering (update-only: only new addresses call ledger)
        assignments = self._clusterer.cluster_coin(coin, reg)
        apply_cluster_assignments(reg, assignments)

        # Concentration
        conc_result = concentration.measure(coin, reg, coin_ctx, self._top_n)
        if conc_result is None:
            conc_result = {
                "cluster_notionals": {}, "top_n_clusters": [],
                "top_n_long_notional": 0.0, "top1_share": 0.0,
                "real_concentration": 0.0, "tracked_long_notional": 0.0,
                "distinct_clusters": 0,
            }

        # Coverage gate — single check with fresh data
        tracked_long_fresh = conc_result.get("tracked_long_notional", 0.0)
        cov_ok, cov_estimate = coverage.coverage_gate(
            coin, tracked_long_fresh, oi_usd, self._coverage_min
        )

        # Load prev state for churn (must use canonical cluster_ids)
        state = self._wl.load_state(coin)
        prev_top_n: Set[str] = set(state.get("prev_top_n", []))
        prev_notionals: Dict[str, float] = state.get("prev_cluster_notionals", {})
        conc_history: List[Tuple[int, float]] = state.get("concentration_history", [])
        oi_history: List[float] = state.get("oi_history", [])
        top_n_notional_history: List[float] = state.get("top_n_notional_history", [])

        # Churn — BEFORE eviction (issue #10 ordering)
        curr_top_n = set(conc_result.get("top_n_clusters", []))
        curr_notionals = conc_result.get("cluster_notionals", {})
        churn_result = cohort.compute_churn(prev_top_n, curr_top_n, prev_notionals, curr_notionals)

        # Eviction — AFTER churn (issue #10)
        self._registry.evict_stale(coin, prev_top_n)

        # Warmup check
        elapsed_days = (time.time() - self._start_ts) / 86_400
        warmup_elapsed = elapsed_days >= self._warmup_days

        # Update histories
        now_ms = int(time.time() * 1000)
        real_conc = conc_result.get("real_concentration", 0.0)
        conc_history = (conc_history + [(now_ms, real_conc)])[-40:]  # keep buffer
        oi_history = (oi_history + [oi_usd])[-40:]
        top_n_notional_history = (top_n_notional_history + [conc_result.get("top_n_long_notional", 0.0)])[-40:]

        # Signal evaluation
        sig_result = signal.evaluate(
            coin=coin,
            concentration_history=conc_history,
            oi_history=oi_history,
            churn=churn_result,
            top_n_long_notional_history=top_n_notional_history,
            coin_ctx=coin_ctx,
            cfg=self._cfg,
            coverage_ok=cov_ok,
            warmup_elapsed=warmup_elapsed,
        )
        sig_result["coverage_estimate"] = cov_estimate

        # Anti-signal (only spend l2Book call if gates close to firing)
        anti_result = {"veto": False, "reason": "not_evaluated"}
        if sig_result.get("gates_pass") or sig_result.get("should_shadow_alert"):
            anti_result = anti_signal.evaluate(coin, conc_result, coin_ctx, self._anti_client)

        # Health metrics (drains throttle counter)
        prev_active = self._prev_active.get(coin, 0)
        prev_reg_size = self._prev_reg_size.get(coin, 0)
        reg_after = self._registry.get_all_entries(coin)
        health = coverage.registry_health(coin, reg_after, cov_estimate, prev_active, prev_reg_size)
        self._prev_active[coin] = health["active_address_count"]
        self._prev_reg_size[coin] = health["total_address_count"]

        # Persist scan state
        self._wl.save_state(coin, {
            "last_scan_ts": now_ms,
            "prev_top_n": list(curr_top_n),
            "prev_cluster_notionals": curr_notionals,
            "concentration_history": conc_history,
            "oi_history": oi_history,
            "top_n_notional_history": top_n_notional_history,
            "warmup_start_ts": int(self._start_ts * 1000),
            "coverage_adequate_since_ts": state.get("coverage_adequate_since_ts",
                                                     now_ms if cov_ok else None),
        })

        # Log scan metrics
        self._wl.log_scan(coin, sig_result, churn_result, health, anti_result, conc_result)

        # Schedule next scan based on tier
        tier = _tier_for_coin(conc_history, oi_history)
        interval = {"A": self._interval_a, "B": self._interval_b, "C": self._interval_c}[tier]
        self._next_scan[coin] = time.time() + interval

        # Alerts
        if sig_result.get("detected") and not anti_result.get("veto"):
            fire(coin, sig_result, conc_result, coin_ctx, self._cfg)
        elif sig_result.get("should_shadow_alert") and not anti_result.get("veto"):
            fire_shadow(coin, sig_result, conc_result, coin_ctx, self._cfg)


def _tier_for_coin(conc_history: List[Tuple[int, float]], oi_history: List[float]) -> str:
    """Assign scan tier based on concentration and OI trends."""
    if len(conc_history) < 2 or len(oi_history) < 2:
        return "B"  # not enough data → normal cadence

    conc_values = [v for _, v in conc_history]
    conc_rising = conc_values[-1] > conc_values[-2]
    oi_rising = oi_history[-1] > oi_history[-2]

    if conc_rising and oi_rising:
        return "A"
    if conc_values[-1] < 0.01 and oi_history[-1] < 1_000_000:
        return "C"  # very thin / dead
    return "B"
