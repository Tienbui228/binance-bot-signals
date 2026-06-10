## CODE_MAP_V2

Status: generated from repo 2026-06-02. Owns the REAL file/owner map.
Source of truth = repo grep, not memory. Companion to ARCHITECTURE_AND_ADR_V2.md.

---

### 1. Live Python file tree

```
./hl_scanner.py
./oi_scanner.py
./regime/__init__.py
./regime/regime_normalizer.py
./scanner/__init__.py
./scanner/dispatch/__init__.py
./scanner/dispatch/router.py
./scanner/domain.py
./scanner/regime/__init__.py
./scanner/regime/classifier.py
./scanner/strategies/__init__.py
./scanner/strategies/_accumulation_features.py
./scanner/strategies/hyperliquid/__init__.py
./scanner/strategies/hyperliquid/_client.py
./scanner/strategies/hyperliquid/alert.py
./scanner/strategies/hyperliquid/anti_signal.py
./scanner/strategies/hyperliquid/clustering.py
./scanner/strategies/hyperliquid/cohort.py
./scanner/strategies/hyperliquid/concentration.py
./scanner/strategies/hyperliquid/coverage.py
./scanner/strategies/hyperliquid/market_context.py
./scanner/strategies/hyperliquid/registry.py
./scanner/strategies/hyperliquid/scanner.py
./scanner/strategies/hyperliquid/signal.py
./scanner/strategies/hyperliquid/universe.py
./scanner/strategies/hyperliquid/watchlist/__init__.py
./scanner/strategies/hyperliquid/watchlist/watchlist_manager.py
./scanner/strategies/long_accumulation_continuation.py
./scanner/strategies/oi_range_breakout.py
```

*(29 files total, excluding .venv and \_\_pycache\_\_)*

---

### 2. Owner by layer (real owner NOW)

| Layer | Real owner | Inline or module |
|---|---|---|
| Runtime orchestration | `oi_scanner.py` | god-file (ADR-1) |
| Data foundation / API client — Binance | `oi_scanner.py` | inline |
| Data foundation / API client — Bybit | `oi_scanner.py` | inline; kill switch `bybit.enabled` in config.yaml |
| Regime classify | `scanner/regime/classifier.py` | module |
| Regime normalize / persist | `regime/regime_normalizer.py` | module |
| Strategy — long_accumulation_continuation | `scanner/strategies/long_accumulation_continuation.py` + `scanner/strategies/_accumulation_features.py` | module; LIVE, `enabled: true` |
| Strategy — oi_range_breakout | `scanner/strategies/oi_range_breakout.py` | module; ready, `enabled: false` |
| Strategy — hyperliquid whale accum | `hl_scanner.py` + `scanner/strategies/hyperliquid/*` | SHADOW — separate process, NOT wired into oi_scanner.py; kill switch `hl_whale_accum.enabled: false` |
| Delivery metadata | `oi_scanner.py` — `evaluate_manual_tradable` @ L1576 | inline |
| Veto / cooldown | `oi_scanner.py` — `should_send` @ L1476 | inline |
| Dispatch | `scanner/dispatch/router.py` — `route_dispatch_v1` | module |
| Lifecycle truth write | `oi_scanner.py` — `save_signal` L1086, `save_pending` L1117, `_sync_confirmed_pending_row` L950, `close_pending` L1231 | inline |
| Storage / CSV infra | `oi_scanner.py` — `read_csv` L967, `write_csv` L989, `append_csv` L1041 | inline |
| Telegram / format | `oi_scanner.py` | inline |
| Domain type hints (stale) | `scanner/domain.py` | used only by `scanner/dispatch/router.py` as type hint source — NOT runtime source of truth (see §7) |

---

### 3. oi_scanner.py anchor table

| Symbol | Line |
|---|---|
| `CODE_BUILD_ID` | L22 |
| `class Signal` | L38 |
| `class PendingSetup` | L132 |
| `signal_fields =` | L271 |
| `pending_fields =` | L310 |
| `self.signals_file` | L263 |
| `self.pending_file` | L265 |
| `self.results_file` | L264 |
| `_sync_confirmed_pending_row` | L950 |
| `read_csv` | L967 |
| `write_csv` | L989 |
| `append_csv` | L1041 |
| `save_signal` | L1086 |
| `save_pending` | L1117 |
| `close_pending` | L1231 |
| `should_send` (veto/cooldown) | L1476 |
| `evaluate_manual_tradable` (delivery) | L1576 |
| `build_pending_oi_range_breakout_setup` | L1920 |
| `build_pending_long_accumulation_continuation_setup` | L1995 |
| `_process_oi_range_breakout_pending` | L2245 |
| `scan_once` | L2970 |

---

### 4. Deleted — do NOT recreate

Verified NOT FOUND in repo 2026-06-02:

```
NOT FOUND: scanner/lifecycle.py
NOT FOUND: scanner/storage.py
NOT FOUND: scanner/binance_client.py
NOT FOUND: scanner/market_math.py
NOT FOUND: delivery/delivery_state_evaluator.py
NOT FOUND: veto/veto_engine.py
NOT FOUND: dispatch/dispatch_router.py
NOT FOUND: build_daily_review_pack.py
NOT FOUND: scanner/review_service.py
NOT FOUND: review_capture_runtime.py
```

---

### 5. Strategy state (khớp ADR-3)

**Active (3 families):**

| Strategy | Status | config toggle | Wiring in oi_scanner.py |
|---|---|---|---|
| `long_accumulation_continuation` | LIVE | `strategy.long_accumulation_continuation.enabled: true` | detect L1672–1673; confirm L1732–1745 |
| `oi_range_breakout` | ready, disabled | `strategy.oi_range_breakout.enabled: false` | detect L1664–1665; confirm L1720–1725 |
| `hyperliquid` whale accum | SHADOW — separate process | `hl_whale_accum.enabled: false` | NOT wired into oi_scanner.py |

**Dead inline — pending delete Round 5:**

| Item | Location | Note |
|---|---|---|
| `_infer_strategy_from_row` | `oi_scanner.py` L2745 | Legacy inference for old CSV rows missing `strategy` field; returns `"short_exhaustion_retest"` for SHORT side — revives deleted strategy label |
| simulate block `strategy = "short_exhaustion_retest"` | `oi_scanner.py` ~L3291 | Test/debug utility, not a detection path |
| `regime_fit_short_exhaustion` field | `scanner/regime/classifier.py` L9, 34, 44, 52 | Computed in every `RegimeVerdict` but never consumed |

`short_exhaustion_retest.py` does **not exist** in `scanner/strategies/` — no separate file remains.

---

### 6. Regime label set (REAL — replaces "5-label" in old docs)

**Exactly 3 canonical labels** (source: `scanner/regime/classifier.py` + `regime/regime_normalizer.py`):

```
trend_continuation_friendly      ← bullish / up / continuation aliases map here
broad_weakness_sell_pressure     ← bearish / down / sell_pressure aliases map here
unclear_mixed                    ← default; range / chop / neutral / unknown aliases map here
```

`regime/regime_normalizer.py` normalizes all legacy aliases → these 3 labels. Never returns a legacy label.

**NOT present in code:** `chop_fake`, `post_pump`, `broad_weakness` (without `_sell_pressure`), or any 4th/5th label. Any doc claiming "5-label" is wrong.

---

### 7. KNOWN RISKS / TECH DEBT

**domain.py divergence — mìn hẹn giờ, CHƯA nổ:**

`scanner/domain.py` defines `Signal` @ L84 and `PendingSetup` @ L154 — **stale subset**, missing ~20 fields compared to the runtime `Signal` in `oi_scanner.py`. Fields present in `oi_scanner.Signal` but absent from `domain.Signal` include: `range_width_pct`, `vol_24h_usdt`, `cross_exchange_confirmed`, `bybit_vol_24h_usdt`, `oi_delta_abs_1h`, `setup_quality_band`, `accumulation_score`, `pos_now`, `acct_now`, `gap_now`, `retail_now`, `taker_now`, `pos_slope_30d`, `gap_slope_30d`, `retail_slope_30d`, `pos_trend_3v14`, `oi_trend_3v14`, `pos_min_30d`, `pos_recovery_from_min`, `retail_min_30d`, `retail_recovery`, `taker_7d_avg`, `market_cap_usd`.

`scanner/dispatch/router.py` L3 imports `domain.Signal` as type hint, but at runtime receives `oi_scanner.Signal` (wired at `oi_scanner.py` L14 import + L3062 call site). Python does not enforce the type hint — no crash today.

**Safe ONLY because** `route_dispatch_v1` reads exactly one field: `signal.score` — present and same type in both definitions.

**Risk:** any expansion of `router.py` that reads a second field absent from `domain.Signal` → **silent wrong behavior or AttributeError**, not a loud failure. Failure mode: router reads a field that exists in `domain.Signal` default (e.g. `strategy: "legacy_5m_retest"`) instead of the live value — wrong dispatch, no exception.

`domain.py` also carries `strategy` default `"legacy_5m_retest"` — a deleted strategy name.

→ Unify contract (make router import from oi_scanner or extract shared contract) is the priority seam refactor. This is **not "under control"** — it will break silently when router is next touched.

---

**short_exhaustion_retest dead inline — pending delete Round 5:**

Three sites in live `.py` files keep the deleted strategy alive as dead code:
- `_infer_strategy_from_row` @ `oi_scanner.py` L2745: infers `"short_exhaustion_retest"` for old CSV rows without `strategy` field
- simulate block @ `oi_scanner.py` ~L3291: test utility hardcodes the label
- `regime_fit_short_exhaustion` field @ `scanner/regime/classifier.py` L9, 34, 44, 52: computed in every verdict, never consumed downstream

Not a runtime bug — no active strategy produces this label. Dọn Round 5.

---

**retest.enabled orphaned config:**

`process_pending_setups` loads `retest_cfg = self.cfg["retest"]` @ L1681. Both active strategies (`oi_range_breakout` and `long_accumulation_continuation`) immediately take the **no-retest path** (`continue` before any `retest_cfg` usage). `retest_cfg` is loaded but never read within either branch. `retest.enabled` is only printed at startup (L2952–2954), not guarding any code path.

Toggle is dead for all currently active strategies. Config key and associated startup prints are orphaned. Dọn Round 5.

---

### 8. Phase R1 top-movers research

NOT present in this repo. No `research/top_movers/` directory exists. Out of scope for oi_scanner.py runtime.
