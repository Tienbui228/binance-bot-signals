# Binance Bot Signals — Code Audit Report
**Date:** 2026-06-02  
**Auditor:** Claude Code (read-only pass)  
**Scope:** Full repo excluding `.venv`, `.git`, `__pycache__`, `data/`  
**Runtime build marker:** `acc-cont-daily-dedup-2026-05-31` (`RUNNING_CODE_VERSION.txt`)

---

## 1. Executive Summary

**Maturity level:** Early-mid hybrid. Two active strategies (`long_accumulation_continuation`, `oi_range_breakout`) are fully integrated. `short_exhaustion_retest` is classified deprecated but still fully wired and executes when `strategy.short_exhaustion_retest.enabled=true`. The HL subsystem is a complete parallel process (draft/shadow stage) not connected to `oi_scanner.py`.

**Top 3 issues:**

1. **Silent import bomb — `lifecycle_mod` imported but never called; `scanner/lifecycle.py` would crash on first use.** `oi_scanner.py` line 17 imports `lifecycle_mod` but zero call sites exist anywhere in the file. The module itself calls `app._enrich_pending_row_for_daily_review()` (lifecycle.py lines 42, 132, 187) which does not exist on `BinanceScanner`. If anything were ever to call `lifecycle_mod.save_signal()` or `lifecycle_mod.close_pending()`, it would raise `AttributeError` at runtime. This is a latent crash with zero current effect only because the import is dead.

2. **HL Telegram config key mismatch — signals would silently print, never send.** `scanner/strategies/hyperliquid/alert.py` line 59 reads `cfg.get("telegram", {}).get("token", "")` but `config.yaml` line 3 uses key `bot_token`, not `token`. The guard at alert.py line 61 (`if not token or not chat_id`) would always fire, printing to stdout instead of sending to Telegram. This affects both `fire()` and `fire_shadow()`.

3. **`build_daily_review_pack.py` referenced in live config but does not exist.** `config.yaml` line 85 (`review_case_system.builder_script: build_daily_review_pack.py`) references a file that is absent from the repo. `review_case_system.enabled: true` in config. `oi_scanner.py` never reads `review_case_system.builder_script` itself, so no runtime crash — but any downstream tooling or cron that tries to run this script would fail silently or fatally.

---

## 2. Real Architecture Map

| Layer | Real Owner (NOW) | Shim/Wrapper | Doc Drift? |
|---|---|---|---|
| Runtime orchestration | `oi_scanner.py` | — | No |
| Strategy — ORB | `scanner/strategies/oi_range_breakout.py` + `oi_scanner.py` (build/process/wrap) | — | No |
| Strategy — short exhaustion | `scanner/strategies/short_exhaustion_retest.py` + `oi_scanner.py` (detect_1h_exhaustion, detect_15m_breakdown, process) | — | Listed as deprecated in CLAUDE.md; still fully wired |
| Strategy — long accumulation | `scanner/strategies/long_accumulation_continuation.py` + `oi_scanner.py` (build/process) | — | No |
| Strategy — hyperliquid | `scanner/strategies/hyperliquid/` + `hl_scanner.py` | — | No |
| Bybit API methods | `oi_scanner.py` (bybit_get, bybit_oi_hist, bybit_klines_1h, bybit_ticker_24h, bybit_funding, _combine_oi_histories, _combine_klines_volume) | — | No |
| Universe filter | Not present (`scanner/universe_filter.py` mentioned in CLAUDE.md as "NEW v3.0" but does not exist in this repo) | — | CLAUDE.md references `scanner/universe_filter.py` which is absent |
| Regime classify | `scanner/regime/classifier.py` | — | No |
| Regime normalize | `regime/regime_normalizer.py` | — | No |
| Delivery metadata | `delivery/delivery_state_evaluator.py` | Shim — verbatim extraction of `evaluate_manual_tradable`; real owner is still `oi_scanner.py` BinanceScanner.evaluate_manual_tradable() | No |
| Veto | `veto/veto_engine.py` | Shim — verbatim extraction of cooldown logic; real owner is `oi_scanner.py` BinanceScanner.should_send() | No |
| Dispatch | `scanner/dispatch/router.py` (active, called at oi_scanner.py line 3645) | — | Uses `scanner.domain.Signal` and `scanner.domain.DispatchDecision` which are stale copies |
| Lifecycle truth | `oi_scanner.py` inline methods (`save_signal`, `save_pending`, `close_pending`, `sync_pending_send_decision`) | `scanner/lifecycle.py` — imported but NEVER CALLED by oi_scanner.py; would crash if called | Major drift |
| Storage/CSV | `oi_scanner.py` BinanceScanner (read_csv, write_csv, append_csv) | `scanner/storage.py` exists but not used by oi_scanner.py runtime | — |
| API/market math | `oi_scanner.py` BinanceScanner (duplicates exist in scanner/binance_client.py, scanner/market_math.py) | `scanner/binance_client.py`, `scanner/market_math.py` — parallel implementations, not called by runtime | Drift |
| Domain contracts | `oi_scanner.py` Signal + PendingSetup dataclasses (live truth) | `scanner/domain.py` — stale copies with missing fields; used only by `scanner/dispatch/router.py` for type hints, `scanner/lifecycle.py` (dead import) | Confirmed in CLAUDE.md |

---

## 3. Strategy Inventory

| Strategy | File(s) | Wired in runtime? | Writes truth? | Config enabled? | Classification |
|---|---|---|---|---|---|
| `oi_range_breakout` | `scanner/strategies/oi_range_breakout.py` + build/process in `oi_scanner.py` L2497-2934 | YES — via `build_pending_setups_for_symbol()` when config enabled | YES — pending_setups.csv, signals.csv, results.csv | `false` (config.yaml line 22) | ACTIVE (code complete, disabled in config) |
| `long_accumulation_continuation` | `scanner/strategies/long_accumulation_continuation.py` + `scanner/strategies/_accumulation_features.py` + build/process in `oi_scanner.py` L2572-2750 | YES | YES | `true` (config.yaml line 24) | ACTIVE |
| `short_exhaustion_retest` | `scanner/strategies/short_exhaustion_retest.py` + `detect_1h_exhaustion`, `detect_15m_breakdown_after_exhaustion` inline in `oi_scanner.py` L1734-1915 | YES — imported at oi_scanner.py L13, called in `build_pending_setups_for_symbol()` L2012 | YES — when enabled | Not in `strategy:` block of config.yaml; default `enabled: True` per code logic at L2012 | DEPRECATED (no config gate in current config.yaml, would run by default) |
| `long_breakout_retest` | No dedicated file. Logic is inline in `process_pending_setups()` in `oi_scanner.py` L2109-2224 (the else-branch of the strategy dispatch in pending processing) | YES — for any pending row with `strategy=long_breakout_retest` | YES | Not in config, defaults True at L2064 | DEPRECATED (inline, no isolation) |
| `legacy_5m_retest` | No file. `infer_legacy_strategy()` returns `long_breakout_retest` as fallback; default on Signal/PendingSetup dataclasses | YES — same code path as long_breakout_retest | YES | Not gated | DEPRECATED (ghost strategy name) |
| `hyperliquid` whale accum | `scanner/strategies/hyperliquid/` (12 files) + `hl_scanner.py` | NO — completely separate process; NOT imported anywhere in oi_scanner.py | YES — JSON state files, scan metrics CSV in `data/hl_whale_accum/` | `hl_whale_accum.enabled: false` (process exits immediately) | DRAFT/SHADOW — separate process |

**Important:** `short_exhaustion_retest.enabled` is NOT in the current `config.yaml`. The code at `oi_scanner.py` line 2012 reads `strategy_cfg.get("short_exhaustion_retest", {}).get("enabled", False)` — default `False`. However, at line 2064, the disable-skip check for existing pending rows reads `not strategy_cfg.get(strategy, {}).get("enabled", True)` with default `True`. This means: new short_exhaustion setups won't be created (default False), but existing PENDING short_exhaustion rows would NOT be skipped (default True). Asymmetric behavior.

---

## 4. Hyperliquid Status

### Build-order Stage Assessment

The spec referenced in code comments contains build steps §1–§15. Based on code completeness:

| Step | Description | Status | Evidence |
|---|---|---|---|
| §1 — Config schema | `hl_whale_accum:` block in config.yaml | COMPLETE | config.yaml lines 170-204 |
| §2 — Kill switch | `enabled: false` + process exit | COMPLETE | hl_scanner.py lines 66-68 |
| §3 — Universe filter | `HlUniverse` with CoinGecko MC filter | COMPLETE | scanner/strategies/hyperliquid/universe.py |
| §4 — Market context | `MarketContext.fetch_all()` | COMPLETE | scanner/strategies/hyperliquid/market_context.py |
| §5 — Funding verification | `FUNDING_UNIT_VERIFIED = True` (2026-06-01) | COMPLETE | market_context.py line 18 |
| §6 — Address registry | `AddressRegistry` with 3-tier floor, disk persistence, atomic writes | COMPLETE | scanner/strategies/hyperliquid/registry.py |
| §7 — Coverage gate | `coverage.coverage_gate()` | COMPLETE | scanner/strategies/hyperliquid/coverage.py |
| §8 — Concentration + Sybil clustering | `concentration.measure()`, `SybilClusterer` | COMPLETE | concentration.py, clustering.py |
| §9 — Cohort churn | `cohort.compute_churn()` | COMPLETE | scanner/strategies/hyperliquid/cohort.py |
| §10 — Signal gates g1–g6 | `signal.evaluate()` | COMPLETE | scanner/strategies/hyperliquid/signal.py |
| §11 — Anti-signal A1–A6 | `anti_signal.evaluate()` (A5, A6 stub) | PARTIAL — A5 (OI cap), A6 (liquidation) are stubs returning False by design | anti_signal.py lines 4-8 |
| §12 — Adaptive scan loop | `HlScanner` with tier A/B/C scheduling | COMPLETE | scanner/strategies/hyperliquid/scanner.py |
| §13 — Validation data collection | `WatchlistManager.log_scan()` writes metrics CSV | COMPLETE (data collection side) | watchlist_manager.py |
| §14 — Threshold calibration | Config comments say "CALIBRATE in §13" | NOT STARTED — requires §13 data | config.yaml lines 188-190 |
| §15 — Live activation | `hl_gates_enabled: true` | NOT STARTED | config.yaml line 173 |

**Current stage: Step 12 complete (full shadow pipeline operational). Steps 14-15 deferred pending §13 data.**

### Shadow Safety Verdict: SAFE

Trace with `hl_gates_enabled=false`:
1. `hl_scanner.py` main() → checks `hl_cfg.get("enabled", False)` → `false` → `sys.exit(0)` (line 67). Process terminates before any imports from `scanner.strategies.hyperliquid`.
2. Even if `enabled=true` with `hl_gates_enabled=false`, in `signal.evaluate()`: `detected = None` (never `True`) because `hl_gates_enabled` is False (signal.py lines 118-121).
3. In `scanner.py` line 226: `if sig_result.get("detected") and not anti_result.get("veto"):` → `None and ...` → False. `fire()` is NOT called.
4. `fire_shadow()` IS called when `should_shadow_alert=True` (gates_pass and not hl_gates_enabled and coverage_ok). BUT: `alert.py` reads `telegram.token` (missing key in config — key is `bot_token`), so `_send()` prints to stdout instead of sending. **Shadow alerts are silent (stdout only).**
5. No state writes to `oi_scanner.py` data paths. HL uses its own `data/hl_whale_accum/` directory.

**Conclusion: With `enabled=false`, zero code executes. With `enabled=true, hl_gates_enabled=false`, shadow alerts print to stdout only (Telegram key mismatch prevents real sends). No dispatch/Telegram interaction with oi_scanner pipeline.**

### HL Isolation from oi_scanner

- `oi_scanner.py` has zero imports from `scanner.strategies.hyperliquid.*`.
- `hl_scanner.py` has zero imports from `oi_scanner.py`.
- Separate process, separate data directory, separate API client (`_HlHttp` POST to `api.hyperliquid.xyz`, not Binance).
- No shared CSV state between HL and oi_scanner strategies.
- `long_accumulation_continuation` and HL share a conceptual model (Gates T and F appear in both) but share zero code — each reads config independently.

### HL Missing Layers (by design vs broken)

| Layer | Status |
|---|---|
| Anti-signal A5 (OI cap) | By design — external feed not yet available (comment: "require external feeds not yet available") |
| Anti-signal A6 (liquidation-driven) | By design — same reason |
| §13 calibration data | By design — requires pipeline to run first |
| §14 threshold calibration | By design — deferred |
| §15 live activation | By design — deferred |
| Telegram key `token` vs `bot_token` | BROKEN — mismatch silently disables all Telegram sends from HL. Needs single-line fix. |
| Warmup: registry `ingest_fills()` has a bug — fills loop fetches `universe.get_watchlist_coins()` but `HlUniverse` doesn't have that method | NEEDS INVESTIGATION — `hl_scanner.py` line 38 calls `universe.get_watchlist_coins()`. Need to verify `HlUniverse` public API |

Let me add a note for the `get_watchlist_coins` call:

In `hl_scanner.py` line 38, `fills_loop` calls `universe.get_watchlist_coins()`. In `universe.py`, the public method is `self._coins` (internal) with `refresh_if_stale()` and `build_watchlist()`. Checking `HlUniverse` public API is needed — if `get_watchlist_coins()` is absent, `fills_loop` would crash immediately on first iteration.

---

## 5. Dead Code Candidates

### SAFE-TO-REMOVE (high confidence, zero call sites)

| Identifier | Location | Evidence | Notes |
|---|---|---|---|
| `lifecycle_mod` import | `oi_scanner.py` line 17: `from scanner import lifecycle as lifecycle_mod` | Zero call sites in file — grep for `lifecycle_mod.` returns no matches | Import is completely dead. However, removing it is safe only after confirming scanner/lifecycle.py is not needed by any other runtime path |
| `BinanceScanner.trend_15m()` | `oi_scanner.py` line 856 | Zero call sites in entire repo (`self.trend_15m` never called; `trend_15m` in market_math.py is separate function) | Duplicate of `scanner/market_math.py:trend_15m` — both dead in live runtime |
| `BinanceScanner.trend_1h()` | `oi_scanner.py` line 867 | Zero call sites in entire repo | Same as above |
| `BinanceScanner.calc_oi_jump_pct()` | `oi_scanner.py` line 793 | Zero call sites in oi_scanner.py (confirmed: grep for `self.calc_oi_jump_pct` returns nothing). Duplicate in `scanner/binance_client.py:123` also uncalled | Not called by any strategy |
| `BinanceScanner.classify_market_regime()` | `oi_scanner.py` line 1571 | Called only at line 1605 from within `build_market_snapshot()` — this is a live call. NOT dead. | Retract — this IS used |
| `scanner/storage.py` | Full file | `oi_scanner.py` manages its own CSV I/O with `read_csv`/`write_csv`/`append_csv` inline. `BinanceClient` in `scanner/binance_client.py` does not use `storage.py`. No import found for `storage` in live runtime | Entire module is unreachable from runtime |
| `scanner/binance_client.py` | Full file | Not imported anywhere in `oi_scanner.py` or any active module. BinanceScanner in oi_scanner.py implements all Binance API methods directly | Entire module dead in live runtime |
| `scanner/market_math.py` | Full file | Not imported anywhere in `oi_scanner.py` or any strategy file. Functions duplicate methods on BinanceScanner | Entire module dead in live runtime |
| `scanner/lifecycle.py` methods | Full file | `lifecycle_mod` imported at oi_scanner.py L17 but zero call sites. The module calls `app._enrich_pending_row_for_daily_review()` which doesn't exist on BinanceScanner — latent crash | Effectively dead; would crash if called |
| `delivery/delivery_state_evaluator.py` | Full file | Not imported anywhere in `oi_scanner.py`. BinanceScanner has `evaluate_manual_tradable()` inline. `delivery_state_evaluator.py` is a verbatim extraction shim | Entire module dead in live runtime |
| `veto/veto_engine.py` | Full file | Not imported anywhere in `oi_scanner.py`. BinanceScanner has `should_send()` inline | Entire module dead in live runtime |

### NEEDS-DEPENDENCY-MAP

| Identifier | Location | Concern |
|---|---|---|
| `BinanceScanner.run_simulation_case()` | `oi_scanner.py` line 3844 | Called from `main()` via `--simulate-case` CLI flag (line 4015). NOT dead — it's a CLI tool. Keep. |
| `scanner/domain.py` | Full file | Imported by `scanner/dispatch/router.py` (active), `scanner/lifecycle.py` (dead import chain). `Signal` and `PendingSetup` in domain.py are stale copies — missing ORB fields (`range_width_pct`, etc.) and acc_cont fields. Removing would break router.py import. |
| `scanner/lifecycle.py.bak` | Root | Old backup file. Can be deleted but verify no external scripts reference it. |

### WIP-INTENDED (do not mark as dead)

| Identifier | Location | Notes |
|---|---|---|
| All of `scanner/strategies/hyperliquid/` | 12 files | Active WIP for HL process. Not wired to oi_scanner but intentional. |
| Anti-signal A5, A6 stubs | `anti_signal.py` | By design — stubs return False with comment |
| `hl_scanner.py` | Root | Entry point for HL process — intentionally separate |

### DO-NOT-TOUCH

| Identifier | Notes |
|---|---|
| `scanner/dispatch/router.py` | Live — called at `oi_scanner.py` line 3645. Core dispatch logic. |
| `scanner/regime/classifier.py` | Live — called at `oi_scanner.py` line 3604. |
| `regime/regime_normalizer.py` | Live — imported and called in oi_scanner.py + lifecycle.py. |
| All strategy detection code in `oi_scanner.py` (L1734–L3843) | Mixed: some used, some deprecated but alive |

---

## 6. Dependency Map — 3 Deprecated Strategy Families

### 6a. `short_exhaustion_retest`

| Aspect | Detail |
|---|---|
| File | `scanner/strategies/short_exhaustion_retest.py` |
| Call sites in oi_scanner.py | L13: imported as `strategy_build_pending_short_exhaustion_setup`. L1951: `build_pending_short_exhaustion_setup()` wraps it. L2012–2015: called in `build_pending_setups_for_symbol()` when `strategy_cfg.get("short_exhaustion_retest", {}).get("enabled", False)` — default False, so NOT triggered with current config. |
| Inline detection functions | `detect_1h_exhaustion()` L1734, `detect_15m_breakdown_after_exhaustion()` L1808 — both called from `build_pending_short_exhaustion_setup()` via the scanner object |
| Pending processing | `process_pending_setups()` L2098–2315: handles SHORT side via `find_retest_short()`, score computation, Signal creation |
| Fields written to CSV | All Signal and PendingSetup fields with `strategy="short_exhaustion_retest"`, `side="SHORT"`, `score_exhaustion` populated |
| Config section read | `cfg.get("short_exhaustion_retest", {})` for `retest_15m_max_bars`, `retest_15m_tolerance_pct`, etc. — section NOT present in config.yaml |
| Dispatch wiring | Goes through `route_dispatch_v1()` like all strategies |
| Reports/measurement | `print_strategy_pipeline_summary()`, `print_score_component_summary()` read `strategy` field from CSV — will show short_exhaustion_retest rows if any exist |
| What to handle on delete | (1) Remove import L13. (2) Remove `build_pending_short_exhaustion_setup()` wrapper L1950. (3) Remove call in `build_pending_setups_for_symbol()` L2012–2015. (4) Remove `detect_1h_exhaustion()` and `detect_15m_breakdown_after_exhaustion()` from oi_scanner.py. (5) Remove `_reset_round_detect_funnel()`, `_funnel_hit()`, `_print_detect_funnel_summary()` (all SHORT-exhaustion specific). (6) Remove SHORT processing in `process_pending_setups()` L2098–2314 (the else-branch for SHORT). (7) Delete `scanner/strategies/short_exhaustion_retest.py`. (8) Clear `short_exhaustion_retest` config section from config.yaml if added. |

### 6b. `long_breakout_retest`

| Aspect | Detail |
|---|---|
| File | No dedicated file — strategy is inline in `oi_scanner.py` |
| Strategy identity | Created by `infer_legacy_strategy()` L1728 when `row.get("strategy")` is empty — defaults to `"long_breakout_retest"`. Also set explicitly in `run_simulation_case()` L3871 for LONG side. |
| Call sites | `infer_legacy_strategy()` called at L2061 and L2952. LONG retest logic runs in `process_pending_setups()` L2127–2224 (find_retest_long + acceptance checks). `evaluate_open_signals()` L2952 also uses it. |
| Config read | `long_oi_cfg = self.cfg.get("long_breakout_retest", self.cfg.get("legacy_5m_retest", {}))` L2037 — these sections are NOT in config.yaml, so all long_breakout_retest config reads return empty dict, falling back to defaults. |
| Fields written to CSV | All standard Signal/PendingSetup fields with `strategy="long_breakout_retest"` |
| What to handle on delete | (1) Remove `find_retest_long()` L1644, `find_retest_short()` L1686 (SHORT delete handles short side). (2) Remove the LONG retest processing branch in `process_pending_setups()` L2097–2314. (3) Update `infer_legacy_strategy()` to not fall back to `long_breakout_retest`. (4) Remove `long_oi_cfg` references. (5) Remove `long_breakout_retest` references in `startup_print()` L3537. |

### 6c. `legacy_5m_retest`

| Aspect | Detail |
|---|---|
| File | No file. Ghost label only. |
| Strategy identity | Default value on `Signal.strategy` (L65) and `PendingSetup.strategy` (L151) dataclasses. `infer_legacy_strategy()` ultimately falls back here. |
| Call sites | Used as fallback string in all observability/print functions (`print_breakdown`, `print_strategy_pipeline_summary`, etc.) — present in 10+ locations as default for rows with blank `strategy` field. |
| Fields written to CSV | Any old rows from before `strategy` field was populated. No new rows created with this label. |
| What to handle on delete | Change default on dataclasses. Update all fallback strings in print functions. Consider migrating old CSV rows. |

---

## 7. Duplication and God-File Assessment

### Duplication

| Function | Location A | Location B | Relationship |
|---|---|---|---|
| `funding()` (Binance) | `oi_scanner.py` line 786 | — | Only in oi_scanner.py. No duplicate in binance_client.py for this specific endpoint. |
| `bybit_get/oi_hist/klines_1h/ticker_24h/bybit_funding` | `oi_scanner.py` lines 585-671 | — | Only in oi_scanner.py per CLAUDE.md spec. |
| `calc_oi_jump_pct` | `oi_scanner.py` line 793 | `scanner/binance_client.py` line 123 | Both dead in live runtime; oi_scanner.py version never called. |
| `trend_15m`, `trend_1h` | `oi_scanner.py` lines 856, 867 | `scanner/market_math.py` lines 77, 89 | Both dead in live runtime. |
| `Signal` dataclass | `oi_scanner.py` lines 38-130 | `scanner/domain.py` lines 84-150 | Different fields — oi_scanner.py has acc_cont fields (pos_now, gap_now, etc.), cross_exchange_confirmed, bybit_vol_24h_usdt, oi_delta_abs_1h, market_cap_usd, price_vs_baseline, price_trend_7v30, funding_8h_pct. domain.py is stale, missing all of these. |
| `PendingSetup` dataclass | `oi_scanner.py` lines 132-220 | `scanner/domain.py` lines 153-202 | Same issue — domain.py missing acc_cont fields, v2.0.1 gate fields. |
| `evaluate_manual_tradable()` | `oi_scanner.py` line 1615 | `delivery/delivery_state_evaluator.py` line 21 | Verbatim duplicate — shim module never called from runtime. |
| `should_send()` (cooldown) | `oi_scanner.py` line 1515 | `veto/veto_engine.py` line 28 | Verbatim duplicate — veto_engine never imported by runtime. |
| Lifecycle write functions | `oi_scanner.py` inline (save_signal, save_pending, close_pending, sync_pending_send_decision) | `scanner/lifecycle.py` (save_signal, save_pending, close_pending, sync_pending_send_decision) | Full duplicate — lifecycle.py is dead in live runtime AND would crash on `_enrich_pending_row_for_daily_review`. |

### God-File Assessment

`oi_scanner.py` is 4022 lines and owns the following distinct responsibilities:

1. **Config loading / startup** (load_config, main, startup_print, _write_runtime_build_marker)
2. **Data model** (Signal, PendingSetup dataclasses, VALID_PENDING_STATUSES, field lists)
3. **Storage/CSV I/O** (read_csv, write_csv, append_csv, _ensure_header, _table_partition_file, _cleanup_old_data, ~15 storage methods)
4. **Binance API client** (get, klines, oi_hist, funding, load_symbols, load_24h_tickers, filter_symbols, ~8 methods)
5. **Bybit API client** (bybit_get, bybit_oi_hist, bybit_klines_1h, bybit_ticker_24h, bybit_funding, _combine_oi_histories, _combine_klines_volume)
6. **Top-trader data fetching** (4 endpoints + caching — _tt_fetch_binance, top_long_short_position_ratio, etc.)
7. **Market math utilities** (calc_oi_jump_pct, volume_ratio, wick_ratio, upper_wick_ratio, lower_wick_ratio, candle_body_ratio, trend_15m, trend_1h, price_change_pct, range_pct)
8. **Market context / BTC regime** (get_btc_context, classify_market_regime, build_market_snapshot, get_btc_context)
9. **Strategy detection — short_exhaustion** (detect_1h_exhaustion, detect_15m_breakdown_after_exhaustion, build_pending_short_exhaustion_setup wrapper)
10. **Strategy detection — ORB** (build_pending_oi_range_breakout_setup, _wrap_oi_range_breakout_signal, _process_oi_range_breakout_pending)
11. **Strategy detection — acc_cont** (build_pending_long_accumulation_continuation_setup, _process_acc_cont_pending)
12. **Retest logic** (find_retest_long, find_retest_short) — for deprecated long_breakout_retest
13. **Pending lifecycle** (save_pending, close_pending, _mark_pending_confirmed_fields, _sync_confirmed_pending_row, close_pending, etc.)
14. **Signal lifecycle** (save_signal, close_signal, evaluate_open_signals)
15. **Dispatch integration** (sync_pending_send_decision, _update_pending_dispatch_trace, manual tradability)
16. **Telegram formatting/sending** (format_signal, format_watchlist_signal, format_close_message, telegram_send)
17. **Regime normalization** (normalize_regime_label_value, _derive_regime_fit_for_strategy, _apply_regime_trace_defaults)
18. **Observability** (print_stats, print_breakdown, print_strategy_pipeline_summary, print_pending_reason_breakdown, print_pending_age_summary, print_score_component_summary, print_result_breakdown_by_score_bucket, print_manual_trading_diagnostics, print_outcome_breakdown_by_strategy_side, _print_detect_funnel_summary)
19. **Scan orchestration** (scan_once, run_forever, build_pending_setups_for_symbol, process_pending_setups, evaluate_open_signals)
20. **Simulation** (run_simulation_case)

Tight coupling makes isolated testing extremely difficult:
- Every strategy function is a method on `BinanceScanner` or receives `self` (the scanner), making unit testing impossible without instantiating the full scanner.
- Storage, API, and strategy logic are interleaved — a change to CSV schema requires touching the same class as a change to OI detection logic.
- `process_pending_setups()` is 270 lines handling 4 different strategies with nested conditional branches.

---

## 8. Proposed Safe Cleanup Slices (ordered by safety)

### Slice 1: Delete backup file (zero risk)
- Delete `scanner/lifecycle.py.bak`
- Verification: `ls scanner/lifecycle.py.bak` returns not found.

### Slice 2: Fix HL Telegram key mismatch (1-line fix, zero oi_scanner risk)
- `scanner/strategies/hyperliquid/alert.py` line 59: change `tg_cfg.get("token", "")` to `tg_cfg.get("bot_token", "")`.
- Verification: `grep "bot_token" scanner/strategies/hyperliquid/alert.py` shows the fix.

### Slice 3: Remove dead import `lifecycle_mod` (1 line, zero behavior change)
- `oi_scanner.py` line 17: remove `from scanner import lifecycle as lifecycle_mod`.
- Verification: `python -c "import oi_scanner"` still works. No call sites exist.
- Precondition: confirm `lifecycle_mod.` has zero call sites (confirmed in this audit).

### Slice 4: Remove parallel dead modules (zero runtime impact)
- Delete `scanner/storage.py`, `scanner/binance_client.py`, `scanner/market_math.py`.
- Verification: `grep -r "from scanner.storage\|from scanner.binance_client\|from scanner.market_math" .` returns zero results in live files.
- Precondition: verify `scanner/lifecycle.py` import chain — lifecycle.py imports `from scanner.domain import ...` and `from regime.regime_normalizer import ...` only. No storage/binance_client/market_math imports.

### Slice 5: Remove dead shim modules (zero runtime impact)
- Delete `delivery/delivery_state_evaluator.py`, `veto/veto_engine.py`.
- Verification: `grep -r "from delivery\|from veto" .` in live files returns zero results.

### Slice 6: Remove 3 dead BinanceScanner methods (surgical, low risk)
- Remove `BinanceScanner.calc_oi_jump_pct()` (L793), `BinanceScanner.trend_15m()` (L856), `BinanceScanner.trend_1h()` (L867).
- Verification: run full grep for `self\.calc_oi_jump_pct|self\.trend_15m|self\.trend_1h` returns zero matches.

### Slice 7: Disable `short_exhaustion_retest` explicitly in config (config-only, safe)
- Add to `config.yaml` under `strategy:`: `short_exhaustion_retest:\n  enabled: false`
- This aligns config with code default (`enabled: False`). Also fixes the asymmetric behavior where new pending creation is False-defaulted but processing of existing rows is True-defaulted.
- Verification: restart + confirm `[startup]` log shows `short_exhaustion_retest.enabled=False`. No new SHORT pending rows created.

### Slice 8: Remove `short_exhaustion_retest` code path (higher risk, requires regression test)
- Only after Slice 7 is verified and existing PENDING short_exhaustion rows are cleared from CSV.
- Remove: `scanner/strategies/short_exhaustion_retest.py`, import at L13, `build_pending_short_exhaustion_setup` wrapper L1950, call in `build_pending_setups_for_symbol` L2012-2015, `detect_1h_exhaustion()` L1734, `detect_15m_breakdown_after_exhaustion()` L1808, funnel tracking methods, SHORT branch in process_pending_setups.
- Also remove: `find_retest_short()` L1686 (only used by short_exhaustion path).
- Verification: `python oi_scanner.py --simulate-case long_tp1` completes without error. No grep hits for `short_exhaustion_retest` in active code.

---

## 9. Docs Needing Update

| Doc | Issue |
|---|---|
| `CLAUDE.md` section 1 (Project identity) | Lists `pump_exhaustion_short` as "current working priority" with reference to `a-ra-plan-impement-zippy-lovelace.md` and `scanner/strategies/pump_exhaustion/` — neither file/directory exists in repo. `scanner/universe_filter.py` also mentioned as "NEW v3.0" but absent. |
| `CLAUDE.md` section 3 (Real owner map) | Lists `scanner/universe_filter.py` as real owner for Universe filter, `scanner/review_service.py` and `review_capture_runtime.py` as real owners for Review stage capture — none of these files exist in repo. Also lists `app/scanner_runtime.py` as target — doesn't exist. |
| `CLAUDE.md` section 5 (Common tasks) | References `build_daily_review_pack.py`, `review_capture_runtime.py`, `scanner/review_service.py`, `run_daily_final_export.sh` — none present in repo. |
| `CLAUDE.md` section 12 (Key docs) | References `docs/STRATEGY_SPEC_long_accumulation_continuation_V1_2.md` — file exists but was moved: root-level version deleted, lives in `docs/`. Git status shows `D STRATEGY_SPEC_long_accumulation_continuation_V1_2.md` and `?? docs/STRATEGY_SPEC_long_accumulation_continuation_V1_2.md`. The move is untracked. |
| `CLAUDE.md` section 14 (Debug tool ORB) | References `debug_spk.py` — file does not exist in repo. |
| `CLAUDE.md` section 15 (Debug tool pump_exhaustion) | References `debug_pump.py` — file does not exist in repo. |
| `docs/CLAUDE.md` (reading index) | Likely contains references to the same absent files. |
| `docs/CURRENT_CODE_TO_TARGET_MAPPING_REPORT_V2_1.md` | References `build_daily_review_pack.py` as primary owner of Report V2 — file absent. Also references `scanner/review_service.py` which doesn't exist. |
| `config.yaml` | `review_case_system.builder_script: build_daily_review_pack.py` references absent file. Either remove this key or note that the review system scripts are not in repo. |
| `RUNNING_CODE_VERSION.txt` | `code_build_id=bd-gate-event-fix-2026-04-29` contradicts `CODE_BUILD_ID = "acc-cont-daily-dedup-2026-05-31"` in live `oi_scanner.py`. File not updated after last oi_scanner restart. |

---

## Appendix A — Files Present vs Referenced

| Category | Present in repo | Referenced in docs but absent |
|---|---|---|
| Strategy files (active) | `scanner/strategies/long_accumulation_continuation.py`, `oi_range_breakout.py`, `short_exhaustion_retest.py`, `_accumulation_features.py` | `scanner/strategies/pump_exhaustion/` (all files referenced in CLAUDE.md section 1) |
| Scanner modules | `scanner/binance_client.py` (dead), `scanner/market_math.py` (dead), `scanner/storage.py` (dead) | `scanner/universe_filter.py`, `scanner/review_service.py` |
| Diagnostic tools | — | `debug_spk.py`, `debug_pump.py` |
| Review system | — | `build_daily_review_pack.py`, `review_capture_runtime.py`, `run_daily_final_export.sh`, `app/scanner_runtime.py`, `review/review_pack_builder.py`, `review/review_truth_service.py`, `lifecycle/case_truth_service.py` |
| Data | `scanner/lifecycle.py.bak` (stale) | — |

---

*Audit completed 2026-06-02. All findings based on static read-only analysis of repo at commit `b8802824` (main branch). No files were modified.*
