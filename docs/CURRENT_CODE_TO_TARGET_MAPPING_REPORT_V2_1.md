# CURRENT_CODE_TO_TARGET_MAPPING_REPORT_V2_1

Status: current code reality to target mapping with Report V2 priority and deferred validation note
Updated: 2026-04-28

## Purpose
Standardize where Report V2 work should happen today, what modules own report semantics, and what must not be changed while implementing the daily review artifact.

## Key point
The project is still operationally centered on `oi_scanner.py`, but Daily Review Report V2 must be implemented downstream-only.

## Additional note
Live validation for `pre_pending` / `pending_open` capture remains deferred until fresh cases after `CUT_MS` are available.
Historical reclassification to `capture_failed` is not proof of fresh post-fix failure.

## Recent changes (2026-04-28) — what changed in code reality

| Change | Files affected |
|--------|---------------|
| Bybit added as second OI/volume data source | `oi_scanner.py` (6 new Bybit methods), `config.yaml` (`bybit.enabled` toggle) |
| MAX cross-exchange OI formula | `oi_scanner.py` (`build_pending_oi_range_breakout_setup`), `scanner/strategies/oi_range_breakout.py` |
| Vol gate removed from ORB strategy | `scanner/strategies/oi_range_breakout.py` |
| `oi_delta_abs_1h` field added (USDT absolute delta, 1h window) | `scanner/strategies/oi_range_breakout.py`, `oi_scanner.py` (Signal dataclass + signal_fields + _process_pending) |
| Cross-exchange fields added: `cross_exchange_confirmed`, `bybit_oi_delta_pct`, `bybit_vol_ok` | `oi_scanner.py` (PendingSetup + pending_fields + _wrap signal) |
| `bybit_vol_24h_usdt` field added to Signal | `oi_scanner.py` (Signal dataclass + signal_fields + _process_pending) |
| Funding rate (Binance + Bybit) added to ORB Telegram signal | `oi_scanner.py` (`bybit_funding()`, `_process_oi_range_breakout_pending`, `format_signal`) |
| `debug_spk.py` standalone gate diagnostic tool added | `debug_spk.py` (new file, repo root) |

---

## Mapping table

| Current file | What it does today | Report V2 target owner | Priority | Safe to change now? | Notes |
|---|---|---|---|---|---|
| `build_daily_review_pack.py` | External DOCX review rendering and last-mile packaging | `review/review_pack_builder.py` | Highest | Yes | Primary owner for Report V2 layout, summary sections, and DOCX rendering. |
| `scanner/review_service.py` | Stage capture, snapshot helpers, review-stage side effects, some review truth shaping | `review/review_truth_service.py` | Highest | Yes, carefully | Use for truth reads and stage aggregation only. Do not add hidden semantic repair. |
| `scanner/lifecycle.py` | Pending/signal persistence helpers, confirm/send sync, close semantics, row enrichment | `lifecycle/case_truth_service.py` | Highest | Only if needed for field exposure | Report V2 may read these fields, but must not rewrite their meaning. |
| `oi_scanner.py` | Operational center. Contains all live dataclasses (PendingSetup, Signal), field lists (signal_fields, pending_fields), strategy orchestration, Binance API client, all Bybit integration methods (bybit_get, bybit_oi_hist, bybit_klines_1h, bybit_ticker_24h, bybit_funding, _combine_oi_histories, _combine_klines_volume), MAX cross-exchange OI formula, funding rate fetch for ORB, Telegram message formatting, and dispatch routing | `app/scanner_runtime.py` plus extracted services | High | Carefully, in small slices only | Only touch if a required field is not flowing into truth and there is no downstream-only path to fix it. Bybit methods living here are pragmatic; target architecture extracts them to a separate client. |
| `scanner/strategies/oi_range_breakout.py` | ORB strategy detection. Now uses MAX(Binance, Bybit) OI delta via `_override_oi_delta_pct` config key. Vol gate removed. Computes `oi_delta_abs_1h` (USDT absolute, informational). Returns `cross_exchange_confirmed`, `bybit_oi_delta_pct`, `bybit_vol_ok`, `oi_delta_abs_1h` in signal dict | same | High | Yes, for strategy logic | Real owner of ORB detection. Do not patch dispatch shim or report builder for strategy logic. |
| `scanner/strategies/long_breakout_retest.py` | Long breakout + retest strategy | same | High | Yes, for strategy logic | Separate family from ORB — never merge. |
| `scanner/strategies/short_exhaustion_retest.py` | Short exhaustion strategy | same | High | Yes, for strategy logic | Separate family. |
| `scanner/strategies/long_accumulation_continuation.py` | Accumulation continuation strategy | same | High | Yes, for strategy logic | Separate family. |
| `scanner/regime/classifier.py` | Regime classification | same | High | Yes | Real owner for regime label. |
| `regime/regime_normalizer.py` | Regime normalize/persist | same | High | Yes | Pairs with classifier. |
| `delivery/delivery_state_evaluator.py` | Delivery metadata annotation | same | Medium | Yes | Annotation only, NOT a gate. |
| `veto/veto_engine.py` | Hard-no veto layer | same | Medium | Carefully | Only hard-no layer in V1. Veto extraction is currently partial — check runtime call flow in `oi_scanner.py` before assuming module owns all hard-no behavior. |
| `scanner/dispatch/router.py` | Dispatch routing | same | Medium | Yes | Real dispatch owner. Do NOT patch `dispatch/dispatch_router.py` (shim only). |
| `scanner/domain.py` | Legacy dataclass copies of PendingSetup/Signal | `contracts/*.py` | Low | Do NOT modify | Live runtime flows through `oi_scanner.py` definitions. `scanner/domain.py` has stale copies — they lag the real dataclasses. Patching here has no live effect and creates confusion. |
| `review/review_pack_builder.py` | Canonical render home after migration | same | Highest | Yes | Preferred place for new V2 summary render helpers if already active in runtime. |
| `review/review_truth_service.py` | Canonical review truth read home after migration | same | High | Yes | Use for clean summary aggregation wrappers, not truth mutation. |
| `scanner/storage.py` | Partitioned CSV infra | `data/storage_adapter.py` | Medium | Yes | Keep boring. Not a place for report semantics. |
| `scanner/binance_client.py` | Standalone Binance API client (used by scanner modules) | same | Medium | Yes | Has its own `funding()` method mirroring the one in `oi_scanner.py`. |
| `scanner/market_math.py` | Market math utilities | same | Medium | Yes | Shared math helpers. |
| `config.yaml` | Main live config. Now includes `bybit.enabled` kill switch | same | High | Yes, for config-only changes | Config-only changes must not touch any `.py` file. When changing ORB thresholds, also update `debug_spk.py` CFG dict to match. |
| `debug_spk.py` | Standalone gate diagnostic tool. Traces all 6 ORB gates for a given symbol with live Binance/CoinGecko data. Does not affect bot state. | same | Low | Yes | Run to debug why a token is not entering ORB signal. CFG values must match `config.yaml`. Does not replace bot-level unit tests. |
| `review_capture_runtime.py` | Stage capture runtime | `review/review_truth_service.py` | High | Yes, carefully | Real owner of stage capture alongside `scanner/review_service.py`. |
| `dispatch/dispatch_router.py` | Migration shim only | `scanner/dispatch/router.py` | Low | Do NOT patch for logic | Not the real owner. Logic changes must go to `scanner/dispatch/router.py`. |

---

## Real owner map (quick reference)

| Layer | Real owner NOW |
|---|---|
| Runtime orchestration | `oi_scanner.py` |
| Strategy — ORB | `scanner/strategies/oi_range_breakout.py` |
| Strategy — long breakout | `scanner/strategies/long_breakout_retest.py` |
| Strategy — short exhaustion | `scanner/strategies/short_exhaustion_retest.py` |
| Strategy — long accumulation | `scanner/strategies/long_accumulation_continuation.py` |
| Bybit API methods | `oi_scanner.py` (pragmatic; target: separate Bybit client) |
| Regime classify | `scanner/regime/classifier.py` |
| Regime normalize/persist | `regime/regime_normalizer.py` |
| Delivery metadata | `delivery/delivery_state_evaluator.py` |
| Veto | `veto/veto_engine.py` |
| Dispatch | `scanner/dispatch/router.py` |
| Lifecycle truth | `scanner/lifecycle.py` |
| Review stage capture | `scanner/review_service.py` + `review_capture_runtime.py` |
| Report V2 rendering | `build_daily_review_pack.py` |
| Gate diagnostic | `debug_spk.py` |

## Files that look canonical but are NOT the real owner
- `dispatch/dispatch_router.py` — migration shim, do not patch for logic changes
- `review/review_pack_builder.py` — future home, not active render owner yet
- `review/review_truth_service.py` — future home, not active stage-truth owner yet
- `contracts/*.py` — target only; live runtime still flows through `oi_scanner.py` dataclasses
- `scanner/domain.py` — stale legacy copies; live runtime uses `oi_scanner.py` definitions

---

## Safe implementation order for Report V2
1. Add summary render sections first.
2. Keep current per-case detail intact.
3. Gate decisions on semantic health.
4. Add advanced boards only after semantic and content sections render correctly.
5. Keep `capture_failed` historical classification visible.
6. Validate fresh-case live capture separately after `CUT_MS`.
7. Move auto-export last.

---

## Phase R1 downstream research mapping addendum

This document remains the **Report V2 mapping doc**.
The section below is an addendum so future AI does not confuse Report V2 downstream review work with the new Phase R1 downstream research pipeline.

### Key point for R1
Phase R1 Daily Top Movers Research is:
- downstream only
- market-data only
- separate from lifecycle truth and review truth
- not a live signal scanner
- not a Report V2 replacement

### Phase R1 mapping table

| Current file | What it does today | Phase R1 target owner | Priority | Safe to change now? | Notes |
|---|---|---|---|---|---|
| `scripts/run_daily_top_movers_research.py` | Main orchestration entry point for daily top movers research | same | Highest | Yes | Real owner for the daily research run. |
| `research/top_movers/research_binance_client.py` | Research-only proxy endpoint client | same | Highest | Yes | Separate from live scanner client by design. |
| `research/top_movers/daily_selector.py` | Top 10 gainers/losers selection, breadth, regime context | same | Highest | Yes | Owns daily selection context, not runtime selection. |
| `research/top_movers/canonical_move.py` | Canonical move detection for one token × one day | same | Highest | Yes | Owns proven-move selection, not live detect logic. |
| `research/top_movers/anchor_detector.py` | P0–P4 anchor detection on 5m timeline | same | Highest | Yes | Owns anchor semantics for research only. |
| `research/top_movers/proxy_features.py` | Proxy formulas, rolling z-score, flow composites | same | Highest | Yes | Canonical owner for R1 proxy semantics. |
| `research/top_movers/decision_mapping.py` | Taxonomy, resolution labels, strategy mapping, caution logic | same | Highest | Yes | Owns report-facing research labels. |
| `research/top_movers/case_builder.py` | Case rows + anchor rows + per-case field assembly | same | Highest | Yes | Central end-to-end R1 field assembler. |
| `research/top_movers/image_renderer.py` | 5 embedded chart images per token | same | High | Yes | Owns visual artifact creation for R1. |
| `research/top_movers/signature_ledger.py` | Daily signature candidates, summary, ledger replace/upsert | same | Highest | Yes | Canonical owner for repeated-signature evidence logic. |
| `research/top_movers/docx_report_builder.py` | Full daily research pack DOCX | same | Highest | Yes | Primary render owner for R1 reading artifact. |
| `research/top_movers/report_builder.py` | Markdown fallback report | same | Medium | Yes | Secondary artifact only. |
| `build_daily_review_pack.py` | Daily Review Report V2 rendering | `review/review_pack_builder.py` | N/A for R1 | No | Out of scope for R1; do not cross-patch. |
| `scanner/review_service.py` | Review truth reads / stage capture helpers | `review/review_truth_service.py` | N/A for R1 | No | R1 must not read or mutate lifecycle/review truth here. |
| `scanner/lifecycle.py` | Pending/signal/result truth semantics | `lifecycle/case_truth_service.py` | N/A for R1 | No | R1 is separate from lifecycle truth. |
| `oi_scanner.py` | Live operational center | `app/scanner_runtime.py` plus extracted services | N/A for R1 | No | R1 must not wire into live runtime. |

### Safe implementation order for Phase R1 follow-up work
1. Keep daily selection and canonical move stable first.
2. Change proxy / taxonomy semantics before changing report wording.
3. Keep case CSVs and DOCX in sync.
4. Treat signature candidates and ledger as a downstream evidence layer.
5. Preserve the distinction between repeated signatures and case-level theses.
6. Keep the pipeline read-only against live truth.

### Hard boundary reminder
Do not treat Phase R1 as:
- a Report V2 extension
- a live strategy patch path
- a lifecycle truth writer
- a universe-wide signal validation engine
