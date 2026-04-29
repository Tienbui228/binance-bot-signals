# CLAUDE.md — Binance Bot Signals Project

> Read this file fully before touching any code. It exists to prevent wrong-file patches,
> broken semantics, and architecture drift. Updated: 2026-04-30.

---

## 1. Project identity (locked)

- **Manual-trading signal bot** — not auto-trading. Signals go to a human via Telegram.
- Python 3.12.3, `.venv` at repo root.
- Live on Ubuntu VPS, runtime process is `oi_scanner.py` running inside a `screen` session.
- Five active strategy families — keep them **always separate** in code, review, measurement:
  - `long_breakout_retest`
  - `short_exhaustion_retest` ← BD gate 3-bar scan fix applied 2026-04-29
  - `long_accumulation_continuation`
  - `oi_range_breakout` — has Bybit OI + MAX formula + funding rate
  - `pump_exhaustion_short` ← **current working priority**: v3.0 integration into oi_scanner.py
- Current working priority: **pump_exhaustion v3.0** — integrating `scanner/strategies/pump_exhaustion/` into `oi_scanner.py`, sharing UniverseFilter + BinanceScanner client. Plan: `a-ra-plan-impement-zippy-lovelace.md`.

---

## 2. Layer architecture (locked order)

```
Data Foundation
  → Regime
    → Strategy Thesis
      → Delivery Metadata   ← annotation only, NOT the main gate
        → Veto              ← ONLY hard-no layer in V1
          → Dispatch        ← routes publication mode, never redetects strategy
            → Lifecycle Truth write
              → Review Truth / Stage Capture   ← canonical truth
                → Report / DOCX rendering      ← downstream only
                  → Research / Measurement     ← Phase R1, offline
```

**Invariants that must never be broken:**
- `confirmed` ≠ `sent` — keep these distinct always
- Not-yet-reached timestamps → store/render `not_reached_yet`, never fake timestamps
- Veto is the **only** hard-no layer. Delivery and dispatch must not silently behave like veto.
- Dispatch never redetects strategy — it only routes based on upstream outputs.
- Reports are downstream renderers — they may aggregate and rank, never repair truth.
- Strategy families stay separate — never merge or collapse them implicitly.

---

## 3. Current code reality — hybrid state

The repo is **not yet fully migrated** to target architecture.
Some target modules exist but are wrappers/shims. Patching them instead of the real owner is a silent failure.

### Real owner map (always check this before patching)

| Layer | **Real owner NOW** | Target owner later | Shim / wrapper (do NOT treat as real owner) |
|---|---|---|---|
| Runtime orchestration | `oi_scanner.py` | `app/scanner_runtime.py` | — |
| Strategy — ORB | `scanner/strategies/oi_range_breakout.py` | same | — |
| Strategy — long breakout | `scanner/strategies/long_breakout_retest.py` | same | — |
| Strategy — short exhaustion | `scanner/strategies/short_exhaustion_retest.py` | same | — |
| Strategy — long accumulation | `scanner/strategies/long_accumulation_continuation.py` | same | — |
| Strategy — pump exhaustion | `scanner/strategies/pump_exhaustion/` (discovery, scanner, outcome, alert, scoring, detectors, classifiers, watchlist) | same | `pump_exhaustion_short/` ← old standalone, keep untouched |
| Universe filter | `scanner/universe_filter.py` (NEW — v3.0) | same | — |
| Bybit API methods | `oi_scanner.py` (pragmatic; 6 methods: bybit_get, bybit_oi_hist, bybit_klines_1h, bybit_ticker_24h, bybit_funding, _combine_*) | separate Bybit client (target) | — |
| Regime classify | `scanner/regime/classifier.py` | same | — |
| Regime normalize/persist | `regime/regime_normalizer.py` | same | — |
| Delivery metadata | `delivery/delivery_state_evaluator.py` | same | — |
| Veto | `veto/veto_engine.py` | same | — |
| Dispatch | `scanner/dispatch/router.py` | same | `dispatch/dispatch_router.py` ← shim only |
| Lifecycle truth | `scanner/lifecycle.py` | `lifecycle/case_truth_service.py` | — |
| Review stage capture | `scanner/review_service.py` + `review_capture_runtime.py` | `review/review_truth_service.py` | `review/review_truth_service.py` ← not real owner yet |
| Report V2 rendering | `build_daily_review_pack.py` | `review/review_pack_builder.py` | `review/review_pack_builder.py` ← not real owner yet |
| Legacy runtime models | `scanner/domain.py` | `contracts/*.py` | `contracts/*.py` ← target only; runtime still uses **`oi_scanner.py`** definitions (NOT domain.py) |
| Storage/CSV infra | `scanner/storage.py` | same | — |
| API/market math | `scanner/binance_client.py`, `scanner/market_math.py` | same | — |
| ORB gate diagnostic | `debug_spk.py` (repo root) | same | — |
| Phase R1 research | `scripts/run_daily_top_movers_research.py` + `research/top_movers/*` | same | — |

### Files that look canonical but are NOT the real owner yet
- `dispatch/dispatch_router.py` — migration shim, do not patch for logic changes
- `review/review_pack_builder.py` — future home, not active render owner
- `review/review_truth_service.py` — future home, not active stage-truth owner
- `contracts/*.py` — important for migration, but live runtime still flows through `oi_scanner.py` dataclasses
- `scanner/domain.py` — **stale legacy copies** of PendingSetup/Signal. Live `oi_scanner.py` definitions are the truth. Fields added to `oi_scanner.py` (e.g., `oi_delta_abs_1h`, `bybit_vol_24h_usdt`, `cross_exchange_confirmed`, `funding_pct`) are NOT in `scanner/domain.py`. Do not patch domain.py expecting live effect.

---

## 4. Config toggles that matter (`config.yaml`)

Critical toggles to check before behavior changes:

```yaml
strategy:
  long_breakout_retest.enabled        # on/off per strategy family
  short_exhaustion_retest.enabled
  long_accumulation_continuation.enabled

retest:
  enabled                             # disables full retest gate for long_breakout_retest

bybit:
  enabled: true                       # kill switch — false disables ALL Bybit fetching for ORB
                                      # affects: OI combine, volume combine, vol_24h, funding rate

pump_exhaustion:
  enabled: true/false                 # kill switch — false skips all 4 integration points in oi_scanner.py
                                      # threads never start, scan_once block skipped, zero effect on main bot

universe_filter:
  enabled: true                       # shared across strategies (pump_exhaustion uses this)

review_snapshots:
  enabled
  save_pre_pending                    # controls pre_pending stage capture

review_case_system:
  enabled
  builder_script                      # which script runs Report V2

observability:
  enabled                             # all debug/pipeline output sub-flags under this

volume_gate_mode: SOFT_TAG            # vs HARD — changes liquidity gate behavior
```

**Config files in scope for runtime:**
- `config.yaml` — main live config
- `config_noreview.yaml` — no review system
- `config_nosnap.yaml` — no snapshots

Never declare a patch complete without checking which config the running process actually loaded.

---

## 5. Common tasks → exact file clusters

### Strategy logic is wrong
**Primary:** `scanner/strategies/<family>.py` + `config.yaml`
**Also check:** `oi_scanner.py`, `scanner/domain.py`
**Report consumer only if output fields changed:** `build_daily_review_pack.py`
**Do NOT start with:** report builder, dispatch shim, veto file

### ORB signal missing / OI data wrong / Bybit not included
**Primary:** `scanner/strategies/oi_range_breakout.py` (detection + MAX formula) + `oi_scanner.py` (`build_pending_oi_range_breakout_setup`, Bybit methods)
**Config check:** `bybit.enabled` in `config.yaml`; `debug_spk.py` CFG must match `config.yaml`
**Diagnostic:** run `python debug_spk.py` (change SYMBOL first) — shows which gate fails and actual values
**Do NOT patch:** `scanner/domain.py` (stale copies, no live effect), `dispatch/dispatch_router.py`

### Funding rate in ORB Telegram signal wrong or missing
**Primary:** `oi_scanner.py` — `bybit_funding()` method, `_process_oi_range_breakout_pending()` (fetch block), `format_signal()` ORB block
**Formula:** `funding_pct = binance_fr + bybit_fr` (current cycle, both in %)
**Note:** `funding_pct` is fetched live at confirmation time, not stored at pending creation

### Regime label / fit looks wrong
**Primary:** `scanner/regime/classifier.py` + `regime/regime_normalizer.py`
**Also check:** `oi_scanner.py`, `scanner/lifecycle.py`, `contracts/regime_result.py`

### confirmed / sent / close semantics are wrong
**Primary:** `scanner/lifecycle.py`
**Also check:** `oi_scanner.py`, `scanner/domain.py`, `scanner/review_service.py`
**Report is downstream only** — do not start debugging here

### Stage capture / snapshot status wrong (`pre_pending`, `pending_open`, etc.)
**Primary:** `scanner/review_service.py` + `review_capture_runtime.py`
**Also check:** `scanner/lifecycle.py`, `oi_scanner.py`
**Validation:** fresh rows only after `CUT_MS`

### Report V2 section / semantics looks wrong
**Primary:** `build_daily_review_pack.py`
**Also check:** `review/review_pack_builder.py` (keep aligned), `scanner/review_service.py` if truth field missing, `run_daily_final_export.sh`
**Rule:** stay downstream-only unless a missing truth field forces upstream work

### Dispatch / MAIN_SIGNAL / WATCHLIST logic wrong
**Primary:** `scanner/dispatch/router.py`
**Also check:** `oi_scanner.py`, `scanner/domain.py`, `contracts/dispatch_result.py`
**Do NOT patch:** `dispatch/dispatch_router.py` alone

### Veto / NO_SEND behavior wrong
**Primary:** `veto/veto_engine.py` + `oi_scanner.py`
**Caution:** veto extraction is currently partial — check runtime call flow in `oi_scanner.py` before assuming the module owns all hard-no behavior

### Adding a new field end-to-end
Always touch in this order:
1. Source layer owner (strategy/regime/delivery/veto/dispatch/lifecycle)
2. `oi_scanner.py` integration path — dataclass definition, field lists (`signal_fields` / `pending_fields`), wrap/process functions
3. `scanner/domain.py` **only if** the field is used by a non-ORB strategy that still flows through domain.py; for ORB fields, skip domain.py entirely
4. Matching `contracts/*.py` for target contract (awareness only, does not affect live runtime)
5. Persistence writer (`scanner/lifecycle.py` or review truth writer)
6. Report builder if visible downstream

Never claim "added field" after changing only one file.

---

## 6. Mandatory plan format before coding

For every code task, produce this before writing any code:

**Problem summary** — what is broken in plain language
**Layer diagnosis** — which layer (pipeline / strategy / delivery / dispatch / lifecycle-review / measurement)
**Constraints** — what must not change, what is deferred
**Exact file set:**
  - primary real owner
  - secondary sync files
  - files explicitly not to touch
**Validation plan** — what rows/logs to inspect, fresh-case only if runtime patch, exact pass condition
**Risks / caveats**

Then code.

If the task is ambiguous, ask a precise clarifying question. Do not assume broader scope.

---

## 7. Runtime patch discipline (mandatory)

Python loads source into memory at process start. Editing a file after process start does NOT change running behavior. Old `screen` sessions silently keep old code alive.

### After every runtime patch:
```bash
# 1. Verify patch hit disk
grep "<changed_function_or_marker>" oi_scanner.py

# 2. Update build marker in oi_scanner.py
CODE_BUILD_ID = "description-YYYY-MM-DD"

# 3. Kill old screen sessions
screen -list
screen -X -S <session_name> quit

# 4. Record CUT_MS AFTER killing old sessions
python3 -c "import time; print(int(time.time() * 1000))"

# 5. Start fresh session
screen -S bot python oi_scanner.py

# 6. Verify process start time is AFTER patch time
ps -eo pid,lstart,cmd | grep oi_scanner | grep -v grep
ls -la oi_scanner.py

# 7. Verify build marker
cat RUNNING_CODE_VERSION.txt
```

### Validation rule
Only judge behavior from rows where `created_ts_ms >= CUT_MS` or `confirmed_ts_ms >= CUT_MS`.
Old rows in cumulative daily CSVs do NOT prove anything about the patched runtime.

### Infrastructure pass ≠ behavior pass
A patch can pass syntax check, import check, file grep, and screen startup — and still fail real behavior if the process was not restarted.

---

## 8. Report V2 rules (locked)

- Report is **downstream-only** — it may aggregate, rank, summarize, separate
- Report must **NOT** repair truth, invent timestamps, relabel missing stage evidence, or rewrite `case_close_type`
- **Semantic health is a hard gate** — expose it before any optimization summary
- Separate **truth-clean rows** from **semantic-broken rows** explicitly
- `capture_failed` on historical rows = better historical classification, NOT proof of current live regression
- To prove a current live regression: inspect fresh rows after `CUT_MS` only

### Stage status values (locked)
- `captured` — stage was captured
- `not_reached_yet` — stage has not happened yet (never use fake timestamp)
- `capture_failed` — historical gap, snapshot was never taken
- `missing_unexpected` — unresolved semantic issue

### `case_close_type` values (locked)
- `true_close`
- `fallback_4h_snapshot`
- `not_due_yet` → close time must be `not_reached_yet`, not a real timestamp

---

## 9. Phase R1 research pipeline (hard boundary)

Phase R1 is a **separate downstream research pipeline**. It must never wire into live bot runtime.

**Real owner:** `scripts/run_daily_top_movers_research.py`
**Module cluster:** `research/top_movers/*`
**Config:** `research_top_movers_config.yaml`
**Outputs:** `data/research_output/top_movers/`, `data/research_cache/`

**Files that are OUT OF SCOPE for any R1 task:**
`oi_scanner.py`, `scanner/lifecycle.py`, `scanner/review_service.py`,
`build_daily_review_pack.py`, `scanner/strategies/*`, `scanner/regime/classifier.py`,
`config.yaml`, `config_noreview.yaml`, `config_nosnap.yaml`

---

## 10. What is deferred (do not open without explicit approval)

- Sprint 3B / 5-label regime expansion
- Strategy threshold tuning
- Veto expansion
- Dispatch redesign
- Big-bang module rewrite
- Optimization from semantically broken or mixed old/new CSV rows

---

## 11. Measurement / optimization discipline

- Optimize from canonical truth and decision trace, not raw winrate tables alone
- If semantic trust is broken, stop and fix truth first
- Do not draw conclusions from rows that mix pre-fix and post-fix data
- Prefer one-rule-at-a-time interventions
- Keep regime, strategy family, delivery, veto, dispatch logically separable in analysis
- Historical `capture_failed` rows are visible but excluded from optimization conclusions

---

## 12. Key docs (read-only, do not modify)

Located in `docs/` or project root:

| Doc | Purpose |
|---|---|
| `Binance_Bot_Architecture_Blueprint_V1_4.md` | North-star architecture, deferred items |
| `IMPLEMENTATION_CONTRACT_V1_1.md` | Invariants, layer contracts, priority lock |
| `REVIEW_SYSTEM_SEMANTIC_SPEC_V2_5.md` | Stage semantics, Report V2 rules |
| `FIELD_PROPAGATION_MAP_V1.md` | Where each field is decided / propagated / persisted / rendered |
| `binance_bot_detailed_code_mapping_audit_2026-04-05.md` | Real owner vs target owner per layer |
| `CODE_OWNERSHIP_AND_CHANGE_IMPACT_MAP_V1.md` | Full repo change-impact map |
| `CURRENT_CODE_TO_TARGET_MAPPING_REPORT_V2_1.md` | Report V2 implementation map + real owner map for all layers (updated 2026-04-28) |
| `RUNTIME_DEPLOY_TEST_GUARDRAILS.md` | Runtime patch validation rules |
| `POST_PATCH_CHECKLIST.md` | Mandatory checklist after any runtime patch |
| `WORKED_EXAMPLES_V1_1.md` | Concrete semantic examples for Report V2 |
| `measurement_summary_template_v1_5.md` | Measurement template with data quality gate |

---

## 13. Lesson learned — incidents (mandatory reading before any patch)

### Incident 2026-04-22: "Sửa 1 dòng config → bot crash fatal loop"

**Yêu cầu ban đầu:** Đổi `max_range_width_pct: 20 → 60` trong `config.yaml` — 1 dòng duy nhất.

**Điều đã xảy ra:** Trong cùng session, Claude tự ý thêm code vào `oi_scanner.py` (thêm ORB detail fields vào PendingSetup, thêm logic process_pending_setups) mà user không yêu cầu. Những thay đổi này kéo theo chuỗi crash + rollback + re-crash kéo dài nhiều vòng.

**Root cause thực sự của crash:** Một pending row trên VPS có `created_ts_ms = 'not_evaluated'` (dữ liệu bị corrupt từ version cũ). Code trong `scanner/review_service.py:556` dùng `float(x or 0)` — pattern này crash nếu `x` là string truthy nhưng không phải số (vd: `'not_evaluated'`). Lỗi này **tồn tại trước** mọi thay đổi trong session, nhưng bị che khuất bởi scope creep.

**Fix cuối cùng:** Wrap float conversion trong try/except ở `review_service.py` — 10 dòng thay 2 dòng. `oi_scanner.py` không cần thay đổi gì.

---

**Lessons — Claude PHẢI tuân thủ:**

#### L1. Strict scope — không tự mở rộng phạm vi
Nếu user yêu cầu thay 1 dòng config → chỉ thay đúng 1 dòng đó. **Không bao giờ** thêm code vào file khác "vì tiện" hoặc "để hoàn thiện thêm". Mọi thay đổi ngoài yêu cầu phải được user đồng ý trước bằng văn bản rõ ràng.

#### L2. CSV field parsing phải luôn defensive
Bất kỳ field nào đọc từ CSV và cast sang `int`/`float` đều phải dùng try/except:
```python
# SAI — crash nếu value là 'not_evaluated', 'N/A', etc.:
val = int(float(row.get("field") or 0))

# ĐÚNG — skip row bị corrupt thay vì crash toàn bộ loop:
try:
    _v = row.get("field")
    val = int(float(_v)) if _v not in (None, "") else 0
except (ValueError, TypeError):
    val = 0
```
VPS CSVs chứa rows được ghi bởi code cũ/buggy. Garbage values LUÔN tồn tại trong production data.

#### L3. `float(x or 0)` là pattern nguy hiểm
`x or 0` chỉ fallback khi `x` là falsy (`None`, `""`, `0`, `False`). Nếu `x = 'not_evaluated'` → truthy → `float('not_evaluated')` → crash. Luôn dùng try/except thay vì `or 0`.

#### L4. Thêm traceback vào mọi catch-all exception handler
`[fatal loop warn] {e}` không có traceback → mất hàng giờ debug. Pattern đúng:
```python
except Exception as e:
    import traceback
    print(f"[fatal loop warn] {e}")
    traceback.print_exc()
```
Giữ traceback logging vĩnh viễn trong `run_forever()` — không tốn gì, tiết kiệm nhiều.

#### L5. Trước khi rollback: xác định exact crash line trước
Rollback mù (không biết crash ở dòng nào) → có thể rollback sai file, lỗi vẫn còn. Luôn thêm traceback → đọc line number → chỉ fix đúng file đó.

#### L6. Mọi config-only request → chỉ sửa config
Nếu task là thay đổi một giá trị trong `config.yaml`, không touch bất kỳ `.py` file nào trừ khi user nói rõ.

---

## 14. Debug tool — ORB gate diagnostic

**File:** `debug_spk.py` (repo root)

Script standalone dùng để trace live tại sao 1 token **không** đi vào hệ thống ORB. Chạy local, không cần bot đang chạy.

### Cách dùng

```bash
# Sửa SYMBOL ở đầu file, rồi chạy:
python debug_spk.py
```

Đổi `SYMBOL = "AXLUSDT"` thành token cần debug.

### 6 gates được trace

| Gate | Điều kiện | Nguồn data |
|------|-----------|------------|
| 1 | Symbol có trong Binance USDT perp + 24h volume ≥ min, ≤ max + rank ≤ max_symbols | `/fapi/v1/ticker/24hr` |
| 2 | Có ≥ 13 bars lịch sử OI (5m) | `/futures/data/openInterestHist` |
| 3 | OI delta 1h ≥ oi_spike_min_pct (5%) | tính từ bar[-1] vs bar[-13] |
| 4 | Market cap < max_market_cap_usd (CoinGecko) | CoinGecko API |
| 5 | Range width trong [min, max]% + ATR ratio ≤ atr_ratio_max | 20 bars 1h klines |
| 6 | Risk % trong [min_risk_pct, max_risk_pct] | SL = range_low × (1 - stop_buffer) |

Output `[PASS]` / `[FAIL]` / `[SKIP]` cho từng gate + giá trị thực để so sánh trực tiếp.

### Khi nào dùng

- User báo "token X không vào bot" → đổi SYMBOL, chạy script, đọc gate nào FAIL
- Cần biết OI thực tế hiện tại, range width, risk % của 1 token cụ thể
- Verify config thresholds có hợp lý với market hiện tại không

### Config thresholds trong script

Các giá trị trong `CFG` dict ở đầu file phải khớp với `config.yaml` section `oi_range_breakout` và `scanner`. Khi thay đổi config, **cập nhật `debug_spk.py` CFG tương ứng**:

```python
CFG = {
    "min_quote_volume_usdt_24h": 2_000_000,
    "max_quote_volume_usdt_24h": 300_000_000,
    "max_symbols": 300,
    "oi_spike_min_pct": 5.0,
    "max_market_cap_usd": 500_000_000,
    "max_range_width_pct": 60.0,
    "min_risk_pct": 0.5,
    "max_risk_pct": 60.0,
    ...
}
```

> Script này **không** sửa CSV, không ảnh hưởng bot đang chạy — chỉ đọc data từ Binance/CoinGecko và in kết quả.

---

## 15. Debug tool — pump_exhaustion gate diagnostic

**File:** `debug_pump.py` (repo root)

Script standalone dùng để trace tại sao 1 token **không** đi vào hệ thống `pump_exhaustion_short`. Chạy local, không cần bot đang chạy. Thresholds đọc trực tiếp từ `config.yaml`.

### Cách dùng

```bash
# Mặc định dùng SYMBOL ở đầu file:
python debug_pump.py

# Hoặc override qua CLI:
python debug_pump.py AEROUSDT
python debug_pump.py BTC        # tự thêm USDT
```

### Gates được trace (theo thứ tự)

| Gate | Điều kiện | Nguồn data |
|------|-----------|------------|
| UNIVERSE | Market cap ≤ 500M USD | CoinGecko API hoặc `data/eligible_universe.json` |
| WATCHLIST | Token đã trong watchlist chưa? → hiện state + anchors | `data/pump_exhaustion/watchlist.json` |
| D1 | Base detection valid (vol spike tìm thấy, window không trending) | 200 × 1h klines |
| D2 | pump_pct ≥ 35% (giá từ base → peak) | 200 × 1h klines |
| D3 | pump_vol_ratio ≥ 3x (vol tại peak vs avg 20 bars trước) | 200 × 1h klines |
| D4 | peak_age_h ≤ 72h (peak chưa quá cũ) | 200 × 1h klines |
| D5 | room_pct ≥ 15% (giá hiện tại vẫn còn đủ cao trên base) | 200 × 1h klines |
| D8 | anchor order valid: p0_ts < p1_ts | timestamps |
| SCANNER | 5m quote_vol_median ≥ 10,000 USDT | 288 × 5m klines |
| OI | Snapshot OI 6h cuối | `/futures/data/openInterestHist` |

Output `[PASS]` / `[FAIL]` / `[INFO]` cho từng gate với giá trị thực.

### Khi nào dùng

- User báo "token X không vào pump_exhaustion" → chạy script, đọc gate nào FAIL
- Token đã trong watchlist → hiển thị state machine hiện tại (DISCOVERED / BREAKDOWN_CONFIRMED / RETEST_WAITING / FAILED_RETEST_CONFIRMED)
- Cần biết pump_pct, room_pct, peak_age thực tế của 1 token

### State machine của pump_exhaustion

```
DISCOVERED
  → [detect_breakdown] → BREAKDOWN_CONFIRMED
    → [detect_retest] → RETEST_WAITING → FAILED_RETEST_CONFIRMED
      → [score_case] → OUTCOME_PENDING (signal fired nếu score ≥ 6/12)
  → EXCLUDED (peak_too_old / room_too_small / false_break_reclaim / ...)
```

### Config thresholds

Script đọc từ `config.yaml` sections `pump_exhaustion.discovery`, `pump_exhaustion.scan`, `universe_filter`. Khi thay đổi config, script tự cập nhật — không cần sửa tay.

> Script này **không** sửa CSV, không ảnh hưởng bot đang chạy.

---

## 16. Phase R1 — V4-1 pipeline run + validation

> **Status: V4-1 VALIDATED — 2026-04-29. All 12 checks passed.**

V4-1 adds: 7d dump selection (top 10 tokens by 7d decline), V4 case identity fields (`research_case_id`, `selection_horizon`, `runtime_linkage_status`, etc.), and fills all previously-blocking Layer 0-1 fields (`case_inclusion_reason`, `semantic_clean_flag`, `exclusion_reason`, `dataset_batch`, `day_range_pct`, `intraday_expansion_pct`, `rank_volume_24h`, `notional_volume_usd`, `rank_abs_change_24h`).

Sau khi implement V4-1 (hoặc bất kỳ thay đổi nào trong `research/top_movers/*`), phải chạy pipeline để tạo data rồi mới validate được.

### Bước 1: Chạy pipeline để tạo data

```bash
python3 scripts/run_daily_top_movers_research.py --day YYYY-MM-DD
```

Dùng ngày đã qua (complete daily bar). Ví dụ:

```bash
python3 scripts/run_daily_top_movers_research.py --day 2026-04-25
```

Output: `data/research_output/top_movers/YYYY-MM-DD/csv/daily_case_dataset_YYYY-MM-DD.csv`

### Bước 2: Validate V4-1 output

```bash
python3 scripts/validate_v4_1.py YYYY-MM-DD
```

Script chạy theo đúng thứ tự sau (12 checks — phải pass hết):

| # | Check | Pass condition |
|---|-------|----------------|
| 1 | `research_case_id` format | `{symbol}_{research_day}_{selection_horizon}` — 0 mismatch |
| 2 | `selection_horizon` populated & valid | Values trong `{"1d","7d","1d_gainers","1d_losers"}`, 0 null |
| 3 | 7d cases tồn tại | `n_7d > 0` |
| 4 | Layer 0 blocking fields populated | `case_inclusion_reason`, `semantic_clean_flag`, `exclusion_reason`, `dataset_batch` — không phải all-null |
| 5 | Layer 1 blocking fields populated | `day_range_pct`, `intraday_expansion_pct`, `rank_volume_24h`, `notional_volume_usd`, `rank_abs_change_24h` — không phải all-null |
| 6 | `live_universe_eligible_flag` có mặt | Column tồn tại, không bị missing |
| 7 | Dedup: 1d ∩ 7d phải là 2 rows riêng | Mỗi symbol overlap có ≥ 2 rows |
| 8 | `also_in_7d_top10` đúng trên 1d overlap rows | Tất cả 1d rows của overlap symbols có flag = True |
| 9 | `runtime_equivalent_case_id` là null | Không có non-null values (V4-1 chưa làm linkage) |
| 10 | `case_id` format không đổi | `{YYYYMMDD}_{symbol}_{side}` — tất cả rows match |
| 11 | Phase 2A-2E fields còn đủ | `resolution_label`, `data_quality_flag`, `decision_grade`, `future_1h/4h_max_favor_pct`, `research_eligible_YN` — vẫn hiện diện |
| 12 | Import isolation | `grep "from scanner"` và `grep "import oi_scanner"` trong `research/top_movers/` → zero results |

Exits 0 nếu tất cả pass, exits 1 nếu có lỗi.

**Lưu ý:** Pipeline phải chạy trên VPS (venv build trên Linux, không chạy được local Windows).

---

## 17. Phase R1 — V4-2 pipeline run + validation

> **Status: V4-2 IMPLEMENTED — 2026-04-30. Pending validation run.**

V4-2 fixes Layer 2 BLOCKING: thêm 10 fields bị thiếu vào `case_builder.py` — không cần API call thêm, tất cả lấy từ `AnchorPoint.bar` dict và timestamp arithmetic.

**Fields được thêm:**

| Field | Source | Note |
|---|---|---|
| `p1_price` | `anchors.p1.bar["high"]` | HIGH của P1 bar (peak_high) |
| `peak_close` | `anchors.p1.bar["close"]` | CLOSE của P1 bar |
| `p3_price` | `anchors.p3.bar["close"]` | CLOSE của P3 retest bar, null khi không có retest |
| `bars_p0_to_p1` | timestamp arithmetic (1h) | Thường = 0 (sub-hour detection) |
| `bars_p1_to_p2` | timestamp arithmetic (1h) | Thường = 0 (fast breakdown) |
| `bars_p2_to_p3` | timestamp arithmetic (5m) | Null khi p3_ts_ms là None |
| `bars_p3_to_p4` | timestamp arithmetic (5m) | Null khi p3/p4 là None |
| `anchor_reason_code` | map từ `anchor_validity_reason` | `clean` / `p1_fallback` / `p1_p2_fallback` / `unknown:*` |
| `peak_age_hours` | hours từ P1 đến cuối research day | Float |
| `case_spans_days` | P0.date ≠ P4.date | Thay thế default `False` của V4-1 |

**Files đã thay đổi:**
- `research/top_movers/case_builder.py` — thêm 10 fields + 3 helpers (`_map_anchor_reason_code`, `_compute_peak_age_hours`, `_compute_case_spans_days`)
- `research/top_movers/signature_ledger.py` — update `_LAYER_CONTRACT[2]["required_fields"]`
- `scripts/validate_v4_2.py` — validation script mới (11 checks)

### Bước 1: Chạy pipeline để tạo data

```bash
python3 scripts/run_daily_top_movers_research.py --day YYYY-MM-DD
```

### Bước 2: Validate V4-2 output

```bash
python3 scripts/validate_v4_2.py YYYY-MM-DD
```

| # | Check | Pass condition |
|---|-------|----------------|
| 1 | 10 new Layer 2 fields có mặt | Không all-null (p3_price/bars_p2_to_p3/bars_p3_to_p4/peak_age_hours null_ok) |
| 2 | `p1_price >= peak_close` | 0 cases vi phạm |
| 3 | `p1_price` vs `range_high` | WARN nếu >30% cases lệch >5% (không phải hard fail) |
| 4 | Bar counts non-negative | `bars_p0_to_p1 >= 0`, `bars_p2_to_p3 >= 1` cho non-null |
| 5 | `anchor_reason_code` values hợp lệ | Trong `{clean, p1_fallback, p1_p2_fallback}` hoặc `unknown:*` |
| 6 | `anchor_reason_code` correlation | `all_anchors_auto_detected` → `clean`, 0 mismatch |
| 7 | `peak_age_hours` dương | 0 negative values |
| 8 | `case_spans_days` không null | 0 null values |
| 9 | Layer 2 full coverage | 20 required fields đều có mặt trong schema |
| 10 | Phase 2 fields không biến mất | `resolution_label`, `anchor_conflict_flag`, etc. vẫn còn |
| 11 | V4-1 fields backward compat | `research_case_id`, `selection_horizon`, etc. vẫn còn |

Exits 0 nếu tất cả pass, exits 1 nếu có lỗi.

**Lưu ý:** `bars_p0_to_p1 == 0` cho phần lớn cases là đúng (P0→P1 xảy ra trong cùng 1h bar). Không phải bug.
