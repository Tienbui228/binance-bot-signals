# CLAUDE.md — Binance Bot Signals Project

> Read this file fully before touching any code. It exists to prevent wrong-file patches,
> broken semantics, and architecture drift. Updated: 2026-04-22.

---

## 1. Project identity (locked)

- **Manual-trading signal bot** — not auto-trading. Signals go to a human via Telegram.
- Python 3.12.3, `.venv` at repo root.
- Live on Ubuntu VPS, runtime process is `oi_scanner.py` running inside a `screen` session.
- Three active strategy families — keep them **always separate** in code, review, measurement:
  - `long_breakout_retest`
  - `short_exhaustion_retest`
  - `long_accumulation_continuation`
- Current working priority: **Daily Review Report V2** (downstream-only, no truth repair).

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
| Strategy — long breakout | `scanner/strategies/long_breakout_retest.py` | same | — |
| Strategy — short exhaustion | `scanner/strategies/short_exhaustion_retest.py` | same | — |
| Strategy — long accumulation | `scanner/strategies/long_accumulation_continuation.py` | same | — |
| Regime classify | `scanner/regime/classifier.py` | same | — |
| Regime normalize/persist | `regime/regime_normalizer.py` | same | — |
| Delivery metadata | `delivery/delivery_state_evaluator.py` | same | — |
| Veto | `veto/veto_engine.py` | same | — |
| Dispatch | `scanner/dispatch/router.py` | same | `dispatch/dispatch_router.py` ← shim only |
| Lifecycle truth | `scanner/lifecycle.py` | `lifecycle/case_truth_service.py` | — |
| Review stage capture | `scanner/review_service.py` + `review_capture_runtime.py` | `review/review_truth_service.py` | `review/review_truth_service.py` ← not real owner yet |
| Report V2 rendering | `build_daily_review_pack.py` | `review/review_pack_builder.py` | `review/review_pack_builder.py` ← not real owner yet |
| Legacy runtime models | `scanner/domain.py` | `contracts/*.py` | `contracts/*.py` ← target only; runtime still uses domain.py |
| Storage/CSV infra | `scanner/storage.py` | same | — |
| API/market math | `scanner/binance_client.py`, `scanner/market_math.py` | same | — |
| Phase R1 research | `scripts/run_daily_top_movers_research.py` + `research/top_movers/*` | same | — |

### Files that look canonical but are NOT the real owner yet
- `dispatch/dispatch_router.py` — migration shim, do not patch for logic changes
- `review/review_pack_builder.py` — future home, not active render owner
- `review/review_truth_service.py` — future home, not active stage-truth owner
- `contracts/*.py` — important for migration, but live runtime still flows through `scanner/domain.py`

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
2. `oi_scanner.py` integration path
3. `scanner/domain.py` if runtime uses legacy dataclass
4. Matching `contracts/*.py` for target contract
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
| `CURRENT_CODE_TO_TARGET_MAPPING_REPORT_V2_1.md` | Report V2 implementation map |
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
