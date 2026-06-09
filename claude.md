# CLAUDE.md — Binance Bot Signals Project

> Read this file fully before touching any code. It exists to prevent wrong-file patches,
> broken semantics, and architecture drift. Updated: 2026-06-02 (Cleanup round 1).

---

## ADR — Architecture Decisions (2026-06-02)

Three permanent decisions recorded here so future patches are not misled by older doc sections below:

**ADR-1: `oi_scanner.py` is the permanent runtime home. Layered migration cancelled.**
Modules that were extracted as shims (`scanner/lifecycle.py`, `scanner/storage.py`,
`scanner/binance_client.py`, `scanner/market_math.py`, `delivery/delivery_state_evaluator.py`,
`veto/veto_engine.py`) have been **deleted**. `oi_scanner.py` implements all logic inline.
Do not recreate these files.

**ADR-2: `review_case_system` / Report V2 permanently dropped.**
`build_daily_review_pack.py`, `scanner/review_service.py`, `review_capture_runtime.py`,
`run_daily_final_export.sh` do not exist in this repo. `review_case_system` config block removed.

**ADR-3: Strategy state as of 2026-06-02.**
- LIVE: `long_accumulation_continuation`, `oi_range_breakout` (code complete, config disabled)
- SHADOW (separate process, draft): `hyperliquid` whale accumulation (`hl_scanner.py`)
- DEPRECATED (pending delete — next cleanup round): `short_exhaustion_retest`, `long_breakout_retest`, `legacy_5m_retest`
- REMOVED: `pump_exhaustion` (deleted 2026-06-02)

---

## 1. Project identity (locked)

- **Manual-trading signal bot** — not auto-trading. Signals go to a human via Telegram.
- Python 3.12.3, `.venv` at repo root.
- Live on Ubuntu VPS, runtime process is `oi_scanner.py` running inside a `screen` session.
- Active strategy families (see ADR-3 above for full state):
  - `long_accumulation_continuation` ← LIVE, enabled
  - `oi_range_breakout` ← built + wired, config disabled
  - `hyperliquid` whale accum ← SHADOW, separate process (`hl_scanner.py`)
  - `short_exhaustion_retest` ← DEPRECATED, pending code delete
  - `long_breakout_retest` ← DEPRECATED, inline in oi_scanner.py, pending delete
- `pump_exhaustion` has been **removed** from this repo (2026-06-02).

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

| Layer | **Real owner NOW** | Notes |
|---|---|---|
| Runtime orchestration | `oi_scanner.py` | God-file by ADR-1; migration cancelled |
| Strategy — ORB | `scanner/strategies/oi_range_breakout.py` + `oi_scanner.py` | Config disabled; code complete |
| Strategy — short exhaustion | `scanner/strategies/short_exhaustion_retest.py` + `oi_scanner.py` | **DEPRECATED** — pending delete |
| Strategy — long accumulation | `scanner/strategies/long_accumulation_continuation.py` + `scanner/strategies/_accumulation_features.py` | LIVE |
| Strategy — hyperliquid | `scanner/strategies/hyperliquid/` + `hl_scanner.py` | SHADOW — separate process |
| Strategy — long breakout | inline in `oi_scanner.py` | **DEPRECATED** — no separate file |
| Bybit API methods | `oi_scanner.py` (bybit_get, bybit_oi_hist, bybit_klines_1h, bybit_ticker_24h, bybit_funding, _combine_*) | — |
| Regime classify | `scanner/regime/classifier.py` | — |
| Regime normalize/persist | `regime/regime_normalizer.py` | — |
| Delivery metadata | `oi_scanner.py` inline (`evaluate_manual_tradable`) | `delivery/` DELETED (ADR-1) |
| Veto | `oi_scanner.py` inline (`should_send`) | `veto/` DELETED (ADR-1) |
| Dispatch | `scanner/dispatch/router.py` | — |
| Lifecycle truth write | `oi_scanner.py` inline (save_signal, save_pending, close_pending) | `scanner/lifecycle.py` DELETED (ADR-1) |
| Storage/CSV infra | `oi_scanner.py` inline (read_csv, write_csv, append_csv) | `scanner/storage.py` DELETED (ADR-1) |
| API/market math | `oi_scanner.py` inline | `scanner/binance_client.py`, `scanner/market_math.py` DELETED (ADR-1) |
| Domain models (stale) | `scanner/domain.py` | Stale — used only by `scanner/dispatch/router.py` for type hints |
| Review / Report V2 | **REMOVED** | See ADR-2 |
| Phase R1 research | Not in this repo | Separate VPS-only pipeline |

### Files that look canonical but are NOT the real owner
- `scanner/domain.py` — **stale legacy copies** of PendingSetup/Signal. Live `oi_scanner.py` definitions are the truth. Fields added to `oi_scanner.py` (e.g., `oi_delta_abs_1h`, `bybit_vol_24h_usdt`, `cross_exchange_confirmed`, `funding_pct`) are NOT in `scanner/domain.py`. Do not patch domain.py expecting live effect. Used only as type hint source by `scanner/dispatch/router.py`.

**Deleted (do not recreate):** `dispatch/dispatch_router.py`, `contracts/*.py`, `delivery/`, `veto/`, `scanner/lifecycle.py`, `scanner/storage.py`, `scanner/binance_client.py`, `scanner/market_math.py`

---

## 4. Config toggles that matter (`config.yaml`)

Critical toggles to check before behavior changes:

```yaml
strategy:
  short_exhaustion_retest.enabled     # DEPRECATED — keep false
  long_accumulation_continuation.enabled
  oi_range_breakout.enabled

retest:
  enabled                             # disables full retest gate for long_breakout_retest

bybit:
  enabled: true                       # kill switch — false disables ALL Bybit fetching for ORB
                                      # affects: OI combine, volume combine, vol_24h, funding rate

review_snapshots:
  enabled
  save_pre_pending                    # controls pre_pending stage capture

observability:
  enabled                             # all debug/pipeline output sub-flags under this
```

**Config file in scope for runtime:** `config.yaml` — only one live config file.

Never declare a patch complete without checking which config the running process actually loaded.

---

## 5. Common tasks → exact file clusters

### Strategy logic is wrong
**Primary:** `scanner/strategies/<family>.py` + `config.yaml`
**Also check:** `oi_scanner.py`, `scanner/domain.py`

### ORB signal missing / OI data wrong / Bybit not included
**Primary:** `scanner/strategies/oi_range_breakout.py` (detection + MAX formula) + `oi_scanner.py` (`build_pending_oi_range_breakout_setup`, Bybit methods)
**Config check:** `bybit.enabled` in `config.yaml`
**Do NOT patch:** `scanner/domain.py` (stale copies, no live effect)

### Funding rate in ORB Telegram signal wrong or missing
**Primary:** `oi_scanner.py` — `bybit_funding()` method, `_process_oi_range_breakout_pending()` (fetch block), `format_signal()` ORB block
**Formula:** `funding_pct = binance_fr + bybit_fr` (current cycle, both in %)
**Note:** `funding_pct` is fetched live at confirmation time, not stored at pending creation

### Regime label / fit looks wrong
**Primary:** `scanner/regime/classifier.py` + `regime/regime_normalizer.py`
**Also check:** `oi_scanner.py`

### confirmed / sent / close semantics are wrong
**Primary:** `oi_scanner.py` inline — `save_signal()`, `close_pending()`, `sync_pending_send_decision()`
**Also check:** `scanner/domain.py`

### Dispatch / MAIN_SIGNAL / WATCHLIST logic wrong
**Primary:** `scanner/dispatch/router.py`
**Also check:** `oi_scanner.py`, `scanner/domain.py`

### Veto / NO_SEND behavior wrong
**Primary:** `oi_scanner.py` — `should_send()` method (cooldown gate)
**Note:** `veto/veto_engine.py` has been deleted (ADR-1). All veto logic is in `oi_scanner.py`.

### Adding a new field end-to-end
Always touch in this order:
1. Source layer owner (strategy file or inline in `oi_scanner.py`)
2. `oi_scanner.py` dataclass definition + field lists (`signal_fields` / `pending_fields`) + wrap/process functions
3. `scanner/domain.py` only if the field is used by dispatch router (type hints only)

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

### Phần 1 — Trên máy Windows (Claude làm)

```bash
# 1. Verify patch hit disk
grep "<changed_function_or_marker>" oi_scanner.py

# 2. Bump build marker
CODE_BUILD_ID = "description-YYYY-MM-DD"

# 3. Commit AND push — MANDATORY (VPS pulls from GitHub, local commit alone is invisible to VPS)
git add <files>
git commit -m "..."
git push origin main
```

### Phần 2 — Trên VPS (user tự chạy)

```bash
# Bước 1: Kill bot — dùng pkill, KHÔNG dùng cat oi_scanner.lock (lock file hay bị empty)
pkill -f oi_scanner.py; sleep 2
ps -eo pid,cmd | grep oi_scanner | grep -v grep   # phải trống

# Bước 2: Pull code mới
cd /root/binance_bot_signals
git pull
grep "CODE_BUILD_ID = " oi_scanner.py   # xác nhận build mới

# Bước 3: Lấy CUT_MS (sau khi đã kill xong)
CUT_MS=$(python3 -c "import time;print(int(time.time()*1000))"); echo CUT_MS=$CUT_MS

# Bước 4: Start bot
screen -dmS bot python3 /root/binance_bot_signals/oi_scanner.py
sleep 3

# Bước 5: Xác nhận
ps -eo pid,lstart,cmd | grep oi_scanner | grep -v grep   # 2 dòng (SCREEN + python) = 1 instance, bình thường
cat /root/binance_bot_signals/oi_scanner.lock             # có pid
cat /root/binance_bot_signals/RUNNING_CODE_VERSION.txt | grep code_build_id   # build id mới
```

### Ghi chú quan trọng

| Điểm | Lý do |
|---|---|
| Dùng `pkill -f oi_scanner.py` | Lock file hay bị empty sau test second instance — không dùng làm kill target |
| 2 PID trong ps là bình thường | 1 SCREEN wrapper + 1 python process = 1 instance duy nhất |
| `git push` trước, `git pull` sau | VPS pull từ GitHub — thiếu push thì VPS không lấy được code |
| Flock guard tự enforce | Nếu start nhầm instance 2, nó tự exit với `[startup] ERROR` và không ghi CSV |

### Validation rule
Only judge behavior from rows where `created_ts_ms >= CUT_MS` or `confirmed_ts_ms >= CUT_MS`.
Old rows in cumulative daily CSVs do NOT prove anything about the patched runtime.

### Infrastructure pass ≠ behavior pass
A patch can pass syntax check, import check, file grep, and screen startup — and still fail real behavior if the process was not restarted.

---

## 8. Report V2 — REMOVED (see ADR-2)

Report V2 / review_case_system has been permanently dropped. Files `build_daily_review_pack.py`,
`scanner/review_service.py`, `review_capture_runtime.py`, `run_daily_final_export.sh` do not exist
in this repo. Do not recreate them.

---

## 9. Phase R1 research pipeline

**Not present in this repo.** Phase R1 runs as a separate VPS-only pipeline.
Do not add R1 imports or references to `oi_scanner.py` or any live bot file.

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

**Reading index with quick-lookup-by-task:** [`docs/CLAUDE.md`](docs/CLAUDE.md) — read this to find which doc applies to your task.

Docs present in this repo (under `docs/`):

| Doc | Purpose |
|---|---|
| `docs/CLAUDE.md` | Reading index — quick-lookup-by-task |
| `docs/CURRENT_CODE_TO_TARGET_MAPPING_REPORT_V2_1.md` | Owner map (2026-04-28) — note: some entries stale after 2026-06-02 cleanup |
| `docs/STRATEGY_SPEC_long_accumulation_continuation_V1_2.md` | Full spec for acc_cont — all 8 gates, features, config, shadow mode v2.0.1 |
| `AUDIT_REPORT_2026-06-02.md` | Full code audit report (2026-06-02) |

Other docs referenced in older versions of this file (`Binance_Bot_Architecture_Blueprint`, `IMPLEMENTATION_CONTRACT`, `REVIEW_SYSTEM_SEMANTIC_SPEC`, etc.) are **not present in this repo** — they may exist on VPS only.

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

> **DELETED 2026-06-02.** `debug_spk.py` was removed during cleanup (dead standalone script). ORB gate logic lives inline in `scanner/strategies/oi_range_breakout.py`. To debug why a token fails ORB gates, add temporary print statements there and re-run locally.

---

## 15. Debug tool — pump_exhaustion gate diagnostic

> **DELETED 2026-06-02.** `debug_pump.py` was removed during cleanup (pump_exhaustion strategy dropped entirely — ADR-3). All pump_exhaustion code is gone.

---

## 16. Phase R1 — pipeline docs (V4-1, V4-2, V4-4)

> **NOT IN THIS REPO — 2026-06-02.** The entire Phase R1 research pipeline (`scripts/run_daily_top_movers_research.py`, `research/top_movers/*`, `scripts/validate_v4_*.py`) was removed during the 2026-06-02 cleanup. These scripts are not present in the current codebase. The V4-1/V4-2/V4-4 validation documentation is preserved in git history only.

---

## 17. (merged into §16)

---

## 19. (merged into §16)

---

## 18. Mandatory code audit after every implementation

**Audit là bước bắt buộc sau khi code xong, trước khi báo task hoàn thành.**

### Quy trình audit

Sau khi implement xong toàn bộ files, đọc lại từng đoạn code đã viết và kiểm tra:

| Hạng mục | Những gì cần kiểm tra |
|---|---|
| **Dead code** | Variables được tính nhưng không bao giờ dùng (ví dụ: assign rồi không reference) |
| **Index / off-by-one** | Array slicing, list indexing, range() bounds |
| **None propagation** | Mọi path có thể trả về None — caller có guard không? |
| **Type assumptions** | Dict key access (`bar["high"]`) vs list index (`bar[2]`) — phải khớp với data source thực tế |
| **Guard completeness** | Tất cả edge cases (empty list, None input, zero divisor) đều được handle |
| **Stale comments** | Comment nói "V4-2" nhưng code là V4-3, comment nói "not yet in schema" nhưng đã có |
| **Weight/formula integrity** | Weighted sum weights cộng lại = 1.0 (hoặc được renormalize đúng), formula direction đúng |
| **Backward compat** | Fields cũ không bị xóa hay rename trong quá trình thêm fields mới |

### Cách thực hiện

1. Đọc lại từng file đã modify — **không bỏ qua file nào**, kể cả file chỉ sửa 1 dòng
2. Với mỗi function mới: trace qua ít nhất 2 code paths (happy path + failure path)
3. Chạy quick smoke test bằng Python inline nếu có thể (không cần full pipeline)
4. Báo cáo rõ: bugs tìm thấy, bugs đã fix, những gì verify là đúng

### Ví dụ bugs thường gặp (từ V4-3, 2026-04-30)

- **Dead variable**: `_pbe_norm` được tính nhưng không có trong `_weighted_sum` — detect bằng cách đọc lại toàn bộ function và trace từng variable
- **Stale comment**: `# V4-2:` bao gồm V4-3 field — detect bằng cách đọc comment và so với code thực

### Khi nào bắt buộc

- Sau bất kỳ implementation nào, kể cả patch nhỏ 1 dòng
- Trước khi báo "done" với user
- Trước khi chạy validation script trên VPS

**Không được báo task hoàn thành mà không audit.**
