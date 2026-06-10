# ARCHITECTURE_AND_ADR_V2

Status: canonical architecture + decision record for the CURRENT system
Updated: 2026-06-02
Supersedes: Binance_Bot_Architecture_Blueprint_V1_4, IMPLEMENTATION_CONTRACT_V1_1,
REVIEW_SYSTEM_SEMANTIC_SPEC_V2_5, FIELD_PROPAGATION_MAP_V1,
CODE_OWNERSHIP_AND_CHANGE_IMPACT_MAP_V1, binance_bot_detailed_code_mapping_audit_2026-04-05,
CURRENT_CODE_TO_TARGET_MAPPING_REPORT_V2_1, WORKED_EXAMPLES_V1_1, measurement_summary_template_v1_5.

> Those older docs described a layered-migration / Report-V2 / 5-label-regime world that
> has been cancelled (see ADRs below). They are archived, not authoritative. If anything
> in an old doc conflicts with this file, THIS file wins.

---

## 1. What this system is

A **manual-trading signal bot** for Binance Futures. It detects setups and publishes
signals for a human to trade by hand. It does **not** auto-trade.

- Runs as a daemon inside `screen` on a VPS (Ubuntu).
- `oi_scanner.py` is the single operational center (see ADR-1).
- Data persists as day/month-partitioned CSVs under `data/` (`pending/`, `signals/`, `results/`).

---

## 2. Logical layering (separation of concerns)

The pipeline is kept logically separate even though most of it lives inside one file:

```
data foundation → regime → strategy thesis → delivery metadata
  → veto → dispatch → lifecycle truth → research / measurement
```

These are **logical** layers, not (yet) separate modules. Keeping them mentally separate
is what lets analysis and future extraction stay clean.

---

## 3. Locked invariants

These hold regardless of code shape. Breaking one is a truth-integrity bug, not a style choice.

- **Delivery metadata is annotation, not a gate.** It may inform dispatch; it must not
  overwrite a strategy verdict.
- **Veto is the only hard-no layer.** Only veto may force NO_SEND on an otherwise-detected setup.
- **Dispatch routes publication only.** It never re-detects strategy; it acts on outputs
  from regime / thesis / delivery / veto.
- **`confirmed` ≠ `sent` ≠ `close`.** These three are distinct states and must never be collapsed.
- **Not-yet-reached timestamps use explicit status text** (e.g. `not_reached_yet`). Never a fake time.
- **Strategy families stay separate** in code, analysis, and measurement. Never merge them implicitly.
- **Review/report/DOCX outputs are downstream renderers.** They may aggregate and rank.
  They must never repair truth, invent timestamps, or relabel cases.
- **Lifecycle truth is canonical.** Everything else reads from it.

---

## 4. Architecture Decision Records (ADR)

### ADR-1 — `oi_scanner.py` is the official operational center. Layered migration is CANCELLED.

The earlier attempt to split the system into `scanner/lifecycle.py`, `scanner/storage.py`,
`scanner/binance_client.py`, `scanner/market_math.py`, `delivery/delivery_state_evaluator.py`,
`veto/veto_engine.py` produced **two parallel sources of truth** (dead fork + live code), and
nobody could tell which was running. That class of confusion is exactly what causes silent
false-validation. Those split modules have been **deleted**.

- `oi_scanner.py` (~4000 lines) is a **deliberate** god file. It owns runtime orchestration,
  the `Signal` / `PendingSetup` dataclasses + field lists, strategy orchestration, Binance +
  Bybit clients, dispatch routing, Telegram, and lifecycle truth — all inline.
- **Do not recreate** the deleted modules. If a future extraction happens, it follows the
  one-source-of-truth-per-commit rule (see Section 6) and uses NEW names, not the dead ones.

Modules that ARE still live and real (not part of the cancelled migration):
- `scanner/regime/classifier.py` + `regime/regime_normalizer.py` (regime)
- `scanner/dispatch/router.py` (dispatch — the top-level `dispatch/dispatch_router.py` shim was deleted)
- `scanner/strategies/*` (per-strategy detection)

`scanner/domain.py` exists but is **NOT** the runtime source of truth. It holds a stale
subset copy of `Signal` / `PendingSetup` (missing ~20 fields vs the live dataclasses in
`oi_scanner.py`) and is imported only by `scanner/dispatch/router.py` as a type hint.
Runtime always constructs the `oi_scanner.py` versions. This is a known divergence risk
(documented in CODE_MAP_V2.md §7), safe today only because router reads a single shared
field (`score`). Unifying this contract is a priority seam if/when extraction happens — do
not treat the duplication as "under control."

> The exact live file tree is owned by CODE_MAP_V2.md, which is generated from the repo,
> not from memory.

### ADR-2 — Review system / Report V2 is CANCELLED.

`review_case_system`, `build_daily_review_pack.py`, and the `review/*` cluster do **not**
exist in the repo. All "current priority = Report V2" language in old docs is dead.
There is no DOCX review pack pipeline in the live bot. (Phase R1 top-movers research, if
present, is a separate downstream subsystem and is out of scope for this doc.)

### ADR-3 — Strategy state.

**ACTIVE (3):**
- `long_accumulation_continuation` — LIVE. (Spec: STRATEGY_SPEC_long_accumulation_continuation.)
- `oi_range_breakout` (ORB) — fully coded, `enabled: false` in config, awaiting activation.
  Uses MAX(Binance, Bybit) OI.
- `hyperliquid` whale accumulation — SHADOW, runs as a **separate process** (`hl_scanner.py`),
  NOT wired into `oi_scanner.py`. `hl_whale_accum.enabled: false`.

**DELETED (code + data) in Cleanup Rounds 1–4:**
- `pump_exhaustion` (Round 1), `short_exhaustion_retest` (Round 2),
  `long_breakout_retest` (Round 3), `legacy_5m_retest` + `infer_legacy_strategy` (Round 4).

> `short_exhaustion_retest` has no separate strategy file, but three **dead-inline**
> remnants survive and are pending delete in a future Round 5 (none is a live detection
> path): `_infer_strategy_from_row` (oi_scanner.py), a simulate/test block (oi_scanner.py),
> and the `regime_fit_short_exhaustion` field still computed in every regime verdict
> (classifier.py). See CODE_MAP_V2.md §5/§7. Also orphaned: the `retest.enabled` config key
> (read into memory, gates nothing). These are cleanup, not truth bugs.

**Kept on purpose (shared, do not delete):**
- `regime_fit_long_breakout` (assigned to every LONG setup as a shared fit field)
- `run_simulation_case` (test harness)

### ADR-4 — Regime label set stays at the CURRENT live set.

The old "5-label expansion / Sprint 3B" is **not** approved and not open. Use only the
label strings the live classifier actually emits. CODE_MAP_V2.md records the real set;
do not assume the old 5-label taxonomy.

---

## 5. Lifecycle truth (the part most likely to bite)

- A signal row is written at **SEND**, not at confirm (confirmed-but-unsent cases have no
  signal row — that is correct, not a bug).
- `setup_id` is the canonical join key across pending → signals → results. As of
  2026-06-02 the confirm path copies `pending.setup_id` into the signal so all three files
  share one key. (Build marker: `round4-legacy-removal+setup-id-join-2026-06-02`.)
- Historical rows written before that fix have mismatched `setup_id` across files and
  **cannot** be retro-joined. Do not fake or rewrite them. Any historical measurement must
  go through a separate downstream mapping table, never by writing back into truth.
- When measuring "confirmed cases and their outcome", the correct baseline is
  **confirmed AND sent**, not all confirmed (unsent cases never reach results by design).

---

## 6. Refactor / extraction policy (if/when it happens)

Refactoring the god file is an **investment in maintainability, not a fix and not a
go-live gate.** A working god file is safer than a half-migrated system.

If extraction is undertaken, it is opportunistic (extract a seam when you're already
changing it for a real reason), one seam per commit, leaf-to-trunk:

1. Each commit has **exactly one source of truth**: create the new module, wire
   `oi_scanner.py` to it, and **delete the inline block in the same commit.** Never keep both.
2. Extract verbatim — zero behavior change. Logic changes go in a separate commit.
3. Validate behavior identical pre/post on fresh rows, single-writer (see RUNTIME_VALIDATION_DISCIPLINE).
4. Suggested seam order (low→high risk): API client → market math → storage/CSV →
   contracts (Signal/PendingSetup) → telegram/format → strategy orchestration → lifecycle truth (last).
5. Never resurrect a deleted module name (ADR-1).

---

## 7. What "approved" means right now

Approved by default: bugfixes that preserve behavior, truth-integrity fixes, validation work.

NOT approved by default (needs explicit sign-off):
- strategy threshold tuning
- veto expansion
- regime expansion / Sprint 3B / 5-label
- any broad refactor
- optimization conclusions drawn from mixed old/new CSV rows or from semantically broken rows

---

## 8. Document set for this system

| Doc | Owns |
|---|---|
| ARCHITECTURE_AND_ADR_V2.md (this file) | architecture, invariants, ADRs, policy |
| CODE_MAP_V2.md | the real file/owner map, generated from the repo |
| RUNTIME_VALIDATION_DISCIPLINE.md | single-writer, fresh-row, CUT_MS, post-patch checklist |
| STRATEGY_SPEC_long_accumulation_continuation | the one live strategy spec |

Anything not in this set is archived. Do not cite archived docs as current truth.
