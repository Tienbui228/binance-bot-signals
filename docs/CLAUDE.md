# docs/CLAUDE.md — Reading index for documentation folder

This file is loaded automatically by Claude Code when working in this directory.
All key reference documents for the project are listed here with purpose and reading trigger.

---

## Documents in this folder

### `CURRENT_CODE_TO_TARGET_MAPPING_REPORT_V2_1.md`
**What it covers:** Real owner vs target owner per layer, Report V2 priority, what is safe to change now.  
**Read when:** Before patching any file — verify you are touching the real owner, not a migration shim.  
**Key output:** The mapping table on page 2 tells you which files are real owners today vs future migration targets.

---

## Strategy specs (repo root)

### `STRATEGY_SPEC_long_accumulation_continuation_V1_2.md`
**What it covers:** Full implementation spec for `long_accumulation_continuation` strategy — all 8 gates, feature computation, data sources, output contract, config schema, shadow mode (v2.0.1), validation plan.  
**Read when:** Any work touching `scanner/strategies/long_accumulation_continuation.py`, `scanner/strategies/_accumulation_features.py`, or the `long_accumulation_continuation` section of `config.yaml`.  
**Current version:** V1.2 (updated 2026-06-01). Gate count: 5 live + 3 shadow (gates_v201_enabled: false, activate 2026-06-18).

---

## Referenced documents (not in this repo — may be on VPS or shared drive)

These are listed in `CLAUDE.md` Section 12. If not found locally, ask the user for the file or consult the root `CLAUDE.md` summary of their contents.

| Document | Purpose | When to read |
|---|---|---|
| `Binance_Bot_Architecture_Blueprint_V1_4.md` | North-star architecture, deferred items | Before proposing any structural change |
| `IMPLEMENTATION_CONTRACT_V1_1.md` | Layer invariants (confirmed ≠ sent, veto-only hard-no, dispatch never re-detects) | Before any cross-layer work |
| `REVIEW_SYSTEM_SEMANTIC_SPEC_V2_5.md` | Stage semantics, Report V2 rules, `case_close_type` values | Before touching review/report code |
| `FIELD_PROPAGATION_MAP_V1.md` | Where each field is decided / propagated / persisted / rendered | Before adding any new field end-to-end |
| `CODE_OWNERSHIP_AND_CHANGE_IMPACT_MAP_V1.md` | Full repo change-impact map | Before touching any file not obviously owned by you |
| `RUNTIME_DEPLOY_TEST_GUARDRAILS.md` | Runtime patch validation rules — CUT_MS discipline, screen restart | After every code change before deploying to VPS |
| `POST_PATCH_CHECKLIST.md` | Mandatory checklist after any runtime patch | After every VPS deploy |
| `WORKED_EXAMPLES_V1_1.md` | Concrete semantic examples for Report V2 | When debugging report stage or close semantics |
| `measurement_summary_template_v1_5.md` | Measurement template with data quality gate | Before any optimization/tuning analysis |
| `binance_bot_detailed_code_mapping_audit_2026-04-05.md` | Detailed audit of real owner vs target owner | When the mapping report is insufficient |

---

## Quick lookup by task

| Task | Primary doc to read first |
|---|---|
| Adding a new strategy | `IMPLEMENTATION_CONTRACT_V1_1.md` → `FIELD_PROPAGATION_MAP_V1.md` → relevant `STRATEGY_SPEC_*.md` |
| Changing `long_accumulation_continuation` logic | `STRATEGY_SPEC_long_accumulation_continuation_V1_2.md` |
| Patching `oi_scanner.py` | `CURRENT_CODE_TO_TARGET_MAPPING_REPORT_V2_1.md` mapping table → `POST_PATCH_CHECKLIST.md` |
| Adding a new field end-to-end | `FIELD_PROPAGATION_MAP_V1.md` → `CURRENT_CODE_TO_TARGET_MAPPING_REPORT_V2_1.md` |
| Report V2 / review system work | `REVIEW_SYSTEM_SEMANTIC_SPEC_V2_5.md` → `WORKED_EXAMPLES_V1_1.md` |
| VPS deploy / restart | `RUNTIME_DEPLOY_TEST_GUARDRAILS.md` → `POST_PATCH_CHECKLIST.md` |
| Regime label / classifier work | `scanner/regime/classifier.py` (read for actual label strings) → `IMPLEMENTATION_CONTRACT_V1_1.md` |
| Optimization / tuning | `measurement_summary_template_v1_5.md` — data quality gate before conclusions |

---

## Reading order for a new contributor

1. `CLAUDE.md` (repo root) — project identity, architecture layers, real owner map, lessons learned
2. `docs/CURRENT_CODE_TO_TARGET_MAPPING_REPORT_V2_1.md` — what is real owner today vs target
3. `IMPLEMENTATION_CONTRACT_V1_1.md` — invariants that must never be broken
4. `FIELD_PROPAGATION_MAP_V1.md` — how fields travel end-to-end
5. `RUNTIME_DEPLOY_TEST_GUARDRAILS.md` + `POST_PATCH_CHECKLIST.md` — deploy discipline
6. Strategy spec for the family you are working on
