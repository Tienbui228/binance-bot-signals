# STRATEGY_SPEC_long_accumulation_continuation_V1_3

Status: implemented and live
Created: 2026-05-20 (V1.0)
Updated: 2026-06-03 (V1.3 — gate6/7 permanently decoupled from blocking; gate8 stays shadow with
no auto-activation; `oi_trend_3v14` multi-day shadow tag added; `oi_vs_baseline` naming clarified)
Family: `long_accumulation_continuation`
Side: long only (V1)
Implementation files: exist in repo

> **Sign-off note (ADR §7):** the V1.3 gate changes (remove gate6/7 from blocking; keep gate8
> shadow; add oi_trend shadow tag) are **threshold/policy tuning that carried explicit owner
> sign-off** in the 2026-06-03 dispatch-fix session. They are NOT default-approved behavior change;
> they were decided and validated (FIX-3 + FIX-5 PASS, fresh rows ≥ CUT_MS 1780479816000).

---

## Changelog

| Version | Date | Changes |
|---|---|---|
| V1.0 | 2026-05-20 | Initial implementation — 3 hard gates, score informational |
| V1.1 | ~2026-05-28 | Gate 4 added (score floor, `gate.min_score`); Gate 5 added (`pos_now_min` hard floor); `oi_delta_min_pct` lowered 5.0 → 3; market cap display in signal; daily dedup (1 signal per token per UTC day) |
| V1.2 | 2026-06-01 | v2.0.1 shadow gates: Gate 6 (T-30d), Gate 7 (T-7d), Gate 8 (F-funding); `series_close` + `funding_8h_pct` inputs; `price_vs_baseline`, `price_trend_7v30` features; `gates_v201_enabled` toggle; `debug_gate_v201.py` diagnostic tool |
| **V1.3** | **2026-06-03** | **Gate 6/7 (price-trend) PERMANENTLY decoupled from `setup_detected` — shadow tags only, no Day-30 activation (FIX-3, commit 7697d729). Gate 8 (funding) remains shadow; the `gates_v201_enabled` flag now gates ONLY gate8, and there is NO automatic Day-30 flip — activation needs explicit owner sign-off. Added `oi_trend_3v14` multi-day OI trend feature + `oi_trend_pass/fail` shadow tag (threshold >1%, from a NEW daily OI series, FIX-5 commit 4dd612a3). Clarified `oi_vs_baseline` reason_tag = `oi_delta_1h_pct` (Gate 2 input, 1h window) — NOT multi-day OI trend.** |

---

## 0. Mandatory pre-code reading

Before writing any code, view the CURRENT living-doc set (older docs are archived — see
ARCHITECTURE_AND_ADR_V2 §8):

1. `ARCHITECTURE_AND_ADR_V2.md` — architecture, invariants, ADRs, approval policy
2. `CODE_MAP_V2.md` — real file/owner map + anchor line table (generated from repo)
3. `RUNTIME_VALIDATION_DISCIPLINE.md` — single-writer, fresh-row, CUT_MS, post-patch checklist
4. This spec

Then view current code state in `oi_scanner.py`: `Signal` dataclass + `signal_fields` (L38/L271),
`PendingSetup` + `pending_fields` (L132/L310), `build_pending_long_accumulation_continuation_setup`
(L1995), acc_cont detect/confirm wiring (L1672–1673 / L1732–1745), the daily-OI fetch for
`oi_trend_3v14` (openInterestHist 1d, ~L2024–2043), `scan_once` (L2970). And the strategy module
`scanner/strategies/long_accumulation_continuation.py` + `scanner/strategies/_accumulation_features.py`.

**Stop and confirm with owner if any divergence from this spec is found.**

---

## 1. Problem statement

Detect long accumulation continuation setups based on:
- Top trader position structure (loaded or actively building)
- Concentration intensity (gap between size-weighted and count-weighted top ratios)
- OI confirmation (capital actually entering)
- Retail not yet FOMO-crowded long

Different from ORB:
- ORB detects OI range breakout + price breakout
- Accumulation detects positioning structure regardless of price range

→ Separate strategy family. Do not merge with ORB.

**Thesis note (relevant to V1.3 gate decisions):** acc_cont deliberately fires while price is
*correcting / not yet running* (OI building during a wave-2 / pullback). This is exactly why the
price-trend gates 6/7 were removed from blocking — a price-below-baseline floor contradicts the
strategy's own premise. See §16 (the bet).

---

## 2. Locked decisions

| Decision | Value | Changed in |
|---|---|---|
| Strategy family name | `long_accumulation_continuation` | V1.0 |
| Side | long only (V1) | V1.0 |
| **Blocking hard gates** | **5** (gate1–gate5) | V1.0=3, V1.1=5, **V1.3 confirms 5** |
| **gate8 (funding)** | **shadow tag, flag-gated by `gates_v201_enabled` (currently false); NO auto-activation** | V1.2 shadow → **V1.3: no Day-30 flip** |
| **gate6 / gate7 (price-trend T)** | **PERMANENTLY shadow tags — never block, flag-independent** | **V1.3 (FIX-3)** |
| **oi_trend_3v14 (multi-day OI)** | **shadow tag `oi_trend_pass/fail`, threshold >1%, never blocks** | **V1.3 (FIX-5)** |
| Score role | Gate 4 (score floor) + quality annotation | V1.1 |
| Score range | 0–100 (integer) | V1.0 |
| Score weights | gap 30 + pos_now 20 + pos_trend 20 + BTC regime 30 = 100 | V1.0 |
| BTC regime score mapping | bearish=0, neutral=10, bull=30 | V1.0 |
| Retail FOMO handling | hard gate (Gate 3) | V1.0 |
| OI delta floor | `oi_delta_min_pct >= 3%` | V1.1 |
| Insufficient history | `setup_detected = False`, no exception | V1.0 |
| Daily dedup | 1 signal per token per UTC day | V1.1 |
| **Regime in bearish** | **does NOT block long; regime already priced into score (gate4). No multi-layer regime penalty.** | **V1.3 (FIX-2: bearish no longer downgrades MAIN_SIGNAL→WATCHLIST)** |

> **What changed about Day-30 (was 2026-06-18):** V1.2 said the v2.0.1 gates would "activate" on
> Day-30. **Cancelled.** Gate6/7 are permanent shadow (no activation possible). Gate8 stays shadow
> until BOTH (a) the funding unit is empirically confirmed (§5 Gate F note) AND (b) shadow data
> supports it AND (c) owner signs off. No calendar-driven flip.

---

## 3. Layer placement

Strategy Thesis layer. Respects locked invariants:
- Delivery metadata is annotation, not a gate here
- Veto layer is not expanded for this strategy
- Dispatch routes based on output, does not re-detect
- Lifecycle truth is not overwritten by strategy code

---

## 4. File ownership

### Primary files (exist)
- `scanner/strategies/long_accumulation_continuation.py` — strategy detection + scoring + shadow tags
- `scanner/strategies/_accumulation_features.py` — pure feature computation, unit-testable

### Sync files (updated)
- `oi_scanner.py` — Signal/PendingSetup fields + lists, `_process_pending` integration, detect call
  site, the 4 top-trader fetches, the daily klines fetch (Gate T), the funding fetch (Gate F), and
  the **daily OI series fetch** (`openInterestHist` 1d → `series_oi_1d`, for `oi_trend_3v14`, V1.3)
- `config.yaml` — `long_accumulation_continuation` section

### Diagnostic tool
- `debug_gate_v201.py` — standalone Gate T + Gate F diagnostic (gate6/7/8 pass/fail per symbol).
  Still useful as a shadow inspector; note gate6/7/8 are non-blocking in live code.

### NOT to touch
- `scanner/strategies/oi_range_breakout.py` (read for pattern only)
- `veto/...` equivalents — veto/cooldown lives inline in `oi_scanner.py` `should_send` (L1476); do
  not expand
- `scanner/regime/classifier.py` + `regime/regime_normalizer.py` (read-only for label values)
- `scanner/dispatch/router.py` (no logic change — strategy emits, dispatch decides; note the
  dispatch DECISION lives inline in `oi_scanner.py` ~L3055–3170, router only reads `score`)
- `scanner/lifecycle.py` does NOT exist (deleted) — use inline lifecycle API in `oi_scanner.py`

---

## 5. Data source spec

All endpoints public on `https://fapi.binance.com`, no API key required.

### Top-trader series (V1.0)

| Logical series | Endpoint | Period | Limit |
|---|---|---|---|
| Top trader position ratio | `/futures/data/topLongShortPositionRatio` | `1d` | `30` |
| Top trader account ratio | `/futures/data/topLongShortAccountRatio` | `1d` | `30` |
| Global (retail) account ratio | `/futures/data/globalLongShortAccountRatio` | `1d` | `30` |
| Taker buy/sell ratio | `/futures/data/takerlongshortRatio` | `1d` | `30` |

Field extraction: pos/acct/retail use `longAccount` (fraction long, e.g. `0.62`) — NOT
`longShortRatio`. taker uses `buySellRatio`. **Sort ascending by timestamp before any trend/slope.**

### Daily klines for Gate T shadow tags (V1.2)

| Source | Endpoint | Interval | Limit |
|---|---|---|---|
| Daily close prices | `/fapi/v1/klines` | `1d` | `31` |

`limit=31` → `[:-1]` → 30 complete bars. Index 4 = close. Fail-open: fetch fails → `series_close=[]`
→ `price_vs_baseline=0.0`, `price_trend_7v30=0.0` → gate6/7 tag = pass (but gates are non-blocking).

### Funding rate for Gate F shadow tag (V1.2)

`self.funding(symbol)` → Binance `/fapi/v1/premiumIndex` → `lastFundingRate × 100.0` (percent).
Fail-open: fetch fails → `funding_8h_pct=0.0` → gate8 tag = pass.
**Funding unit still UNCONFIRMED** — gate8 cannot be activated until the per-unit-time meaning of
`lastFundingRate` is empirically verified against the Binance UI cadence.

### Daily OI series for `oi_trend_3v14` shadow tag (V1.3) — NEW, separate from Gate 2

| Source | Endpoint | Period | Limit | Field |
|---|---|---|---|---|
| Daily OI history | `/futures/data/openInterestHist` | `1d` | `15` | `sumOpenInterestValue` |

→ `series_oi_1d` (list of daily OI notionals). Cache TTL ~1h. Fail-open: fetch fails or `<2` rows →
`series_oi_1d=None` → `oi_trend_3v14=0.0` in features (no raise).

> **CRITICAL — two distinct OI sources, do not conflate (the V1.3 naming fix):**
> | Tag / field | Source | Window | Role |
> |---|---|---|---|
> | `oi_delta_1h_pct` (shown in reason_tags as `oi_vs_baseline=X%`) | 1h/5-min OI history (`oi_1h_history`) | ~60 min | **Gate 2 input (blocking)** |
> | `oi_trend_3v14` (tag `oi_trend_pass/fail`) | daily `series_oi_1d` (`openInterestHist 1d`) | 14 days | **shadow only** |
> These are separate variables, separate API calls, separate cache keys. The reason_tag name
> `oi_vs_baseline` is the **1h** Gate-2 delta, NOT the multi-day trend — the similar names are a
> readability trap, not a code conflation.

### Caching
- Cache key: `(symbol, date_utc, endpoint_name)`. TTL ≥ 1 hour.
- 1 symbol = 4 top-trader fetches + 1 daily klines + 1 funding + 1 daily-OI fetch (V1.3).

### Error handling
- Network/4xx/5xx/parse error: log and return `None`. Downstream treats `None` series as
  `insufficient_history`. Strategy must NOT raise to runtime orchestrator on data failure.

---

## 6. Feature computation spec

File: `scanner/strategies/_accumulation_features.py`. Pure functions, no global state, no I/O.

### Inputs

```
series_pos:    List[float]          # top trader position ratio (long fraction)
series_acct:   List[float]          # top trader account ratio
series_retail: List[float]          # global account ratio
series_taker:  List[float]          # taker buy/sell ratio
series_oi:     List[float]          # DAILY OI series (series_oi_1d) — optional, for oi_trend_3v14 (V1.3)
series_close:  List[float] | None   # daily close prices — V1.2, keyword-only, default None
oi_delta_1h_pct: float              # Gate 2 input — computed in oi_scanner from 1h/5m OI, passed in
```

### Computed scalars (additions in bold)

| Field | Formula | Version |
|---|---|---|
| `pos_now` | `series_pos[-1]` | V1.0 |
| `acct_now` | `series_acct[-1]` | V1.0 |
| `gap_now` | `pos_now - acct_now` | V1.0 |
| `retail_now` | `series_retail[-1]` | V1.0 |
| `taker_now` | `series_taker[-1]` | V1.0 |
| `pos_slope_30d` | `polyfit(range(N), series_pos, 1)[0]` | V1.0 |
| `gap_slope_30d` | slope of `[p-a for p,a in zip(pos, acct)]` | V1.0 |
| `retail_slope_30d` | slope of `series_retail` | V1.0 |
| `pos_trend_3v14` | `mean(pos[-3:]) - mean(pos[-14:])` | V1.0 |
| `pos_min_30d`, `pos_recovery_from_min`, `retail_min_30d`, `retail_recovery`, `taker_7d_avg` | as V1.0 | V1.0 |
| `price_vs_baseline` | `close[-1]/mean(close) - 1.0` (needs `len(close) >= 14`) | V1.2 |
| `price_trend_7v30` | `mean(close[-7:])/mean(close[-30:]) - 1.0` (same guard) | V1.2 |
| **`oi_trend_3v14`** | **`(mean(series_oi[-3:]) - mean(series_oi[-14:])) / mean(series_oi[-14:]) * 100.0`** | **V1.3** |

> **`oi_trend_3v14` (V1.3):** relative % of the 3-day-avg daily OI vs the 14-day-avg baseline.
> Requires `series_oi` with `>= 14` points. Guard: if `base <= 0` or `< 14` points → `0.0`
> (fail-open). It is a NORMALIZED %, so it is comparable across tokens of different OI size.
> (Note: the V1.0 `oi_trend_3v14` was an *absolute* mean-diff used only inside score; V1.3 redefines
> it as the normalized % feeding the shadow tag. The score components are unaffected — score does
> not depend on this field. If any score path still references an absolute oi_trend, verify during
> the next touch.)

### History sufficiency rules

```
required = [series_pos, series_acct, series_retail, series_taker]
# series_oi (daily) and series_close excluded from required — shorter by design, fail-open

if any(s is None or len(s) == 0 for s in required):  return insufficient_history
if min(len(s) for s in required) < 14:               return insufficient_history
partial = min(len(s) for s in required) < 30
```

---

## 7. Hard gate logic

File: `scanner/strategies/long_accumulation_continuation.py`

### Blocking gates 1–5 — all must be True for `setup_detected`

```
Gate 1 — Whale OR-condition:
    (pos_trend_3v14 > config.pos_trend_min) OR (pos_now >= config.pos_now_high)

Gate 2 — OI confirmation:
    oi_delta_1h_pct >= config.oi_delta_min_pct          # 1h/5m source; reused from ORB pipeline

Gate 3 — Retail cap:
    retail_now < config.retail_max

Gate 4 — Composite score floor:
    score >= config.gate.min_score

Gate 5 — Whale position hard floor:
    pos_now >= config.pos_now_min
```

### `setup_detected` assignment (current code — FIX-3, 2026-06-03)

```python
if gates_v201_enabled:                                  # flag currently False
    setup_detected = gate1 and gate2 and gate3 and gate4 and gate5 and gate8
else:
    setup_detected = gate1 and gate2 and gate3 and gate4 and gate5
```

- **gate6 and gate7 do NOT appear in either branch** — permanently removed from blocking.
- gate8 is the ONLY v2.0.1 gate still wired to the flag, and the flag is False (shadow).

### Shadow tags (computed but NEVER block) — written inside `if setup_detected:`

```
gate6_pass/gate6_fail_v201_price_30d   # price_vs_baseline >= price_vs_baseline_min
gate7_pass/gate7_fail_v201_price_7d    # price_trend_7v30 >= price_trend_7v30_min
gate8_pass/gate8_fail_v201_funding     # funding_8h_pct < funding_8h_pct_max   (also flag-gates setup only if flag True)
oi_trend_pass / oi_trend_fail          # oi_trend_3v14 > 1.0   (V1.3, daily OI multi-day trend)
oi_vs_baseline=X%                      # = oi_delta_1h_pct (Gate 2 input, 1h) — see §5 naming note
```

All tags are always written for `setup_detected=True` rows for shadow-diff analysis. None of
gate6/gate7/oi_trend ever changes `setup_detected`.

### Current default config values

```yaml
pos_trend_min: 0.0
pos_now_high: 0.68
pos_now_min: 0.65
oi_delta_min_pct: 3
retail_max: 0.70
gate:
  min_score: 65
gates_v201_enabled: false        # gates ONLY gate8; no auto Day-30 flip (V1.3)
price_vs_baseline_min: -0.02     # gate6 SHADOW tag threshold (non-blocking)
price_trend_7v30_min: -0.02      # gate7 SHADOW tag threshold (non-blocking)
funding_8h_pct_max: 0.03         # gate8 funding cap (shadow; unit unconfirmed)
oi_trend_min_pct: 1.0            # oi_trend shadow tag threshold (V1.3, non-blocking)
```

### Critical implementation rules
- `oi_delta_1h_pct` MUST reuse the existing 1h OI delta source (ORB pipeline). Do not reimplement,
  and do not feed `series_oi_1d` into Gate 2.
- `insufficient_history`: short-circuit, `setup_detected=False`, do not compute score.
- Any blocking gate fails: `setup_detected=False`, populate `setup_reason_tags` with failed gates,
  still compute score for diagnostics.
- Fail-open for v2.0.1 + oi_trend data (fetch fails → defaults → tags = fail/pass, never raises,
  never blocks).

---

## 8. Score logic

(Unchanged from V1.2.) Score is computed regardless of gate outcome. Range 0–100 integer.
`score = score_gap + score_pos_now + score_pos_trend + score_btc_regime`. Components, BTC-regime
mapping (bull=30 / neutral=10 / bear=0, default neutral on unknown label), and quality bands
(STRONG ≥70, MODERATE ≥40, else WEAK) are as in V1.2 §8.

> Score is gate4 (`score >= gate.min_score`). With regime=bearish contributing 0 to the 30-pt
> regime block, a bearish acc_cont must clear `min_score=65` on the remaining 70 pts — a narrow
> 65–70 window. This is the deliberate bearish defense (see §16 bet).

---

## 9. Output contract

### Decision fields
- `setup_detected: bool`
- `setup_quality_band: str` — STRONG / MODERATE / WEAK
- `setup_reason_tags: List[str]` — blocking gate pass/fail + participation + `oi_vs_baseline` tag;
  ALWAYS includes gate6/gate7/gate8 shadow tags and (V1.3) `oi_trend_pass/fail`
- `accumulation_score: int` — 0–100

### Raw feature fields
- `pos_now`, `acct_now`, `gap_now`, `retail_now`, `taker_now`
- `pos_slope_30d`, `gap_slope_30d`, `retail_slope_30d`
- `pos_trend_3v14`, `pos_min_30d`, `pos_recovery_from_min`, `retail_min_30d`, `retail_recovery`,
  `taker_7d_avg`
- `oi_delta_1h_pct` (Gate 2, 1h source)
- `price_vs_baseline: float`, `price_trend_7v30: float`, `funding_8h_pct: float` (V1.2; 0.0 default)
- **`oi_trend_3v14: float`** — V1.3; normalized %, 0.0 default when daily OI unavailable / `<14` pts

### Diagnostic fields
- `insufficient_history`, `partial_history`, `score_breakdown`, `btc_regime_label`,
  `strategy = "long_accumulation_continuation"`, `strategy_family = "long_accumulation_continuation"`

> **Field propagation (the #1 failure mode):** each NEW field (`oi_trend_3v14`) MUST be added to:
> (1) `Signal` dataclass + `signal_fields` (if persisted to signals), (2) `PendingSetup` +
> `pending_fields` (it is persisted at pending stage). Missing from the list → silent CSV drop.
> Verified present on fresh rows in V1.3 validation.

---

## 10. Config schema (current state)

```yaml
long_accumulation_continuation:
  enabled: true

  # ── Blocking hard gates (gate1–gate5) ─────────────────────────────────────
  pos_trend_min: 0.0
  pos_now_high: 0.68
  pos_now_min: 0.65
  oi_delta_min_pct: 3
  retail_max: 0.70
  gate:
    min_score: 65

  # ── Shadow gates / tags — NON-BLOCKING ────────────────────────────────────
  gates_v201_enabled: false        # gates ONLY gate8; NO automatic Day-30 activation (V1.3)
  price_vs_baseline_min: -0.02     # gate6 shadow tag (price-trend; permanently non-blocking)
  price_trend_7v30_min: -0.02      # gate7 shadow tag (permanently non-blocking)
  funding_8h_pct_max: 0.03         # gate8 funding cap (shadow; unit unconfirmed, sign-off to enable)
  oi_trend_min_pct: 1.0            # oi_trend shadow tag (V1.3; daily OI 3v14 %, non-blocking)

  # ── Chart-based S/R levels ────────────────────────────────────────────────
  stop_buffer_pct: 0.3
  range_lookback_bars_1h: 14
  tp_lookback_bars_1h: 48
  tp2_extension_pct: 1.0
  min_tp1_distance_pct: 3.0
  min_risk_pct: 0.5
  max_risk_pct: 35.0

  # ── Pending expiry ────────────────────────────────────────────────────────
  max_pending_bars_5m: 3

  # ── Data fetch ────────────────────────────────────────────────────────────
  history_period: "1d"
  history_limit: 30
  cache_ttl_seconds: 3600

  # ── Score weights (baked into code — audit reference only) ────────────────
  btc_regime_score_map:
    bull:    ["trend_continuation_friendly"]
    neutral: ["unclear_mixed"]
    bearish: ["broad_weakness_sell_pressure"]

  quality_band_strong_min: 70
  quality_band_moderate_min: 40
```

---

## 11. Field propagation map

| Field | Decided in | Persisted in | Stage |
|---|---|---|---|
| `strategy`, `strategy_family`, `setup_detected`, `setup_quality_band`, `setup_reason_tags`, `accumulation_score` | strategy module | lifecycle CSV (Signal/signal_fields) | signal |
| raw features (pos_now, gap_now, …) | strategy module | same | signal |
| `btc_regime_label` | regime layer | same | signal |
| `price_vs_baseline`, `price_trend_7v30`, `funding_8h_pct` | feature compute + build fn | PendingSetup/pending_fields | pending (shadow analysis) |
| **`oi_trend_3v14`** | **feature compute (daily OI) + build fn** | **PendingSetup/pending_fields** | **pending (shadow analysis), V1.3** |

---

## 12. Edge cases

| Case | Behavior |
|---|---|
| `< 14` top-trader history points | `setup_detected=False`, `insufficient_history=True`, no score |
| 14–29 points | partial window, `partial_history=True` |
| One required endpoint None | insufficient_history, no partial-fill |
| `oi_delta_1h_pct` not computable | `setup_detected=False`, reason `oi_delta_unavailable` |
| `regime_label` missing/unknown | score uses neutral bucket (10), log warning |
| All blocking gates fail | `setup_detected=False`, full feature dict still returned |
| Daily klines fetch fails (gate6/7 input) | defaults 0.0 → gate6/7 tag pass — **non-blocking regardless** |
| Funding fetch fails (gate8 input) | `funding_8h_pct=0.0` → gate8 tag pass (flag-gated; flag False = no effect) |
| **Daily OI fetch fails / `<14` pts (oi_trend)** | **`oi_trend_3v14=0.0` → `oi_trend_fail` tag, no raise — non-blocking** |
| gate6/gate7 fail on a below-baseline token | tag written, **still `setup_detected` if gate1–5 pass** (by design — acc_cont fires during corrections) |
| Same symbol fires again same UTC day | deduped — 1 signal/token/UTC day |

---

## 13. Validation plan

### Phase 1 — Infrastructure pass
- [ ] Files created, imports resolve
- [ ] Unit test `_accumulation_features.py`: insufficient / partial / full paths + price fields + `oi_trend_3v14` (>=14 daily OI, `<14`, base=0 fail-open)
- [ ] `config.yaml` parses

### Phase 2 — Runtime restart discipline (RUNTIME_VALIDATION_DISCIPLINE)
- [ ] Bump `CODE_BUILD_ID`
- [ ] Kill daemon by **PID** (the daemon runs WITHOUT screen — `screen -list` empty ≠ no daemon)
- [ ] Record `CUT_MS` after kill, before start
- [ ] Start fresh; verify `lstart ≈ written_at` and new marker in `RUNNING_CODE_VERSION.txt`

### Phase 3 — Behavior pass on fresh rows only (`created_ts_ms >= CUT_MS`)
- [ ] acc_cont rows: feature fields non-null, gate tags present
- [ ] **V1.3 shadow checks:**
  - `oi_trend_3v14` is a real % (not all-zero) on tokens with ≥14 daily OI bars
  - `oi_trend_pass/fail` in reason_tags; `oi_trend_fail` rows still CONFIRMED (tag does NOT block)
  - gate6/gate7 fail tags appear on below-baseline tokens that are still CONFIRMED+SENT (non-blocking proof)
  - `oi_vs_baseline` (1h Gate 2) and `oi_trend_3v14` (daily) are distinct values per row
  - Gate 2 `oi_delta_1h_pct` unchanged (regression — 5m source intact)
- [ ] `oi_range_breakout` behavior unchanged (regression)

> **V1.3 validation result (2026-06-03, CUT_MS=1780479816000, marker `add-oi-trend-shadow-2026-06-03`):**
> PASS. 5 fresh acc_cont rows; `oi_trend_3v14` range −14.75%…+10.97%; tags non-blocking
> (3 `oi_trend_fail` rows CONFIRMED+SENT); Gate 2 unchanged; no exceptions; no double-writer.
> FIX-2 confirmed live (PARTIUSDT, regime=`broad_weakness_sell_pressure` → MAIN_SIGNAL → SENT).
> FIX-3 confirmed live (gate6/7 fail tags present, rows still SENT).

### gate8 activation (NOT calendar-driven — replaces V1.2 "Day-30")
gate8 may move from shadow to blocking ONLY when ALL hold:
1. funding unit empirically confirmed (§5 Gate F) — the `×8` / unit question resolved
2. shadow data shows `gate8_fail` correctly tagging crowded-long tokens
3. explicit owner sign-off (ADR §7 — this is policy/threshold change)
Then: set `gates_v201_enabled: true`, restart (kill-by-PID + new CUT_MS), judge fresh rows only.

---

## 14. Diagnostic tool — Gate T+F check

`debug_gate_v201.py` (repo root). Standalone gate6/7/8 pass/fail per symbol from live Binance data,
no bot running required. Note: gate6/7 are non-blocking shadow tags; gate8 shadow + flag-gated.

```bash
python debug_gate_v201.py FFUSDT PARTIUSDT BERAUSDT
```

---

## 15. Out of scope (V1)

Short side; score as dispatch confidence beyond gate4; multi-timeframe pos ratio; per-asset dynamic
tuning; cross-exchange top-trader proxies; veto expansion; auto-pause on concurrent signals;
anti-correlation with ORB.

(Short distribution is a SEPARATE future family `short_distribution` in its own project — see
SHORT_SIGNAL_THESIS_v1. Never merged into this family; never revives `short_exhaustion_retest`.)

---

## 16. Risks and caveats

- **The bet (V1.3 — watch this as signals fire):** with gate6/7 removed AND the bearish→WATCHLIST
  downgrade removed (FIX-2), the only bearish defense is gate1–5 (score ≥65 with regime=0 → narrow
  65–70 window, Gate 2 OI-delta, pos floor, retail cap). The owner is betting score + Gate 2
  separates a real wave-3 from a falling knife. UNPROVEN (bear month, no winning case yet). The
  `oi_trend` shadow tag exists precisely to collect evidence: if early bearish signals turn out to
  be falling knives, consider promoting `oi_trend` to a real gate (it already has the tag). **Do
  NOT promote on small N — the V1.3 validation set is n=5, far too few. Collect ≥2–4 weeks of fresh
  multi-regime rows with outcomes first.** Tag now = data to decide later, not a conclusion.
- **`oi_vs_baseline` naming trap:** the reason_tag `oi_vs_baseline=X%` is the **1h Gate-2 delta**,
  not multi-day OI. The multi-day trend is `oi_trend_3v14` / `oi_trend_pass-fail`. Any shadow
  analysis MUST keep the two OI signals separate; do not aggregate them as one "OI" metric.
- **gate8 funding unit unconfirmed:** do not activate gate8 until the per-unit-time meaning of
  `lastFundingRate` is verified. Activating on a wrong unit would gate on a meaningless threshold.
- **Baseline exclusivity (gate6 shadow):** `baseline = mean(close)` over 30d including a pump period
  → mean inflated → post-pump price easily below mean. This is WHY gate6 was removed from blocking
  (it would reject exactly the re-accumulation-after-pullback setups acc_cont targets). It survives
  only as a shadow observation.
- **Fail-open design:** data errors on gate6/7/8 and oi_trend default to non-blocking. Acceptable —
  none of these gates block; do not block when data unavailable.
- **Selection bias:** 5 blocking gates × continuous score = many combinations, small per-combo
  samples. No optimization conclusions for ≥2–4 weeks of fresh data.
- **Endpoint reliability:** top-trader + openInterestHist endpoints less reliable than core
  OI/kline. Cache + None handling non-optional. +1 daily-OI call/symbol/scan (V1.3) — monitor 429.
- **CSV readers must use DictReader + try/except** on new columns (`price_vs_baseline`,
  `price_trend_7v30`, `funding_8h_pct`, and V1.3 `oi_trend_3v14`); old rows have empty strings.

---

## 17. Done criteria (V1.3)

1. Files in §4 modified per spec; import check passes
2. Fresh post-CUT_MS rows show full populated output incl. `oi_trend_3v14` for ≥3 distinct symbols
3. `oi_trend_pass/fail` visible in reason_tags AND never blocks `setup_detected`
4. gate6/7 fail tags visible AND never block (rows still SENT)
5. `oi_trend_3v14` column present and non-empty in pending CSV
6. Gate 2 `oi_delta_1h_pct` unchanged; ORB unchanged (regression)
7. No exceptions / double-writer on fresh rows
   — **All 1–7 met in 2026-06-03 validation (see §13).**

---

## 18. Out-of-spec divergence protocol

If during implementation Claude Code finds a field with conflicting semantics, an endpoint shape
mismatch, a runtime pattern mismatch, or a need to change any §2 locked decision:
→ **Stop. Surface the conflict to owner. Do not silently adapt.**
