# STRATEGY_SPEC_long_accumulation_continuation_V1_2

Status: implemented and live  
Created: 2026-05-20 (V1.0)  
Updated: 2026-06-01 (V1.2 — gate4, gate5, v2.0.1 shadow gates)  
Family: `long_accumulation_continuation`  
Side: long only (V1)  
Implementation files: exist in repo

---

## Changelog

| Version | Date | Changes |
|---|---|---|
| V1.0 | 2026-05-20 | Initial implementation — 3 hard gates, score informational |
| V1.1 | ~2026-05-28 | Gate 4 added (score floor, `gate.min_score`); Gate 5 added (`pos_now_min` hard floor); `oi_delta_min_pct` lowered 5.0 → 3; market cap display in signal; daily dedup (1 signal per token per UTC day) |
| V1.2 | 2026-06-01 | v2.0.1 shadow gates: Gate 6 (T-30d), Gate 7 (T-7d), Gate 8 (F-funding); `series_close` + `funding_8h_pct` inputs; `price_vs_baseline`, `price_trend_7v30` features; `gates_v201_enabled` toggle; `debug_gate_v201.py` diagnostic tool |

---

## 0. Mandatory pre-code reading

Before writing any code, view in this order:

1. `CODE_OWNERSHIP_AND_CHANGE_IMPACT_MAP_V1.md` — file ownership rules
2. `FIELD_PROPAGATION_MAP_V1.md` — how fields travel end-to-end
3. `CURRENT_CODE_TO_TARGET_MAPPING_REPORT_V2_1.md` — real owners now
4. `IMPLEMENTATION_CONTRACT_V1_1.md` — invariants
5. `RUNTIME_DEPLOY_TEST_GUARDRAILS.md` + `POST_PATCH_CHECKLIST.md` — validation discipline

Then view current code state:

6. `oi_scanner.py` — find: `Signal` dataclass, `signal_fields` list, `PendingSetup` dataclass, `pending_fields` list, `_process_pending` function, strategy detection orchestration block, dispatch wiring
7. `scanner/strategies/oi_range_breakout.py` — **read for pattern only** (return shape, field naming, error handling). Do NOT copy gate logic.
8. `scanner/binance_client.py` — confirm top-trader endpoints absent (per user: confirmed absent)
9. `scanner/regime/classifier.py` + `regime/regime_normalizer.py` — find exact `regime_label` string values currently emitted
10. `config.yaml` — find existing strategy config block shape

**Stop and confirm with user if any divergence from this spec is found.**

---

## 1. Problem statement

Add a 5th strategy family that detects long accumulation continuation setups based on:
- Top trader position structure (loaded or actively building)
- Concentration intensity (gap between size-weighted and count-weighted top ratios)
- OI confirmation (capital actually entering)
- Retail not yet FOMO-crowded long

Different from ORB:
- ORB detects OI range breakout + price breakout
- Accumulation detects positioning structure regardless of price range

→ Separate strategy family. Do not merge with ORB.

---

## 2. Locked decisions

| Decision | Value | Changed in |
|---|---|---|
| Strategy family name | `long_accumulation_continuation` | V1.0 |
| Side | long only (V1) | V1.0 |
| Number of hard gates | **8** (5 live + 3 shadow v2.0.1) | V1.0=3, V1.1=5, V1.2=8 |
| Score role | **Gate 4** (score floor) + quality annotation | V1.1 — was informational only in V1.0 |
| Score range | 0–100 (integer) | V1.0 |
| Score weights | gap 30 + pos_now 20 + pos_trend 20 + BTC regime 30 = 100 | V1.0 |
| BTC regime score mapping | bearish=0, neutral=10, bull=30 | V1.0 |
| Retail FOMO handling | hard gate (Gate 3) | V1.0 |
| OI delta floor | `oi_delta_min_pct >= 3%` | V1.1 — was 5% in V1.0 |
| Insufficient history | `setup_detected = False`, no exception | V1.0 |
| Daily dedup | 1 signal per token per UTC day | V1.1 |
| v2.0.1 gates activation | shadow mode (`gates_v201_enabled: false`) until Day 30 = 2026-06-18 | V1.2 |

---

## 3. Layer placement

Strategy Thesis layer. Must respect locked invariants:

- Delivery metadata is annotation, not a gate here
- Veto layer is not expanded for this strategy
- Dispatch routes based on output, does not re-detect
- Lifecycle truth is not overwritten by strategy code

---

## 4. File ownership

### Primary files (exist)
- `scanner/strategies/long_accumulation_continuation.py` — strategy detection + scoring
- `scanner/strategies/_accumulation_features.py` — pure feature computation, unit-testable

### Sync files (updated)
- `scanner/binance_client.py` — 4 top-trader fetch methods
- `oi_scanner.py` — Signal dataclass fields, signal_fields, PendingSetup fields, pending_fields, _process_pending integration, strategy detection call site
- `config.yaml` — `long_accumulation_continuation` section

### Diagnostic tool (V1.2)
- `debug_gate_v201.py` — standalone Gate T + Gate F diagnostic (fetches live Binance data, reports gate6/7/8 pass/fail per symbol)

### NOT to touch
- `scanner/strategies/oi_range_breakout.py`
- `scanner/strategies/long_breakout_retest.py`
- `scanner/strategies/short_exhaustion_retest.py`
- `veto/veto_engine.py`
- `scanner/regime/classifier.py` (read-only for label values)
- `regime/regime_normalizer.py` (read-only)
- `dispatch/dispatch_router.py` (migration shim)
- `scanner/dispatch/router.py` (no logic change — strategy just emits, dispatch decides)
- `build_daily_review_pack.py` (downstream auto-picks fields if propagation correct)
- `scanner/lifecycle.py` (use existing API only, no semantic change)
- `scanner/review_service.py`, `review_capture_runtime.py`

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

Required params per call: `symbol`, `period`, `limit`.

Field extraction:
- pos / acct / retail: use `longAccount` field (fraction long, e.g. `0.62`). Do NOT use `longShortRatio`.
- taker: use `buySellRatio` field.
- **Sort ascending by timestamp before computing trend/slope.**

### Daily klines for Gate T (V1.2)

| Source | Endpoint | Interval | Limit |
|---|---|---|---|
| Daily close prices | `/fapi/v1/klines` | `1d` | `31` |

Usage: `limit=31` → `[:-1]` → 30 complete bars (drops forming bar).
Returns list of lists; index 4 = close price.
Fail-open: fetch fails → `series_close=[]` → `price_vs_baseline=0.0`, `price_trend_7v30=0.0` → gates 6+7 PASS.

### Funding rate for Gate F (V1.2)

| Source | Method | Returns |
|---|---|---|
| `self.funding(symbol)` in `oi_scanner.py` | Binance `/fapi/v1/premiumIndex` | `lastFundingRate × 100.0` (percent) |

BSB example: lastFundingRate=0.00035 → `self.funding()` returns 0.035 → gate8 threshold `< 0.03` → FAIL.
Fail-open: fetch fails → `funding_8h_pct=0.0` → gate8 PASS.

### Caching
- Cache key: `(symbol, date_utc, endpoint_name)`.
- TTL: 1 hour minimum.
- 1 symbol = 4 top-trader fetches + 1 daily klines fetch + 1 funding fetch.

### Error handling
- Network/4xx/5xx/parse error: log and return `None`.
- Downstream treats `None` series as `insufficient_history`.
- Strategy must NOT raise to runtime orchestrator on data failure.

---

## 6. Feature computation spec

File: `scanner/strategies/_accumulation_features.py`

Pure functions. No global state. No I/O (data passed in by caller).

### Inputs

```
series_pos:    List[float]          # top trader position ratio (long fraction)
series_acct:   List[float]          # top trader account ratio
series_retail: List[float]          # global account ratio
series_taker:  List[float]          # taker buy/sell ratio
series_oi:     List[float]          # open interest history (reuse existing source)
series_close:  List[float] | None   # daily close prices — V1.2, keyword-only, default None
```

### Computed scalars

| Field | Formula | Version |
|---|---|---|
| `pos_now` | `series_pos[-1]` | V1.0 |
| `acct_now` | `series_acct[-1]` | V1.0 |
| `gap_now` | `pos_now - acct_now` | V1.0 |
| `retail_now` | `series_retail[-1]` | V1.0 |
| `taker_now` | `series_taker[-1]` | V1.0 |
| `pos_slope_30d` | `numpy.polyfit(range(N), series_pos, 1)[0]` | V1.0 |
| `gap_slope_30d` | slope of `[p-a for p,a in zip(series_pos, series_acct)]` | V1.0 |
| `retail_slope_30d` | slope of `series_retail` | V1.0 |
| `pos_trend_3v14` | `mean(series_pos[-3:]) - mean(series_pos[-14:])` | V1.0 |
| `oi_trend_3v14` | `mean(series_oi[-3:]) - mean(series_oi[-14:])` | V1.0 |
| `pos_min_30d` | `min(series_pos)` | V1.0 |
| `pos_recovery_from_min` | `pos_now - pos_min_30d` | V1.0 |
| `retail_min_30d` | `min(series_retail)` | V1.0 |
| `retail_recovery` | `retail_now - retail_min_30d` | V1.0 |
| `taker_7d_avg` | `mean(series_taker[-7:])` | V1.0 |
| `price_vs_baseline` | `close[-1] / mean(close) - 1.0` (requires `len(series_close) >= 14`) | **V1.2** |
| `price_trend_7v30` | `mean(close[-7:]) / mean(close[-30:]) - 1.0` (same guard) | **V1.2** |

`price_vs_baseline` and `price_trend_7v30` default to `0.0` when `series_close` is None or has fewer than 14 bars (fail-open by design).

### History sufficiency rules

```
required = [series_pos, series_acct, series_retail, series_taker]
# series_oi and series_close excluded from required — shorter by design

if any(s is None or len(s) == 0 for s in required):
    return {"insufficient_history": True, ...}

if min(len(s) for s in required) < 14:
    return {"insufficient_history": True, ...}

partial = min(len(s) for s in required) < 30
```

---

## 7. Hard gate logic

File: `scanner/strategies/long_accumulation_continuation.py`

### Gates 1–5 — all must be True for `setup_detected = True`

```
Gate 1 — Whale OR-condition:
    (pos_trend_3v14 > config.pos_trend_min)       # trend rising
    OR
    (pos_now >= config.pos_now_high)               # already loaded

Gate 2 — OI confirmation:
    oi_delta_1h_pct >= config.oi_delta_min_pct     # capital entering

Gate 3 — Retail cap:
    retail_now < config.retail_max                 # retail not FOMO-crowded

Gate 4 — Composite score floor:
    score >= config.gate.min_score                 # minimum signal quality (V1.1)

Gate 5 — Whale position hard floor:
    pos_now >= config.pos_now_min                  # absolute minimum (V1.1)
```

### Gates 6–8 — v2.0.1 shadow gates (V1.2)

Controlled by `gates_v201_enabled` toggle. Tags always written regardless of toggle.

```
Gate 6 (T-30d) — Price trend floor:
    price_vs_baseline >= config.price_vs_baseline_min    # price ≥ mean(30d) − 2%

Gate 7 (T-7d) — Momentum floor:
    price_trend_7v30 >= config.price_trend_7v30_min      # 7d avg ≥ 30d avg − 2%

Gate 8 (F) — Funding neutral:
    funding_8h_pct < config.funding_8h_pct_max           # not crowded-long
```

When `gates_v201_enabled: false` (shadow mode):
- Gates 6/7/8 computed but do NOT block `setup_detected`
- Tags `gate6_pass/fail`, `gate7_pass/fail`, `gate8_pass/fail` written to `reason_tags` for every setup_detected=True row
- "Would-be-blocked" set visible in CSV for shadow diff analysis

When `gates_v201_enabled: true` (live mode, activate 2026-06-18):
- `setup_detected = gate1 and gate2 and gate3 and gate4 and gate5 and gate6 and gate7 and gate8`

### Current default config values

```yaml
pos_trend_min: 0.0         # Gate 1: pos_trend_3v14 must be strictly positive
pos_now_high: 0.68         # Gate 1: whale loaded threshold (OR-gate)
pos_now_min: 0.65          # Gate 5: whale position floor — hard minimum
oi_delta_min_pct: 3        # Gate 2: 1h OI delta floor (lowered from 5.0 in V1.1)
retail_max: 0.70           # Gate 3: retail FOMO cap
gate:
  min_score: 65            # Gate 4: composite score minimum
gates_v201_enabled: false
price_vs_baseline_min: -0.02   # Gate 6 (T): 30d trend floor
price_trend_7v30_min: -0.02    # Gate 7 (T): 7d momentum floor
funding_8h_pct_max: 0.03       # Gate 8 (F): crowded-long cap (per-8h %)
```

### Critical implementation rules

- `oi_delta_1h_pct` MUST reuse the existing 1h OI delta computation source used by ORB pipeline. Do not reimplement.
- If `insufficient_history = True`: short-circuit, return `setup_detected = False`, reason `insufficient_history`, do not compute score.
- If any gate fails: `setup_detected = False`, populate `setup_reason_tags` with all failed gate names. Still compute score for diagnostic purposes.
- If all live gates pass: `setup_detected = True`, compute score, populate full output.
- Fail-open for v2.0.1 data (klines/funding fetch fails → default 0.0 → gates PASS). Intentional: do not block when data unavailable.

---

## 8. Score logic

Score is computed regardless of gate outcome (for diagnostic). Range 0–100, integer.

**Score is gate4** (gate `score >= gate.min_score`). It also annotates quality band.

### Components

```
score = score_gap + score_pos_now + score_pos_trend + score_btc_regime
```

### score_gap (max 30)

```python
if gap_now >= 0.20:    30
elif gap_now >= 0.15:  22
elif gap_now >= 0.10:  14
elif gap_now >= 0.05:  7
else:                   0
```

### score_pos_now (max 20)

```python
if pos_now >= 0.70:    20
elif pos_now >= 0.65:  15
elif pos_now >= 0.60:  10
elif pos_now >= 0.55:  5
else:                   0
```

### score_pos_trend (max 20)

```python
if pos_trend_3v14 > 0.02 and pos_recovery_from_min >= 0.03:  20
elif pos_trend_3v14 > 0.01:                                   15
elif pos_trend_3v14 > 0.0:                                    10
elif pos_trend_3v14 > -0.01:                                  5
else:                                                         0
```

### score_btc_regime (max 30)

```python
bull bucket    → 30   # labels: ["trend_continuation_friendly"]
neutral bucket → 10   # labels: ["unclear_mixed"]
bearish bucket →  0   # labels: ["broad_weakness_sell_pressure"]
```

If `regime_label` is unknown/missing/unrecognized: default to neutral (10), log warning. Do NOT fail.

Mapping table in `config.yaml` under `long_accumulation_continuation.btc_regime_score_map` — tunable without code change.

### Quality band

```
score >= quality_band_strong_min (70)   → STRONG
score >= quality_band_moderate_min (40) → MODERATE
else                                    → WEAK
```

---

## 9. Output contract

Strategy detect function returns a dict with these fields:

### Decision fields
- `setup_detected: bool`
- `setup_quality_band: str` — `STRONG` / `MODERATE` / `WEAK`
- `setup_reason_tags: List[str]` — gate pass/fail tags + participation label + oi_vs_baseline tag when setup_detected=True; always includes gate6/7/8 tags (V1.2)
- `accumulation_score: int` — 0–100

### Raw feature fields
- `pos_now`, `acct_now`, `gap_now`, `retail_now`, `taker_now`
- `pos_slope_30d`, `gap_slope_30d`, `retail_slope_30d`
- `pos_trend_3v14`, `oi_trend_3v14`
- `pos_min_30d`, `pos_recovery_from_min`
- `retail_min_30d`, `retail_recovery`
- `taker_7d_avg`
- `oi_delta_1h_pct` (reused from existing computation)
- `price_vs_baseline: float` — V1.2; 0.0 default when data unavailable
- `price_trend_7v30: float` — V1.2; 0.0 default
- `funding_8h_pct: float` — V1.2; 0.0 default

### Diagnostic fields
- `insufficient_history: bool`
- `partial_history: bool`
- `score_breakdown: dict` — `{"gap": int, "pos_now": int, "pos_trend": int, "btc_regime": int}`
- `btc_regime_label: str`
- `strategy: str = "long_accumulation_continuation"`
- `strategy_family: str = "long_accumulation_continuation"`

---

## 10. Config schema (current state)

```yaml
long_accumulation_continuation:
  # ── Hard gates (all 8 thresholds) ─────────────────────────────────────────
  pos_trend_min: 0.0         # Gate 1: pos_trend_3v14 must be strictly positive
  pos_now_high: 0.68         # Gate 1: whale loaded threshold (OR-gate with pos_trend)
  pos_now_min: 0.65          # Gate 5: whale position floor — hard minimum
  oi_delta_min_pct: 3        # Gate 2: 1h OI delta floor
  retail_max: 0.70           # Gate 3: retail FOMO cap
  gate:
    min_score: 65            # Gate 4: composite score minimum (0-100 scale)
  # v2.0.1 light gates — shadow mode (activate Day 30 = 2026-06-18)
  gates_v201_enabled: false
  price_vs_baseline_min: -0.02     # Gate 6 (T): 30d price trend floor (-2% tolerance)
  price_trend_7v30_min: -0.02      # Gate 7 (T): 7d momentum floor (-2% tolerance)
  funding_8h_pct_max: 0.03         # Gate 8 (F): crowded-long cap (per-8h Binance %, 0.03 = 0.03%)

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
  # gap=30, pos_now=20, pos_trend=20, btc_regime=30 → sum=100
  btc_regime_score_map:
    bull:
      - "trend_continuation_friendly"
    neutral:
      - "unclear_mixed"
    bearish:
      - "broad_weakness_sell_pressure"

  # ── Quality band thresholds ───────────────────────────────────────────────
  quality_band_strong_min: 70
  quality_band_moderate_min: 40
```

---

## 11. Field propagation map

| Field | Decided in | Propagated through | Persisted in | Rendered in |
|---|---|---|---|---|
| `strategy`, `strategy_family` | `long_accumulation_continuation.py` | `oi_scanner.py` (Signal + signal_fields) | lifecycle CSV | review pack auto-picks |
| `setup_detected`, `setup_quality_band`, `setup_reason_tags`, `accumulation_score` | same | same | same | same |
| All raw features (pos_now, gap_now, etc.) | same | same | same | same |
| `btc_regime_label` | read from regime layer | same | same | same |
| `price_vs_baseline`, `price_trend_7v30`, `funding_8h_pct` | feature compute + build function | PendingSetup + pending_fields | pending CSV | shadow analysis |

**Claude Code must explicitly add each new field name** to:
1. Signal dataclass in `oi_scanner.py`
2. `signal_fields` list in `oi_scanner.py`
3. PendingSetup dataclass + `pending_fields` list for pending-stage fields

If any field is missing from `signal_fields`, it will silently drop during CSV write. **This is the #1 failure mode.**

---

## 12. Edge cases

| Case | Behavior |
|---|---|
| Symbol newly listed (< 14 history points) | `setup_detected = False`, `insufficient_history = True`, no score |
| Symbol with 14–29 points | Compute with partial window, flag `partial_history = True` |
| One endpoint returns None, others OK | Treat as insufficient_history, do not partial-fill |
| Endpoint returns empty list | Same as None |
| `oi_delta_1h_pct` not computable | `setup_detected = False`, reason `oi_delta_unavailable` |
| `regime_label` missing/unknown | Score uses neutral bucket (10), log warning |
| All gates fail | `setup_detected = False`, full feature dict still returned for diagnostic |
| Score computation raises | Catch, log, set score = 0, do not fail strategy |
| Network timeout during fetch | Return None, downstream → insufficient_history |
| Daily klines fetch fails (Gate T) | `series_close=[]` → `price_vs_baseline=0.0`, `price_trend_7v30=0.0` → gates 6+7 PASS (fail-open) |
| Funding fetch fails (Gate F) | `funding_8h_pct=0.0` → gate 8 PASS (fail-open) |
| `< 14` daily bars | Same 0.0 defaults → gates 6+7 PASS |
| `gates_v201_enabled: false` | Gates 6/7/8 computed and tagged, do NOT block `setup_detected` |
| Same symbol fires again same UTC day | Deduped — only 1 signal per token per UTC day (V1.1) |

---

## 13. Validation plan

### Phase 1 — Infrastructure pass
- [ ] All files created, imports resolve
- [ ] Unit test `_accumulation_features.py`: insufficient, partial, full history paths + v2.0.1 price fields
- [ ] Strategy module importable in isolation
- [ ] `config.yaml` parses with new section
- [ ] One-shot dry call on 1 symbol with debug logging; confirm all 8 gate evaluations logged

### Phase 2 — Runtime restart discipline
- [ ] Bump `CODE_BUILD_ID`
- [ ] `screen -list` → kill all old sessions
- [ ] Record `CUT_MS` after kill, before start
- [ ] Start fresh runtime
- [ ] `cat RUNNING_CODE_VERSION.txt` shows new build marker

### Phase 3 — Behavior pass on fresh rows only
- [ ] Wait for fresh rows `created_ts_ms >= CUT_MS`
- [ ] Inspect acc_cont rows: all feature fields non-null, gate tags present
- [ ] **V1.2 shadow cross-checks** (must pass before Day 30 activation):
  - `gate8_fail_v201_funding_crowded` must appear on at least one high-funding token (funding > 0.03%). If never seen → funding unit is wrong.
  - `gate6_fail_v201_price_30d` must appear on markdown tokens (price well below 30d mean).
  - Monitor `gate6_fail` rate on re-accumulation candidates. If it fires on tokens with whale pos rising but price below mean → widen floor from -0.02 to -0.05 before Day 30.
- [ ] Confirm `oi_range_breakout` behavior unchanged (regression check)

### Day 30 activation (2026-06-18)
After shadow validation passes:
1. Set `gates_v201_enabled: true` in `config.yaml`
2. Restart process (kill screen + new CUT_MS + new screen session)
3. Judge only rows with `created_ts_ms >= new CUT_MS`

---

## 14. Diagnostic tool — Gate T+F check

**File:** `debug_gate_v201.py` (repo root)

Standalone script for checking gate6/7/8 pass/fail for any token(s). Fetches live Binance data, no bot running required.

```bash
# Default: FFUSDT + auto-discover BAS* symbols
python debug_gate_v201.py

# Specific symbols
python debug_gate_v201.py FFUSDT BASUSDT SKYAIUSDT
```

Output: per-symbol `price_vs_baseline`, `price_trend_7v30`, `funding_8h_pct` with PASS/FAIL verdict.

---

## 15. Out of scope (V1)

- Short side accumulation
- Score used as dispatch confidence band beyond gate4 floor
- Multi-timeframe pos ratio (1d only)
- Dynamic threshold tuning per asset
- Cross-exchange (Bybit) top-trader proxies
- Veto layer expansion
- Auto-pause if too many concurrent accumulation signals
- Anti-correlation check with ORB

---

## 16. Risks and caveats

- **Baseline exclusivity (V1.2 Gate T)**: `baseline = mean(close)` over 30d including any pump period → mean inflated → post-pump price easily < mean → `price_vs_baseline < -0.02` easy to fail. Floor -0.02 is stricter than it sounds for re-accumulation-after-pump tokens. Shadow validation is the gate for this risk. If confirmed, widen to -0.05 before Day 30.
- **Gate F scope (V1.2)**: Plan assumed a `price_vs_baseline < 0.10` upper cap existed from v2.0 filter. It does NOT in current code. Gate F may fire on higher-momentum entries than expected. Shadow will reveal scope.
- **Fail-open design**: Data errors on Gate T and Gate F return 0.0 defaults → gates pass. Deep-markdown tokens with data errors slip through. Acceptable for "light gate" — do not block when data unavailable.
- **Selection bias**: 5 hard gates × continuous score creates many combinations. Initial sample per combination will be small. Do not draw optimization conclusions for at least 2–4 weeks of fresh data.
- **Endpoint reliability**: Binance top-trader endpoints have lower reliability than core OI/kline endpoints. Cache + None handling is non-optional.
- **BTC regime coupling**: 30% of score depends on regime layer. Score map is in config to absorb label drift.
- **Rate limit**: +1 daily klines + 1 funding call per symbol per scan (V1.2). Monitor for 429 on first deploy.
- **CSV readers must use DictReader**: V1.2 adds 3 new columns (`price_vs_baseline`, `price_trend_7v30`, `funding_8h_pct`). Old CSV rows have empty string in these columns. Wrap `float(row["price_vs_baseline"])` in try/except in any consumer.

---

## 17. Done criteria

V1.2 implementation is considered complete when:

1. All files in Section 4 created/modified per spec
2. Import check passes: `from scanner.strategies.long_accumulation_continuation import detect_long_accumulation_continuation`
3. Fresh post-CUT_MS rows show full populated output for at least 3 distinct symbols that trigger `setup_detected = True`
4. Gate tags gate6/7/8 visible in `reason_tags` column of pending CSV
5. `price_vs_baseline`, `price_trend_7v30`, `funding_8h_pct` columns present and non-empty in pending CSV
6. ORB strategy behavior unchanged on fresh rows (no regression)
7. Shadow cross-checks from Section 13 Phase 3 pass
8. 48+ hours of live data collected with no exceptions from new data fetches

---

## 18. Out-of-spec divergence protocol

If during implementation Claude Code finds:
- A field already exists with conflicting semantics
- An endpoint shape differs from this spec
- Runtime orchestration pattern doesn't match assumed pattern
- Any locked decision in Section 2 needs to change

→ **Stop. Surface the conflict to user. Do not silently adapt.**
