# SHORT_SIGNAL_THESIS_v1 — for backtest data collection

Status: THESIS ONLY. No code, no strategy, no go-live. Input spec for a separate
data-collection + backtest project.
Created: 2026-06-03
Family (future, if validated): `short_distribution` — a NEW strategy family, separate from
`long_accumulation_continuation`. NOT a revival of the deleted `short_exhaustion_retest`
(deleted Round 2 — do not reuse its name or logic).

---

## 0. Why this document exists

A previous exploratory pass (low-cap <1B universe, Binance top-trader + OI + funding +
retail, May 2026) tried to find a "distribution signature" to warn on long positions. Key
findings that CONSTRAIN any short thesis (do not relearn these the hard way):

1. **OI does NOT weaken before a top.** In dumped tokens, OI *rose* into the peak (median
   +12%, up to +380%) and only fell *after*. "OI weakening" is a LATE signal, not early.
2. **Top-trader pos/acct peaks AFTER price** (lead/lag median +50h). The absolute peak of
   positioning lags the price peak — positioning is a lagging, noisy series.
3. **Funding does NOT discriminate** in low-cap (median ~0.005%, full coverage 179/179).
   Drop the "funding hot = top" intuition for this segment.
4. **Retail FALLS before the peak** (80% of cases), it does not FOMO-buy the top. The
   owner's model: retail is the fuel; when retail stops entering, whales take profit → dump.
5. **Dumps are SLOW.** Peak→trough median ~15 days, fastest ~4 days, **zero** dumped in
   ≤3 days. A short entered "at the top" then waits ~2 weeks to pay, bleeding through
   chop/funding/squeezes. And signal-positive cases dumped *slower* (14.7d) than
   signal-negative (10.5d) — the distribution signature did NOT correlate with faster dumps.
6. **The May data could not test any thesis** because it was a one-directional bear month:
   ~96% of the low-cap universe dropped ≥30%, leaving only ~6 "survivor" tokens — no
   control group. AND tokens that peaked early (the market-driven crash) had no pre-peak
   top-trader data (Binance retention ~30 days). Both walls block hindsight testing.

→ **Consequence:** this short thesis CANNOT be validated on Binance's 30-day retained data.
It requires forward-logged, multi-regime data with a real control group. That is the job
of the separate project this document feeds.

---

## 1. Core reframe — short the CONTINUATION, not the top

Symmetric to `long_accumulation_continuation` (which does NOT catch the bottom — it catches
"accumulation already happened + price continuing up"):

`short_distribution` should NOT try to call the top (finding 5 shows that's slow and the
signal is mistimed). It should catch **"distribution already happened + structure already
broke + downtrend continuing"** — entering the *middle* of an established downtrend, where
momentum is confirmed, not guessing the peak.

This avoids the "enter early, wait 2 weeks" trap: entry is gated on a confirmed structural
break, not on a positioning signature near a high.

---

## 2. Two competing hypotheses (backtest must distinguish them)

The owner has not committed to one. Backtest BOTH and let data decide — do not assume.

**H-A — "Top distribution" (short at/near the peak):**
A token shows distribution signature (whale pos/acct falling + retail falling, "in-phase
exit") near a local high → short there, target the subsequent drop.
> Prior data leans AGAINST this (findings 2, 5): mistimed and slow. Include it only to
> confirm/deny rigorously, not because it looks promising.

**H-B — "Distribution continuation" (short after the break):** ← reframe, untested
A token that (a) had a run-up, (b) has since BROKEN structure (lost its accumulation/
support zone by some %), (c) shows downtrend continuation (lower highs, OI behavior, etc.)
→ short the continuation, not the top.
> Untested by the May pass. Symmetric to the live long strategy. The more defensible
> candidate, but unproven — backtest decides.

**H0 (null, for both):** the entry signal appears about equally in tokens that keep falling
AND tokens that bounce/recover → no discrimination → no edge → do not build.

---

## 3. What "works" must mean (define before measuring — avoid hindsight self-deception)

For EITHER hypothesis, an entry rule has edge only if, measured against a **control group**:

- tokens firing the signal continue down (to some target, e.g. −15% / −30% from entry)
  at **materially higher frequency** than tokens not firing it, AND
- the continuation happens within a **tradeable horizon** (if it takes 15 days like the
  long-side dumps, the short bleeds — horizon matters as much as direction), AND
- the **bounce/squeeze rate** (signal fires, then price rips up instead) is low enough that
  asymmetric short risk (unbounded loss) is survivable.

No auto-verdict. The backtest prints frequencies + horizons + bounce rates for signal-group
vs control-group, with n and coverage. Human reads.

---

## 4. Data to LOG (this is the deliverable spec for the other project)

Forward-log, per coin in the low-cap (<1B) universe, at a fixed cadence (hourly is fine;
dumps are slow so even 4–6h suffices, but hourly gives flexibility). Persist to its OWN
store (CSV/db) — never write into the live bot's data/.

**Universe (reuse existing tooling):**
- Binance USDT-perp symbols → CoinGecko market cap → keep < $1B (the `build_watchlist_lowcap`
  approach already built). Refresh weekly. Log the excluded/ambiguous set (fail-closed).

**Per-coin time series (timestamped, append-only):**
| Field | Source (Binance public) | Why |
|---|---|---|
| close, high, low | `/fapi/v1/klines` 1h | price structure, break detection, drawdown |
| open interest | `/futures/data/openInterestHist` 1h | OI behavior (NOTE finding 1: rises into top) |
| top pos ratio | `/futures/data/topLongShortPositionRatio` 1h | whale positioning (lags price — finding 2) |
| top acct ratio | `/futures/data/topLongShortAccountRatio` 1h | whale account side |
| global retail | `/futures/data/globalLongShortAccountRatio` 1h | retail fuel (falls pre-peak — finding 4) |
| taker buy/sell | `/futures/data/takerlongshortRatio` 1h | order-flow pressure |
| funding | `/fapi/v1/fundingRate` | record it, but finding 3: weak in low-cap |
| markPx, oraclePx | mark/oracle | squeeze / oracle-divergence guard (see §6) |

**Critical: log FORWARD, continuously.** The whole point is to escape the 30-day retention
wall. Start now; the value compounds. After ~1–2 months it spans multiple regimes (bear,
chop, up) → a real control group becomes possible.

**Also derive & store per snapshot (or compute at backtest time from raw):**
- distance from recent N-day high (for "broke structure" detection, H-B)
- max drawdown after each local peak, AND **time to −15% / −30%** (not just to trough —
  finding 5 caveat: trough ≠ first −30%; measure the threshold crossing explicitly)
- rolling changes (pre-24h etc.) using a "return None if no real point within ±3h" rule
  (do NOT nearest-fill across gaps — that produced fake 0.0s in the prior pass)

---

## 5. Control group is MANDATORY (the thing the May pass lacked)

The backtest is meaningless without it. Define symmetrically:

- **CONT (continued down):** after the entry trigger, price falls ≥30% more (H-A: from peak;
  H-B: from break point).
- **BOUNCE (did not continue):** after the trigger, price falls <15% / recovers.
- GREY (15–30%): excluded, to keep groups far apart.

Then: signal-fire-rate in CONT vs BOUNCE. If similar → H0 → no edge. This is exactly the
DUMP-vs-SURVIVE comparison the May data could not populate; forward multi-regime data can.

---

## 6. Short-specific risk layer (design now, because short ≠ inverted long)

Short carries asymmetric, unbounded risk. Any future `short_distribution` strategy MUST
carry an anti-squeeze layer BEFORE it can size real risk. Log the inputs now so the
backtest can study them (mirrors HL spec §11 anti-signal):

- **thin book / low depth** → squeeze risk (small-cap rips on little volume)
- **mark/oracle divergence** → manipulation around liquidation
- **funding extreme** → crowded short = squeeze fuel (even if weak as a top-signal, matters
  for short risk)
- **post-liquidation-cascade** → entering after a flush = late short / trap
- **vertical bounce signature** → price already snapping back

A short that fires the distribution signal but sits in a thin-book squeeze-prone coin must
be vetoed, not taken. The backtest should measure how often signal-fires coincide with
these danger states.

---

## 7. Architecture constraints (when/if this becomes a strategy — not now)

- NEW family `short_distribution`, its own file under `scanner/strategies/`, its own config
  block, own gate logic. Separate from `long_accumulation_continuation` — never merged
  (locked invariant: strategy families stay separate).
- Do NOT revive `short_exhaustion_retest` (deleted Round 2) — different concept, different
  name, fresh code.
- Lifecycle/delivery/veto/dispatch unchanged: the short strategy only EMITS a detected
  setup; dispatch routes, veto is the only hard-no, delivery annotates. Same layering.
- Shadow first (config-disabled), validate on fresh rows, then activate — same discipline
  as the long strategy and the HL shadow.
- SL/TP for short = symmetric to long: anchor to structure (stop above the broken zone /
  recent swing high + buffer; targets at next support), NOT a fixed % wish. Asymmetric risk
  means stop discipline is even more important than long.

---

## 8. Sequencing recommendation (carry this mindset into the other project)

1. The other project's ONLY near-term job: **stand up the forward logger** (§4) on the
   <1B universe. Cheap, read-only, runs in the background. This is the single highest-value
   action — it turns "untestable" into "testable in 1–2 months".
2. Do NOT build the short strategy from May data or any single-regime hindsight. The May
   pass proved that path produces unfalsifiable guesses.
3. When the logger has multi-regime coverage with a real control group (§5): run the
   backtest on H-A vs H-B (§2), print raw contrast + horizon + bounce rate (§3), human
   decides. Only then write a strategy spec.
4. The live long strategy (`long_accumulation_continuation`) going live + its own data
   accumulating is independent and should not wait on this.

---

## 9. One-line summary

Short by catching a CONFIRMED downtrend continuation (H-B), not by calling the top (H-A,
which prior data suggests is slow and mistimed) — but treat both as hypotheses, log forward
multi-regime data with a mandatory control group and an anti-squeeze layer, and let the
backtest decide before any code exists.
