# Directional Selector Null Test — Pre-Registered Protocol

## Purpose

Determine whether the strategy's directional selector contains measurable
directional information beyond random BUY/SELL selection at the same market
opportunities.

This is a diagnostic test, not a strategy optimization.

## Data

Instrument: EUR_USD

Primary diagnostic sample:
- Discovery + Validation combined.
- Final TEST holdout remains sealed and MUST NOT be used to design,
  modify, or interpret the test procedure.

Limitation:
- Validation has already been inspected during previous diagnostics.
- Therefore this experiment is diagnostic evidence, not a pristine
  confirmatory statistical test.

## Opportunity timestamps

Use the same historical timestamps at which the frozen strategy generated
an actionable BUY or SELL opportunity.

Do not add arbitrary bars or search for more favorable timestamps.

## Counterfactual construction

At every opportunity timestamp, independently reconstruct:

- BUY hypothesis
- SELL hypothesis

using the historical information available at that timestamp only.

Each side MUST use the existing strategy machinery independently, including:

- ATR risk
- structural risk
- swing-derived risk
- structural barriers
- stop
- target
- minimum RR geometry

Do NOT mirror the original trade's risk onto the opposite direction.

No future information may participate in counterfactual construction.

## Execution

Resolve both counterfactuals using the same frozen historical execution model:

- historical bid/ask candles
- spread
- entry slippage
- exit slippage
- latency configuration
- identical horizon
- TIMEOUT explicit
- AMBIGUOUS explicit
- DATA_INSUFFICIENT explicit
- ENTRY_INVALIDATED explicit

No execution parameter may be changed for this experiment.

## Paired primary sample

The formal directional randomization test uses episodes for which BUY and SELL
produce outcomes that are comparable under the pre-existing outcome semantics.

Report explicitly:

- total opportunities
- both sides comparable
- BUY-only valid
- SELL-only valid
- neither comparable
- TIMEOUT counts
- AMBIGUOUS counts
- invalid/data-integrity states

Do not silently discard non-comparable observations.

## Null distribution

For every paired comparable episode, randomly choose BUY or SELL with
probability 0.5.

Generate:

- 20,000 Monte Carlo random strategies
- fixed RNG seed for reproducibility

This is a high-precision Monte Carlo approximation, not an exact enumeration.

For every simulation calculate expectancy_r using the same outcome semantics
as the bot.

## Condition 1 — Directional skill

Pre-registered one-sided threshold:

The bot's expectancy must exceed the 95th percentile of the Monte Carlo
null distribution.

Also report:

- null mean
- null median
- 95th percentile
- bot expectancy
- exact empirical percentile/rank of bot within the simulated null

Failure condition:

If bot expectancy does not exceed the pre-registered 95th percentile,
the hypothesis that the current directional selector demonstrates directional
skill is NOT supported.

## One-sided-valid robustness diagnostic

Episodes where only BUY or only SELL is valid are excluded from the formal
paired Monte Carlo hypothesis test.

Report separately, descriptively:

- number of one-sided-valid episodes
- fraction where the bot selected the uniquely valid side

This is a robustness diagnostic only and has no additional significance
threshold.

## Condition 2 — Economic edge

Calculate the bot's net expectancy after the frozen realistic execution costs.

Use a bootstrap confidence interval for mean expectancy:

- 90% CI
- fixed RNG seed
- bootstrap procedure fixed before observing results

Economic-edge condition:

The LOWER bound of the 90% bootstrap CI must be > 0R.

A positive point estimate alone is insufficient.

## Interpretation matrix

Condition 1 PASS + Condition 2 PASS:
Directional skill detected and economically positive evidence exists.

Condition 1 PASS + Condition 2 FAIL:
Directional information may exist, but it is insufficient to demonstrate
positive net edge after execution costs.

Condition 1 FAIL:
Current directional selector has not demonstrated skill beyond the
pre-registered random-direction null. Do not respond by tuning its thresholds
against this sample.

## Oracle diagnostic

An ex-post oracle may also select the better BUY/SELL result at each paired
episode.

This is NOT a strategy and NOT evidence of tradable skill.

Report oracle expectancy only as an estimate of the amount of directional
opportunity available at the sampled timestamps.

A merely positive oracle expectancy is not itself considered meaningful
evidence.

## Research integrity

No scoring weights, thresholds, execution assumptions, episode definitions,
or sample boundaries may be changed after observing this test in order to
improve its result.

The final TEST holdout remains untouched.


## Test B — Extended shadow directional diagnostic

This is a NEW, explicitly separate research-only diagnostic added after Test A.
It does not alter, rescue, reinterpret, or replace the frozen Test A result.

Purpose:
- Increase directional-comparison coverage without changing production gates.
- Determine whether directional information remains detectable when the opposite
  side was rejected exclusively by `minimum_rr` and/or `barrier_room_ok`.

Construction:
- Preserve the exact BUY/SELL hypothesis produced by `_direction_hypothesis`.
- Preserve its original entry, stop, target, risk, structural calculations,
  execution assumptions, horizon, spread, slippage, and latency.
- For Test B resolution only, ignore exactly two safety checks:
  `minimum_rr` and `barrier_room_ok`.
- No target is moved or reconstructed to satisfy minimum RR.
- No stop or risk is modified.
- Every other frozen safety requirement remains mandatory.
- Production decisions remain unchanged.

Reporting:
- Test A remains reported independently.
- Test B has its own natural sample size; no target n is required or pursued.
- TIMEOUT, AMBIGUOUS, ENTRY_INVALIDATED and other non-comparable outcomes remain
  non-comparable and are not converted into WIN/LOSS.
- Report bot expectancy, opposite-side shadow expectancy, their difference,
  null mean/median/P95, empirical percentile, bootstrap 90% CI, and ex-post oracle.

Pre-registered Test B thresholds:
- 20,000 Monte Carlo randomizations, one-sided P95 threshold.
- Condition 1 PASS only if bot expectancy > null P95.
- 20,000 bootstrap samples with 90% CI.
- Condition 2 PASS only if the lower bootstrap CI bound > 0R.

Decision rule:
- If Test B does not exceed null P95, current directional-selector skill is not
  supported by the extended diagnostic; do not tune selector thresholds to this sample.
- If Test B exceeds P95 but fails the economic condition, directional information
  may exist but positive net edge is not demonstrated.
- If both conditions pass, investigate the source and stability of the edge before
  any production change.

The final TEST holdout remains sealed.

## Neither-valid group

Episodes where neither side satisfies the original frozen geometry remain outside
Test B. They may be characterized later only under a separately pre-registered
Test C; they are not forced into the present experiment.


## Post-Test-B component diagnostic
Descriptive only. Records frozen BUY/SELL evidence on Test-B-comparable episodes. No weights, thresholds, gates, execution assumptions, or sample boundaries change. Final TEST holdout remains sealed.


## E1 evidence-size classification (fixed before analysis)
The effective E1 sample size is classified with fixed thresholds that must not be adjusted after seeing results:
- `n < 15`: `UNDERPOWERED`
- `15 <= n < 30`: `WEAK_LIMITED_EVIDENCE`
- `n >= 30`: `USABLE`

## E1 mechanically conditioned component exclusion
`m1_score_contribution` is not part of the E1/BH-FDR family when the analytical population is conditioned on M1 confirmation of the selected/actionable side. Under that construction it is mechanically conditioned by the chosen-side definition and therefore is not statistically independent directional evidence. The exclusion must remain explicit; no post-hoc substitute is introduced to improve apparent results.
