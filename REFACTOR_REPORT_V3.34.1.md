# Refactor Report — v3.34.1 Cost Accounting Guardrail

## Finding reviewed
An audit suggested subtracting `cost_r` from nominal WIN/LOSS R after `resolve_outcome` had already shifted TP/SL against the trade. That would count the same transaction cost twice.

## Change
- Added `economic_realized_r()` to make the accounting identity explicit.
- Historical replay now derives realized R from the effective barrier and subtracts `cost_r` exactly once.
- Added regression tests for cost-aware WIN and LOSS accounting.
- Updated runtime version to 3.34.1.

## Invariant
With the current shifted-barrier model:
- WIN: gross midpoint move to effective TP = nominal RR + cost_r; net = nominal RR.
- LOSS: gross midpoint move to effective SL = -(1 - cost_r); net = -1R.

Therefore `rr - cost_r` / `-(1 + cost_r)` would be a double charge unless the resolver is changed to use unshifted barriers.
