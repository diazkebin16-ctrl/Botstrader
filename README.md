# Market Alert V3.16 — Controlled Canary Deployment

## Architecture

Existing production/scanner path remains on **OANDA Practice**.

Candidate deployment is isolated behind a separate canary account:

`Trade Memory → Adaptive Learning → Candidate → Validation → Paper → Deployment Manager → Canary`

The candidate can never bypass the Deployment Manager or the existing Adaptive Risk Engine.

## States

`READY_FOR_REVIEW → APPROVED_FOR_CANARY → CANARY_LIVE → LIMITED_PRODUCTION → FULL_PRODUCTION_ELIGIBLE`

Failure/control states:

`CANARY_PAUSED`, `ROLLED_BACK`, `CANARY_REJECTED`.

`FULL_PRODUCTION_ELIGIBLE` is only an eligibility state. V3.16 deliberately does **not**
replace the blue/current production strategy and does not automatically allocate 100%.

## Explicit live enablement

Candidate live execution defaults to OFF. Real canary orders require all of:

- `DEPLOYMENT_LIVE_EXECUTION_ENABLED=true`
- `CANARY_OANDA_ENV=live`
- `OANDA_CANARY_ACCOUNT_ID`
- `OANDA_CANARY_TOKEN`
- explicit `approve-canary`
- explicit `start-canary`
- live account health check

The normal scanner account remains on the existing practice endpoint.

## Gradual allocation

The controlled ladder is:

`5% → 10% → 25% → 50% → FULL_PRODUCTION_ELIGIBLE`

Every increase requires an explicit promotion request and a passing promotion gate.

## Promotion gates

They require:

- minimum live trades
- minimum live days
- minimum regimes observed
- positive expectancy
- profit factor >= 1.10
- acceptable stability
- no operational errors
- no live divergence
- promotion cooldown
- maximum promotions per seven days
- bounded exposure increase
- Risk Engine approval

Failure returns `HOLD_CURRENT_LEVEL`.

## Risk authority

For Candidate live trades the existing Adaptive Risk Engine is recomputed using the canary
account health context. `BLOCK` or `EMERGENCY_STOP` is a hard veto. Candidate sizing can only
scale **downward** from configured base units.

## Failure-safe behavior

Missing regime, missing/paused Director recommendation, Risk Engine failure/block, corrupt data,
broker health failure, restart recovery hold, or kill switch all block new Candidate orders.

A system-level kill switch also blocks new production orders.

## Rollback

Rollback:

- immediately disables new Candidate trades
- sets Candidate allocation to zero
- preserves the previous production version
- never deletes the Candidate version
- does not blindly market-close open Candidate positions
- existing positions retain broker-side SL/TP and continue reconciliation

## Restart recovery

Deployment state persists in SQLite. Any Candidate that was `CANARY_LIVE` or
`LIMITED_PRODUCTION` is restored with:

- `resume_required=1`
- `new_trades_enabled=0`

An explicit health-checked `/resume` is required before new orders resume.

## Kill switches

Scopes:

- `SYSTEM`
- `ALL_CANDIDATES`
- `CANDIDATE:<candidate_id>`

## API

- `GET /api/deployment`
- `GET /api/deployment/{candidate_id}`
- `POST /api/deployment/{candidate_id}/approve-canary`
- `POST /api/deployment/{candidate_id}/start-canary`
- `POST /api/deployment/{candidate_id}/resume`
- `POST /api/deployment/{candidate_id}/promote`
- `POST /api/deployment/{candidate_id}/pause`
- `POST /api/deployment/{candidate_id}/rollback`
- `POST /api/deployment/kill-switch`
- `POST /api/deployment/reconcile`

Capital preservation has priority over exposure growth. There is no automatic promotion.
