# Automation V3 Remote Runner

Automation V3 runs on demand in GitHub Actions. The continuous Railway `Botstrader` service remains unchanged and does not execute research workloads.

## Authority boundary

- Research, governed code changes, tests, merge, and PAPER deployment may be automated.
- `BOTS_V3_PRODUCTION_AUTHORITY` is always `false`.
- `TRADING_ENVIRONMENT` must be `PAPER`.
- `PRIMARY_OANDA_ENV` must be `practice`.
- The only allowed OANDA endpoint for the adapters is `https://api-fxpractice.oanda.com`.
- There is no LIVE fallback and no rollback adapter is configured unless a safe Railway prior-deployment restore primitive is added later.

## One-time setup

Create one repository Actions secret:

- `RAILWAY_TOKEN` — a least-privilege Railway project token for `captivating-curiosity`.

The workflow uses the ephemeral GitHub Actions `GITHUB_TOKEN` with `contents: write`; no persistent GitHub PAT is required. OANDA Practice credentials are read at runtime from the existing Railway service variables, masked immediately, and are never committed.

If repository policy globally blocks write-enabled `GITHUB_TOKEN`, enable Actions read/write workflow permissions once for this repository. Do not create a PAT unless repository policy makes the ephemeral token impossible.

## Phone trigger

After this branch is reviewed and merged to `main`:

1. Open GitHub Actions for `Automation V3 Remote Optimizer` from the phone.
2. Choose **Run workflow**.
3. Select one supported instrument only: `AUD_USD`, `EUR_USD`, `GBP_USD`, `USD_JPY`, or `USD_CAD`.
4. Start the workflow.

The same workflow also supports authenticated `repository_dispatch` event type `automation-v3-optimize` with `client_payload.instrument`, so a trusted ChatGPT/GitHub orchestration layer can trigger it without exposing a public command endpoint.

## Persistence and status

Research state and historical caches are restored/saved with GitHub Actions cache under an instrument-specific key. Governed JSON evidence and compact status are uploaded as workflow artifacts; raw `data/` caches are excluded from artifacts and are never committed to Git.

Compact status fields include:

- `run_id`
- `instrument`
- `current_stage`
- `lookback`
- `terminal_state`
- `candidate`
- `paper_deployment_status`
- `last_error`
- `production_authority=false`

The same status JSON is written into the GitHub Actions job summary for phone-readable inspection.

## Code-change adapter contract

`automation_v3_code_change_adapter.py` does not convert free-form research prose into source code. It applies only explicit `code_changes` from `paper_release_plan.json`, using `replace_text` operations bound to an exact pre-edit SHA-256. Protected LIVE files, secret/transient files, LIVE markers, and secret markers are rejected.

If V3 produces a research candidate but no executable `code_changes`, the adapter exits nonzero. This is intentional fail-closed behavior; it never fabricates an implementation.

## PAPER deployment

The Railway adapter deploys the exact clean Git checkout using `railway up` to the existing `Botstrader` service. It records the new deployment ID and then verifies:

- tracked deployment reaches `SUCCESS`;
- local checkout still matches the expected SHA and is clean;
- Railway service variables remain PAPER / OANDA Practice;
- requested instrument is enabled;
- recent runtime logs contain no startup traceback/failure;
- the Railway service domain responds without HTTP 5xx;
- OANDA Practice account summary succeeds with the existing Railway credentials.

No deployment is executed merely by installing this remote runner. The first real asset optimization remains a separate acceptance run after IA #1 review.
