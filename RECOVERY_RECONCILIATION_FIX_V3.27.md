# V3.27 — Recovery / reconciliation integrity fix

## Incident addressed
A local Trade Memory / active-management row (example trade `95`) could remain `OPEN` after the OANDA Practice account no longer exposed that trade. The old Trade Memory reconciliation retried `/trades/{id}`, received HTTP 404, and left the active row untouched. Recovery then correctly detected an internal-only position and stayed permanently in `SAFE_MODE` / `RECONCILIATION_REQUIRED`.

## Complete correction
- Recovery now has an explicit **practice-only orphan quarantine** path.
- A broker-missing local trade is quarantined only when there is no unresolved submission/order intent for that broker trade id.
- Quarantine closes **active management only** and marks Trade Memory as `BROKER_MISSING`, never `CLOSED`.
- No exit price, P/L, win/loss, or learning label is fabricated.
- Production/live semantics remain fail-closed: missing broker trades continue to produce `RECONCILIATION_REQUIRED` and `SAFE_MODE`.
- After a practice orphan is quarantined, reconciliation reports `MINOR_MISMATCH`; the existing risk verification gate is still required before SAFE_MODE can exit.

## Persistence correction
Database/model paths now resolve in this order:
1. explicit `DB_PATH` / `MODEL_PATH`;
2. `RAILWAY_VOLUME_MOUNT_PATH` when mounted;
3. `/data` when mounted;
4. local ephemeral path as a last fallback.

Learning diagnostics now report the resolved persistence state instead of assuming only `/data` can be persistent.

## Configuration
`RECOVERY_PRACTICE_ORPHAN_QUARANTINE=true` is enabled by default but is effective only when `PRIMARY_OANDA_ENV=practice`.

For Railway, mount a persistent volume and set its mount path (recommended `/data`) or set `DB_PATH` explicitly to a file inside that volume.

## Verification
- Python compilation: PASS
- Full pytest suite: **151 passed**
- Added regression coverage proving:
  - practice orphan -> quarantined, excluded from learning, no fabricated close;
  - live semantics -> remains reconciliation-required / SAFE_MODE.
