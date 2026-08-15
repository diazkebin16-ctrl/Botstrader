# Production Operator Runbook — V3.23

## General rule
Capital preservation overrides availability and promotion. When state is uncertain: **no new real orders**.

## Broker disconnect
1. Confirm `PRODUCTION_SUSPENDED` / Recovery SAFE_MODE.
2. Do not resend UNKNOWN orders.
3. Reconnect, fetch account/positions/open orders/recent fills.
4. Reconcile broker vs internal vs Trade Memory.
5. Verify Risk Engine and protective orders.
6. Resume only through the Production Resume Gate, which restarts at MINIMAL_LIVE.

## Market data failure
Block new entries. Verify freshness/timestamps/feed integrity. Do not interpret no data as no movement. Resume only after fresh data and health checks.

## Unknown order
Mark `ORDER_STATUS_UNKNOWN`; never duplicate-resubmit. Query broker/reconcile. Use confirmed broker state as source of truth.

## Position mismatch
Classify as P0 if material or uncontrolled. Suspend production, reconcile quantities/average entry/protections, recalculate risk, document root cause.

## Emergency Stop
Emergency Stop blocks new entries and survives restart. Reset requires authorized recovery + health checks; reset never implies full production resume.

## Drawdown breach
Risk containment first. Suspend or downgrade according to deterministic limits. Do not increase size to recover losses.

## System CRITICAL
Freeze adaptation, suspend production if capital/state integrity is uncertain, reconcile, recover infrastructure, then re-evaluate.

## Release rollback
Rollback code/config to LAST_KNOWN_GOOD through Change Management. Do **not** roll back financial state. Reconcile live positions after software rollback.

## Production Suspension
Continue position monitoring, Risk Engine, reconciliation and observability. Promotions remain blocked. Resume sequence: incident resolved → reconciliation → health check → MINIMAL_LIVE → observation.

## Required evidence before promotion
Minimum trades + minimum days + execution quality + clean reconciliation + acceptable drawdown + no P0/P1 incidents + healthy System Evaluation + normal Governance + Risk Ready + good data quality. Profit alone is never sufficient.
