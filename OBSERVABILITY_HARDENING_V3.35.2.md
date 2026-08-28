# BotsTrader V3.35.2 — Observability Hardening

Scope is intentionally limited to observability lifecycle and shadow-ML governance. No strategy thresholds, risk limits, leverage, order authority, minimum_rr, or execution policy are changed.

- Recover stale startup-security alerts after a verified SECURITY_READY startup.
- Recover missed-heartbeat alerts as soon as liveness is demonstrably fresh.
- Central Ensemble alert identity is stable by event + symbol, not decision UUID.
- Weekend/market-closed stale/abstention conditions do not remain active operational alerts.
- Engine-local Ensemble alert history is rate-limited and stores a compact diagnostic summary.
- Shadow ML artifacts are gated by fixed validation rules: ROC AUC > 0.5 and accuracy >= majority baseline. Rejected candidates do not overwrite the active artifact.
- Existing historical data is not deleted by this change.
