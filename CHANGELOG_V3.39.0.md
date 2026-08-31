# BotsTrader V3.39.0

- Adds isolated USDJPY_PHASE2_FORWARD_V1 to the existing per-instrument PAPER/Practice forward-experiment mechanism.
- USD/JPY Phase-1 strategic openings: bypass canonical M1 confirmation and QUALITY:EXTENSION only while the USD/JPY experiment is active.
- Frozen Phase-2 admission rule: chosen_legacy_score >= 33.0. BUY uses legacy V331 BUY score; SELL uses legacy V331 SELL score.
- Adds attribution fields for experiment identity, chosen score, threshold/pass, M1/extension bypass state, time-gate snapshot, Safety, strategic eligibility, execution/rejection reason.
- Persists pre-entry experiment identity/gate result in trade memory entry context.
- No changes to Safety, risk, leverage, global time gates, recovery, post-fill protection, trade management, execution idempotency, or broker protections.
