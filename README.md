# Market Alert V2.9 — Adaptive Risk Engine

Mantiene el motor dual BUY/SELL.

- TP inicial mínimo: 7 pips.
- R:R inicial mínimo: 1.5:1.
- SL adaptativo: estructura + ATR M1 + ATR M5.
- Piso de seguridad del SL: 3 pips, para evitar stops microscópicos.
- Si una barrera fuerte no deja espacio para alcanzar el objetivo completo, no opera.
- Una barrera no puede reducir el TP por debajo de 7 pips ni de 1.5R.
- El estado muestra stop_pips y target_pips para auditoría.

OANDA PRACTICE ONLY.


## V2.9 — Closed-loop learning
- M1 confirmation is now an execution trigger; BUY/SELL hypotheses without it are still recorded for learning but not executed.
- Duplicate snapshots from the same M1 candle/direction no longer inflate the signal/learning sample set.
- Strong barriers are hard room vetoes only while genuinely unbroken; confirmed broken levels are already skipped by structural context.
- When new outcomes resolve and the labeled sample threshold is reached, model retraining is attempted immediately instead of waiting only for the hourly maintenance cycle.
- Learning telemetry now exposes pending, ambiguous, timeout counts and whether the configured DB path is the recommended persistent `/data` volume.
- Existing V2.8 adaptive risk rules remain: minimum 7-pip target, dynamic stop, minimum 1.5R, dual BUY/SELL hypotheses, contextual barriers, trend runner, and practice-only execution.
