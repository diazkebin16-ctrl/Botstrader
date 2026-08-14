# Market Alert V3.8 — Weekend Context Research

OANDA PRACTICE ONLY.

Implementado y probado localmente; pendiente de Railway.

Durante el cierre Forex el investigador recopila contexto/noticias por hora sin crear WIN/LOSS falsos. Al reabrir congela el resumen, guarda precio de apertura y mide reacción a 1h, 4h, 12h y 24h. Las señales de las primeras 24h reciben contexto de fin de semana y alimentan hipótesis WEEKEND_CONTEXT usando operaciones canónicas y shadow simulations.

Endpoints: GET /api/research/weekends y POST /api/research/weekends/collect.
