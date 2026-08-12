# Market Alert V1.8 — Validation & Execution Audit

Preparada para prueba prolongada en OANDA Practice.

- Validación ML temporal walk-forward (`TimeSeriesSplit`).
- Calibración por bandas: confianza predicha vs win rate observado.
- Brier score para evaluar probabilidades.
- Win rate por umbral de confianza.
- Precio esperado vs fill real y slippage en pips.
- Verificación posterior de Stop Loss y Take Profit; `PROTECTION_ERROR` si falta alguno.
- Salud de estrategia: win rate, Profit Factor en R, expectancy y setups más/menos efectivos.
- Mantiene aprendizaje y confianza adaptativa.
- Sin límite diario de operaciones.
- OANDA PRACTICE ONLY.

Nuevos endpoints:
- `/api/health/strategy`
- `/api/health/thresholds`
- `/api/execution-audit`
