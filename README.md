# Market Alert V3.7 — Parallel Filter Evolution

OANDA PRACTICE ONLY.

## Estado
Implementado y probado localmente. Pendiente de validación con datos reales en Railway.

## Mejoras
- Investiga muchas hipótesis en paralelo; cada filtro conserva su propio historial/evidencia.
- Puede activar varios filtros aprendidos a la vez si son compatibles.
- No hay máximo numérico fijo: compatibilidad, cobertura y evidencia limitan el crecimiento.
- Detecta contradicciones lógicas y mide historial conjunto antes de combinar reglas.
- Cada filtro activo tiene su propio ciclo de revisión de 50 evidencias canónicas.
- Si pasa el primer bloque, queda CONFIRMED.
- Sigue revisándose en bloques posteriores de 50; si deja de rendir, se revierte individualmente.
- Retirar un filtro no afecta a los demás filtros sanos.
- Las combinaciones quedan registradas en `research_rule_compatibility`.

## Seguridad
Los filtros base de seguridad siguen siendo inmutables.
La auto-evolución solo puede añadir/quitar filtros aprendidos veto-only.
No puede relajar Safety, R:R mínimo, TP mínimo, stop ni forzar BUY/SELL.

## Endpoints
- GET `/api/research/active-rule`
- GET `/api/research/compatibility`
- POST `/api/research/promote`
- POST `/api/research/review-active`
