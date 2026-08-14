# Market Alert V2.5 — Structural Room

Correcciones sobre V2.4:

- R:R mínimo real **1.5:1**.
- Ejemplo: stop de 5 pips => el mercado debe ofrecer al menos 7.5 pips de recorrido útil.
- Antes de ejecutar, busca la resistencia/soporte confirmado más cercano usando **H1 y M15**.
- Si esa barrera aparece antes de completar 1.5R, **no ejecuta**.
- El Take Profit nunca se coloca detrás de una barrera estructural conocida; queda ligeramente antes.
- Se conserva el filtro anti-entrada-tardía de V2.4.
- No existe espera fija de 15/30 minutos.
- Se corrigió Trend Runner: el `managed_target` es ahora el Take Profit que realmente se envía a OANDA Practice.
- Incluso Trend Runner respeta la resistencia/soporte estructural y no extiende el TP más allá de ella.
- Break-even, profit lock y trailing siguen activos.
- **OANDA PRACTICE ONLY.**
