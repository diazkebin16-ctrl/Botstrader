# Market Alert V2.6 — Contextual Structure

La estructura deja de ser una regla binaria.

## Soporte y resistencia
- Cada nivel recibe un **score de importancia** según temporalidad, número de reacciones y proximidad.
- Nivel débil: no bloquea; solo resta un poco de confianza.
- Nivel medio: tampoco bloquea; resta más confianza.
- Nivel fuerte: puede bloquear si no deja al menos 1.5R de recorrido real.
- Un nivel roto no se considera barrera activa cuando la ruptura tiene cierre suficiente y confirmación posterior.
- Si una resistencia/soporte ya fue rota con confirmación, el bot busca la **siguiente barrera relevante**.

## Breakout
- Una mecha aislada no cuenta como ruptura.
- Se requiere cierre más allá del nivel por un margen escalado con ATR.
- La continuación y el retest exitoso aumentan la validez de la ruptura.

## Entrada
- Se mantiene R:R mínimo 1.5R frente a barreras fuertes.
- Se mantiene el filtro anti-entrada-tardía.
- No hay cooldown fijo.

## Gestión
- Break-even, profit lock, trailing y Trend Runner continúan activos.
- Trend Runner solo se limita por una barrera realmente fuerte y no rota.
- OANDA PRACTICE ONLY.
