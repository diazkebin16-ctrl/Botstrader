# Market Alert V2.7 — Dual Hypothesis

Esta versión elimina el bloqueo direccional de M15.

## Cambio principal
En cada ciclo se evalúan **dos hipótesis independientes**:
- BUY score
- SELL score

H1 y M15 siguen teniendo mucho peso, pero son evidencia, no un interruptor que impide estudiar el lado contrario.

## Transición de tendencia
- M1 por sí solo no fuerza una reversión.
- M5 y M1 pueden empezar a elevar el escenario contrario aunque H1/M15 aún conserven la tendencia previa.
- Si BUY y SELL quedan demasiado cerca, el sistema espera.
- Una operación completamente contra H1 y M15 exige un score excepcionalmente alto.
- Esto permite detectar gradualmente una transición sin confundir un simple retroceso con un cambio de tendencia.

## Estructura y gestión
Se conserva V2.6:
- soportes/resistencias débiles reducen confianza, no bloquean;
- barreras fuertes pueden bloquear si no dejan 1.5R;
- rupturas confirmadas dejan de ser barreras activas;
- Trend Runner, break-even, profit lock y trailing permanecen;
- OANDA PRACTICE ONLY.

## Diagnóstico
El estado ahora expone `buy_score`, `sell_score`, `direction_edge`, `direction_state` e `hypotheses` para poder ver qué estaba pensando en ambos sentidos.


## V2.7.1 — Corrección crítica
Se corrige el error `no such table: decisions`.

El filtro de reentrada estaba consultando una tabla inexistente llamada `decisions`.
La tabla real creada por el proyecto es `decision_log`.

No se cambió ninguna regla de trading, confianza, BUY/SELL, estructura, R:R,
Trend Runner, break-even ni trailing. Esta versión solo corrige ese bug.
