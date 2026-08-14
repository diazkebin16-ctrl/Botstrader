# Market Alert V2.8 — Adaptive Risk Engine

Mantiene el motor dual BUY/SELL.

- TP inicial mínimo: 7 pips.
- R:R inicial mínimo: 1.5:1.
- SL adaptativo: estructura + ATR M1 + ATR M5.
- Piso de seguridad del SL: 3 pips, para evitar stops microscópicos.
- Si una barrera fuerte no deja espacio para alcanzar el objetivo completo, no opera.
- Una barrera no puede reducir el TP por debajo de 7 pips ni de 1.5R.
- El estado muestra stop_pips y target_pips para auditoría.

OANDA PRACTICE ONLY.
