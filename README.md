# Market Alert V2.4 — Entry Timing

Mantiene todo lo incorporado en V2.3 y añade control de calidad de entrada.

## Cambios nuevos
- Mantiene R:R mínimo de 1.5:1.
- Evita perseguir el precio después de un impulso ya extendido.
- Si el precio está demasiado extendido (>1.20 ATR por defecto), espera retroceso o una nueva estructura.
- En extensiones intermedias exige evidencia/confianza mayor.
- Puede rechazar una entrada si el espacio disponible hasta soporte/resistencia no permite al menos 1.5R.
- Evita reentrar en la misma vela después de una operación.
- **No existe cooldown fijo de 15 ni 30 minutos.** Si aparece una oportunidad nueva y válida, puede tomarla.

## Gestión heredada de V2.3
- Break-even alrededor de +1R.
- Protección de beneficio alrededor de +1.5R.
- Trailing y Trend Runner para movimientos fuertes.
- OANDA PRACTICE ONLY.
