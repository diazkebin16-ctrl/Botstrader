# Market Alert V2.0 — Adaptive Discovery

Esta versión cambia el enfoque de decisión para que coincida con el objetivo del sistema:

- **Los factores de mercado ya no son reglas de todo-o-nada.** M5, pullbacks, M1, extensión, sesión y noticias se observan y se registran como evidencia.
- **Umbral base de ejecución: 65%** (`EXECUTION_MIN_CONFIDENCE=0.65`).
- **Solo quedan vetos duros de seguridad de ejecución:** dirección válida, precios finitos, riesgo positivo, RR mínimo y volatilidad no extrema.
- **Descubrimiento de patrones:** el bot genera regímenes e interacciones (sesión×volatilidad, tendencia×momentum, volatilidad×extensión, etc.).
- **Una señal descubierta NO influye por verla 2 o 3 veces.** Requiere al menos **100 resultados resueltos** y una ventaja estadística mínima antes de recibir peso.
- Los patrones validados pueden sumar **o restar** confianza. Si dejan de funcionar, su peso se recalcula y disminuye/cambia.
- El ML sigue siendo secundario y no domina la decisión.
- Se conserva el supervisor/watchdog 24/7 de V1.9.
- Se corrige el error de aprendizaje `no such column: resolved_at` y la compatibilidad del registro de entrenamiento.
- Sigue siendo **OANDA PRACTICE ONLY**.

## Qué mirar después del despliegue

`/health`:
- `ok: true`
- `scanner.worker_running: true`
- `scanner.stale_effective: false`

Panel principal:
- `cycles` y `successful_cycles` deben seguir aumentando.
- `required_confidence` debe mostrar 0.65 salvo penalización automática por mal rendimiento reciente.
- Ya no debe aparecer `Hard filters: m5_structure...`; los rechazos normales deben decir `Adaptive gate`.

`/api/discovery`:
- muestra patrones candidatos y validados.
- `validated=1` solo después de cumplir la muestra mínima y el edge configurado.

## Nota de validación
Los patrones descubiertos son hipótesis estadísticas, no garantías. La validación debe continuar en Practice antes de considerar cualquier uso con dinero real.
