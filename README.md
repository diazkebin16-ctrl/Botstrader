# Market Alert V2.1 — Adaptive Bootstrap Learning

Esta versión corrige el problema observado en V2.0 donde la confianza bootstrap permanecía en 50% y no permitía empezar a generar operaciones demo.

## Nuevo arranque adaptativo
Antes de tener suficientes operaciones resueltas, el sistema calcula una confianza provisional usando evidencia ponderada:

- contexto y fuerza M15;
- estructura M5;
- primer/segundo retroceso;
- confirmación M1;
- alineación de momentum;
- R:R;
- extensión;
- régimen de volatilidad;
- sesión;
- noticias;
- Quality Score como evidencia secundaria.

Estos factores **no tienen que cumplirse todos**.

La confianza bootstrap empieza alrededor de 50%, puede bajar si hay evidencia negativa y puede subir hasta un máximo conservador de 78%. Por tanto, una oportunidad convincente puede superar el umbral de ejecución de 65% incluso antes de que existan 60-100 muestras.

## Transición hacia aprendizaje real
- Desde 20 resultados resueltos, la confianza provisional empieza a mezclarse gradualmente con el win rate observado.
- A partir de 60 muestras, el motor empírico toma el control.
- A partir de 100 muestras, los patrones descubiertos pueden validarse y recibir peso.
- El ML sigue siendo secundario.

## Seguridad
Solo siguen como vetos absolutos:
- dirección válida;
- precios finitos;
- riesgo positivo;
- R:R mínimo;
- volatilidad extrema/no razonable.

Sigue siendo **OANDA PRACTICE ONLY**.

## Qué debes observar
En las decisiones ya no debería aparecer siempre:
`confianza 50.0%`

Podrás ver valores distintos, por ejemplo 58%, 64%, 69%, etc., según la evidencia del setup.

Si `dynamic_confidence >= required_confidence` y no hay Safety veto, AUTO_TRADE puede ejecutar la orden en Practice.
