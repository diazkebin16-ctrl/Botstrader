# Market Alert V1.9 — Supervised 24/7 Worker

Corrige el fallo de V1.8 donde Railway podía seguir ONLINE mientras el scanner quedaba detenido en `cycles: 1`.

- Mantiene referencia explícita a las tareas asyncio.
- Supervisor reinicia el worker si se cae.
- Watchdog comprueba el heartbeat cada 30 s.
- Si pasan más de 180 s sin un scan, cancela y reinicia el scanner.
- `/health` deja de reportar `ok: true` cuando el scanner está estancado.
- `/api/status` muestra `cycles`, `successful_cycles`, `worker_restarts` y edad del último scan.
- OANDA PRACTICE ONLY, sin endpoint live.
- Conserva auto-trade demo, aprendizaje, confianza adaptativa y auditoría de ejecución.

Después de desplegar, `cycles` debe aumentar aproximadamente cada minuto.
