# V3.27 Railway persistence contract

The bot can detect and use persistent storage, but application code cannot create a Railway Volume.

Required for learning/model history to survive redeploys:

1. Attach a persistent Railway Volume to the Botstrader service.
2. Mount it at `/data` (recommended), or use another mount path and set `PERSISTENT_STORAGE_PATH` to that path.
3. Recommended runtime variables when mounted at `/data`:
   - `PERSISTENT_STORAGE_PATH=/data`
   - `DB_PATH=/data/market_alert.db`
   - `MODEL_PATH=/data/market_alert_model.joblib`
4. Keep `PERSISTENCE_REQUIRED=false` while using OANDA Practice if you want the bot to remain available even when a volume is absent.
5. For any future production deployment, set `PERSISTENCE_REQUIRED=true`; startup then fails closed if durable storage is not actually mounted.

Verification endpoints:
- `/api/storage` must report `status: PERSISTENT`, `persistent: true`, `db_persistent: true`, and `model_persistent: true`.
- `/api/learning` must report `persistent_db_configured: true` and `persistent_db_recommended: false`.

Do not create a normal `/data` directory inside the container and assume it is durable. The code intentionally refuses to label storage persistent unless a configured/mounted persistent base was detected before DB/model initialization.
