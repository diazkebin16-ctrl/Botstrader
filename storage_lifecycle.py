from __future__ import annotations

from dataclasses import dataclass
from typing import Dict
import sqlite3


@dataclass(frozen=True)
class RetentionPolicy:
    ensemble_signals: int = 50000
    ensemble_alerts: int = 10000
    ensemble_outputs: int = 20000
    observability_recovered_alerts: int = 5000
    external_research_observations: int = 50000


class StorageLifecycleManager:
    """Bound non-authoritative high-cardinality telemetry/research storage.

    This manager only deletes old shadow/research/observability rows. It never
    touches recovery, production, security, governance, live-trade, or order
    state tables. Deletion frees SQLite pages for reuse; physical compaction is
    an explicit offline maintenance operation, not a runtime side effect.
    """

    def __init__(self, db_path: str, policy: RetentionPolicy | None = None):
        self.db_path = db_path
        self.policy = policy or RetentionPolicy()

    def conn(self):
        c = sqlite3.connect(self.db_path, timeout=30)
        c.row_factory = sqlite3.Row
        c.execute("PRAGMA journal_mode=WAL")
        c.execute("PRAGMA busy_timeout=5000")
        return c

    @staticmethod
    def _exists(c, table: str) -> bool:
        return c.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone() is not None

    def _cap_by_rowid(self, c, table: str, keep: int, order_col: str) -> int:
        if keep <= 0 or not self._exists(c, table):
            return 0
        cur = c.execute(
            f'''DELETE FROM "{table}" WHERE rowid IN (
                    SELECT rowid FROM "{table}" ORDER BY "{order_col}" DESC, rowid DESC LIMIT -1 OFFSET ?
                )''',
            (int(keep),),
        )
        return max(0, int(cur.rowcount or 0))

    def prune(self) -> Dict[str, int]:
        c = self.conn()
        deleted: Dict[str, int] = {}
        try:
            c.execute("BEGIN IMMEDIATE")
            deleted["ensemble_signals"] = self._cap_by_rowid(c, "ensemble_signals", self.policy.ensemble_signals, "ts")
            deleted["ensemble_alerts"] = self._cap_by_rowid(c, "ensemble_alerts", self.policy.ensemble_alerts, "id")
            deleted["ensemble_outputs"] = self._cap_by_rowid(c, "ensemble_outputs", self.policy.ensemble_outputs, "ts")
            deleted["external_research_observations"] = self._cap_by_rowid(
                c, "external_research_observations", self.policy.external_research_observations, "id"
            )
            if self._exists(c, "observability_alerts"):
                cur = c.execute(
                    '''DELETE FROM observability_alerts
                       WHERE status!='ACTIVE' AND rowid IN (
                         SELECT rowid FROM observability_alerts
                         WHERE status!='ACTIVE'
                         ORDER BY last_seen DESC, rowid DESC LIMIT -1 OFFSET ?
                       )''',
                    (int(self.policy.observability_recovered_alerts),),
                )
                deleted["observability_alerts"] = max(0, int(cur.rowcount or 0))
            else:
                deleted["observability_alerts"] = 0
            c.commit()
        except Exception:
            c.rollback()
            raise
        finally:
            c.close()
        return deleted
