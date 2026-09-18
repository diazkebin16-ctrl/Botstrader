from __future__ import annotations

import os
import shutil
from dataclasses import dataclass
from typing import Any, Dict, Optional
import sqlite3


@dataclass(frozen=True)
class RetentionPolicy:
    ensemble_signals: int = 50000
    ensemble_alerts: int = 10000
    ensemble_outputs: int = 20000
    observability_recovered_alerts: int = 5000
    external_research_observations: int = 50000
    # Per-cycle telemetry that previously had no bound at all. One decision
    # cycle per minute is ~43.8k rows a month for each of these, and the
    # database file is the dominant cost of this deployment: it sets the volume
    # usage and, through the page cache, most of the container's memory.
    #
    # multi_asset_decision_cycles carries five JSON blobs per row, is written
    # by _persist_multi_asset_cycle every cycle and is read by nothing in the
    # codebase; counterfactual_tracker_events is likewise write-only.
    # observability_traces is only ever looked up by correlation/signal id,
    # which is a recent-diagnostics access pattern.
    multi_asset_decision_cycles: int = 5000
    observability_traces: int = 50000
    counterfactual_tracker_events: int = 50000
    # Resolved shadow opportunities feed the counterfactual statistics, and
    # those queries walk the whole history, so this bound is deliberately far
    # looser. OPEN rows are never touched: they are still awaiting an outcome.
    counterfactual_resolved_opportunities: int = 200000


class StorageLifecycleManager:
    """Bound non-authoritative high-cardinality telemetry/research storage.

    This manager only deletes old shadow/research/observability rows. It never
    touches recovery, production, security, governance, live-trade, or order
    state tables. Deletion frees SQLite pages for reuse; returning that space
    to the filesystem is :meth:`compact`, an explicit space-checked operation
    and not a per-cycle side effect.
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

    def _cap_closed_by_rowid(self, c, table: str, keep: int, order_col: str,
                             status_col: str, open_value: str) -> int:
        """Cap a table without ever deleting a row that is still unresolved."""
        if keep <= 0 or not self._exists(c, table):
            return 0
        cur = c.execute(
            f'''DELETE FROM "{table}" WHERE "{status_col}"!=? AND rowid IN (
                    SELECT rowid FROM "{table}" WHERE "{status_col}"!=?
                    ORDER BY "{order_col}" DESC, rowid DESC LIMIT -1 OFFSET ?
                )''',
            (open_value, open_value, int(keep)),
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
            deleted["multi_asset_decision_cycles"] = self._cap_by_rowid(
                c, "multi_asset_decision_cycles", self.policy.multi_asset_decision_cycles, "ts"
            )
            deleted["observability_traces"] = self._cap_by_rowid(
                c, "observability_traces", self.policy.observability_traces, "created_ts"
            )
            deleted["counterfactual_tracker_events"] = self._cap_by_rowid(
                c, "counterfactual_tracker_events", self.policy.counterfactual_tracker_events, "id"
            )
            # Ordered by rowid, not market_time: insertion order is the order we
            # want and it is the one order SQLite can walk without sorting a
            # six-figure table on every pass.
            deleted["counterfactual_opportunities"] = self._cap_closed_by_rowid(
                c, "counterfactual_opportunities", self.policy.counterfactual_resolved_opportunities,
                "rowid", "status", "OPEN",
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

    def storage_report(self) -> Dict[str, Any]:
        """File size, reclaimable pages and free space on the same filesystem."""
        c = self.conn()
        try:
            page_size = int(c.execute("PRAGMA page_size").fetchone()[0])
            page_count = int(c.execute("PRAGMA page_count").fetchone()[0])
            freelist = int(c.execute("PRAGMA freelist_count").fetchone()[0])
        finally:
            c.close()
        directory = os.path.dirname(os.path.abspath(self.db_path)) or "."
        free_bytes: Optional[int]
        try:
            free_bytes = int(shutil.disk_usage(directory).free)
        except OSError:
            free_bytes = None
        return {
            "db_path": self.db_path,
            "page_size": page_size,
            "page_count": page_count,
            "freelist_count": freelist,
            "logical_bytes": page_size * page_count,
            "reclaimable_bytes": page_size * freelist,
            "live_bytes": page_size * max(0, page_count - freelist),
            "free_bytes": free_bytes,
        }

    def compact(self, *, min_reclaim_bytes: int = 256 * 1024 * 1024,
                headroom_bytes: int = 256 * 1024 * 1024) -> Dict[str, Any]:
        """VACUUM, but only when it is both worth doing and safe to do.

        VACUUM rewrites the database into a fresh file before replacing the
        original, so while it runs the volume holds two copies. On a volume
        that is already most of the way full, that is precisely the operation
        that finishes it off. So the free space is measured first and the
        compaction is declined, not attempted, when the room is not there.

        Caller's responsibility: never run this while the market is open. The
        rewrite holds an exclusive lock for as long as copying the live pages
        takes.
        """
        report = self.storage_report()
        reclaimable = int(report["reclaimable_bytes"])
        free = report["free_bytes"]
        required = int(report["live_bytes"]) + int(headroom_bytes)
        if reclaimable < int(min_reclaim_bytes):
            return {"compacted": False, "reason": "NOT_ENOUGH_TO_RECLAIM",
                    "required_free_bytes": required, **report}
        if free is not None and free < required:
            return {"compacted": False, "reason": "INSUFFICIENT_FREE_SPACE",
                    "required_free_bytes": required, **report}
        before = int(report["logical_bytes"])
        c = sqlite3.connect(self.db_path, timeout=120)
        try:
            c.execute("PRAGMA busy_timeout=120000")
            c.execute("VACUUM")
        finally:
            c.close()
        after = self.storage_report()
        return {
            "compacted": True,
            "reason": "OK",
            "required_free_bytes": required,
            "bytes_before": before,
            "bytes_after": int(after["logical_bytes"]),
            "bytes_reclaimed": max(0, before - int(after["logical_bytes"])),
            **after,
        }
