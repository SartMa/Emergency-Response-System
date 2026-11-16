"""SQLite-backed versioned key-value store for one storage node (DESIGN.md §5.2, ADR-04).

Every key has a monotonically increasing `version`:
  * primary writes are conditional (compare-and-set on the version, ADR-06) and bump it by 1;
  * replica writes carry the primary's version and are applied only if newer;
  * deletes are versioned tombstones, so a resync can never bring a deleted key back.

`kind`, `type` and `status` are copied out of the JSON value into indexed columns. That is a
*local secondary index* (DDIA ch. 7): each node can answer "available water resources" for
the keys it holds, and the coordinator merges the answers from all shards (scatter-gather).
"""

import hashlib
import json
import sqlite3
import threading
import time
from dataclasses import dataclass
from pathlib import Path


@dataclass
class Record:
    key: str
    value: dict | None
    version: int
    deleted: bool

    def to_json(self) -> dict:
        return {"key": self.key, "value": self.value, "version": self.version, "deleted": self.deleted}


class VersionMismatch(Exception):
    def __init__(self, current_version: int, exists: bool):
        super().__init__(f"version mismatch (current={current_version}, exists={exists})")
        self.current_version = current_version
        self.exists = exists


def canonical(value: dict | None) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def value_hash(value_text: str) -> str:
    return hashlib.sha1(value_text.encode()).hexdigest()[:16]


def _index_fields(key: str, value: dict | None) -> tuple[str, str | None, str | None]:
    kind = key.split(":", 1)[0]
    if not isinstance(value, dict):
        return kind, None, None
    return kind, value.get("type"), value.get("status")


class KVStore:
    def __init__(self, path: str | Path):
        self.path = str(path)
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._db = sqlite3.connect(self.path, check_same_thread=False, isolation_level=None)
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=NORMAL")
        self._db.execute(
            """CREATE TABLE IF NOT EXISTS kv (
                   key        TEXT PRIMARY KEY,
                   kind       TEXT NOT NULL,
                   type       TEXT,
                   status     TEXT,
                   value      TEXT NOT NULL,
                   version    INTEGER NOT NULL,
                   deleted    INTEGER NOT NULL DEFAULT 0,
                   updated_at REAL NOT NULL)"""
        )
        self._db.execute("CREATE INDEX IF NOT EXISTS idx_kind_type_status ON kv(kind, type, status)")
        # One lock = one writer at a time. Requests run in FastAPI's thread pool, and the
        # read-check-write of a CAS must not interleave with another write to the same key.
        self._lock = threading.Lock()

    def close(self) -> None:
        self._db.close()

    # ---------------------------------------------------------------- reads
    def get(self, key: str, include_deleted: bool = False) -> Record | None:
        with self._lock:
            row = self._db.execute("SELECT value, version, deleted FROM kv WHERE key=?", (key,)).fetchone()
        if row is None or (row[2] and not include_deleted):
            return None
        return Record(key, json.loads(row[0]), row[1], bool(row[2]))

    def query(self, kind: str, type_: str | None = None, status: str | None = None) -> list[Record]:
        sql = "SELECT key, value, version FROM kv WHERE deleted=0 AND kind=?"
        args: list = [kind]
        if type_:
            sql += " AND type=?"
            args.append(type_)
        if status:
            sql += " AND status=?"
            args.append(status)
        with self._lock:
            rows = self._db.execute(sql, args).fetchall()
        return [Record(k, json.loads(v), ver, False) for k, v, ver in rows]

    def digest(self) -> dict[str, list]:
        """{key: [version, value_hash, deleted]} for every key, tombstones included.
        Comparing two digests tells a recovering node exactly which keys differ (anti-entropy)."""
        with self._lock:
            rows = self._db.execute("SELECT key, version, value, deleted FROM kv").fetchall()
        return {k: [ver, value_hash(v), bool(d)] for k, ver, v, d in rows}

    def stats(self) -> dict:
        with self._lock:
            counts = dict(self._db.execute("SELECT kind, COUNT(*) FROM kv WHERE deleted=0 GROUP BY kind").fetchall())
            version_sum = self._db.execute("SELECT COALESCE(SUM(version), 0) FROM kv").fetchone()[0]
        return {"counts": counts, "keys": sum(counts.values()), "version_sum": version_sum}

    # ---------------------------------------------------------------- primary writes
    def put_primary(self, key: str, value: dict, expected_version: int | None) -> int:
        """Conditional write (compare-and-set). Returns the new version.

        expected_version=None -> unconditional (not used by our services, kept for the raw API)
        expected_version=0    -> create: the key must not exist
        expected_version=n    -> update: the stored version must be exactly n
        """
        with self._lock:
            row = self._db.execute("SELECT version, deleted FROM kv WHERE key=?", (key,)).fetchone()
            current = row[0] if row else 0
            exists = bool(row) and not row[1]
            if expected_version is not None:
                if expected_version == 0 and exists:
                    raise VersionMismatch(current, exists)
                if expected_version > 0 and (not exists or current != expected_version):
                    raise VersionMismatch(current, exists)
            new_version = current + 1  # versions keep growing even across delete + re-create
            self._write(key, value, new_version, deleted=False)
            return new_version

    def delete_primary(self, key: str, expected_version: int | None) -> int | None:
        """Writes a tombstone. Returns the tombstone's version, or None if the key is absent."""
        with self._lock:
            row = self._db.execute("SELECT version, deleted FROM kv WHERE key=?", (key,)).fetchone()
            if row is None or row[1]:
                return None
            if expected_version is not None and row[0] != expected_version:
                raise VersionMismatch(row[0], True)
            new_version = row[0] + 1
            self._write(key, None, new_version, deleted=True)
            return new_version

    # ---------------------------------------------------------------- replication
    def apply_replica(self, key: str, value: dict | None, version: int, deleted: bool) -> bool:
        """Apply a write forwarded from the primary, only if it is newer than what we have.
        Replica writes for the same key can arrive out of order; the version check makes that safe."""
        with self._lock:
            row = self._db.execute("SELECT version FROM kv WHERE key=?", (key,)).fetchone()
            if row and row[0] >= version:
                return False
            self._write(key, value, version, deleted)
            return True

    def force_apply(self, key: str, value: dict | None, version: int, deleted: bool) -> None:
        """Resync only: make our copy identical to the primary's, even if ours looks "newer".
        A higher local version means a write that was never acknowledged (we crashed or were
        cut off before replicating it). The primary is authoritative, so it is discarded
        (DESIGN.md §5.5, DDIA ch. 6, "Handling node outages")."""
        with self._lock:
            self._write(key, value, version, deleted)

    def hard_delete(self, key: str) -> None:
        with self._lock:
            self._db.execute("DELETE FROM kv WHERE key=?", (key,))

    def _write(self, key: str, value: dict | None, version: int, deleted: bool) -> None:
        kind, type_, status = _index_fields(key, value)
        self._db.execute(
            """INSERT INTO kv(key, kind, type, status, value, version, deleted, updated_at)
               VALUES(?,?,?,?,?,?,?,?)
               ON CONFLICT(key) DO UPDATE SET kind=excluded.kind, type=excluded.type,
                 status=excluded.status, value=excluded.value, version=excluded.version,
                 deleted=excluded.deleted, updated_at=excluded.updated_at""",
            (key, kind, type_, status, canonical(value), version, int(deleted), time.time()),
        )
