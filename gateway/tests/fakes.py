"""Dobles en memoria para probar el reconciliador sin PostgreSQL ni Storage Nodes.

  * InMemoryMetadata   -> sustituye a MetadataRepository (mismas firmas y la
                          misma semántica que las consultas SQL que usa el
                          reconciliador, incluidas las guardas de remove/add).
  * InMemoryStorage    -> sustituye a StorageClient: un "disco" por nodo,
                          push nodo->nodo que verifica el SHA-256 en origen
                          (409) y en destino, y fallos de red inyectables.
  * InMemoryMembership -> la clase Membership REAL con la persistencia
                          anulada: is_readable / is_writable /
                          counts_for_durability son reglas del sistema, y
                          reimplementarlas en el doble probaría el doble en
                          lugar del código.

Todo es determinista: los ids de objeto y de blob salen de un contador y el
tiempo de la cola de borrados y de la edad de los blobs lo marca un reloj
falso (`Clock`) que el test avanza a mano.
"""
from __future__ import annotations

import hashlib
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, AsyncIterator

from app.config import Settings
from app.membership import ACTIVE, DOWN, HEALTHY, Membership, NodeState
from app.reconciler import Reconciler
from app.storage_client import StorageError


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def make_settings(**overrides: Any) -> Settings:
    """Ajustes pensados para tests: sin esperas y con gracia de borrado 0."""
    values: dict[str, Any] = dict(
        database_url="memory://",
        replication_factor=2,
        write_acks=2,
        max_object_bytes=64 * 1024 * 1024,
        storage_timeout_s=1,
        health_interval_s=1,
        health_timeout_s=1,
        suspect_after=1,
        down_after=3,
        repair_delay_s=30,
        reconcile_interval_s=10,
        scrub_interval_s=300,
        orphan_grace_s=60,
        delete_grace_s=0,
        max_copies_per_cycle=200,
    )
    values.update(overrides)
    return Settings(**values)


class Clock:
    """Reloj monótono falso (segundos)."""

    def __init__(self, start: float = 1_000.0):
        self.now = start

    def advance(self, seconds: float) -> None:
        self.now += seconds


# ================================================================ base de datos
class FakeDatabase:
    """Sólo lo que usan Membership y Reconciler fuera del repositorio."""

    def __init__(self) -> None:
        self.lock_available = True

    async def execute(self, sql: str, *args: Any) -> str:
        return "UPDATE 1"

    async def fetch(self, sql: str, *args: Any) -> list[dict]:
        return []

    @asynccontextmanager
    async def advisory_lock(self, key: int) -> AsyncIterator[bool]:
        yield self.lock_available


class FakeEvents:
    def __init__(self) -> None:
        self.items: list[dict] = []

    async def record(self, type_: str, message: str, *, node_id: str | None = None,
                     object_id: str | None = None, data: Any = None) -> None:
        self.items.append({"type": type_, "message": message, "node_id": node_id, "object_id": object_id})

    def types(self) -> list[str]:
        return [e["type"] for e in self.items]


# ================================================================== membresía
class InMemoryMembership(Membership):
    def add_node(self, node_id: str, *, weight: float = 1.0, membership: str = ACTIVE,
                 health: str = HEALTHY) -> NodeState:
        now = datetime.now(timezone.utc)
        node = NodeState(
            node_id=node_id, url=f"http://{node_id}:9000", weight=weight, membership=membership,
            health=health, consecutive_failures=0, last_seen_at=now,
            down_since=now if health == DOWN else None, joined_at=now, used_bytes=0, blob_count=0,
        )
        self._nodes[node_id] = node
        return node

    def set_health(self, node_id: str, health: str, *, down_for_s: float = 0.0) -> None:
        """Fija la salud observada. `down_for_s` = cuánto lleva caído (para REPAIR_DELAY)."""
        node = self._nodes[node_id]
        node.health = health
        node.down_since = (
            datetime.now(timezone.utc) - timedelta(seconds=down_for_s) if health == DOWN else None
        )


# =================================================================== metadatos
class InMemoryMetadata:
    """Implementa las operaciones de MetadataRepository que usa el reconciliador."""

    def __init__(self, clock: Clock):
        self.clock = clock
        self.objects: dict[str, dict] = {}
        self.replicas: dict[tuple[str, str], dict] = {}  # (object_id, node_id) -> fila
        self.deletions: dict[tuple[str, str], dict] = {}  # (blob_id, node_id) -> fila
        self.remove_calls: list[dict] = []  # intentos de remove_replica (para las aserciones)

    # ------------------------------------------------------------ helpers de test
    def insert_object(self, object_id: str, bucket: str, key: str, blob_id: str, data: bytes) -> dict:
        obj = {
            "id": object_id, "bucket": bucket, "key": key, "status": "ACTIVE", "version": 1,
            "blob_id": blob_id, "checksum": sha256(data), "size": len(data),
            "content_type": "application/octet-stream", "created_at": self.clock.now,
        }
        self.objects[object_id] = obj
        return obj

    def insert_replica(self, object_id: str, node_id: str, blob_id: str, status: str = "OK") -> None:
        self.replicas[(object_id, node_id)] = {
            "object_id": object_id, "node_id": node_id, "blob_id": blob_id, "status": status,
            "last_verified_at": None,
        }

    def replica(self, object_id: str, node_id: str) -> dict | None:
        return self.replicas.get((object_id, node_id))

    def nodes_with(self, object_id: str, status: str = "OK") -> set[str]:
        blob = self.objects[object_id]["blob_id"]
        return {n for (o, n), r in self.replicas.items()
                if o == object_id and r["status"] == status and r["blob_id"] == blob}

    def _queue_deletion(self, blob_id: str, node_id: str, reason: str, label: str) -> None:
        self.deletions.setdefault((blob_id, node_id), {
            "blob_id": blob_id, "node_id": node_id, "reason": reason, "label": label,
            "attempts": 0, "last_error": None, "created_at": self.clock.now,
        })

    # ------------------------------------------------------------------ objetos
    async def get_object(self, object_id: str) -> dict | None:
        obj = self.objects.get(object_id)
        return dict(obj) if obj else None

    async def active_objects_page(self, after_id: str, limit: int) -> list[dict]:
        ids = sorted(i for i, o in self.objects.items() if o["status"] == "ACTIVE" and i > after_id)
        return [dict(self.objects[i]) for i in ids[:limit]]

    async def cleanup_stale_pending(self, grace_s: float) -> int:
        stale = [i for i, o in self.objects.items()
                 if o["status"] == "PENDING" and o["version"] == 0
                 and o["created_at"] < self.clock.now - grace_s]
        for i in stale:
            del self.objects[i]
        return len(stale)

    # ------------------------------------------------------------------ réplicas
    async def replicas_of(self, object_id: str) -> list[dict]:
        rows = [dict(r) for (o, _), r in self.replicas.items() if o == object_id]
        return sorted(rows, key=lambda r: r["node_id"])

    async def replicas_for(self, object_ids: list[str]) -> dict[str, list[dict]]:
        result: dict[str, list[dict]] = {oid: [] for oid in object_ids}
        for (oid, _), row in self.replicas.items():
            if oid in result:
                result[oid].append(dict(row))
        return result

    async def replicas_on_node(self, node_id: str) -> list[dict]:
        rows = []
        for (oid, nid), r in self.replicas.items():
            if nid == node_id:
                obj = self.objects[oid]
                rows.append({"object_id": oid, "blob_id": r["blob_id"], "status": r["status"],
                             "checksum": obj["checksum"], "bucket": obj["bucket"], "key": obj["key"]})
        return rows

    async def count_replicas_on_node(self, node_id: str) -> int:
        return sum(1 for (_, nid) in self.replicas if nid == node_id)

    async def set_replica_status(self, object_id: str, node_id: str, blob_id: str, status: str) -> bool:
        row = self.replicas.get((object_id, node_id))
        if row is None or row["blob_id"] != blob_id or row["status"] == status:
            return False
        row["status"] = status
        return True

    async def mark_verified(self, node_id: str, object_ids: list[str]) -> None:
        for oid in object_ids:
            if (row := self.replicas.get((oid, node_id))) is not None:
                row["last_verified_at"] = self.clock.now

    async def add_replica(self, object_id: str, node_id: str, blob_id: str) -> bool:
        cur = self.objects.get(object_id)
        if cur is None or cur["status"] != "ACTIVE" or cur["blob_id"] != blob_id:
            return False
        prev = self.replicas.get((object_id, node_id))
        if prev is not None and prev["blob_id"] != blob_id:
            self._queue_deletion(prev["blob_id"], node_id, "stale-version", f"{cur['bucket']}/{cur['key']}")
        self.insert_replica(object_id, node_id, blob_id, "OK")
        self.deletions.pop((blob_id, node_id), None)
        return True

    async def remove_replica(self, object_id: str, node_id: str, blob_id: str, reason: str,
                             min_remaining: int) -> bool:
        self.remove_calls.append({"object_id": object_id, "node_id": node_id, "reason": reason,
                                  "min_remaining": min_remaining})
        cur = self.objects.get(object_id)
        if cur is None:
            return False
        remaining = sum(1 for (o, n), r in self.replicas.items()
                        if o == object_id and n != node_id and r["status"] == "OK"
                        and r["blob_id"] == cur["blob_id"])
        if remaining < min_remaining:
            return False
        row = self.replicas.get((object_id, node_id))
        if row is None or row["blob_id"] != blob_id:
            return False
        del self.replicas[(object_id, node_id)]
        self._queue_deletion(blob_id, node_id, reason, f"{cur['bucket']}/{cur['key']}")
        return True

    # ---------------------------------------------------------- cola de borrados
    async def pending_deletions(self, min_age_s: float, limit: int = 1000) -> list[dict]:
        rows = [dict(d) for d in self.deletions.values() if self.clock.now - d["created_at"] >= min_age_s]
        return sorted(rows, key=lambda d: d["created_at"])[:limit]

    async def pending_deletion_blob_ids(self, node_id: str) -> set[str]:
        return {b for (b, n) in self.deletions if n == node_id}

    async def is_referenced(self, blob_id: str, node_id: str) -> bool:
        return any(n == node_id and r["blob_id"] == blob_id for (_, n), r in self.replicas.items())

    async def complete_deletion(self, blob_id: str, node_id: str) -> None:
        self.deletions.pop((blob_id, node_id), None)

    async def fail_deletion(self, blob_id: str, node_id: str, error: str) -> None:
        if (row := self.deletions.get((blob_id, node_id))) is not None:
            row["attempts"] += 1
            row["last_error"] = error[:500]

    async def drop_deletions_for_node(self, node_id: str) -> None:
        for k in [k for k in self.deletions if k[1] == node_id]:
            del self.deletions[k]


# ===================================================================== storage
@dataclass
class Disk:
    blobs: dict[str, bytes] = field(default_factory=dict)
    stored_at: dict[str, float] = field(default_factory=dict)
    online: bool = True  # False -> errores de red (nodo caído)
    reject_writes: bool = False  # True -> rechaza escrituras (p. ej. disco lleno)


class InMemoryStorage:
    """Sustituye a StorageClient con un disco en memoria por nodo."""

    def __init__(self, clock: Clock):
        self.clock = clock
        self.disks: dict[str, Disk] = {}
        self.pushes: list[tuple[str, str, str]] = []  # (origen, destino, blob)

    def disk(self, node_id: str) -> Disk:
        return self.disks.setdefault(node_id, Disk())

    def store(self, node_id: str, blob_id: str, data: bytes, age_s: float = 0.0) -> None:
        disk = self.disk(node_id)
        disk.blobs[blob_id] = data
        disk.stored_at[blob_id] = self.clock.now - age_s

    def _reachable(self, node: NodeState) -> Disk:
        disk = self.disk(node.node_id)
        if not disk.online:
            raise StorageError(f"{node.node_id}: ConnectError", kind="network")
        return disk

    async def list_blobs(self, node: NodeState) -> list[dict]:
        disk = self._reachable(node)
        return [{"blob_id": b, "size": len(d), "age_s": round(self.clock.now - disk.stored_at[b], 1)}
                for b, d in sorted(disk.blobs.items())]

    async def delete_blob(self, node: NodeState, blob_id: str) -> None:
        disk = self._reachable(node)
        disk.blobs.pop(blob_id, None)
        disk.stored_at.pop(blob_id, None)

    async def digest(self, node: NodeState, blob_id: str) -> dict | None:
        data = self._reachable(node).blobs.get(blob_id)
        return None if data is None else {"sha256": sha256(data), "size": len(data)}

    async def push_blob(self, source: NodeState, blob_id: str, target: NodeState, checksum: str,
                        object_id: str) -> dict:
        data = self._reachable(source).blobs.get(blob_id)
        if data is None:
            raise StorageError("source-missing", status=404)
        if sha256(data) != checksum:  # el origen nunca envía una copia corrupta
            raise StorageError("source-corrupt", status=409)
        dst = self.disk(target.node_id)
        if not dst.online or dst.reject_writes:
            raise StorageError("target-rejected", status=502)
        self.store(target.node_id, blob_id, data)
        self.pushes.append((source.node_id, target.node_id, blob_id))
        return {"from": source.node_id, "to": target.url, "blob_id": blob_id, "size": len(data)}


# ===================================================================== clúster
class Cluster:
    """Reconciliador real conectado a los dobles, con utilidades para los tests."""

    def __init__(self, *node_ids: str, **settings: Any):
        self.clock = Clock()
        self.settings = make_settings(**settings)
        self.db = FakeDatabase()
        self.events = FakeEvents()
        self.membership = InMemoryMembership(self.db, self.events, self.settings)  # type: ignore[arg-type]
        self.meta = InMemoryMetadata(self.clock)
        self.storage = InMemoryStorage(self.clock)
        self.reconciler = Reconciler(
            self.db, self.meta, self.membership, self.storage, self.events, self.settings  # type: ignore[arg-type]
        )
        self._seq = 0
        for node_id in node_ids or ("storage1", "storage2", "storage3"):
            self.membership.add_node(node_id)

    def put(self, key: str, data: bytes | None = None, *, on: list[str] | None = None,
            bucket: str = "test") -> dict:
        """Objeto ya confirmado con réplicas OK en `on` (por defecto, su ubicación deseada)."""
        self._seq += 1
        object_id, blob_id = f"OBJ{self._seq:06d}", f"BLOB{self._seq:06d}"
        data = data if data is not None else f"contenido de {key}".encode()
        obj = self.meta.insert_object(object_id, bucket, key, blob_id, data)
        for node_id in (on if on is not None else self.desired(object_id)):
            self.storage.store(node_id, blob_id, data)
            self.meta.insert_replica(object_id, node_id, blob_id)
        return obj

    def preference(self, object_id: str) -> list[str]:
        from app.placement import preference_list

        return preference_list(object_id, self.membership.placement_candidates())

    def desired(self, object_id: str) -> list[str]:
        return self.preference(object_id)[: self.settings.replication_factor]

    def has_valid_copy(self, node_id: str, obj: dict) -> bool:
        data = self.storage.disk(node_id).blobs.get(obj["blob_id"])
        return data is not None and sha256(data) == obj["checksum"]

    async def cycle(self) -> dict:
        return await self.reconciler.run_cycle("test")
