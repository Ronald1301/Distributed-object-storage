"""MetadataService: los metadatos del prototipo Ray en un actor, sin PostgreSQL (#35).

Sustituye a ``gateway/app/db.py`` + ``gateway/app/metadata.py`` de native-v1 con la
misma semántica (mismos nombres de métodos y de campos), pero con el estado en
memoria y la durabilidad en ``ray_proto/metadata_log.py`` (WAL + snapshot).

Qué hace cada parte:

* **Ray** ejecuta las llamadas al actor de una en una (actor síncrono,
  ``max_concurrency=1``). Eso sustituye al ``SELECT … FOR UPDATE``: ninguna
  mutación ve a otra a medias. Ray también transporta argumentos y resultados,
  reinicia el proceso si muere (``max_restarts``), permite encontrarlo por nombre
  y, con ``lifetime="detached"``, lo mantiene vivo aunque termine el driver que lo
  creó (hay que matarlo con ``ray.kill``).
* **La aplicación** comprueba precondiciones, escribe cada mutación en el WAL con
  ``fsync`` *antes* de aplicarla y confirmarla, hace snapshots y, al arrancar
  (también tras un reinicio de Ray, que vuelve a llamar a ``__init__``),
  reconstruye el estado desde disco.

Punto único de fallo declarado: hay un solo actor y su directorio vive en el disco
del nodo Ray donde corre. Si se pierde ese disco se pierden los metadatos. Si el
actor cae, lo confirmado sobrevive en disco y se reconstruye al arrancar; mientras
se reinicia no atiende a nadie, y las llamadas en vuelo quedan con resultado
desconocido (ver docs/fase1/dependencias-notas.md).

Reglas para que reproducir el WAL dé el mismo estado:

1. Los métodos públicos sólo leen el estado y validan; si hay que cambiar algo,
   llaman a ``_mutate(op, **args)`` con todos los valores ya resueltos (ULID nuevo,
   ``now``), de modo que ``_apply_<op>`` es determinista.
2. ``_mutate`` escribe el registro y aplica **lo que quedó en disco**.
3. Si la escritura falla, la memoria no cambia y el llamante recibe la excepción.

Interfaz (para el driver de #32; los tiempos son ``float`` epoch en segundos):

* Objetos: ``reserve``, ``commit_version`` (con ``request_id`` opcional para
  reintentos idempotentes), ``mark_deleted``, ``find_live``, ``find_active``,
  ``find_active_with_replicas``, ``get_object``, ``list_objects``,
  ``list_buckets``, ``active_objects_page``, ``all_active_with_replicas``,
  ``cleanup_stale_pending``, ``count_active``.
* Réplicas: ``replicas_of``, ``replicas_for``, ``replicas_on_node``,
  ``count_replicas_on_node``, ``set_replica_status``, ``mark_verified``,
  ``add_replica``, ``remove_replica``.
* Borrados físicos: ``pending_deletions``, ``pending_deletion_blob_ids``,
  ``is_referenced``, ``complete_deletion``, ``fail_deletion``,
  ``drop_deletions_for_node``, ``deletion_backlog``, ``pending_deletions_detail``.
* Nodos (sólo lo duradero; la salud no se guarda): ``upsert_node``,
  ``set_node_membership``, ``set_node_weight``, ``get_node``, ``list_nodes``.
* Operación: ``snapshot``, ``stats``, ``dump_state``, ``close``.
"""
from __future__ import annotations

import copy
import importlib.util
import logging
import os
import sys
import time
from collections import OrderedDict
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any

from ray_proto.metadata_log import CorruptLogError, MetadataLog

log = logging.getLogger("p16.ray.metadata")


def _load_native_ids():
    """Carga ``gateway/app/ids.py`` (sólo stdlib) sin importar el paquete del gateway."""
    module_name = "_p16_native_ids"
    if module_name in sys.modules:
        return sys.modules[module_name]
    source = Path(__file__).resolve().parents[1] / "gateway" / "app" / "ids.py"
    spec = importlib.util.spec_from_file_location(module_name, source)
    if spec is None or spec.loader is None:
        raise ImportError(f"no se pudo cargar ids desde {source}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


_ids = _load_native_ids()
new_ulid = _ids.new_ulid
etag_matches = _ids.etag_matches

DEFAULT_ACTOR_NAME = "p16-metadata"
DEFAULT_NAMESPACE = "p16"
# Ray usa 0 por defecto (no reinicia). -1: reinicia sin límite; el estado sale del disco.
DEFAULT_MAX_RESTARTS = -1
REPLICA_STATUSES = frozenset({"OK", "CORRUPT", "MISSING"})
MEMBERSHIPS = frozenset({"ACTIVE", "DRAINING", "DECOMMISSIONED"})

_OPS = frozenset({
    "reserve", "commit_version", "mark_deleted", "cleanup_stale_pending",
    "set_replica_status", "mark_verified", "add_replica", "remove_replica",
    "complete_deletion", "fail_deletion", "drop_deletions_for_node", "upsert_node",
})


class CommitConflict(Exception):
    pass


class PreconditionFailed(Exception):
    pass


class CorruptStateError(RuntimeError):
    """El estado reconstruido viola un invariante: se falla cerrado."""


class _MetadataService:
    def __init__(
        self,
        root: str | Path,
        *,
        snapshot_every: int = 500,
        max_remembered_requests: int = 10_000,
        clock: Callable[[], float] = time.time,
    ):
        self._clock = clock
        self._snapshot_every = snapshot_every
        self._max_requests = max_remembered_requests
        self._objects: dict[str, dict] = {}
        self._live: dict[tuple[str, str], str] = {}  # (bucket, key) -> id, si status != DELETED
        self._replicas: dict[str, dict[str, dict]] = {}  # object_id -> node_id -> fila
        self._deletions: dict[tuple[str, str], dict] = {}  # (blob_id, node_id) -> fila
        self._nodes: dict[str, dict] = {}
        self._requests: OrderedDict[str, dict] = OrderedDict()  # request_id -> resultado
        self._seq = 0

        started = time.perf_counter()
        self._log = MetadataLog(root)
        snapshot, records, info = self._log.recover()
        if snapshot is not None:
            self._load_state(snapshot["state"])
            self._seq = snapshot["last_seq"]
        for record in records:
            self._apply(record)
        self._verify_invariants()
        self._since_snapshot = len(records)
        self._recovery = {**info, "recovery_ms": round((time.perf_counter() - started) * 1000, 3)}
        self._log.open()
        if snapshot is not None or records:
            log.info("MetadataService recuperado: seq=%d, %s", self._seq, self._recovery)

    # ================================================================ objetos
    def reserve(self, bucket: str, key: str) -> dict:
        """Obtiene (o crea en estado PENDING) el identificador estable del nombre."""
        object_id = self._live.get((bucket, key))
        if object_id is not None:
            return self._public(self._objects[object_id])
        return self._mutate("reserve", object_id=new_ulid(), bucket=bucket, key=key, now=self._clock())

    def commit_version(
        self,
        *,
        object_id: str,
        blob_id: str,
        size: int,
        checksum: str,
        content_type: str,
        user_metadata: dict,
        node_ids: Iterable[str],
        if_match: str | None = None,
        if_none_match: str | None = None,
        request_id: str | None = None,
    ) -> tuple[dict, bool]:
        """Publica atómicamente una versión nueva (punto de linealización del PUT).

        Con ``request_id`` es idempotente: si ese PUT ya se confirmó, devuelve el
        resultado de entonces sin crear otra versión (el blob del reintento queda
        sin referencias y lo recolecta el GC)."""
        if request_id is not None and request_id in self._requests:
            done = self._requests[request_id]
            if done["object_id"] != object_id:
                raise CommitConflict(f"request_id {request_id!r} ya se usó para otro objeto")
            return copy.deepcopy(done["object"]), done["created"]

        cur = self._objects.get(object_id)
        if cur is None or cur["status"] == "DELETED":
            raise CommitConflict("El objeto fue eliminado mientras se escribía; reintente el PUT")
        exists = cur["status"] == "ACTIVE"
        if if_none_match is not None and exists and etag_matches(if_none_match, cur["checksum"]):
            raise PreconditionFailed("If-None-Match: el objeto ya existe")
        if if_match is not None and not (exists and etag_matches(if_match, cur["checksum"])):
            raise PreconditionFailed("If-Match: el ETag actual no coincide")
        nodes = list(node_ids)
        if len(set(nodes)) != len(nodes):
            raise ValueError(f"node_ids repetidos (dos copias en el mismo nodo): {nodes}")

        obj, created = self._mutate(
            "commit_version",
            object_id=object_id, blob_id=blob_id, size=size, checksum=checksum,
            content_type=content_type, user_metadata=user_metadata, node_ids=nodes,
            request_id=request_id, now=self._clock(),
        )
        return obj, created

    def mark_deleted(self, bucket: str, key: str) -> dict | None:
        """Borrado lógico inmediato + borrado físico diferido (cola)."""
        object_id = self._live.get((bucket, key))
        if object_id is None:
            return None
        return self._mutate("mark_deleted", object_id=object_id, now=self._clock())

    def find_live(self, bucket: str, key: str) -> dict | None:
        object_id = self._live.get((bucket, key))
        return None if object_id is None else self._public(self._objects[object_id])

    def find_active(self, bucket: str, key: str) -> dict | None:
        obj = self.find_live(bucket, key)
        return obj if obj is not None and obj["status"] == "ACTIVE" else None

    def find_active_with_replicas(self, bucket: str, key: str) -> dict | None:
        """Objeto + réplicas en la misma llamada: nadie puede confirmar entre medias."""
        obj = self.find_active(bucket, key)
        if obj is not None:
            obj["replicas"] = [
                {"node_id": r["node_id"], "blob_id": r["blob_id"], "status": r["status"]}
                for r in self._replica_rows(obj["id"])
            ]
        return obj

    def get_object(self, object_id: str) -> dict | None:
        obj = self._objects.get(object_id)
        return None if obj is None else self._public(obj)

    def list_objects(self, bucket: str, prefix: str, after: str | None, limit: int) -> list[dict]:
        rows = sorted(
            (o for o in self._objects.values()
             if o["bucket"] == bucket and o["status"] == "ACTIVE" and o["key"].startswith(prefix)
             and (after is None or o["key"] > after)),
            key=lambda o: o["key"],
        )
        return [
            {
                "id": o["id"], "key": o["key"], "size": o["size"], "content_type": o["content_type"],
                "checksum": o["checksum"], "version": o["version"], "updated_at": o["updated_at"],
                "replicas": [r["node_id"] for r in self._replica_rows(o["id"]) if r["status"] == "OK"],
            }
            for o in rows[:limit]
        ]

    def list_buckets(self) -> list[dict]:
        buckets: dict[str, dict] = {}
        for o in self._objects.values():
            if o["status"] == "ACTIVE":
                b = buckets.setdefault(o["bucket"], {"bucket": o["bucket"], "objects": 0, "bytes": 0})
                b["objects"] += 1
                b["bytes"] += o["size"] or 0
        return [buckets[name] for name in sorted(buckets)]

    def active_objects_page(self, after_id: str, limit: int) -> list[dict]:
        ids = sorted(i for i, o in self._objects.items() if o["status"] == "ACTIVE" and i > after_id)
        fields = ("id", "bucket", "key", "blob_id", "checksum", "size", "version")
        return [{f: self._objects[i][f] for f in fields} for i in ids[:limit]]

    def all_active_with_replicas(self) -> list[dict]:
        return [
            {
                "id": o["id"], "blob_id": o["blob_id"], "size": o["size"],
                "replicas": [{"node_id": r["node_id"], "status": r["status"], "blob_id": r["blob_id"]}
                             for r in self._replica_rows(o["id"])],
            }
            for o in sorted(self._objects.values(), key=lambda o: o["id"]) if o["status"] == "ACTIVE"
        ]

    def cleanup_stale_pending(self, grace_s: float) -> int:
        """Elimina reservas PENDING de PUT que nunca llegaron a confirmarse."""
        cutoff = self._clock() - grace_s
        stale = sorted(
            i for i, o in self._objects.items()
            if o["status"] == "PENDING" and o["version"] == 0 and o["created_at"] < cutoff
        )
        if not stale:
            return 0
        return self._mutate("cleanup_stale_pending", object_ids=stale)

    def count_active(self) -> int:
        return sum(1 for o in self._objects.values() if o["status"] == "ACTIVE")

    # ================================================================ réplicas
    def replicas_of(self, object_id: str) -> list[dict]:
        return [
            {"node_id": r["node_id"], "blob_id": r["blob_id"], "status": r["status"],
             "last_verified_at": r["last_verified_at"]}
            for r in self._replica_rows(object_id)
        ]

    def replicas_for(self, object_ids: list[str]) -> dict[str, list[dict]]:
        return {
            oid: [{"object_id": oid, "node_id": r["node_id"], "blob_id": r["blob_id"], "status": r["status"]}
                  for r in self._replica_rows(oid)]
            for oid in object_ids
        }

    def replicas_on_node(self, node_id: str) -> list[dict]:
        rows = []
        for oid in sorted(self._replicas):
            r = self._replicas[oid].get(node_id)
            if r is not None:
                o = self._objects[oid]
                rows.append({"object_id": oid, "blob_id": r["blob_id"], "status": r["status"],
                             "checksum": o["checksum"], "bucket": o["bucket"], "key": o["key"]})
        return rows

    def count_replicas_on_node(self, node_id: str) -> int:
        return sum(1 for by_node in self._replicas.values() if node_id in by_node)

    def set_replica_status(self, object_id: str, node_id: str, blob_id: str, status: str) -> bool:
        """Devuelve True si el estado cambió (evita eventos duplicados)."""
        if status not in REPLICA_STATUSES:
            raise ValueError(f"estado de réplica desconocido: {status!r}")
        row = self._replicas.get(object_id, {}).get(node_id)
        if row is None or row["blob_id"] != blob_id or row["status"] == status:
            return False
        return self._mutate("set_replica_status", object_id=object_id, node_id=node_id,
                            status=status, now=self._clock())

    def mark_verified(self, node_id: str, object_ids: list[str]) -> None:
        present = sorted({oid for oid in object_ids if node_id in self._replicas.get(oid, {})})
        if present:
            self._mutate("mark_verified", node_id=node_id, object_ids=present, now=self._clock())

    def add_replica(self, object_id: str, node_id: str, blob_id: str) -> bool:
        """Registra una copia recién creada, sólo si sigue siendo la versión actual."""
        cur = self._objects.get(object_id)
        if cur is None or cur["status"] != "ACTIVE" or cur["blob_id"] != blob_id:
            return False  # el objeto cambió o se borró: la copia quedará huérfana
        return self._mutate("add_replica", object_id=object_id, node_id=node_id, blob_id=blob_id,
                            now=self._clock())

    def remove_replica(
        self, object_id: str, node_id: str, blob_id: str, reason: str, min_remaining: int
    ) -> bool:
        """Quita una réplica sólo si quedan ``min_remaining`` copias OK de la versión
        actual en otros nodos."""
        cur = self._objects.get(object_id)
        if cur is None:
            return False
        by_node = self._replicas.get(object_id, {})
        remaining = sum(1 for n, r in by_node.items()
                        if n != node_id and r["status"] == "OK" and r["blob_id"] == cur["blob_id"])
        if remaining < min_remaining:
            return False
        row = by_node.get(node_id)
        if row is None or row["blob_id"] != blob_id:
            return False
        return self._mutate("remove_replica", object_id=object_id, node_id=node_id, reason=reason,
                            now=self._clock())

    # ======================================================= cola de borrados
    def pending_deletions(self, min_age_s: float, limit: int = 1000) -> list[dict]:
        cutoff = self._clock() - min_age_s
        rows = [d for d in self._sorted_deletions() if d["created_at"] <= cutoff]
        return [{k: d[k] for k in ("blob_id", "node_id", "reason", "attempts")} for d in rows[:limit]]

    def pending_deletion_blob_ids(self, node_id: str) -> set[str]:
        return {blob_id for (blob_id, nid) in self._deletions if nid == node_id}

    def is_referenced(self, blob_id: str, node_id: str) -> bool:
        return any(
            (r := by_node.get(node_id)) is not None and r["blob_id"] == blob_id
            for by_node in self._replicas.values()
        )

    def complete_deletion(self, blob_id: str, node_id: str) -> None:
        if (blob_id, node_id) in self._deletions:
            self._mutate("complete_deletion", blob_id=blob_id, node_id=node_id)

    def fail_deletion(self, blob_id: str, node_id: str, error: str) -> None:
        if (blob_id, node_id) in self._deletions:
            self._mutate("fail_deletion", blob_id=blob_id, node_id=node_id, error=str(error)[:500])

    def drop_deletions_for_node(self, node_id: str) -> None:
        if any(nid == node_id for (_, nid) in self._deletions):
            self._mutate("drop_deletions_for_node", node_id=node_id)

    def deletion_backlog(self) -> list[dict]:
        counts: dict[str, int] = {}
        for (_, nid) in self._deletions:
            counts[nid] = counts.get(nid, 0) + 1
        return [{"node_id": nid, "pending": counts[nid]} for nid in sorted(counts)]

    def pending_deletions_detail(self, limit: int = 300) -> list[dict]:
        return [{k: d[k] for k in ("blob_id", "node_id", "reason", "label", "attempts")}
                for d in self._sorted_deletions()[:limit]]

    # ================================================================== nodos
    def upsert_node(
        self, node_id: str, address: str | None = None, weight: float = 1.0, membership: str = "ACTIVE"
    ) -> dict:
        """Alta o actualización de un miembro. ``address`` es informativa (p. ej. el
        nombre del actor StorageNode). La salud no se persiste: se re-deriva."""
        weight = float(weight)
        if weight <= 0:
            raise ValueError("el peso debe ser > 0")
        if membership not in MEMBERSHIPS:
            raise ValueError(f"membresía desconocida: {membership!r}")
        cur = self._nodes.get(node_id)
        if cur is not None and (cur["address"], cur["weight"], cur["membership"]) == (address, weight, membership):
            return dict(cur)
        return self._mutate("upsert_node", node_id=node_id, address=address, weight=weight,
                            membership=membership, now=self._clock())

    def set_node_membership(self, node_id: str, membership: str) -> dict:
        cur = self._require_node(node_id)
        return self.upsert_node(node_id, cur["address"], cur["weight"], membership)

    def set_node_weight(self, node_id: str, weight: float) -> dict:
        cur = self._require_node(node_id)
        return self.upsert_node(node_id, cur["address"], weight, cur["membership"])

    def get_node(self, node_id: str) -> dict | None:
        node = self._nodes.get(node_id)
        return None if node is None else dict(node)

    def list_nodes(self) -> list[dict]:
        return [dict(self._nodes[n]) for n in sorted(self._nodes)]

    # ============================================================== operación
    def snapshot(self) -> dict:
        """Escribe el estado completo y vacía el WAL."""
        self._log.write_snapshot(self._dump(), self._seq)
        self._since_snapshot = 0
        return {"last_seq": self._seq}

    def stats(self) -> dict:
        return {
            "pid": os.getpid(),  # permite ver que un reinicio de Ray es otro proceso
            "last_seq": self._seq,
            "records_since_snapshot": self._since_snapshot,
            "objects": len(self._objects),
            "active_objects": self.count_active(),
            "replicas": sum(len(by_node) for by_node in self._replicas.values()),
            "pending_deletions": len(self._deletions),
            "nodes": len(self._nodes),
            "remembered_requests": len(self._requests),
            "recovery": dict(self._recovery),
        }

    def dump_state(self) -> dict:
        """Estado canónico (el mismo que se guarda en el snapshot)."""
        return copy.deepcopy(self._dump())

    def close(self) -> None:
        self._log.close()

    # ================================================== mutación y reproducción
    def _mutate(self, op: str, **args: Any) -> Any:
        record = self._log.append({"seq": self._seq + 1, "op": op, "args": args})
        result = self._apply(record)
        self._since_snapshot += 1
        if self._snapshot_every and self._since_snapshot >= self._snapshot_every:
            try:
                self.snapshot()
            except Exception:  # la mutación ya es duradera en el WAL
                log.exception("No se pudo escribir el snapshot; se sigue con el WAL")
        return result

    def _apply(self, record: dict) -> Any:
        op = record.get("op")
        if op not in _OPS:
            raise CorruptLogError(f"operación desconocida en el WAL: {op!r}")
        result = getattr(self, f"_apply_{op}")(**record["args"])
        self._seq = record["seq"]
        return result

    def _apply_reserve(self, object_id: str, bucket: str, key: str, now: float) -> dict:
        obj = {
            "id": object_id, "bucket": bucket, "key": key, "status": "PENDING", "version": 0,
            "blob_id": None, "size": None, "content_type": None, "checksum": None,
            "user_metadata": {}, "created_at": now, "updated_at": now, "deleted_at": None,
        }
        self._objects[object_id] = obj
        self._live[(bucket, key)] = object_id
        return self._public(obj)

    def _apply_commit_version(
        self, object_id: str, blob_id: str, size: int, checksum: str, content_type: str,
        user_metadata: dict, node_ids: list[str], request_id: str | None, now: float,
    ) -> tuple[dict, bool]:
        cur = self._objects[object_id]
        created = cur["status"] != "ACTIVE"
        label = f"{cur['bucket']}/{cur['key']} v{cur['version']}"
        old = self._replicas.pop(object_id, {})
        for nid in sorted(old):
            self._queue_deletion(old[nid]["blob_id"], nid, "overwritten", label, now)
        cur.update(
            status="ACTIVE", version=cur["version"] + 1, blob_id=blob_id, size=size, checksum=checksum,
            content_type=content_type, user_metadata=user_metadata, updated_at=now,
        )
        if node_ids:
            self._replicas[object_id] = {
                nid: self._new_replica(object_id, nid, blob_id, now) for nid in node_ids
            }
        result = self._public(cur)
        if request_id is not None:
            self._requests[request_id] = {"object_id": object_id, "object": result, "created": created}
            while len(self._requests) > self._max_requests:
                self._requests.popitem(last=False)
        return copy.deepcopy(result), created

    def _apply_mark_deleted(self, object_id: str, now: float) -> dict:
        cur = self._objects[object_id]
        label = f"{cur['bucket']}/{cur['key']}"
        old = self._replicas.pop(object_id, {})
        for nid in sorted(old):
            self._queue_deletion(old[nid]["blob_id"], nid, "object-deleted", label, now)
        cur.update(status="DELETED", deleted_at=now, updated_at=now)
        self._live.pop((cur["bucket"], cur["key"]), None)
        result = self._public(cur)
        result["queued_deletions"] = sorted(old)
        return result

    def _apply_cleanup_stale_pending(self, object_ids: list[str]) -> int:
        for oid in object_ids:
            obj = self._objects.pop(oid)
            if self._live.get((obj["bucket"], obj["key"])) == oid:
                del self._live[(obj["bucket"], obj["key"])]
            self._replicas.pop(oid, None)
        return len(object_ids)

    def _apply_set_replica_status(self, object_id: str, node_id: str, status: str, now: float) -> bool:
        self._replicas[object_id][node_id].update(status=status, updated_at=now)
        return True

    def _apply_mark_verified(self, node_id: str, object_ids: list[str], now: float) -> None:
        for oid in object_ids:
            self._replicas[oid][node_id]["last_verified_at"] = now

    def _apply_add_replica(self, object_id: str, node_id: str, blob_id: str, now: float) -> bool:
        cur = self._objects[object_id]
        by_node = self._replicas.setdefault(object_id, {})
        prev = by_node.get(node_id)
        if prev is not None and prev["blob_id"] != blob_id:
            self._queue_deletion(prev["blob_id"], node_id, "stale-version", f"{cur['bucket']}/{cur['key']}", now)
        if prev is None:
            by_node[node_id] = self._new_replica(object_id, node_id, blob_id, now)
        else:
            prev.update(blob_id=blob_id, status="OK", last_verified_at=now, updated_at=now)
        self._deletions.pop((blob_id, node_id), None)
        return True

    def _apply_remove_replica(self, object_id: str, node_id: str, reason: str, now: float) -> bool:
        cur = self._objects[object_id]
        by_node = self._replicas[object_id]
        row = by_node.pop(node_id)
        if not by_node:
            del self._replicas[object_id]
        self._queue_deletion(row["blob_id"], node_id, reason, f"{cur['bucket']}/{cur['key']}", now)
        return True

    def _apply_complete_deletion(self, blob_id: str, node_id: str) -> None:
        self._deletions.pop((blob_id, node_id), None)

    def _apply_fail_deletion(self, blob_id: str, node_id: str, error: str) -> None:
        row = self._deletions[(blob_id, node_id)]
        row["attempts"] += 1
        row["last_error"] = error

    def _apply_drop_deletions_for_node(self, node_id: str) -> None:
        for k in [k for k in self._deletions if k[1] == node_id]:
            del self._deletions[k]

    def _apply_upsert_node(
        self, node_id: str, address: str | None, weight: float, membership: str, now: float
    ) -> dict:
        node = self._nodes.get(node_id)
        if node is None:
            node = self._nodes[node_id] = {"node_id": node_id, "joined_at": now}
        node.update(address=address, weight=weight, membership=membership)
        return dict(node)

    # ================================================================ helpers
    @staticmethod
    def _public(obj: dict) -> dict:
        return copy.deepcopy(obj)

    @staticmethod
    def _new_replica(object_id: str, node_id: str, blob_id: str, now: float) -> dict:
        return {"object_id": object_id, "node_id": node_id, "blob_id": blob_id, "status": "OK",
                "last_verified_at": now, "created_at": now, "updated_at": now}

    def _replica_rows(self, object_id: str) -> list[dict]:
        by_node = self._replicas.get(object_id, {})
        return [by_node[n] for n in sorted(by_node)]

    def _queue_deletion(self, blob_id: str, node_id: str, reason: str, label: str, now: float) -> None:
        # Como ``ON CONFLICT DO NOTHING``: un borrado ya encolado conserva su motivo y antigüedad.
        self._deletions.setdefault((blob_id, node_id), {
            "blob_id": blob_id, "node_id": node_id, "reason": reason, "label": label,
            "attempts": 0, "last_error": None, "created_at": now,
        })

    def _sorted_deletions(self) -> list[dict]:
        return sorted(self._deletions.values(), key=lambda d: (d["created_at"], d["blob_id"], d["node_id"]))

    def _require_node(self, node_id: str) -> dict:
        node = self._nodes.get(node_id)
        if node is None:
            raise KeyError(node_id)
        return node

    def _dump(self) -> dict:
        return {
            "objects": [self._objects[i] for i in sorted(self._objects)],
            "replicas": [self._replicas[o][n] for o in sorted(self._replicas) for n in sorted(self._replicas[o])],
            "deletions": [self._deletions[k] for k in sorted(self._deletions)],
            "nodes": [self._nodes[n] for n in sorted(self._nodes)],
            "requests": [[rid, rec] for rid, rec in self._requests.items()],
        }

    def _load_state(self, state: dict) -> None:
        for obj in state["objects"]:
            self._objects[obj["id"]] = obj
            if obj["status"] != "DELETED":
                name = (obj["bucket"], obj["key"])
                if name in self._live:
                    raise CorruptStateError(f"dos objetos vivos con el nombre {name}")
                self._live[name] = obj["id"]
        for row in state["replicas"]:
            self._replicas.setdefault(row["object_id"], {})[row["node_id"]] = row
        for row in state["deletions"]:
            self._deletions[(row["blob_id"], row["node_id"])] = row
        for node in state["nodes"]:
            self._nodes[node["node_id"]] = node
        for rid, rec in state["requests"]:
            self._requests[rid] = rec

    def _verify_invariants(self) -> None:
        """Una réplica sólo puede existir para un objeto ACTIVE (como la FK + el commit)."""
        for oid, by_node in self._replicas.items():
            obj = self._objects.get(oid)
            if obj is None or obj["status"] != "ACTIVE" or not by_node:
                raise CorruptStateError(f"réplicas de un objeto inexistente o no activo: {oid}")


try:
    import ray
except ImportError:  # las pruebas de semántica y de recuperación no necesitan Ray
    ray = None

if ray is None:
    MetadataService = _MetadataService
else:
    MetadataService = ray.remote(_MetadataService)


def _check_max_restarts(max_restarts: int) -> None:
    """Exige un valor explícito con reinicio: -1 (sin límite) o un entero >= 1.

    0 (el valor por defecto de Ray) dejaría el servicio muerto tras la primera caída."""
    if not isinstance(max_restarts, int) or isinstance(max_restarts, bool):
        raise TypeError("max_restarts debe ser un entero")
    if max_restarts == 0 or max_restarts < -1:
        raise ValueError("max_restarts debe ser -1 (sin límite) o un entero >= 1")


def get_or_create_metadata_service(
    root: str | Path,
    *,
    name: str = DEFAULT_ACTOR_NAME,
    namespace: str = DEFAULT_NAMESPACE,
    detached: bool = True,
    resources: dict[str, float] | None = None,
    max_restarts: int = DEFAULT_MAX_RESTARTS,
    **service_kwargs: Any,
):
    """Crea el actor con nombre (o devuelve el que ya existe con ese nombre).

    * ``lifetime="detached"`` (``detached=True``, por defecto): el actor no muere con
      el driver que lo creó y cualquier driver posterior lo encuentra por nombre. Sin
      él, Ray lo mata al terminar ese driver y no lo reinicia (no es un fallo, es el
      fin de su vida). Un actor detached sólo se va con ``ray.kill`` o con el clúster.
    * ``max_restarts=-1``: Ray lo reinicia siempre; el ``__init__`` reconstruye el estado.
      Se rechaza 0 (el valor por defecto de Ray).
    * ``max_task_retries=0``: Ray no re-ejecuta a ciegas una mutación interrumpida;
      el resultado queda "desconocido" y el driver reintenta con el mismo ``request_id``.
    * ``max_concurrency=1``: una llamada cada vez (equivale al ``FOR UPDATE``).
    * ``resources``: p. ej. ``{"p16_metadata": 1}`` (declarado en ``ray start
      --resources``) para que siempre vuelva al nodo que tiene su directorio.

    Ojo: con ``get_if_exists=True`` un actor ya existente ignora ``root`` y el resto
    de argumentos. Y si el repo está en una ruta con espacios, no use ``runtime_env``
    en ``ray.init``: Ray 2.60.1 no consigue arrancar los workers (ver
    docs/fase1/dependencias-notas.md); exporte ``PYTHONPATH`` antes de ``ray.init``."""
    _check_max_restarts(max_restarts)
    if ray is None:
        raise RuntimeError("Ray no está instalado: use _MetadataService directamente")
    options: dict[str, Any] = {
        "name": name, "namespace": namespace, "get_if_exists": True,
        "max_restarts": max_restarts, "max_task_retries": 0, "max_concurrency": 1,
    }
    if detached:
        options["lifetime"] = "detached"
    if resources:
        options["resources"] = dict(resources)
    return MetadataService.options(**options).remote(str(root), **service_kwargs)


def get_metadata_service(name: str = DEFAULT_ACTOR_NAME, namespace: str = DEFAULT_NAMESPACE):
    """Descubrimiento: obtiene el actor por nombre desde cualquier driver o actor."""
    if ray is None:
        raise RuntimeError("Ray no está instalado")
    return ray.get_actor(name, namespace=namespace)
