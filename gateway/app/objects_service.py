"""Plano de aplicación: semántica de PUT / GET / HEAD / DELETE / LIST.

El cliente sólo conoce (bucket, key). Este servicio traduce cada operación a
operaciones del plano de datos (blobs en Storage Nodes) y de metadatos.

Contrato de escritura (PUT):
    1. calcular SHA-256 del cuerpo
    2. reservar el id estable del objeto (metadatos, estado PENDING si es nuevo)
    3. elegir nodos con HRW(id) y escribir un blob NUEVO e inmutable en paralelo
       (cada nodo verifica el checksum, hace fsync y rename atómico)
    4. si hay >= WRITE_ACKS confirmaciones -> commit en PostgreSQL (atómico)
    5. responder 201/200. Antes del commit nada es visible para los lectores.
"""
from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass

from .config import Settings
from .errors import ApiError
from .events import EventLog
from .ids import etag_matches, new_ulid, sha256_hex
from .membership import Membership
from .metadata import CommitConflict, MetadataRepository, PreconditionFailed
from .placement import preference_list
from .reconciler import Reconciler
from .storage_client import StorageClient, StorageError

log = logging.getLogger("p16.objects")

BUCKET_RE = re.compile(r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")


def validate_name(bucket: str, key: str) -> None:
    if not BUCKET_RE.match(bucket):
        raise ApiError(400, "Nombre de bucket inválido (3-63 caracteres: a-z, 0-9, '.', '-')", bucket=bucket)
    if not key or len(key.encode()) > 1024 or any(ord(c) < 32 for c in key):
        raise ApiError(400, "Clave inválida (1-1024 bytes, sin caracteres de control)", key=key)


@dataclass
class ReadResult:
    obj: dict
    data: bytes | None  # None => 304 Not Modified
    served_by: str | None
    attempts: list[str]


class ObjectService:
    def __init__(
        self,
        meta: MetadataRepository,
        membership: Membership,
        storage: StorageClient,
        events: EventLog,
        reconciler: Reconciler,
        settings: Settings,
    ):
        self._meta = meta
        self._membership = membership
        self._storage = storage
        self._events = events
        self._reconciler = reconciler
        self._s = settings

    # ===================================================================== PUT
    async def put(
        self,
        bucket: str,
        key: str,
        data: bytes,
        content_type: str,
        user_metadata: dict[str, str],
        if_match: str | None = None,
        if_none_match: str | None = None,
    ) -> dict:
        validate_name(bucket, key)
        checksum = sha256_hex(data)

        reserved = await self._meta.reserve(bucket, key)
        exists = reserved["status"] == "ACTIVE"
        # Comprobación temprana de precondiciones (se repite de forma atómica en el commit)
        if if_none_match is not None and exists and etag_matches(if_none_match, reserved["checksum"]):
            raise ApiError(412, "If-None-Match: el objeto ya existe")
        if if_match is not None and not (exists and etag_matches(if_match, reserved["checksum"])):
            raise ApiError(412, "If-Match: el ETag actual no coincide")

        object_id = reserved["id"]
        blob_id = new_ulid()  # cada versión es un blob nuevo e inmutable
        preference = preference_list(object_id, self._membership.placement_candidates())
        desired = preference[: self._s.replication_factor]

        stored, failures = await self._write_replicas(object_id, blob_id, data, checksum, preference)
        if len(stored) < self._s.write_acks:
            self._discard(blob_id, stored)
            raise ApiError(
                503,
                f"Escritura rechazada: sólo {len(stored)} de {self._s.write_acks} réplicas confirmadas."
                " No se ha publicado ninguna versión nueva.",
                required_acks=self._s.write_acks,
                stored_on=stored,
                failures=failures,
            )

        try:
            obj, created = await self._meta.commit_version(
                object_id=object_id,
                blob_id=blob_id,
                size=len(data),
                checksum=checksum,
                content_type=content_type,
                user_metadata=user_metadata,
                node_ids=stored,
                if_match=if_match,
                if_none_match=if_none_match,
            )
        except PreconditionFailed as exc:
            self._discard(blob_id, stored)
            raise ApiError(412, str(exc)) from exc
        except CommitConflict as exc:
            self._discard(blob_id, stored)
            raise ApiError(409, str(exc)) from exc

        await self._events.record(
            "object.put",
            f"{bucket}/{key} v{obj['version']} ({len(data)} B) escrito en {', '.join(stored)}"
            + ("" if set(stored) == set(desired) else f" (ubicación deseada: {', '.join(desired)})"),
            object_id=object_id,
            data={"blob_id": blob_id, "replicas": stored, "desired": desired, "failures": failures},
        )
        if len(stored) < self._s.replication_factor:
            self._reconciler.repair_soon(object_id)
        if not created:
            self._reconciler.gc_soon()  # borrar los blobs de la versión anterior
        return {"object": obj, "created": created, "stored_on": stored, "desired": desired, "failures": failures}

    async def _write_replicas(
        self, object_id: str, blob_id: str, data: bytes, checksum: str, preference: list[str]
    ) -> tuple[list[str], dict[str, str]]:
        """Escribe en los RF primeros nodos disponibles de la lista de preferencia.
        Si uno falla, se sustituye por el siguiente de la lista (sloppy quorum)."""
        failures: dict[str, str] = {
            n: "omitido: " + self._membership.get(n).health  # type: ignore[union-attr]
            for n in preference[: self._s.replication_factor]
            if not self._membership.is_writable(n)
        }
        candidates = [n for n in preference if self._membership.is_writable(n)]
        rf = self._s.replication_factor
        pending, rest = candidates[:rf], candidates[rf:]
        stored: list[str] = []
        while pending:
            results = await asyncio.gather(
                *(self._put_one(n, blob_id, data, checksum, object_id) for n in pending),
                return_exceptions=True,
            )
            failed = 0
            for node_id, result in zip(pending, results):
                if isinstance(result, BaseException):
                    failures[node_id] = str(result)
                    failed += 1
                else:
                    stored.append(node_id)
            pending, rest = rest[:failed], rest[failed:]
        return stored, failures

    async def _put_one(self, node_id: str, blob_id: str, data: bytes, checksum: str, object_id: str) -> None:
        node = self._membership.get(node_id)
        assert node is not None
        try:
            await self._storage.put_blob(node, blob_id, data, checksum, object_id)
        except StorageError as exc:
            if exc.kind == "network":
                await self._membership.report_data_path_failure(node_id, str(exc))
            raise

    def _discard(self, blob_id: str, node_ids: list[str]) -> None:
        """Borrado best-effort de blobs que nunca se publicaron (si falla, el GC
        de huérfanos los eliminará igualmente)."""

        async def run() -> None:
            for node_id in node_ids:
                node = self._membership.get(node_id)
                if node is not None:
                    try:
                        await self._storage.delete_blob(node, blob_id)
                    except StorageError:
                        pass

        self._reconciler.spawn(run())

    # ===================================================================== GET
    async def get(self, bucket: str, key: str, if_none_match: str | None = None) -> ReadResult:
        validate_name(bucket, key)
        label = f"{bucket}/{key}"
        attempts: list[str] = []
        for _ in range(2):
            obj = await self._meta.find_active_with_replicas(bucket, key)
            if obj is None:
                raise ApiError(404, "Objeto no encontrado", bucket=bucket, key=key)
            if if_none_match is not None and etag_matches(if_none_match, obj["checksum"]):
                return ReadResult(obj, None, None, ["not-modified"])
            result = await self._read_version(obj, label, attempts)
            if result is not None:
                return result
            # Si un PUT/DELETE concurrente cambió la versión mientras leíamos (y el GC
            # ya borró el blob antiguo), se reintenta una vez con la versión actual.
            current = await self._meta.find_active(bucket, key)
            if current is not None and current["blob_id"] != obj["blob_id"]:
                continue
            if current is None:
                raise ApiError(404, "Objeto no encontrado", bucket=bucket, key=key)
            break
        raise ApiError(
            503,
            "El objeto existe pero ninguna réplica válida está accesible en este momento",
            object_id=obj["id"],
            attempts=attempts,
        )

    async def _read_version(self, obj: dict, label: str, attempts: list[str]) -> ReadResult | None:
        """Lee la versión `obj` de la mejor réplica disponible verificando el SHA-256."""
        replicas = [r for r in obj["replicas"] if r["status"] == "OK" and r["blob_id"] == obj["blob_id"]]
        preference = preference_list(obj["id"], self._membership.placement_candidates())
        rank = {node_id: i for i, node_id in enumerate(preference)}
        replicas.sort(key=lambda r: (self._membership.read_order(r["node_id"]), rank.get(r["node_id"], 99)))

        needs_repair = False
        for replica in replicas:
            node_id = replica["node_id"]
            node = self._membership.get(node_id)
            if node is None or not self._membership.is_readable(node_id):
                attempts.append(f"{node_id}=down")
                continue
            try:
                data = await self._storage.get_blob(node, obj["blob_id"])
            except StorageError as exc:
                attempts.append(f"{node_id}=error")
                if exc.kind == "network":
                    await self._membership.report_data_path_failure(node_id, str(exc))
                continue
            if data is None:
                attempts.append(f"{node_id}=missing")
                needs_repair = True
                if await self._meta.set_replica_status(obj["id"], node_id, obj["blob_id"], "MISSING"):
                    await self._events.record(
                        "replica.missing", f"GET {label}: {node_id} no tiene la réplica", node_id=node_id,
                        object_id=obj["id"],
                    )
                continue
            actual = sha256_hex(data)
            if actual != obj["checksum"]:
                attempts.append(f"{node_id}=corrupt")
                needs_repair = True
                if await self._meta.set_replica_status(obj["id"], node_id, obj["blob_id"], "CORRUPT"):
                    await self._events.record(
                        "integrity.corrupt",
                        f"GET {label}: checksum inválido en {node_id} (esperado {obj['checksum'][:12]}…,"
                        f" calculado {actual[:12]}…). Copia descartada.",
                        node_id=node_id,
                        object_id=obj["id"],
                        data={"expected": obj["checksum"], "actual": actual, "detected_by": "GET"},
                    )
                continue
            attempts.append(f"{node_id}=ok")
            if needs_repair:
                self._reconciler.repair_soon(obj["id"])  # read-repair
            return ReadResult(obj, data, node_id, attempts)

        if needs_repair:
            self._reconciler.repair_soon(obj["id"])
        return None

    # ==================================================================== HEAD
    async def head(self, bucket: str, key: str) -> tuple[dict, list[dict]]:
        validate_name(bucket, key)
        obj = await self._meta.find_active_with_replicas(bucket, key)
        if obj is None:
            raise ApiError(404, "Objeto no encontrado", bucket=bucket, key=key)
        return obj, obj["replicas"]

    # ================================================================== DELETE
    async def delete(self, bucket: str, key: str) -> dict | None:
        validate_name(bucket, key)
        deleted = await self._meta.mark_deleted(bucket, key)
        if deleted is not None:
            await self._events.record(
                "object.deleted",
                f"{bucket}/{key} eliminado; borrado físico encolado en"
                f" {', '.join(deleted['queued_deletions']) or '(ningún nodo)'}",
                object_id=deleted["id"],
            )
            self._reconciler.gc_soon()
        return deleted

    # ==================================================================== LIST
    async def list(self, bucket: str, prefix: str, after: str | None, limit: int) -> dict:
        if not BUCKET_RE.match(bucket):
            raise ApiError(400, "Nombre de bucket inválido", bucket=bucket)
        limit = max(1, min(limit, 1000))
        rows = await self._meta.list_objects(bucket, prefix, after, limit + 1)
        truncated = len(rows) > limit
        rows = rows[:limit]
        return {
            "bucket": bucket,
            "prefix": prefix,
            "count": len(rows),
            "truncated": truncated,
            "next_after": rows[-1]["key"] if truncated else None,
            "objects": rows,
        }
