"""Repositorio de metadatos: todas las consultas SQL del sistema.

Regla de oro: los metadatos en PostgreSQL son la fuente de verdad.
  * Un objeto existe  <=> hay una fila en `objects` con status = 'ACTIVE'.
  * Un blob en un nodo es válido <=> hay una fila en `object_replicas`
    (object_id, node_id) con ese blob_id y status = 'OK'.
Todo lo demás que haya en disco es basura (huérfano) y se recolecta.

Los cambios que afectan a varias filas se hacen en una transacción con
bloqueo de la fila del objeto (SELECT ... FOR UPDATE): es el punto de
linealización de cada operación.
"""
from __future__ import annotations

from typing import Iterable

from .db import Database
from .ids import etag_matches, new_ulid

OBJ = (
    "id, bucket, object_key AS key, status, version, blob_id, size, content_type,"
    " checksum, user_metadata, created_at, updated_at, deleted_at"
)
QUEUE_DELETION = (
    "INSERT INTO blob_deletions (blob_id, node_id, reason, label) VALUES ($1, $2, $3, $4)"
    " ON CONFLICT (blob_id, node_id) DO NOTHING"
)


class CommitConflict(Exception):
    pass


class PreconditionFailed(Exception):
    pass


def _escape_like(value: str) -> str:
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


class MetadataRepository:
    def __init__(self, db: Database):
        self._db = db

    # ================================================================ objetos
    async def find_live(self, bucket: str, key: str) -> dict | None:
        return await self._db.fetchrow(
            f"SELECT {OBJ} FROM objects WHERE bucket = $1 AND object_key = $2 AND status <> 'DELETED'",
            bucket, key,
        )

    async def find_active(self, bucket: str, key: str) -> dict | None:
        return await self._db.fetchrow(
            f"SELECT {OBJ} FROM objects WHERE bucket = $1 AND object_key = $2 AND status = 'ACTIVE'",
            bucket, key,
        )

    async def find_active_with_replicas(self, bucket: str, key: str) -> dict | None:
        """Objeto + sus réplicas en UNA sentencia: misma instantánea (snapshot) de
        PostgreSQL, así un commit concurrente no puede dejarnos a medias."""
        return await self._db.fetchrow(
            f"""
            SELECT {OBJ},
                   COALESCE((SELECT jsonb_agg(jsonb_build_object('node_id', r.node_id, 'blob_id', r.blob_id,
                                                                 'status', r.status) ORDER BY r.node_id)
                             FROM object_replicas r WHERE r.object_id = o.id), '[]'::jsonb) AS replicas
            FROM objects o
            WHERE bucket = $1 AND object_key = $2 AND status = 'ACTIVE'
            """,
            bucket, key,
        )

    async def get_object(self, object_id: str) -> dict | None:
        return await self._db.fetchrow(f"SELECT {OBJ} FROM objects WHERE id = $1", object_id)

    async def reserve(self, bucket: str, key: str) -> dict:
        """Obtiene (o crea en estado PENDING) el identificador estable del nombre.

        El id se fija ANTES de escribir datos para que la ubicación (HRW sobre
        el id) sea la misma en todas las versiones del objeto."""
        for _ in range(5):
            await self._db.execute(
                "INSERT INTO objects (id, bucket, object_key, status) VALUES ($1, $2, $3, 'PENDING')"
                " ON CONFLICT (bucket, object_key) WHERE status <> 'DELETED' DO NOTHING",
                new_ulid(), bucket, key,
            )
            row = await self.find_live(bucket, key)
            if row is not None:
                return row
        raise CommitConflict("No se pudo reservar el identificador del objeto")

    async def commit_version(
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
    ) -> tuple[dict, bool]:
        """Publica atómicamente una nueva versión (punto de linealización del PUT).

        Devuelve (objeto, creado). Las réplicas de la versión anterior pasan a la
        cola de borrado en la misma transacción."""
        async with self._db.transaction() as conn:
            cur = await conn.fetchrow(f"SELECT {OBJ} FROM objects WHERE id = $1 FOR UPDATE", object_id)
            if cur is None or cur["status"] == "DELETED":
                raise CommitConflict("El objeto fue eliminado mientras se escribía; reintente el PUT")
            exists = cur["status"] == "ACTIVE"
            if if_none_match is not None and exists and etag_matches(if_none_match, cur["checksum"]):
                raise PreconditionFailed("If-None-Match: el objeto ya existe")
            if if_match is not None and not (exists and etag_matches(if_match, cur["checksum"])):
                raise PreconditionFailed("If-Match: el ETag actual no coincide")

            old = await conn.fetch(
                "DELETE FROM object_replicas WHERE object_id = $1 RETURNING node_id, blob_id", object_id
            )
            label = f"{cur['bucket']}/{cur['key']} v{cur['version']}"
            for row in old:
                await conn.execute(QUEUE_DELETION, row["blob_id"], row["node_id"], "overwritten", label)
            obj = await conn.fetchrow(
                f"""
                UPDATE objects SET status = 'ACTIVE', version = version + 1, blob_id = $2, size = $3,
                       checksum = $4, content_type = $5, user_metadata = $6, updated_at = now()
                WHERE id = $1 RETURNING {OBJ}
                """,
                object_id, blob_id, size, checksum, content_type, user_metadata,
            )
            await conn.executemany(
                "INSERT INTO object_replicas (object_id, node_id, blob_id, status, last_verified_at)"
                " VALUES ($1, $2, $3, 'OK', now())",
                [(object_id, node_id, blob_id) for node_id in node_ids],
            )
            return dict(obj), not exists

    async def mark_deleted(self, bucket: str, key: str) -> dict | None:
        """Borrado lógico inmediato + borrado físico diferido (cola)."""
        async with self._db.transaction() as conn:
            cur = await conn.fetchrow(
                f"SELECT {OBJ} FROM objects WHERE bucket = $1 AND object_key = $2"
                " AND status <> 'DELETED' FOR UPDATE",
                bucket, key,
            )
            if cur is None:
                return None
            old = await conn.fetch(
                "DELETE FROM object_replicas WHERE object_id = $1 RETURNING node_id, blob_id", cur["id"]
            )
            for row in old:
                await conn.execute(QUEUE_DELETION, row["blob_id"], row["node_id"], "object-deleted", f"{bucket}/{key}")
            obj = await conn.fetchrow(
                f"UPDATE objects SET status = 'DELETED', deleted_at = now(), updated_at = now()"
                f" WHERE id = $1 RETURNING {OBJ}",
                cur["id"],
            )
            result = dict(obj)
            result["queued_deletions"] = [r["node_id"] for r in old]
            return result

    async def list_objects(self, bucket: str, prefix: str, after: str | None, limit: int) -> list[dict]:
        return await self._db.fetch(
            """
            SELECT o.id, o.object_key AS key, o.size, o.content_type, o.checksum, o.version, o.updated_at,
                   COALESCE(array_agg(r.node_id ORDER BY r.node_id) FILTER (WHERE r.status = 'OK'),
                            '{}') AS replicas
            FROM objects o LEFT JOIN object_replicas r ON r.object_id = o.id
            WHERE o.bucket = $1 AND o.status = 'ACTIVE' AND o.object_key LIKE $2 ESCAPE '\\'
              AND ($3::text IS NULL OR o.object_key > $3)
            GROUP BY o.id
            ORDER BY o.object_key
            LIMIT $4
            """,
            bucket, _escape_like(prefix) + "%", after, limit,
        )

    async def list_buckets(self) -> list[dict]:
        return await self._db.fetch(
            "SELECT bucket, count(*)::int AS objects, COALESCE(sum(size), 0)::bigint AS bytes"
            " FROM objects WHERE status = 'ACTIVE' GROUP BY bucket ORDER BY bucket"
        )

    async def active_objects_page(self, after_id: str, limit: int) -> list[dict]:
        return await self._db.fetch(
            "SELECT id, bucket, object_key AS key, blob_id, checksum, size, version FROM objects"
            " WHERE status = 'ACTIVE' AND id > $1 ORDER BY id LIMIT $2",
            after_id, limit,
        )

    async def all_active_with_replicas(self) -> list[dict]:
        return await self._db.fetch(
            """
            SELECT o.id, o.blob_id, o.size,
                   COALESCE(jsonb_agg(jsonb_build_object('node_id', r.node_id, 'status', r.status,
                                                         'blob_id', r.blob_id))
                            FILTER (WHERE r.node_id IS NOT NULL), '[]'::jsonb) AS replicas
            FROM objects o LEFT JOIN object_replicas r ON r.object_id = o.id
            WHERE o.status = 'ACTIVE'
            GROUP BY o.id
            """
        )

    async def cleanup_stale_pending(self, grace_s: float) -> int:
        rows = await self._db.fetch(
            "DELETE FROM objects WHERE status = 'PENDING' AND version = 0"
            " AND created_at < now() - make_interval(secs => $1) RETURNING id",
            float(grace_s),
        )
        return len(rows)

    # ================================================================ réplicas
    async def replicas_of(self, object_id: str) -> list[dict]:
        return await self._db.fetch(
            "SELECT node_id, blob_id, status, last_verified_at FROM object_replicas"
            " WHERE object_id = $1 ORDER BY node_id",
            object_id,
        )

    async def replicas_for(self, object_ids: list[str]) -> dict[str, list[dict]]:
        rows = await self._db.fetch(
            "SELECT object_id, node_id, blob_id, status FROM object_replicas WHERE object_id = ANY($1::text[])",
            object_ids,
        )
        result: dict[str, list[dict]] = {oid: [] for oid in object_ids}
        for row in rows:
            result[row["object_id"]].append(row)
        return result

    async def replicas_on_node(self, node_id: str) -> list[dict]:
        return await self._db.fetch(
            """
            SELECT r.object_id, r.blob_id, r.status, o.checksum, o.bucket, o.object_key AS key
            FROM object_replicas r JOIN objects o ON o.id = r.object_id
            WHERE r.node_id = $1
            """,
            node_id,
        )

    async def count_replicas_on_node(self, node_id: str) -> int:
        return await self._db.fetchval("SELECT count(*) FROM object_replicas WHERE node_id = $1", node_id)

    async def set_replica_status(self, object_id: str, node_id: str, blob_id: str, status: str) -> bool:
        """Devuelve True si el estado cambió (evita eventos duplicados)."""
        result = await self._db.execute(
            "UPDATE object_replicas SET status = $4, updated_at = now()"
            " WHERE object_id = $1 AND node_id = $2 AND blob_id = $3 AND status <> $4",
            object_id, node_id, blob_id, status,
        )
        return result.endswith(" 1")

    async def mark_verified(self, node_id: str, object_ids: list[str]) -> None:
        if object_ids:
            await self._db.execute(
                "UPDATE object_replicas SET last_verified_at = now()"
                " WHERE node_id = $1 AND object_id = ANY($2::text[])",
                node_id, object_ids,
            )

    async def add_replica(self, object_id: str, node_id: str, blob_id: str) -> bool:
        """Registra una copia recién creada, sólo si sigue siendo la versión actual."""
        async with self._db.transaction() as conn:
            cur = await conn.fetchrow(
                "SELECT status, blob_id, bucket, object_key FROM objects WHERE id = $1 FOR UPDATE", object_id
            )
            if cur is None or cur["status"] != "ACTIVE" or cur["blob_id"] != blob_id:
                return False  # el objeto cambió o se borró: la copia quedará huérfana y se recolectará
            prev = await conn.fetchrow(
                "SELECT blob_id FROM object_replicas WHERE object_id = $1 AND node_id = $2", object_id, node_id
            )
            if prev is not None and prev["blob_id"] != blob_id:
                await conn.execute(QUEUE_DELETION, prev["blob_id"], node_id, "stale-version",
                                   f"{cur['bucket']}/{cur['object_key']}")
            await conn.execute(
                """
                INSERT INTO object_replicas (object_id, node_id, blob_id, status, last_verified_at)
                VALUES ($1, $2, $3, 'OK', now())
                ON CONFLICT (object_id, node_id) DO UPDATE
                    SET blob_id = EXCLUDED.blob_id, status = 'OK', last_verified_at = now(), updated_at = now()
                """,
                object_id, node_id, blob_id,
            )
            await conn.execute("DELETE FROM blob_deletions WHERE blob_id = $1 AND node_id = $2", blob_id, node_id)
            return True

    async def remove_replica(
        self, object_id: str, node_id: str, blob_id: str, reason: str, min_remaining: int
    ) -> bool:
        """Quita una réplica (y encola su borrado físico) sólo si quedan al menos
        `min_remaining` réplicas OK de la versión actual en otros nodos."""
        async with self._db.transaction() as conn:
            cur = await conn.fetchrow(
                "SELECT blob_id, bucket, object_key FROM objects WHERE id = $1 FOR UPDATE", object_id
            )
            if cur is None:
                return False
            remaining = await conn.fetchval(
                "SELECT count(*) FROM object_replicas WHERE object_id = $1 AND node_id <> $2"
                " AND status = 'OK' AND blob_id = $3",
                object_id, node_id, cur["blob_id"],
            )
            if remaining < min_remaining:
                return False
            row = await conn.fetchrow(
                "DELETE FROM object_replicas WHERE object_id = $1 AND node_id = $2 AND blob_id = $3"
                " RETURNING blob_id",
                object_id, node_id, blob_id,
            )
            if row is None:
                return False
            await conn.execute(QUEUE_DELETION, blob_id, node_id, reason, f"{cur['bucket']}/{cur['object_key']}")
            return True

    # ======================================================= cola de borrados
    async def pending_deletions(self, min_age_s: float, limit: int = 1000) -> list[dict]:
        return await self._db.fetch(
            "SELECT blob_id, node_id, reason, attempts FROM blob_deletions"
            " WHERE created_at < now() - make_interval(secs => $1)"
            " ORDER BY created_at LIMIT $2",
            float(min_age_s), limit,
        )

    async def pending_deletion_blob_ids(self, node_id: str) -> set[str]:
        rows = await self._db.fetch("SELECT blob_id FROM blob_deletions WHERE node_id = $1", node_id)
        return {r["blob_id"] for r in rows}

    async def is_referenced(self, blob_id: str, node_id: str) -> bool:
        return bool(
            await self._db.fetchval(
                "SELECT EXISTS (SELECT 1 FROM object_replicas WHERE blob_id = $1 AND node_id = $2)",
                blob_id, node_id,
            )
        )

    async def complete_deletion(self, blob_id: str, node_id: str) -> None:
        await self._db.execute("DELETE FROM blob_deletions WHERE blob_id = $1 AND node_id = $2", blob_id, node_id)

    async def fail_deletion(self, blob_id: str, node_id: str, error: str) -> None:
        await self._db.execute(
            "UPDATE blob_deletions SET attempts = attempts + 1, last_error = $3"
            " WHERE blob_id = $1 AND node_id = $2",
            blob_id, node_id, error[:500],
        )

    async def drop_deletions_for_node(self, node_id: str) -> None:
        await self._db.execute("DELETE FROM blob_deletions WHERE node_id = $1", node_id)

    # ===================================================== vista para la UI
    async def objects_for_map(self, limit: int) -> list[dict]:
        return await self._db.fetch(
            """
            SELECT o.id, o.bucket, o.object_key AS key, o.version, o.size, o.blob_id, o.content_type,
                   o.updated_at,
                   COALESCE((SELECT jsonb_agg(jsonb_build_object('node_id', r.node_id, 'status', r.status,
                                                                 'blob_id', r.blob_id) ORDER BY r.node_id)
                             FROM object_replicas r WHERE r.object_id = o.id), '[]'::jsonb) AS replicas
            FROM objects o
            WHERE o.status = 'ACTIVE'
            ORDER BY o.updated_at DESC
            LIMIT $1
            """,
            limit,
        )

    async def count_active(self) -> int:
        return await self._db.fetchval("SELECT count(*) FROM objects WHERE status = 'ACTIVE'")

    async def pending_deletions_detail(self, limit: int = 300) -> list[dict]:
        return await self._db.fetch(
            "SELECT blob_id, node_id, reason, label, attempts FROM blob_deletions ORDER BY created_at LIMIT $1",
            limit,
        )

    async def deletion_backlog(self) -> list[dict]:
        return await self._db.fetch(
            "SELECT node_id, count(*)::int AS pending FROM blob_deletions GROUP BY node_id ORDER BY node_id"
        )
