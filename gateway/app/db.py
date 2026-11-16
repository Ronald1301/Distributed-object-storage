"""Acceso a PostgreSQL (servicio de metadatos).

PostgreSQL es la fuente de verdad de los metadatos: qué objetos existen,
cuál es su versión actual (blob + checksum) y dónde están sus réplicas.
El esquema se aplica al arrancar el gateway (idempotente).
"""
from __future__ import annotations

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

import asyncpg

log = logging.getLogger("p16.db")

SCHEMA = """
-- Miembros del clúster (Storage Nodes)
CREATE TABLE IF NOT EXISTS nodes (
    node_id              TEXT PRIMARY KEY,
    url                  TEXT NOT NULL,
    weight               DOUBLE PRECISION NOT NULL DEFAULT 1 CHECK (weight > 0),
    membership           TEXT NOT NULL DEFAULT 'ACTIVE'
                         CHECK (membership IN ('ACTIVE', 'DRAINING', 'DECOMMISSIONED')),
    health               TEXT NOT NULL DEFAULT 'HEALTHY'
                         CHECK (health IN ('HEALTHY', 'SUSPECT', 'DOWN', 'RECOVERING')),
    consecutive_failures INT NOT NULL DEFAULT 0,
    last_seen_at         TIMESTAMPTZ,
    down_since           TIMESTAMPTZ,
    joined_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    used_bytes           BIGINT,
    blob_count           INT
);

-- Objetos lógicos. id = ULID estable; (bucket, object_key) = nombre visible.
CREATE TABLE IF NOT EXISTS objects (
    id            TEXT PRIMARY KEY,
    bucket        TEXT NOT NULL,
    object_key    TEXT NOT NULL,
    status        TEXT NOT NULL CHECK (status IN ('PENDING', 'ACTIVE', 'DELETED')),
    version       INT  NOT NULL DEFAULT 0,
    blob_id       TEXT,              -- blob inmutable de la versión actual
    size          BIGINT,
    content_type  TEXT,
    checksum      TEXT,              -- SHA-256 (hex) de la versión actual
    user_metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at    TIMESTAMPTZ
);
-- Sólo puede haber un objeto "vivo" por nombre; los borrados quedan como lápida.
CREATE UNIQUE INDEX IF NOT EXISTS objects_live_name
    ON objects (bucket, object_key) WHERE status <> 'DELETED';

-- Ubicación real de las réplicas (una fila por objeto y nodo).
CREATE TABLE IF NOT EXISTS object_replicas (
    object_id        TEXT NOT NULL REFERENCES objects(id) ON DELETE CASCADE,
    node_id          TEXT NOT NULL REFERENCES nodes(node_id),
    blob_id          TEXT NOT NULL,
    status           TEXT NOT NULL CHECK (status IN ('OK', 'CORRUPT', 'MISSING')),
    last_verified_at TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (object_id, node_id)
);
CREATE INDEX IF NOT EXISTS object_replicas_node ON object_replicas (node_id);
CREATE INDEX IF NOT EXISTS object_replicas_blob ON object_replicas (blob_id);

-- Cola persistente de borrados físicos pendientes (patrón "outbox").
CREATE TABLE IF NOT EXISTS blob_deletions (
    blob_id    TEXT NOT NULL,
    node_id    TEXT NOT NULL,
    reason     TEXT NOT NULL,
    label      TEXT,              -- "bucket/clave vN" (sólo informativo)
    attempts   INT  NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (blob_id, node_id)
);
ALTER TABLE blob_deletions ADD COLUMN IF NOT EXISTS label TEXT;
CREATE INDEX IF NOT EXISTS objects_blob ON objects (blob_id);

-- Registro de eventos del clúster (para observabilidad y la demo).
CREATE TABLE IF NOT EXISTS cluster_events (
    id        BIGSERIAL PRIMARY KEY,
    ts        TIMESTAMPTZ NOT NULL DEFAULT now(),
    type      TEXT NOT NULL,
    node_id   TEXT,
    object_id TEXT,
    message   TEXT NOT NULL,
    data      JSONB
);
"""

SCHEMA_LOCK = 16_000_001
RECONCILER_LOCK = 16_000_002


async def _init_connection(conn: asyncpg.Connection) -> None:
    await conn.set_type_codec("jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog")


class Database:
    def __init__(self, dsn: str):
        self._dsn = dsn
        self._pool: asyncpg.Pool | None = None

    @property
    def pool(self) -> asyncpg.Pool:
        assert self._pool is not None, "Base de datos no inicializada"
        return self._pool

    async def connect(self, attempts: int = 60) -> None:
        for attempt in range(1, attempts + 1):
            try:
                self._pool = await asyncpg.create_pool(
                    self._dsn, min_size=1, max_size=10, init=_init_connection
                )
                async with self._pool.acquire() as conn:
                    async with conn.transaction():
                        await conn.execute("SELECT pg_advisory_xact_lock($1)", SCHEMA_LOCK)
                        await conn.execute(SCHEMA)
                log.info("Esquema de metadatos listo")
                return
            except (OSError, asyncpg.PostgresError, asyncpg.InterfaceError) as exc:
                if self._pool is not None:
                    await self._pool.close()
                    self._pool = None
                if attempt == attempts:
                    raise
                log.warning("PostgreSQL no disponible (intento %d): %s", attempt, exc)
                await asyncio.sleep(2)

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()

    async def fetch(self, sql: str, *args: Any) -> list[dict]:
        return [dict(r) for r in await self.pool.fetch(sql, *args)]

    async def fetchrow(self, sql: str, *args: Any) -> dict | None:
        row = await self.pool.fetchrow(sql, *args)
        return dict(row) if row is not None else None

    async def fetchval(self, sql: str, *args: Any) -> Any:
        return await self.pool.fetchval(sql, *args)

    async def execute(self, sql: str, *args: Any) -> str:
        return await self.pool.execute(sql, *args)

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[asyncpg.Connection]:
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                yield conn

    @asynccontextmanager
    async def advisory_lock(self, key: int) -> AsyncIterator[bool]:
        """Exclusión mutua entre procesos delegada en PostgreSQL (sin consenso propio)."""
        async with self.pool.acquire() as conn:
            acquired = await conn.fetchval("SELECT pg_try_advisory_lock($1)", key)
            try:
                yield bool(acquired)
            finally:
                if acquired:
                    await conn.execute("SELECT pg_advisory_unlock($1)", key)
