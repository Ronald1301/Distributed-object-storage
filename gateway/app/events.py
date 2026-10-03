"""Registro de eventos del clúster (fallos, reparaciones, corrupción...)."""
from __future__ import annotations

import logging
from typing import Any

from .db import Database
from .live import LiveFeed

log = logging.getLogger("p16.events")


class EventLog:
    def __init__(self, db: Database, feed: LiveFeed):
        self._db = db
        self._feed = feed

    async def record(
        self,
        type_: str,
        message: str,
        *,
        node_id: str | None = None,
        object_id: str | None = None,
        data: Any = None,
    ) -> None:
        log.info("[%s] %s", type_, message)
        self._feed.push("event", type=type_, message=message, node_id=node_id, object_id=object_id, data=data)
        try:
            await self._db.execute(
                "INSERT INTO cluster_events (type, node_id, object_id, message, data)"
                " VALUES ($1, $2, $3, $4, $5)",
                type_, node_id, object_id, message, data,
            )
        except Exception as exc:  # el registro de eventos nunca debe romper una operación
            log.warning("No se pudo registrar el evento %s: %s", type_, exc)

    async def recent(self, limit: int = 50, type_prefix: str | None = None) -> list[dict]:
        return await self._db.fetch(
            "SELECT id, ts, type, node_id, object_id, message, data FROM cluster_events"
            " WHERE ($1::text IS NULL OR type LIKE $1 || '%')"
            " ORDER BY id DESC LIMIT $2",
            type_prefix, limit,
        )
