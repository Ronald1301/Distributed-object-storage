"""Membresía del clúster y máquina de estados de salud de los nodos.

Dos dimensiones independientes por nodo:

  membership (decisión administrativa)       health (observada por el detector)
  ─────────────────────────────────────       ─────────────────────────────────
  ACTIVE          participa en la ubicación   HEALTHY     responde
  DRAINING        se están sacando sus datos  SUSPECT     falló algún sondeo / petición
  DECOMMISSIONED  retirado, ya sin datos      DOWN        falló DOWN_AFTER sondeos seguidos
                                              RECOVERING  volvió tras DOWN; pendiente de
                                                          reconciliación

El estado vive en memoria (el gateway es el único detector de fallos) y se
persiste en la tabla `nodes` para observabilidad y reinicios.
"""
from __future__ import annotations

import logging
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Awaitable, Callable

import asyncpg

from .config import Settings
from .db import Database
from .events import EventLog
from .placement import Candidate

log = logging.getLogger("p16.membership")

HEALTHY, SUSPECT, DOWN, RECOVERING = "HEALTHY", "SUSPECT", "DOWN", "RECOVERING"
ACTIVE, DRAINING, DECOMMISSIONED = "ACTIVE", "DRAINING", "DECOMMISSIONED"

_READ_ORDER = {HEALTHY: 0, RECOVERING: 1, SUSPECT: 2, DOWN: 3}


def _now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class NodeState:
    node_id: str
    url: str
    weight: float
    membership: str
    health: str
    consecutive_failures: int
    last_seen_at: datetime | None
    down_since: datetime | None
    joined_at: datetime
    used_bytes: int | None
    blob_count: int | None

    def as_dict(self) -> dict:
        return asdict(self)


class Membership:
    def __init__(self, db: Database, events: EventLog, settings: Settings):
        self._db = db
        self._events = events
        self._settings = settings
        self._nodes: dict[str, NodeState] = {}
        self._recovering_listeners: list[Callable[[str], Awaitable[None] | None]] = []

    # ------------------------------------------------------------------ lectura
    async def load(self) -> None:
        rows = await self._db.fetch("SELECT * FROM nodes ORDER BY node_id")
        self._nodes = {r["node_id"]: NodeState(**r) for r in rows}
        log.info("Membresía cargada: %s", ", ".join(self._nodes) or "(vacía)")

    def all(self) -> list[NodeState]:
        return sorted(self._nodes.values(), key=lambda n: n.node_id)

    def get(self, node_id: str) -> NodeState | None:
        return self._nodes.get(node_id)

    def placement_candidates(self) -> list[Candidate]:
        """Nodos que entran en la ubicación deseada (HRW). La salud NO influye:
        un fallo transitorio no debe cambiar dónde "deberían" estar los datos."""
        return [Candidate(n.node_id, n.weight) for n in self.all() if n.membership == ACTIVE]

    def is_readable(self, node_id: str) -> bool:
        n = self._nodes.get(node_id)
        return n is not None and n.membership != DECOMMISSIONED and n.health != DOWN

    def is_writable(self, node_id: str) -> bool:
        n = self._nodes.get(node_id)
        return n is not None and n.membership != DECOMMISSIONED and n.health in (HEALTHY, RECOVERING)

    def counts_for_durability(self, node_id: str) -> bool:
        """¿Cuenta una réplica en este nodo para el factor de replicación?

        Un nodo DOWN sigue contando durante REPAIR_DELAY (puede ser un reinicio);
        pasado ese tiempo se considera perdido y sus datos se re-replican.
        """
        n = self._nodes.get(node_id)
        if n is None or n.membership == DECOMMISSIONED:
            return False
        if n.health != DOWN:
            return True
        since = n.down_since or _now()
        return (_now() - since).total_seconds() < self._settings.repair_delay_s

    def read_order(self, node_id: str) -> int:
        n = self._nodes.get(node_id)
        return _READ_ORDER.get(n.health, 9) if n else 9

    def on_recovering(self, listener: Callable[[str], Awaitable[None] | None]) -> None:
        self._recovering_listeners.append(listener)

    # --------------------------------------------------------- cambios de membresía
    async def join(self, node_id: str, url: str, weight: float) -> NodeState:
        row = await self._db.fetchrow(
            """
            INSERT INTO nodes (node_id, url, weight, membership, health, last_seen_at)
            VALUES ($1, $2, $3, 'ACTIVE', 'HEALTHY', now())
            ON CONFLICT (node_id) DO UPDATE SET
                url = EXCLUDED.url,
                membership = CASE WHEN nodes.membership = 'DECOMMISSIONED'
                                  THEN 'ACTIVE' ELSE nodes.membership END
            RETURNING *, (xmax = 0) AS inserted
            """,
            node_id, url, weight,
        )
        assert row is not None
        inserted = row.pop("inserted")
        previous = self._nodes.get(node_id)
        state = NodeState(**row)
        self._nodes[node_id] = state
        if inserted:
            await self._events.record(
                "node.joined",
                f"Nuevo nodo {node_id} ({url}, peso {weight}) se une al clúster",
                node_id=node_id,
            )
        elif previous is not None and previous.membership == DECOMMISSIONED:
            await self._events.record("node.rejoined", f"{node_id} vuelve a ser miembro ACTIVE", node_id=node_id)
        else:
            log.info("%s re-anunciado (%s)", node_id, url)
        return state

    async def set_membership(self, node_id: str, membership: str) -> NodeState:
        node = self._require(node_id)
        if node.membership == membership:
            return node
        await self._db.execute("UPDATE nodes SET membership = $2 WHERE node_id = $1", node_id, membership)
        old = node.membership
        node.membership = membership
        await self._events.record(
            f"node.{membership.lower()}", f"{node_id}: membresía {old} -> {membership}", node_id=node_id
        )
        return node

    async def set_weight(self, node_id: str, weight: float) -> NodeState:
        node = self._require(node_id)
        await self._db.execute("UPDATE nodes SET weight = $2 WHERE node_id = $1", node_id, weight)
        old = node.weight
        node.weight = weight
        await self._events.record("node.weight", f"{node_id}: peso {old} -> {weight}", node_id=node_id)
        return node

    # ------------------------------------------------------ detector de fallos
    async def record_probe_success(self, node_id: str, info: dict) -> None:
        node = self._nodes.get(node_id)
        if node is None:
            return
        new_health = node.health
        if node.health == DOWN:
            new_health = RECOVERING
        elif node.health == SUSPECT:
            new_health = HEALTHY
        await self._apply(
            node,
            health=new_health,
            consecutive_failures=0,
            last_seen_at=_now(),
            down_since=None,
            used_bytes=info.get("used_bytes"),
            blob_count=info.get("blob_count"),
        )

    async def record_probe_failure(self, node_id: str, reason: str) -> None:
        node = self._nodes.get(node_id)
        if node is None:
            return
        failures = node.consecutive_failures + 1
        if node.health == DOWN or failures >= self._settings.down_after:
            new_health = DOWN
        elif failures >= self._settings.suspect_after:
            new_health = SUSPECT
        else:
            new_health = node.health
        down_since = node.down_since if node.health == DOWN else (_now() if new_health == DOWN else None)
        await self._apply(
            node, health=new_health, consecutive_failures=failures, down_since=down_since, reason=reason
        )

    async def report_data_path_failure(self, node_id: str, reason: str) -> None:
        """Detección pasiva: un error de red en una operación real marca el nodo
        como SUSPECT inmediatamente, sin esperar al siguiente sondeo."""
        node = self._nodes.get(node_id)
        if node is not None and node.health == HEALTHY:
            await self._apply(node, health=SUSPECT, reason=f"fallo en operación de datos: {reason}")

    async def mark_healthy(self, node_id: str) -> None:
        node = self._nodes.get(node_id)
        if node is not None and node.health == RECOVERING:
            await self._apply(node, health=HEALTHY, reason="reconciliación completada")

    # ------------------------------------------------------------------ interno
    def _require(self, node_id: str) -> NodeState:
        node = self._nodes.get(node_id)
        if node is None:
            raise KeyError(node_id)
        return node

    async def _apply(self, node: NodeState, *, reason: str | None = None, **changes) -> None:
        old_health = node.health
        for key, value in changes.items():
            setattr(node, key, value)
        try:  # la memoria es la referencia; la persistencia es best-effort
            await self._db.execute(
                """
                UPDATE nodes SET health = $2, consecutive_failures = $3, last_seen_at = $4,
                                 down_since = $5, used_bytes = $6, blob_count = $7
                WHERE node_id = $1
                """,
                node.node_id, node.health, node.consecutive_failures, node.last_seen_at,
                node.down_since, node.used_bytes, node.blob_count,
            )
        except (OSError, asyncpg.PostgresError, asyncpg.InterfaceError) as exc:
            log.warning("No se pudo persistir el estado de %s: %s", node.node_id, exc)
        if node.health == old_health:
            return
        suffix = f" ({reason})" if reason else ""
        await self._events.record(
            f"node.{node.health.lower()}",
            f"{node.node_id}: {old_health} -> {node.health}{suffix}",
            node_id=node.node_id,
        )
        if node.health == RECOVERING:
            for listener in self._recovering_listeners:
                result = listener(node.node_id)
                if result is not None:
                    await result
