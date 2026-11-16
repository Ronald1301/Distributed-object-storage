"""Detector de fallos: sondeo periódico (pull heartbeat) de /health.

No usamos consenso: el gateway es el único observador y decide el estado de
cada nodo con un contador de fallos consecutivos:

    1 fallo  (SUSPECT_AFTER) -> SUSPECT : deja de recibir escrituras nuevas
    3 fallos (DOWN_AFTER)    -> DOWN    : se excluye de lecturas y escrituras
    respuesta tras DOWN      -> RECOVERING -> (reconciliación) -> HEALTHY

Además, cada operación del plano de datos tiene timeout; un error de red en
una petición real marca el nodo como SUSPECT al instante (detección pasiva).
"""
from __future__ import annotations

import asyncio
import logging

from .config import Settings
from .membership import DECOMMISSIONED, Membership, NodeState
from .storage_client import StorageClient, StorageError

log = logging.getLogger("p16.health")


class HealthMonitor:
    def __init__(self, membership: Membership, storage: StorageClient, settings: Settings):
        self._membership = membership
        self._storage = storage
        self._settings = settings

    async def run_forever(self) -> None:
        loop = asyncio.get_running_loop()
        while True:
            started = loop.time()
            try:
                await self.tick()
            except Exception:  # el detector nunca debe morir
                log.exception("Error en el ciclo de health checks")
            elapsed = loop.time() - started
            await asyncio.sleep(max(0.0, self._settings.health_interval_s - elapsed))

    async def tick(self) -> None:
        nodes = [n for n in self._membership.all() if n.membership != DECOMMISSIONED]
        await asyncio.gather(*(self._probe(n) for n in nodes))

    async def _probe(self, node: NodeState) -> None:
        try:
            info = await self._storage.health(node)
            if info.get("node") != node.node_id:
                raise StorageError(f"identidad inesperada: responde {info.get('node')!r}")
        except StorageError as exc:
            await self._membership.record_probe_failure(node.node_id, str(exc))
            return
        await self._membership.record_probe_success(node.node_id, info)
