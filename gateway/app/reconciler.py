"""Reconciliación del estado distribuido (anti-entropía).

Un único bucle de control hace converger el estado REAL (qué hay en cada
nodo) hacia el estado DESEADO (lo que dicen los metadatos + HRW). Con la
misma lógica se resuelven reparación, re-replicación, reequilibrado,
drenado de nodos y recolección de basura:

  Fase 1  Inventario    : cada nodo alcanzable lista sus blobs; se comparan
                          con los metadatos -> réplicas ausentes (MISSING) y
                          blobs huérfanos (sin metadatos -> GC tras gracia).
  Fase 2  Convergencia  : por cada objeto, asegurar copias válidas en sus RF
                          nodos deseados (copiando nodo->nodo desde una
                          réplica válida) y, sólo entonces, retirar sobrantes.
  Fase 3  Cola de borrado: borrados físicos pendientes (DELETE, versiones
                          sobrescritas, réplicas sobrantes) en nodos accesibles.
  Fase 4  Reservas PENDING caducadas (PUT que nunca llegó a confirmar).
  Fase 5  Membresía     : RECOVERING -> HEALTHY; DRAINING vacío -> DECOMMISSIONED.

El scrub profundo pide a cada nodo recalcular el SHA-256 de sus blobs para
detectar corrupción silenciosa aunque nadie lea el objeto.
"""
from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import AsyncIterator, Coroutine

import asyncpg

from .config import Settings
from .db import RECONCILER_LOCK, Database
from .errors import ApiError
from .events import EventLog
from .membership import (
    DECOMMISSIONED,
    DRAINING,
    HEALTHY,
    RECOVERING,
    Membership,
)
from .metadata import MetadataRepository
from .placement import desired_nodes, preference_list, rank_nodes
from .storage_client import StorageClient, StorageError

log = logging.getLogger("p16.reconciler")

_DB_DOWN = (OSError, asyncpg.PostgresConnectionError, asyncpg.InterfaceError)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


@dataclass
class Budget:
    remaining: int


@dataclass
class CycleReport:
    trigger: str
    started_at: str = field(default_factory=_now_iso)
    finished_at: str | None = None
    nodes_synced: list[str] = field(default_factory=list)
    nodes_skipped: dict[str, str] = field(default_factory=dict)
    objects_checked: int = 0
    missing_detected: int = 0
    copies: dict[str, int] = field(default_factory=dict)  # por motivo
    replicas_removed: int = 0
    deletions_done: int = 0
    deletions_failed: int = 0
    orphans_deleted: int = 0
    orphans_waiting: int = 0
    stale_reservations: int = 0
    unavailable_objects: int = 0
    deferred_copies: int = 0
    errors: list[str] = field(default_factory=list)

    def add_copy(self, reason: str) -> None:
        self.copies[reason] = self.copies.get(reason, 0) + 1

    def has_activity(self) -> bool:
        return bool(
            self.missing_detected or self.copies or self.replicas_removed or self.deletions_done
            or self.orphans_deleted or self.stale_reservations or self.errors
        )

    def summary(self) -> str:
        parts = [f"{n} {reason}" for reason, n in sorted(self.copies.items())]
        if self.replicas_removed:
            parts.append(f"{self.replicas_removed} réplicas sobrantes retiradas")
        if self.missing_detected:
            parts.append(f"{self.missing_detected} réplicas ausentes detectadas")
        if self.deletions_done:
            parts.append(f"{self.deletions_done} borrados físicos")
        if self.orphans_deleted:
            parts.append(f"{self.orphans_deleted} huérfanos recolectados")
        if self.deferred_copies:
            parts.append(f"{self.deferred_copies} copias aplazadas (límite por ciclo)")
        if self.errors:
            parts.append(f"{len(self.errors)} errores")
        return ", ".join(parts) or "sin cambios"


def _pct(part: int, total: int) -> str:
    return f"{(100.0 * part / total):.1f}%" if total else "0.0%"


class Reconciler:
    def __init__(
        self,
        db: Database,
        meta: MetadataRepository,
        membership: Membership,
        storage: StorageClient,
        events: EventLog,
        settings: Settings,
    ):
        self._db = db
        self._meta = meta
        self._membership = membership
        self._storage = storage
        self._events = events
        self._s = settings
        self._lock = asyncio.Lock()
        self._pending_repairs: set[str] = set()
        self._repair_scheduled = False
        self._gc_scheduled = False
        self._unavailable_reported: set[str] = set()
        self._background: set[asyncio.Task] = set()
        self.last_cycle: dict | None = None
        self.last_scrub: dict | None = None

    # ============================================================ planificación
    def spawn(self, coro: Coroutine) -> asyncio.Task:
        async def guarded() -> None:
            try:
                await coro
            except Exception:
                log.exception("Error en tarea de fondo")

        task = asyncio.create_task(guarded())
        self._background.add(task)
        task.add_done_callback(self._background.discard)
        return task

    async def run_forever(self) -> None:
        while True:
            await asyncio.sleep(self._s.reconcile_interval_s)
            try:
                await self.run_cycle("periodic")
            except _DB_DOWN as exc:
                log.warning("Reconciliación aplazada: metadatos no disponibles (%s)", exc)
            except Exception:
                log.exception("Error en el ciclo de reconciliación")

    async def scrub_forever(self) -> None:
        while True:
            await asyncio.sleep(self._s.scrub_interval_s)
            try:
                await self.scrub("periodic")
            except _DB_DOWN as exc:
                log.warning("Scrub aplazado: metadatos no disponibles (%s)", exc)
            except Exception:
                log.exception("Error en el scrub periódico")

    def request_cycle(self, trigger: str) -> None:
        self.spawn(self.run_cycle(trigger))

    def repair_soon(self, object_id: str) -> None:
        """Reparación inmediata de un objeto (p. ej. read-repair tras un GET)."""
        self._pending_repairs.add(object_id)
        if not self._repair_scheduled:
            self._repair_scheduled = True
            self.spawn(self._drain_repairs())

    def gc_soon(self) -> None:
        if not self._gc_scheduled:
            self._gc_scheduled = True
            self.spawn(self._gc_now())

    @asynccontextmanager
    async def _exclusive(self) -> AsyncIterator[bool]:
        """Un solo trabajo de reconciliación a la vez: asyncio.Lock dentro del
        proceso y advisory lock de PostgreSQL entre procesos."""
        async with self._lock:
            async with self._db.advisory_lock(RECONCILER_LOCK) as acquired:
                yield acquired

    async def _drain_repairs(self) -> None:
        await asyncio.sleep(0.2)
        async with self._exclusive() as acquired:
            self._repair_scheduled = False
            ids = list(self._pending_repairs)
            self._pending_repairs.clear()
            if not acquired or not ids:
                return
            report = CycleReport("read-repair")
            await self._converge_ids(ids, report)
            await self._process_deletions(report)
            if report.has_activity():
                log.info("read-repair: %s", report.summary())

    async def _gc_now(self) -> None:
        await asyncio.sleep(self._s.delete_grace_s + 0.5)
        async with self._exclusive() as acquired:
            self._gc_scheduled = False
            if acquired:
                report = CycleReport("gc")
                await self._process_deletions(report)

    # ================================================================== ciclo
    async def run_cycle(self, trigger: str = "manual") -> dict:
        async with self._exclusive() as acquired:
            if not acquired:
                return {"trigger": trigger, "skipped": "otro proceso está reconciliando"}
            report = CycleReport(trigger)
            try:
                await self._cycle(report)
            except Exception as exc:  # no dejar el sistema a medias por un error inesperado
                log.exception("Ciclo de reconciliación abortado")
                report.errors.append(f"ciclo abortado: {exc}")
            report.finished_at = _now_iso()
            self.last_cycle = asdict(report)
            if report.has_activity():
                await self._events.record(
                    "reconcile.summary", f"Reconciliación ({trigger}): {report.summary()}", data=self.last_cycle
                )
            return self.last_cycle

    async def _cycle(self, report: CycleReport) -> None:
        # Fase 1: inventario (nodos accesibles)
        for node in self._membership.all():
            if node.membership == DECOMMISSIONED:
                continue
            if node.health not in (HEALTHY, RECOVERING):
                report.nodes_skipped[node.node_id] = node.health
                continue
            try:
                await self._sync_node(node.node_id, report)
                report.nodes_synced.append(node.node_id)
            except StorageError as exc:
                report.errors.append(f"inventario de {node.node_id}: {exc}")

        # Fase 2: convergencia objeto a objeto
        budget = Budget(self._s.max_copies_per_cycle)
        after = ""
        while True:
            page = await self._meta.active_objects_page(after, 500)
            if not page:
                break
            replicas = await self._meta.replicas_for([o["id"] for o in page])
            for obj in page:
                await self._converge(obj, replicas[obj["id"]], report, budget)
            after = page[-1]["id"]

        # Fase 3: borrados físicos pendientes
        await self._process_deletions(report)

        # Fase 4: reservas de PUT que nunca se confirmaron
        report.stale_reservations = await self._meta.cleanup_stale_pending(self._s.orphan_grace_s)

        # Fase 5: transiciones de membresía
        for node_id in report.nodes_synced:
            await self._membership.mark_healthy(node_id)  # sólo afecta a RECOVERING
        for node in self._membership.all():
            if node.membership != DRAINING:
                continue
            if await self._meta.count_replicas_on_node(node.node_id) > 0:
                continue
            pending = await self._meta.pending_deletion_blob_ids(node.node_id)
            if pending and self._membership.is_writable(node.node_id):
                continue  # aún se están borrando sus blobs
            await self._meta.drop_deletions_for_node(node.node_id)
            await self._membership.set_membership(node.node_id, DECOMMISSIONED)

    # ---------------------------------------------------------- fase 1
    async def _sync_node(self, node_id: str, report: CycleReport) -> None:
        node = self._membership.get(node_id)
        assert node is not None
        on_disk = {b["blob_id"]: b for b in await self._storage.list_blobs(node)}
        rows = await self._meta.replicas_on_node(node_id)

        for row in rows:
            if row["status"] == "OK" and row["blob_id"] not in on_disk:
                if await self._meta.set_replica_status(row["object_id"], node_id, row["blob_id"], "MISSING"):
                    report.missing_detected += 1
                    await self._events.record(
                        "replica.missing",
                        f"{node_id} no tiene la réplica de {row['bucket']}/{row['key']} (blob {row['blob_id']})",
                        node_id=node_id,
                        object_id=row["object_id"],
                    )

        referenced = {r["blob_id"] for r in rows}
        queued = await self._meta.pending_deletion_blob_ids(node_id)
        for blob_id, info in on_disk.items():
            if blob_id in referenced or blob_id in queued:
                continue
            # La edad la calcula el propio nodo: no dependemos de relojes sincronizados.
            if info["age_s"] < self._s.orphan_grace_s:
                report.orphans_waiting += 1  # puede ser un PUT en curso
                continue
            await self._storage.delete_blob(node, blob_id)
            report.orphans_deleted += 1
            await self._events.record(
                "gc.orphan",
                f"{node_id}: blob huérfano {blob_id} eliminado (sin metadatos desde hace {int(info['age_s'])} s)",
                node_id=node_id,
            )

    # ---------------------------------------------------------- fase 2
    async def _converge_ids(self, ids: list[str], report: CycleReport) -> None:
        replicas = await self._meta.replicas_for(ids)
        for object_id in ids:
            obj = await self._meta.get_object(object_id)
            if obj is not None and obj["status"] == "ACTIVE":
                await self._converge(obj, replicas[object_id], report, Budget(10**9))

    async def _converge(self, obj: dict, replicas: list[dict], report: CycleReport, budget: Budget) -> None:
        rf = self._s.replication_factor
        blob = obj["blob_id"]
        label = f"{obj['bucket']}/{obj['key']}"
        report.objects_checked += 1

        preference = preference_list(obj["id"], self._membership.placement_candidates())
        desired = preference[:rf]
        rank = {n: i for i, n in enumerate(preference)}

        good = [r["node_id"] for r in replicas if r["status"] == "OK" and r["blob_id"] == blob]
        sources = sorted(
            (n for n in good if self._membership.is_readable(n)),
            key=lambda n: (self._membership.read_order(n), rank.get(n, 99)),
        )
        durable = [n for n in good if self._membership.counts_for_durability(n)]

        if not sources:
            report.unavailable_objects += 1
            if obj["id"] not in self._unavailable_reported:
                self._unavailable_reported.add(obj["id"])
                await self._events.record(
                    "object.unavailable",
                    f"{label}: ninguna réplica válida accesible (registradas: "
                    f"{', '.join(r['node_id'] + ':' + r['status'] for r in replicas) or 'ninguna'})",
                    object_id=obj["id"],
                )
            return
        self._unavailable_reported.discard(obj["id"])

        # 1) destinos: nodos deseados accesibles sin copia válida...
        targets: list[tuple[str, str]] = []
        for node_id in desired:
            if node_id not in good and self._membership.is_writable(node_id):
                targets.append((node_id, self._reason(replicas, node_id, len(durable), good)))
        # ...y, si un deseado lleva demasiado tiempo caído, los siguientes del ranking
        expected = len(durable) + len(targets)
        for node_id in preference[rf:]:
            if expected >= rf:
                break
            if node_id not in good and self._membership.is_writable(node_id):
                targets.append((node_id, "re-replication"))
                expected += 1

        for node_id, reason in targets:
            if not sources:
                break
            if budget.remaining <= 0:
                report.deferred_copies += 1
                continue
            budget.remaining -= 1
            await self._copy(obj, sources, node_id, reason, report, label)

        # 2) retirar réplicas sobrantes, sólo con la ubicación deseada completa
        if targets:
            replicas = await self._meta.replicas_of(obj["id"])
        good_now = {r["node_id"] for r in replicas if r["status"] == "OK" and r["blob_id"] == blob}
        desired_ok = [n for n in desired if n in good_now and self._membership.is_readable(n)]
        for row in replicas:
            node_id = row["node_id"]
            if node_id in desired:
                continue
            bad = row["status"] != "OK" or row["blob_id"] != blob
            if not bad and len(desired_ok) < rf:
                continue  # esta copia todavía es necesaria para mantener RF
            node = self._membership.get(node_id)
            reason = "bad-replica" if bad else ("drain" if node and node.membership == DRAINING else "rebalance")
            if await self._meta.remove_replica(obj["id"], node_id, row["blob_id"], reason, 0 if bad else rf):
                report.replicas_removed += 1

    def _reason(self, replicas: list[dict], node_id: str, durable_count: int, good: list[str]) -> str:
        row = next((r for r in replicas if r["node_id"] == node_id), None)
        if row is not None and row["status"] == "CORRUPT":
            return "repair-corrupt"
        if row is not None and row["status"] == "MISSING":
            return "repair-missing"
        if durable_count < self._s.replication_factor:
            return "re-replication"
        if any((n := self._membership.get(g)) and n.membership == DRAINING for g in good):
            return "drain"
        return "rebalance"

    async def _copy(
        self, obj: dict, sources: list[str], target_id: str, reason: str, report: CycleReport, label: str
    ) -> bool:
        target = self._membership.get(target_id)
        assert target is not None
        for source_id in list(sources):
            source = self._membership.get(source_id)
            assert source is not None
            try:
                await self._storage.push_blob(source, obj["blob_id"], target, obj["checksum"], obj["id"])
            except StorageError as exc:
                if exc.status == 409:  # la copia de origen también está corrupta
                    sources.remove(source_id)
                    if await self._meta.set_replica_status(obj["id"], source_id, obj["blob_id"], "CORRUPT"):
                        await self._events.record(
                            "integrity.corrupt",
                            f"{label}: la copia de {source_id} está corrupta (detectado al copiarla)",
                            node_id=source_id, object_id=obj["id"], data={"detected_by": "push"},
                        )
                    continue
                if exc.status == 404:
                    sources.remove(source_id)
                    await self._meta.set_replica_status(obj["id"], source_id, obj["blob_id"], "MISSING")
                    continue
                if exc.status == 502:  # el problema es el destino
                    report.errors.append(f"copia de {label} hacia {target_id}: {exc}")
                    return False
                report.errors.append(f"origen {source_id} no disponible: {exc}")
                if exc.kind == "network":
                    await self._membership.report_data_path_failure(source_id, str(exc))
                continue
            if await self._meta.add_replica(obj["id"], target_id, obj["blob_id"]):
                report.add_copy(reason)
                await self._events.record(
                    f"replica.{reason}",
                    f"{label}: copia {source_id} -> {target_id} ({reason})",
                    node_id=target_id, object_id=obj["id"],
                    data={"from": source_id, "to": target_id, "blob_id": obj["blob_id"]},
                )
                return True
            return False
        return False

    # ---------------------------------------------------------- fase 3
    async def _process_deletions(self, report: CycleReport) -> None:
        # Se respeta DELETE_GRACE: una lectura que ya resolvió los metadatos de la
        # versión anterior todavía puede descargar su blob (similar a MVCC).
        for row in await self._meta.pending_deletions(self._s.delete_grace_s):
            blob_id, node_id = row["blob_id"], row["node_id"]
            node = self._membership.get(node_id)
            if node is None or node.membership == DECOMMISSIONED:
                await self._meta.complete_deletion(blob_id, node_id)
                continue
            if not self._membership.is_writable(node_id):
                continue  # nodo caído: se reintentará (o lo limpiará el inventario al volver)
            if await self._meta.is_referenced(blob_id, node_id):
                await self._meta.complete_deletion(blob_id, node_id)  # el blob vuelve a ser necesario
                continue
            try:
                await self._storage.delete_blob(node, blob_id)
            except StorageError as exc:
                await self._meta.fail_deletion(blob_id, node_id, str(exc))
                report.deletions_failed += 1
                continue
            await self._meta.complete_deletion(blob_id, node_id)
            report.deletions_done += 1

    # ================================================================== scrub
    async def scrub(self, trigger: str = "manual") -> dict:
        """Scrub profundo: cada nodo recalcula el SHA-256 de sus blobs."""
        async with self._exclusive() as acquired:
            if not acquired:
                return {"trigger": trigger, "skipped": "otro proceso está reconciliando"}
            report: dict = {
                "trigger": trigger, "started_at": _now_iso(), "checked": 0, "ok": 0,
                "corrupt": [], "missing": [], "errors": [], "repaired": 0,
            }
            affected: set[str] = set()
            for node in self._membership.all():
                if node.membership == DECOMMISSIONED or not self._membership.is_readable(node.node_id):
                    continue
                verified: list[str] = []
                for row in await self._meta.replicas_on_node(node.node_id):
                    if row["status"] != "OK":
                        affected.add(row["object_id"])
                        continue
                    report["checked"] += 1
                    label = f"{row['bucket']}/{row['key']}"
                    try:
                        digest = await self._storage.digest(node, row["blob_id"])
                    except StorageError as exc:
                        report["errors"].append(f"{node.node_id}: {exc}")
                        continue
                    if digest is None:
                        report["missing"].append({"node_id": node.node_id, "object": label})
                        affected.add(row["object_id"])
                        if await self._meta.set_replica_status(row["object_id"], node.node_id, row["blob_id"], "MISSING"):
                            await self._events.record(
                                "replica.missing", f"Scrub: {node.node_id} no tiene la réplica de {label}",
                                node_id=node.node_id, object_id=row["object_id"],
                            )
                    elif digest["sha256"] != row["checksum"]:
                        report["corrupt"].append(
                            {"node_id": node.node_id, "object": label, "expected": row["checksum"],
                             "actual": digest["sha256"]}
                        )
                        affected.add(row["object_id"])
                        if await self._meta.set_replica_status(row["object_id"], node.node_id, row["blob_id"], "CORRUPT"):
                            await self._events.record(
                                "integrity.corrupt",
                                f"Scrub: réplica corrupta de {label} en {node.node_id}",
                                node_id=node.node_id, object_id=row["object_id"],
                                data={"expected": row["checksum"], "actual": digest["sha256"], "detected_by": "scrub"},
                            )
                    else:
                        verified.append(row["object_id"])
                        report["ok"] += 1
                await self._meta.mark_verified(node.node_id, verified)

            if affected:  # reparación inmediata de lo encontrado
                repair = CycleReport("scrub-repair")
                await self._converge_ids(sorted(affected), repair)
                await self._process_deletions(repair)
                report["repaired"] = sum(repair.copies.values())
            report["finished_at"] = _now_iso()
            await self._events.record(
                "scrub.summary",
                f"Scrub ({trigger}): {report['checked']} réplicas verificadas, {len(report['corrupt'])} corruptas,"
                f" {len(report['missing'])} ausentes, {report['repaired']} reparadas",
            )
            self.last_scrub = report
            return report

    # ============================================================ observación
    async def distribution(self) -> dict:
        rf = self._s.replication_factor
        candidates = self._membership.placement_candidates()
        nodes = {
            n.node_id: {
                "node_id": n.node_id, "membership": n.membership, "health": n.health, "weight": n.weight,
                "replicas": 0, "bytes": 0, "desired_replicas": 0,
            }
            for n in self._membership.all()
        }
        summary = {"total": 0, "fully_replicated": 0, "misplaced": 0, "under_replicated": 0, "unavailable": 0}
        degraded = 0  # menos de RF copias válidas accesibles ahora mismo (incluye el periodo de gracia)
        for obj in await self._meta.all_active_with_replicas():
            summary["total"] += 1
            good = [r["node_id"] for r in obj["replicas"] if r["status"] == "OK" and r["blob_id"] == obj["blob_id"]]
            desired = desired_nodes(obj["id"], candidates, rf)
            for node_id in good:
                if node_id in nodes:
                    nodes[node_id]["replicas"] += 1
                    nodes[node_id]["bytes"] += obj["size"] or 0
            for node_id in desired:
                nodes[node_id]["desired_replicas"] += 1
            if sum(1 for n in good if self._membership.is_readable(n)) < rf:
                degraded += 1
            if not any(self._membership.is_readable(n) for n in good):
                summary["unavailable"] += 1
            elif sum(1 for n in good if self._membership.counts_for_durability(n)) < rf:
                summary["under_replicated"] += 1
            elif set(good) != set(desired):
                summary["misplaced"] += 1
            else:
                summary["fully_replicated"] += 1
        total = sum(n["replicas"] for n in nodes.values())
        total_desired = sum(n["desired_replicas"] for n in nodes.values())
        for n in nodes.values():
            n["share"] = _pct(n["replicas"], total)
            n["desired_share"] = _pct(n["desired_replicas"], total_desired)
        return {
            "replication_factor": rf,
            "write_acks": self._s.write_acks,
            "objects": {**summary, "degraded": degraded},
            "total_replicas": total,
            "nodes": list(nodes.values()),
            "deletion_backlog": await self._meta.deletion_backlog(),
        }

    def classify(self, good: list[str], desired: list[str]) -> str:
        """Estado de un objeto para la interfaz visual (de peor a mejor)."""
        rf = self._s.replication_factor
        readable = sum(1 for n in good if self._membership.is_readable(n))
        if readable == 0:
            return "unavailable"  # ninguna copia válida accesible
        if sum(1 for n in good if self._membership.counts_for_durability(n)) < rf:
            return "under_replicated"  # se ha perdido una copia: toca re-replicar
        if readable < rf:
            return "degraded"  # una copia inaccesible (dentro del periodo de gracia)
        if set(good) != set(desired):
            return "misplaced"  # copias completas pero no donde dice HRW
        return "ok"

    async def snapshot(self, limit: int = 400) -> dict:
        """Foto completa del sistema para la interfaz visual (/ui)."""
        rf = self._s.replication_factor
        candidates = self._membership.placement_candidates()
        nodes = {
            n.node_id: {
                **n.as_dict(),
                "readable": self._membership.is_readable(n.node_id),
                "writable": self._membership.is_writable(n.node_id),
                "replicas": 0, "desired_replicas": 0, "bytes": 0,
            }
            for n in self._membership.all()
        }
        states = {"ok": 0, "misplaced": 0, "degraded": 0, "under_replicated": 0, "unavailable": 0}
        objects = []
        for o in await self._meta.objects_for_map(limit):
            good = [r["node_id"] for r in o["replicas"] if r["status"] == "OK" and r["blob_id"] == o["blob_id"]]
            desired = desired_nodes(o["id"], candidates, rf)
            state = self.classify(good, desired)
            states[state] += 1
            for node_id in good:
                if node_id in nodes:
                    nodes[node_id]["replicas"] += 1
                    nodes[node_id]["bytes"] += o["size"] or 0
            for node_id in desired:
                nodes[node_id]["desired_replicas"] += 1
            objects.append({**o, "desired": desired, "state": state})
        total = await self._meta.count_active()
        return {
            "replication_factor": rf,
            "write_acks": self._s.write_acks,
            "nodes": list(nodes.values()),
            "objects": objects,
            "states": states,
            "total_objects": total,
            "truncated": total > len(objects),
            "pending_deletions": await self._meta.pending_deletions_detail(),
            "last_reconcile": self.last_cycle,
        }

    async def locate(self, bucket: str, key: str) -> dict:
        obj = await self._meta.find_live(bucket, key)
        if obj is None:
            raise ApiError(404, "Objeto no encontrado", bucket=bucket, key=key)
        rf = self._s.replication_factor
        ranking = rank_nodes(obj["id"], self._membership.placement_candidates())
        desired = [r.node_id for r in ranking[:rf]]
        replicas = await self._meta.replicas_of(obj["id"])

        def health(node_id: str) -> str | None:
            node = self._membership.get(node_id)
            return node.health if node else None

        return {
            "object": {
                k: obj[k]
                for k in ("id", "bucket", "key", "status", "version", "blob_id", "size", "checksum", "content_type")
            },
            "placement": {
                "algorithm": "weighted rendezvous hashing (HRW) sobre el id del objeto",
                "replication_factor": rf,
                "desired": desired,
                "ranking": [
                    {"node_id": r.node_id, "weight": r.weight, "score": round(r.score, 6),
                     "desired": i < rf, "health": health(r.node_id)}
                    for i, r in enumerate(ranking)
                ],
            },
            "replicas": [
                {
                    "node_id": r["node_id"],
                    "blob_id": r["blob_id"],
                    "status": r["status"],
                    "current_version": r["blob_id"] == obj["blob_id"],
                    "desired": r["node_id"] in desired,
                    "node_health": health(r["node_id"]),
                    "last_verified_at": r["last_verified_at"],
                }
                for r in replicas
            ],
        }
