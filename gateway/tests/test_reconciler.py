"""Tests de convergencia del reconciliador con dobles en memoria (tests/fakes.py).

Se ejecuta el Reconciler REAL contra un repositorio de metadatos, un cliente
de storage y una membresía en memoria: sin Docker, sin PostgreSQL y sin red.
Son rápidos y deterministas, y cubren los casos límite que en el e2e son
difíciles de provocar.

    cd gateway && python -m unittest discover -s tests -t . -v
"""
from __future__ import annotations

import unittest

from app.membership import DECOMMISSIONED, DOWN, DRAINING, HEALTHY, RECOVERING

from .fakes import Cluster


def total_copies(result: dict) -> int:
    return sum(result["copies"].values())


class RepairTest(unittest.IsolatedAsyncioTestCase):
    """Réplica MISSING o CORRUPT en un nodo deseado -> se repone desde una copia válida."""

    async def test_missing_replica_is_restored_from_valid_copy(self) -> None:
        c = Cluster()
        obj = c.put("a.txt")
        first, second = c.desired(obj["id"])
        c.storage.disk(second).blobs.clear()  # el nodo perdió el blob (disco borrado)

        result = await c.cycle()

        self.assertEqual(result["missing_detected"], 1)
        self.assertEqual(result["copies"], {"repair-missing": 1})
        self.assertEqual(c.storage.pushes, [(first, second, obj["blob_id"])])
        self.assertTrue(c.has_valid_copy(second, obj))
        self.assertEqual(c.meta.replica(obj["id"], second)["status"], "OK")
        self.assertIn("replica.missing", c.events.types())
        self.assertEqual(result["errors"], [])

    async def test_corrupt_replica_is_replaced_from_valid_copy(self) -> None:
        c = Cluster()
        obj = c.put("a.txt")
        first, second = c.desired(obj["id"])
        c.storage.disk(second).blobs[obj["blob_id"]] = b"bits podridos"
        await c.meta.set_replica_status(obj["id"], second, obj["blob_id"], "CORRUPT")  # p. ej. tras un GET

        result = await c.cycle()

        self.assertEqual(result["copies"], {"repair-corrupt": 1})
        self.assertEqual(c.storage.pushes, [(first, second, obj["blob_id"])])
        self.assertTrue(c.has_valid_copy(second, obj))
        self.assertEqual(c.meta.replica(obj["id"], second)["status"], "OK")

    async def test_scrub_detects_silent_corruption_and_repairs_it(self) -> None:
        c = Cluster()
        obj = c.put("a.txt")
        _, second = c.desired(obj["id"])
        c.storage.disk(second).blobs[obj["blob_id"]] = b"bits podridos"  # nadie lo ha leído aún

        report = await c.reconciler.scrub("test")

        self.assertEqual([x["node_id"] for x in report["corrupt"]], [second])
        self.assertEqual(report["repaired"], 1)
        self.assertTrue(c.has_valid_copy(second, obj))
        self.assertEqual(c.meta.replica(obj["id"], second)["status"], "OK")

    async def test_corrupt_source_is_skipped_for_another_valid_copy(self) -> None:
        c = Cluster()
        obj = c.put("a.txt")
        first, second, third = c.preference(obj["id"])
        # Copia válida fuera de su sitio (escrita con sloppy quorum) y la deseada ausente.
        c.storage.disk(second).blobs.clear()
        c.storage.store(third, obj["blob_id"], c.storage.disk(first).blobs[obj["blob_id"]])
        c.meta.insert_replica(obj["id"], third, obj["blob_id"])
        c.storage.disk(first).blobs[obj["blob_id"]] = b"bits podridos"  # corrupción silenciosa

        result = await c.cycle()

        # first se intenta primero (mejor rango), devuelve 409 y se descarta
        self.assertEqual(c.meta.replica(obj["id"], first)["status"], "CORRUPT")
        self.assertIn("integrity.corrupt", c.events.types())
        self.assertEqual(c.storage.pushes, [(third, second, obj["blob_id"])])
        self.assertTrue(c.has_valid_copy(second, obj))
        # third sigue siendo necesaria: first está corrupta, así que el deseado no está completo
        self.assertIn(third, c.meta.nodes_with(obj["id"]))
        self.assertEqual(result["replicas_removed"], 0)

        await c.cycle()  # se repara first y sólo entonces se retira la copia de third

        self.assertTrue(c.has_valid_copy(first, obj))
        self.assertEqual(c.meta.nodes_with(obj["id"]), {first, second})
        self.assertNotIn(obj["blob_id"], c.storage.disk(third).blobs)

    async def test_corruption_is_never_propagated(self) -> None:
        c = Cluster()
        obj = c.put("a.txt")
        first, second, third = c.preference(obj["id"])
        c.storage.disk(second).blobs.clear()
        c.storage.disk(first).blobs[obj["blob_id"]] = b"bits podridos"  # la única copia está mal

        result = await c.cycle()

        self.assertEqual(c.storage.pushes, [])
        self.assertEqual(total_copies(result), 0)
        for node_id in (second, third):
            self.assertNotIn(obj["blob_id"], c.storage.disk(node_id).blobs)
        self.assertEqual(c.meta.replica(obj["id"], first)["status"], "CORRUPT")
        self.assertEqual(c.meta.replica(obj["id"], second)["status"], "MISSING")

        result = await c.cycle()
        self.assertEqual(result["unavailable_objects"], 1)
        self.assertIn("object.unavailable", c.events.types())


class DownNodeTest(unittest.IsolatedAsyncioTestCase):
    """Nodo DOWN dentro de la gracia -> no se re-replica; fuera de la gracia -> sí."""

    async def test_down_within_grace_does_not_rereplicate(self) -> None:
        c = Cluster(repair_delay_s=30)
        obj = c.put("a.txt")
        _, second, third = c.preference(obj["id"])
        c.membership.set_health(second, DOWN, down_for_s=5)  # puede ser un reinicio

        result = await c.cycle()

        self.assertEqual(result["nodes_skipped"], {second: DOWN})
        self.assertEqual(result["copies"], {})
        self.assertEqual(c.storage.pushes, [])
        self.assertNotIn(obj["blob_id"], c.storage.disk(third).blobs)

    async def test_down_beyond_grace_rereplicates_to_next_in_ranking(self) -> None:
        c = Cluster(repair_delay_s=30)
        obj = c.put("a.txt")
        first, second, third = c.preference(obj["id"])
        c.membership.set_health(second, DOWN, down_for_s=60)  # se da por perdido

        result = await c.cycle()

        self.assertEqual(result["copies"], {"re-replication": 1})
        self.assertEqual(c.storage.pushes, [(first, third, obj["blob_id"])])
        self.assertTrue(c.has_valid_copy(third, obj))
        # la réplica del nodo caído no se toca: puede volver
        self.assertEqual(c.meta.replica(obj["id"], second)["status"], "OK")

    async def test_returning_node_retires_temporary_copy_and_becomes_healthy(self) -> None:
        c = Cluster(repair_delay_s=30)
        obj = c.put("a.txt")
        first, second, third = c.preference(obj["id"])
        c.membership.set_health(second, DOWN, down_for_s=60)
        await c.cycle()
        self.assertEqual(c.meta.nodes_with(obj["id"]), {first, second, third})

        c.membership.set_health(second, RECOVERING)  # el detector lo vuelve a ver
        result = await c.cycle()

        self.assertEqual(c.membership.get(second).health, HEALTHY)
        self.assertEqual(result["replicas_removed"], 1)
        self.assertEqual(c.meta.nodes_with(obj["id"]), {first, second})
        self.assertNotIn(obj["blob_id"], c.storage.disk(third).blobs)


class SurplusTest(unittest.IsolatedAsyncioTestCase):
    """Nunca se retira una copia sobrante si la ubicación deseada no está completa."""

    def _sloppy_object(self, c: Cluster) -> tuple[dict, str, str, str]:
        """Objeto escrito en first + third (second no estaba disponible al hacer el PUT)."""
        probe = c.put("probe.txt", on=[])
        first, second, third = c.preference(probe["id"])
        for node_id in (first, third):
            c.storage.store(node_id, probe["blob_id"], b"contenido de probe.txt")
            c.meta.insert_replica(probe["id"], node_id, probe["blob_id"])
        return probe, first, second, third

    def _removal_attempts(self, c: Cluster, node_id: str) -> list[dict]:
        return [r for r in c.meta.remove_calls if r["node_id"] == node_id]

    async def test_surplus_kept_while_copy_to_desired_node_fails(self) -> None:
        c = Cluster()
        obj, first, second, third = self._sloppy_object(c)
        c.storage.disk(second).reject_writes = True  # p. ej. disco lleno

        result = await c.cycle()

        self.assertEqual(total_copies(result), 0)
        self.assertEqual(len(result["errors"]), 1)
        self.assertEqual(self._removal_attempts(c, third), [])
        self.assertEqual(c.meta.nodes_with(obj["id"]), {first, third})
        self.assertTrue(c.has_valid_copy(third, obj))

        c.storage.disk(second).reject_writes = False
        result = await c.cycle()

        self.assertEqual(result["copies"], {"rebalance": 1})
        self.assertEqual(c.meta.nodes_with(obj["id"]), {first, second})
        self.assertNotIn(obj["blob_id"], c.storage.disk(third).blobs)

    async def test_surplus_kept_while_desired_node_is_down(self) -> None:
        c = Cluster(repair_delay_s=30)
        obj, first, second, third = self._sloppy_object(c)
        for down_for in (5, 60):  # dentro y fuera de la gracia
            c.membership.set_health(second, DOWN, down_for_s=down_for)
            with self.subTest(down_for_s=down_for):
                result = await c.cycle()
                self.assertEqual(result["replicas_removed"], 0)
                self.assertEqual(self._removal_attempts(c, third), [])
                self.assertEqual(c.meta.nodes_with(obj["id"]), {first, third})

    async def test_surplus_kept_when_copy_is_deferred_by_budget(self) -> None:
        c = Cluster(max_copies_per_cycle=0)
        obj, first, _, third = self._sloppy_object(c)

        result = await c.cycle()

        self.assertEqual(result["deferred_copies"], 1)
        self.assertEqual(self._removal_attempts(c, third), [])
        self.assertEqual(c.meta.nodes_with(obj["id"]), {first, third})


class DrainTest(unittest.IsolatedAsyncioTestCase):
    """Nodo en DRAINING -> sus copias se mueven y pasa a DECOMMISSIONED."""

    async def test_draining_node_is_emptied_and_decommissioned(self) -> None:
        c = Cluster()
        objects = [c.put(f"obj-{i}.txt") for i in range(12)]
        node = "storage2"
        on_node = [o for o in objects if node in c.meta.nodes_with(o["id"])]
        self.assertTrue(on_node, "el escenario necesita objetos en el nodo a drenar")

        await c.membership.set_membership(node, DRAINING)
        result = await c.cycle()

        self.assertEqual(result["copies"], {"drain": len(on_node)})
        self.assertEqual(result["replicas_removed"], len(on_node))
        self.assertEqual(c.membership.get(node).membership, DECOMMISSIONED)
        self.assertEqual(c.storage.disk(node).blobs, {})
        self.assertEqual(c.meta.deletions, {})
        for obj in objects:
            desired = c.desired(obj["id"])
            self.assertNotIn(node, desired)
            self.assertEqual(c.meta.nodes_with(obj["id"]), set(desired))
            for node_id in desired:
                self.assertTrue(c.has_valid_copy(node_id, obj))

    async def test_draining_node_stays_until_all_copies_moved(self) -> None:
        c = Cluster(max_copies_per_cycle=1)
        objects = [c.put(f"obj-{i}.txt") for i in range(12)]
        node = "storage2"
        pending = sum(1 for o in objects if node in c.meta.nodes_with(o["id"]))
        self.assertGreater(pending, 1)

        await c.membership.set_membership(node, DRAINING)
        for _ in range(pending - 1):
            await c.cycle()
            self.assertEqual(c.membership.get(node).membership, DRAINING)
        await c.cycle()

        self.assertEqual(c.membership.get(node).membership, DECOMMISSIONED)
        self.assertEqual(c.storage.disk(node).blobs, {})


class BudgetTest(unittest.IsolatedAsyncioTestCase):
    """El límite MAX_COPIES_PER_CYCLE aplaza copias al ciclo siguiente."""

    async def test_copies_are_throttled_and_eventually_converge(self) -> None:
        c = Cluster("storage1", "storage2", max_copies_per_cycle=3)
        objects = [c.put(f"obj-{i}.txt") for i in range(8)]  # RF=2 con 2 nodos: todo en ambos
        c.storage.disk("storage2").blobs.clear()

        expected = [(3, 5), (3, 2), (2, 0), (0, 0)]  # (copias, aplazadas) por ciclo
        for i, (copies, deferred) in enumerate(expected, 1):
            with self.subTest(cycle=i):
                result = await c.cycle()
                self.assertEqual(total_copies(result), copies)
                self.assertEqual(result["deferred_copies"], deferred)
        self.assertTrue(all(c.has_valid_copy("storage2", o) for o in objects))


if __name__ == "__main__":
    unittest.main()
