"""Tests de la estrategia de ubicación (sin dependencias: python -m unittest)."""
from __future__ import annotations

import unittest

from app.ids import ULID_RE, etag_matches, new_ulid
from app.placement import Candidate, desired_nodes, modulo_placement, preference_list, rank_nodes

N_OBJECTS = 20_000


def nodes(*ids: str, weights: dict[str, float] | None = None) -> list[Candidate]:
    weights = weights or {}
    return [Candidate(i, weights.get(i, 1.0)) for i in ids]


OBJECTS = [f"obj-{i:06d}" for i in range(N_OBJECTS)]


class PlacementTest(unittest.TestCase):
    def test_deterministic_and_distinct(self) -> None:
        cluster = nodes("storage1", "storage2", "storage3")
        for oid in OBJECTS[:500]:
            a = desired_nodes(oid, cluster, 2)
            self.assertEqual(a, desired_nodes(oid, list(reversed(cluster)), 2))  # no depende del orden
            self.assertEqual(len(set(a)), 2)  # dos réplicas nunca en el mismo nodo

    def test_balanced_distribution(self) -> None:
        cluster = nodes("storage1", "storage2", "storage3")
        counts = {c.node_id: 0 for c in cluster}
        for oid in OBJECTS:
            for n in desired_nodes(oid, cluster, 2):
                counts[n] += 1
        for n, c in counts.items():
            self.assertAlmostEqual(c / (2 * N_OBJECTS), 1 / 3, delta=0.02, msg=n)

    def test_adding_node_moves_minimum(self) -> None:
        before_cluster = nodes("storage1", "storage2", "storage3")
        after_cluster = nodes("storage1", "storage2", "storage3", "storage4")
        moved = 0
        for oid in OBJECTS:
            before = set(desired_nodes(oid, before_cluster, 2))
            after = set(desired_nodes(oid, after_cluster, 2))
            new = after - before
            # Propiedad clave: los datos sólo se mueven HACIA el nodo nuevo.
            self.assertTrue(new <= {"storage4"}, (oid, before, after))
            moved += len(new)
        # Óptimo teórico: 1/4 de las réplicas pasan al nuevo nodo.
        self.assertAlmostEqual(moved / (2 * N_OBJECTS), 0.25, delta=0.02)

    def test_modulo_moves_much_more(self) -> None:
        before = ["storage1", "storage2", "storage3"]
        after = before + ["storage4"]
        moved = between_old = 0
        for o in OBJECTS:
            new = set(modulo_placement(o, after, 2)) - set(modulo_placement(o, before, 2))
            moved += len(new)
            between_old += len(new - {"storage4"})
        # hash % N mueve ~el doble que HRW y además mueve datos entre nodos antiguos
        self.assertGreater(moved / (2 * N_OBJECTS), 0.45)
        self.assertGreater(between_old / (2 * N_OBJECTS), 0.20)

    def test_removing_node_only_affects_its_objects(self) -> None:
        full = nodes("storage1", "storage2", "storage3", "storage4")
        reduced = nodes("storage1", "storage2", "storage3")
        for oid in OBJECTS[:5000]:
            before = desired_nodes(oid, full, 2)
            after = desired_nodes(oid, reduced, 2)
            if "storage4" not in before:
                self.assertEqual(before, after)

    def test_weights_are_proportional(self) -> None:
        cluster = nodes("storage1", "storage2", "storage3", weights={"storage3": 2.0})
        first = {c.node_id: 0 for c in cluster}
        for oid in OBJECTS:
            first[preference_list(oid, cluster)[0]] += 1
        self.assertAlmostEqual(first["storage3"] / N_OBJECTS, 0.5, delta=0.02)
        self.assertAlmostEqual(first["storage1"] / N_OBJECTS, 0.25, delta=0.02)

    def test_ranking_includes_every_node(self) -> None:
        cluster = nodes("a1", "b2", "c3", "d4")
        ranked = rank_nodes("x", cluster)
        self.assertEqual(sorted(r.node_id for r in ranked), ["a1", "b2", "c3", "d4"])
        self.assertEqual([r.score for r in ranked], sorted((r.score for r in ranked), reverse=True))


class IdsTest(unittest.TestCase):
    def test_ulid_format_and_order(self) -> None:
        a = new_ulid(1_700_000_000_000)
        b = new_ulid(1_700_000_000_001)
        self.assertRegex(a, ULID_RE)
        self.assertLess(a[:10], b[:10])  # ordenables por tiempo
        self.assertNotEqual(new_ulid(), new_ulid())

    def test_etag_matching(self) -> None:
        self.assertTrue(etag_matches('"abc"', "abc"))
        self.assertTrue(etag_matches('W/"x", "abc"', "abc"))
        self.assertTrue(etag_matches("*", "abc"))
        self.assertFalse(etag_matches('"abd"', "abc"))
        self.assertFalse(etag_matches("*", None))


if __name__ == "__main__":
    unittest.main()
