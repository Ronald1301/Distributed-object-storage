"""Compara estrategias de ubicación: equilibrio y datos movidos al pasar de 3 a 4 nodos.

    docker compose exec gateway python -m tools.simulate_placement
    (o, en local, desde gateway/:  python3 -m tools.simulate_placement)
"""
from __future__ import annotations

import statistics
import sys
from typing import Callable, Sequence

from app.placement import Candidate, ConsistentHashRing, desired_nodes, modulo_placement

RF = 2


def evaluate(name: str, place: Callable[[str, Sequence[str]], list[str]], objects: list[str]) -> None:
    before_nodes = ["storage1", "storage2", "storage3"]
    after_nodes = before_nodes + ["storage4"]
    counts = {n: 0 for n in after_nodes}
    moved = 0
    moved_between_old = 0
    for oid in objects:
        before = set(place(oid, before_nodes))
        after = place(oid, after_nodes)
        for n in after:
            counts[n] += 1
        new = set(after) - before
        moved += len(new)
        moved_between_old += len(new - {"storage4"})
    total = RF * len(objects)
    shares = [100 * c / total for c in counts.values()]
    print(
        f"{name:<34} {100 * moved / total:6.1f}%   {100 * moved_between_old / total:6.1f}%"
        f"   {min(shares):5.1f}% - {max(shares):5.1f}%   (σ={statistics.pstdev(shares):.2f})"
    )


def main() -> None:
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 20_000
    objects = [f"obj-{i:07d}" for i in range(n)]
    print(f"{n} objetos, RF={RF}, 3 -> 4 nodos (óptimo teórico: mover 25% de las réplicas)\n")
    print(f"{'estrategia':<34} {'movido':>7}   {'entre viejos':>8}   {'reparto con 4 nodos'}")
    print("-" * 90)
    evaluate("hash(id) % N", lambda o, ns: modulo_placement(o, ns, RF), objects)
    evaluate("anillo consistente, 1 vnode", lambda o, ns: ConsistentHashRing(ns, 1).lookup(o, RF), objects)
    rings: dict[tuple, ConsistentHashRing] = {}

    def ring100(o: str, ns: Sequence[str]) -> list[str]:
        key = tuple(ns)
        if key not in rings:
            rings[key] = ConsistentHashRing(ns, 100)
        return rings[key].lookup(o, RF)

    evaluate("anillo consistente, 100 vnodes", ring100, objects)
    evaluate(
        "rendezvous / HRW (usado en P16)",
        lambda o, ns: desired_nodes(o, [Candidate(x) for x in ns], RF),
        objects,
    )
    print("\n'entre viejos' = réplicas que cambian de un nodo antiguo a otro nodo antiguo (tráfico inútil).")


if __name__ == "__main__":
    main()
