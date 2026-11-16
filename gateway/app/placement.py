"""Estrategia de ubicación: Weighted Rendezvous Hashing (HRW).

Para cada objeto se calcula una puntuación por nodo:

    u     = hash(object_id, node_id) normalizado a (0, 1)
    score = -weight / ln(u)

y los nodos se ordenan de mayor a menor puntuación. Ese orden es la
"lista de preferencia" del objeto: los RF primeros son su ubicación deseada
y los siguientes son los sustitutos naturales si alguno falla.

Propiedades que justifican la elección:
  * Determinista: cualquier componente que conozca los miembros calcula lo mismo.
  * Réplicas siempre en nodos distintos (se toman los RF primeros de un ranking).
  * Reequilibrado mínimo: al añadir un nodo sólo se mueven los objetos que
    pasan a tenerlo entre sus RF primeros (~RF/N de los objetos, 1/N de las
    réplicas); ningún objeto se mueve entre nodos antiguos.
  * Pesos: la probabilidad de que un nodo quede primero es proporcional a su
    peso (capacidad). Es la misma idea que el "straw2" del CRUSH de Ceph.
"""
from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass
from typing import Iterable, Sequence


@dataclass(frozen=True)
class Candidate:
    node_id: str
    weight: float = 1.0


@dataclass(frozen=True)
class Ranked:
    node_id: str
    weight: float
    score: float


def _uniform(object_id: str, node_id: str) -> float:
    digest = hashlib.sha256(f"{object_id}\x00{node_id}".encode()).digest()
    n = int.from_bytes(digest[:8], "big") >> 11  # 53 bits -> double exacto
    return (n + 0.5) / 2**53  # estrictamente dentro de (0, 1)


def rank_nodes(object_id: str, candidates: Iterable[Candidate]) -> list[Ranked]:
    ranked = [
        Ranked(c.node_id, c.weight, -c.weight / math.log(_uniform(object_id, c.node_id)))
        for c in candidates
        if c.weight > 0
    ]
    ranked.sort(key=lambda r: (-r.score, r.node_id))
    return ranked


def preference_list(object_id: str, candidates: Iterable[Candidate]) -> list[str]:
    return [r.node_id for r in rank_nodes(object_id, candidates)]


def desired_nodes(object_id: str, candidates: Iterable[Candidate], rf: int) -> list[str]:
    return preference_list(object_id, candidates)[:rf]


# ---------------------------------------------------------------------------
# Estrategias alternativas: sólo se usan para compararlas (tests y
# tools/simulate_placement.py). El sistema utiliza únicamente HRW.
# ---------------------------------------------------------------------------

def _h64(value: str) -> int:
    return int.from_bytes(hashlib.sha256(value.encode()).digest()[:8], "big")


def modulo_placement(object_id: str, node_ids: Sequence[str], rf: int) -> list[str]:
    """hash(id) % N, con las réplicas en los índices siguientes."""
    start = _h64(object_id) % len(node_ids)
    return [node_ids[(start + i) % len(node_ids)] for i in range(min(rf, len(node_ids)))]


class ConsistentHashRing:
    """Anillo de hashing consistente con nodos virtuales (estilo Dynamo)."""

    def __init__(self, node_ids: Sequence[str], vnodes: int = 100):
        self._points = sorted(
            (_h64(f"{node}#{v}"), node) for node in node_ids for v in range(vnodes)
        )
        self._nodes = len(set(node_ids))

    def lookup(self, object_id: str, rf: int) -> list[str]:
        import bisect

        key = _h64(object_id)
        idx = bisect.bisect(self._points, (key, ""))
        result: list[str] = []
        for i in range(len(self._points)):
            node = self._points[(idx + i) % len(self._points)][1]
            if node not in result:
                result.append(node)
                if len(result) == min(rf, self._nodes):
                    break
        return result
