"""API de operación del clúster (plano de datos / control).

    GET   /cluster/nodes                  membresía + salud de cada nodo
    POST  /cluster/nodes/join             alta de un Storage Node (lo llama el propio nodo)
    POST  /cluster/nodes/{id}/drain       retirar un nodo (sus datos se mueven a otros)
    POST  /cluster/nodes/{id}/activate    volver a incluirlo en la ubicación
    PATCH /cluster/nodes/{id}             cambiar su peso (capacidad)
    GET   /cluster/distribution           réplicas por nodo, estado de replicación
    GET   /cluster/locate?key=&bucket=    ubicación deseada (HRW) vs. réplicas reales
    POST  /cluster/reconcile              ejecutar un ciclo de reconciliación ahora
    POST  /cluster/scrub                  verificar checksums de todas las réplicas ahora
    GET   /cluster/events                 últimos eventos (fallos, reparaciones...)
    GET   /cluster/config                 parámetros y contrato declarado
"""
from __future__ import annotations

from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field

from .errors import ApiError
from .membership import ACTIVE, DECOMMISSIONED, DRAINING
from .services import Services, get_services

router = APIRouter(prefix="/cluster", tags=["cluster"])


class JoinRequest(BaseModel):
    node_id: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{0,62}$")
    url: str = Field(pattern=r"^https?://")
    weight: float = Field(default=1.0, gt=0)


class WeightRequest(BaseModel):
    weight: float = Field(gt=0)


def _node_view(s: Services, node_id: str) -> dict:
    node = s.membership.get(node_id)
    assert node is not None
    return {
        **node.as_dict(),
        "readable": s.membership.is_readable(node_id),
        "writable": s.membership.is_writable(node_id),
        "counts_for_durability": s.membership.counts_for_durability(node_id),
    }


def _require_node(s: Services, node_id: str) -> None:
    if s.membership.get(node_id) is None:
        raise ApiError(404, "Nodo desconocido", node_id=node_id)


@router.get("/nodes")
async def list_nodes(s: Services = Depends(get_services)) -> dict:
    return {
        "replication_factor": s.settings.replication_factor,
        "nodes": [_node_view(s, n.node_id) for n in s.membership.all()],
    }


@router.post("/nodes/join")
async def join(req: JoinRequest, s: Services = Depends(get_services)) -> dict:
    before = s.membership.get(req.node_id)
    await s.membership.join(req.node_id, req.url.rstrip("/"), req.weight)
    if before is None or before.membership == DECOMMISSIONED:
        s.reconciler.request_cycle(f"node-joined:{req.node_id}")  # empieza el reequilibrado
    return _node_view(s, req.node_id)


@router.post("/nodes/{node_id}/drain")
async def drain(node_id: str, s: Services = Depends(get_services)) -> dict:
    _require_node(s, node_id)
    remaining = [n for n in s.membership.all() if n.membership == ACTIVE and n.node_id != node_id]
    if len(remaining) < s.settings.replication_factor:
        raise ApiError(
            409,
            f"No se puede drenar {node_id}: quedarían {len(remaining)} nodos activos"
            f" y el factor de replicación es {s.settings.replication_factor}",
        )
    await s.membership.set_membership(node_id, DRAINING)
    s.reconciler.request_cycle(f"node-draining:{node_id}")
    return _node_view(s, node_id)


@router.post("/nodes/{node_id}/activate")
async def activate(node_id: str, s: Services = Depends(get_services)) -> dict:
    _require_node(s, node_id)
    await s.membership.set_membership(node_id, ACTIVE)
    s.reconciler.request_cycle(f"node-activated:{node_id}")
    return _node_view(s, node_id)


@router.patch("/nodes/{node_id}")
async def set_weight(node_id: str, req: WeightRequest, s: Services = Depends(get_services)) -> dict:
    _require_node(s, node_id)
    await s.membership.set_weight(node_id, req.weight)
    s.reconciler.request_cycle(f"weight-changed:{node_id}")
    return _node_view(s, node_id)


@router.get("/distribution")
async def distribution(s: Services = Depends(get_services)) -> dict:
    return await s.reconciler.distribution()


@router.get("/locate")
async def locate(key: str, bucket: str = "default", s: Services = Depends(get_services)) -> dict:
    return await s.reconciler.locate(bucket, key)


@router.post("/reconcile")
async def reconcile(s: Services = Depends(get_services)) -> dict:
    return await s.reconciler.run_cycle("manual")


@router.post("/scrub")
async def scrub(s: Services = Depends(get_services)) -> dict:
    return await s.reconciler.scrub("manual")


@router.get("/events")
async def events(limit: int = 30, type: str | None = None, s: Services = Depends(get_services)) -> dict:
    return {"events": await s.events.recent(max(1, min(limit, 500)), type)}


@router.get("/feed")
async def feed(after: int = 0, s: Services = Depends(get_services)) -> dict:
    """Actividad nueva desde `after` (after=-1: sólo sincronizar el contador)."""
    return {"boot": s.feed.boot_id, "seq": s.feed.seq, "items": s.feed.since(after)}


@router.get("/map")
async def cluster_map(limit: int = 400, s: Services = Depends(get_services)) -> dict:
    """Foto del estado para la interfaz visual: nodos, objetos, réplicas y borrados pendientes."""
    return {"boot": s.feed.boot_id, **(await s.reconciler.snapshot(max(1, min(limit, 2000))))}


@router.get("/config")
async def config(s: Services = Depends(get_services)) -> dict:
    return {"settings": s.settings.public(), "contract": contract(s)}


def contract(s: Services) -> dict:
    st = s.settings
    return {
        "placement": "Weighted Rendezvous Hashing (HRW) sobre el id estable del objeto",
        "redundancy": f"replicación completa, RF={st.replication_factor} copias en nodos distintos",
        "durability": (
            f"un PUT responde 2xx sólo tras {st.write_acks} escrituras con fsync y SHA-256 verificado"
            " en nodos distintos, y tras el commit de metadatos en PostgreSQL"
        ),
        "fault_tolerance": f"sin pérdida de datos confirmados con hasta {st.write_acks - 1} nodo(s) perdidos"
        " de forma permanente; lectura disponible mientras quede 1 réplica válida",
        "consistency": (
            "lectura-tras-escritura fuerte por clave: GET/HEAD/LIST leen los metadatos confirmados"
            " y los blobs son inmutables y verificados; concurrencia last-writer-wins"
            " (o If-Match / If-None-Match para control optimista)"
        ),
        "integrity": "SHA-256 extremo a extremo: al escribir (nodo), al leer (gateway) y en scrub periódico",
        "repair": (
            f"reconciliación cada {st.reconcile_interval_s:g}s; re-replicación de un nodo DOWN"
            f" tras {st.repair_delay_s:g}s; read-repair inmediato al detectar corrupción"
        ),
    }
