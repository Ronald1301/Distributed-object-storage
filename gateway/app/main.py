"""Gateway del almacén de objetos distribuido P16.

Punto de entrada de los clientes (plano de aplicación) y coordinador del
plano de datos: ubicación, replicación, detección de fallos y reparación.
"""
from __future__ import annotations

import asyncio
import logging
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncIterator
from urllib.parse import unquote

import asyncpg
from fastapi import FastAPI, Request
from fastapi.responses import FileResponse

from . import api_cluster, api_objects
from .config import load_settings
from .db import Database
from .errors import ApiError
from .events import EventLog
from .health_monitor import HealthMonitor
from .live import LiveFeed
from .membership import Membership
from .metadata import MetadataRepository
from .objects_service import ObjectService
from .reconciler import Reconciler
from .responses import PrettyJSONResponse
from .services import Services
from .storage_client import StorageClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
log = logging.getLogger("p16.gateway")

UI_FILE = Path(__file__).parent / "static" / "ui.html"
# Peticiones de sondeo de la interfaz visual: no se registran en el log
QUIET_PATHS = ("/health", "/ui", "/cluster/feed", "/cluster/map", "/cluster/config", "/cluster/locate")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = load_settings()
    db = Database(settings.database_url)
    await db.connect()
    feed = LiveFeed()
    events = EventLog(db, feed)
    membership = Membership(db, events, settings)
    await membership.load()
    storage = StorageClient(settings)
    meta = MetadataRepository(db)
    reconciler = Reconciler(db, meta, membership, storage, events, settings)
    objects = ObjectService(meta, membership, storage, events, reconciler, settings)
    monitor = HealthMonitor(membership, storage, settings)
    # Un nodo que vuelve de DOWN dispara una reconciliación inmediata.
    membership.on_recovering(lambda node_id: reconciler.request_cycle(f"node-recovering:{node_id}"))

    app.state.services = Services(
        settings, db, events, membership, storage, meta, reconciler, objects, monitor, feed
    )
    tasks = [
        asyncio.create_task(monitor.run_forever(), name="health-monitor"),
        asyncio.create_task(reconciler.run_forever(), name="reconciler"),
        asyncio.create_task(reconciler.scrub_forever(), name="scrubber"),
    ]
    await events.record(
        "gateway.started",
        f"Gateway iniciado (RF={settings.replication_factor}, W={settings.write_acks})",
    )
    try:
        yield
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await storage.close()
        await db.close()


app = FastAPI(
    title="P16 · Almacén de objetos distribuido",
    version="1.0.0",
    lifespan=lifespan,
    default_response_class=PrettyJSONResponse,
)


def _object_route(path: str) -> tuple[str, str | None] | None:
    """(bucket, clave) si la ruta es de la API de objetos."""
    parts = path.split("/")
    if len(parts) >= 2 and parts[1] == "objects":
        return "default", unquote("/".join(parts[2:])) or None
    if len(parts) >= 4 and parts[1] == "buckets" and parts[3] == "objects":
        return unquote(parts[2]), unquote("/".join(parts[4:])) or None
    return None


def _track_error(request: Request, status: int, message: str, extra: dict) -> None:
    """Las peticiones fallidas también se muestran en la interfaz visual."""
    services = getattr(request.app.state, "services", None)
    route = _object_route(request.url.path)
    if services is None or route is None:
        return
    op = "LIST" if request.method == "GET" and route[1] is None else request.method
    details = {k: v for k, v in extra.items() if k in ("attempts", "stored_on", "failures", "required_acks")}
    services.feed.push("request", op=op, bucket=route[0], key=route[1], status=status, error=message, **details)


@app.exception_handler(ApiError)
async def api_error_handler(request: Request, exc: ApiError) -> PrettyJSONResponse:
    _track_error(request, exc.status, exc.message, exc.extra)
    return PrettyJSONResponse(exc.body(), status_code=exc.status)


# Sin metadatos no se puede garantizar el contrato: el gateway rechaza con 503
# en lugar de servir datos sin verificar (se prioriza consistencia sobre disponibilidad).
@app.exception_handler(OSError)
@app.exception_handler(asyncpg.PostgresConnectionError)
@app.exception_handler(asyncpg.InterfaceError)
async def metadata_unavailable(request: Request, exc: Exception) -> PrettyJSONResponse:
    log.warning("Metadatos no disponibles: %s", exc)
    _track_error(request, 503, "PostgreSQL no disponible", {})
    return PrettyJSONResponse(
        {"error": "Servicio de metadatos (PostgreSQL) no disponible; reintente más tarde", "detail": str(exc)},
        status_code=503,
    )


@app.middleware("http")
async def access_log(request: Request, call_next):
    started = time.perf_counter()
    response = await call_next(request)
    if not request.url.path.startswith(QUIET_PATHS):
        log.info(
            "%s %s -> %d (%.1f ms)",
            request.method, request.url.path, response.status_code, (time.perf_counter() - started) * 1000,
        )
    return response


@app.get("/")
async def root(request: Request) -> dict:
    s: Services = request.app.state.services
    return {
        "service": "P16 - servicio distribuido de almacenamiento de objetos",
        "api": {
            "objects": "PUT|GET|HEAD|DELETE /objects/{key}  (bucket 'default')",
            "objects_in_bucket": "PUT|GET|HEAD|DELETE /buckets/{bucket}/objects/{key}",
            "list": "GET /objects?prefix=&limit=&after=  |  GET /buckets/{bucket}/objects",
            "cluster": "/cluster/nodes, /cluster/distribution, /cluster/locate?key=, /cluster/events",
            "ui": "/ui  (interfaz visual en vivo)",
            "docs": "/docs",
        },
        "contract": api_cluster.contract(s),
    }


@app.get("/health")
async def health(request: Request) -> dict:
    s: Services = request.app.state.services
    try:
        await s.db.fetchval("SELECT 1")
    except Exception as exc:
        raise ApiError(503, "metadata-db no disponible", detail=str(exc)) from exc
    nodes = s.membership.all()
    return {
        "status": "ok",
        "role": "gateway",
        "metadata_db": "ok",
        "nodes": {n.node_id: n.health for n in nodes},
    }


@app.get("/ui", include_in_schema=False)
async def ui() -> FileResponse:
    """Interfaz visual en vivo del almacén."""
    return FileResponse(UI_FILE, media_type="text/html", headers={"Cache-Control": "no-store"})


app.include_router(api_objects.router)
app.include_router(api_cluster.router)
