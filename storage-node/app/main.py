"""Storage Node: almacena blobs inmutables en su propio volumen.

No conoce objetos, buckets ni réplicas: sólo blobs identificados por ULID.
Toda la lógica distribuida vive en el gateway; el nodo ofrece primitivas
sencillas y verificables:

    GET    /health                  estado + uso (lo sondea el detector de fallos)
    GET    /blobs                   inventario (para la reconciliación)
    PUT    /blobs/{id}              guardar (verifica X-Checksum-SHA256, fsync, rename atómico)
    GET    /blobs/{id}              leer (HEAD también)
    GET    /blobs/{id}/digest       recalcular SHA-256 desde disco (scrub)
    POST   /blobs/{id}/push         copiar este blob a otro nodo (reparación nodo->nodo)
    DELETE /blobs/{id}              borrar (idempotente)
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import time
from contextlib import asynccontextmanager
from typing import AsyncIterator

import httpx
from fastapi import FastAPI, Request, Response
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel

from .blob_store import BlobStore, ChecksumMismatch, InvalidBlobId

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")

NODE_ID = os.environ.get("NODE_ID", "storage-local")
NODE_URL = os.environ.get("NODE_URL", "http://localhost:9000")
GATEWAY_URL = os.environ.get("GATEWAY_URL", "http://localhost:8080")
NODE_WEIGHT = float(os.environ.get("NODE_WEIGHT", "1"))
DATA_DIR = os.environ.get("DATA_DIR", "/data")
PUSH_TIMEOUT_S = float(os.environ.get("PUSH_TIMEOUT_SECONDS", "10"))

log = logging.getLogger(f"p16.{NODE_ID}")
store = BlobStore(DATA_DIR)
started_at = time.time()
_usage_cache: dict = {"at": 0.0, "count": 0, "bytes": 0}


async def register_with_gateway(http: httpx.AsyncClient) -> None:
    """Alta en el clúster (join). Se reintenta hasta que el gateway responda."""
    delay = 1.0
    payload = {"node_id": NODE_ID, "url": NODE_URL, "weight": NODE_WEIGHT}
    while True:
        try:
            resp = await http.post(f"{GATEWAY_URL}/cluster/nodes/join", json=payload, timeout=5)
            if resp.status_code < 300:
                state = resp.json()
                log.info("Registrado en el clúster: membership=%s health=%s", state["membership"], state["health"])
                return
            log.warning("Join rechazado (HTTP %d): %s", resp.status_code, resp.text[:200])
        except httpx.HTTPError as exc:
            log.warning("Gateway no disponible (%s); reintento en %.0f s", type(exc).__name__, delay)
        await asyncio.sleep(delay)
        delay = min(delay * 2, 10)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    leftovers = store.init()
    if leftovers:
        log.warning("Eliminados %d ficheros temporales de escrituras interrumpidas", leftovers)
    app.state.http = httpx.AsyncClient()
    join_task = asyncio.create_task(register_with_gateway(app.state.http))
    log.info("Storage node %s listo en %s (datos en %s)", NODE_ID, NODE_URL, DATA_DIR)
    try:
        yield
    finally:
        join_task.cancel()
        await app.state.http.aclose()


app = FastAPI(title=f"P16 Storage Node ({NODE_ID})", lifespan=lifespan)


@app.exception_handler(InvalidBlobId)
async def invalid_blob_id(_: Request, exc: InvalidBlobId) -> JSONResponse:
    return JSONResponse({"error": "blob_id inválido", "blob_id": str(exc)}, status_code=400)


def _usage() -> tuple[int, int]:
    now = time.time()
    if now - _usage_cache["at"] > 3:
        items = store.inventory()
        _usage_cache.update(at=now, count=len(items), bytes=sum(i["size"] for i in items))
    return _usage_cache["count"], _usage_cache["bytes"]


@app.get("/health")
async def health() -> dict:
    count, used = _usage()
    return {
        "status": "ok",
        "node": NODE_ID,
        "uptime_s": round(time.time() - started_at, 1),
        "blob_count": count,
        "used_bytes": used,
        "weight": NODE_WEIGHT,
    }


@app.get("/blobs")
async def inventory() -> dict:
    items = await asyncio.to_thread(store.inventory)
    return {"node": NODE_ID, "count": len(items), "blobs": items}


@app.put("/blobs/{blob_id}", status_code=201)
async def put_blob(blob_id: str, request: Request) -> JSONResponse:
    expected = request.headers.get("x-checksum-sha256", "").strip().lower()
    if len(expected) != 64:
        return JSONResponse({"error": "falta la cabecera X-Checksum-SHA256"}, status_code=400)
    try:
        sha, size = await store.write(blob_id, request.stream(), expected, request.headers.get("x-object-id"))
    except ChecksumMismatch as exc:
        log.warning("Rechazado blob %s: %s", blob_id, exc)
        return JSONResponse(
            {"error": "checksum-mismatch", "expected": exc.expected, "actual": exc.actual}, status_code=422
        )
    _usage_cache["at"] = 0.0
    return JSONResponse({"node": NODE_ID, "blob_id": blob_id, "sha256": sha, "size": size}, status_code=201)


@app.api_route("/blobs/{blob_id}", methods=["GET", "HEAD"])
async def get_blob(blob_id: str) -> Response:
    info = store.info(blob_id)
    if info is None:
        return JSONResponse({"error": "not-found", "blob_id": blob_id}, status_code=404)
    headers = {"X-Node-Id": NODE_ID}
    if info["recorded_sha256"]:
        # checksum registrado al escribir; el gateway recalcula el real y compara
        headers["X-Recorded-SHA256"] = info["recorded_sha256"]
    return FileResponse(info["path"], media_type="application/octet-stream", headers=headers)


@app.get("/blobs/{blob_id}/digest")
async def digest(blob_id: str) -> JSONResponse:
    info = store.info(blob_id)
    result = await asyncio.to_thread(store.digest, blob_id)
    if result is None or info is None:
        return JSONResponse({"error": "not-found", "blob_id": blob_id}, status_code=404)
    return JSONResponse(
        {"node": NODE_ID, "blob_id": blob_id, **result, "recorded_sha256": info["recorded_sha256"]}
    )


class PushRequest(BaseModel):
    target_url: str
    expected_sha256: str
    object_id: str | None = None


@app.post("/blobs/{blob_id}/push")
async def push(blob_id: str, req: PushRequest, request: Request) -> JSONResponse:
    """Copia nodo -> nodo usada por la reparación y el reequilibrado.
    Nunca propaga corrupción: verifica la copia local antes de enviarla y el
    destino vuelve a verificarla al recibirla."""
    data = await asyncio.to_thread(store.read, blob_id)
    if data is None:
        return JSONResponse({"error": "source-missing", "node": NODE_ID}, status_code=404)
    actual = hashlib.sha256(data).hexdigest()
    if actual != req.expected_sha256:
        log.warning("Push de %s abortado: la copia local está corrupta", blob_id)
        return JSONResponse(
            {"error": "source-corrupt", "node": NODE_ID, "expected": req.expected_sha256, "actual": actual},
            status_code=409,
        )
    http: httpx.AsyncClient = request.app.state.http
    headers = {
        "Content-Type": "application/octet-stream",
        "X-Checksum-SHA256": req.expected_sha256,
        "X-Object-Id": req.object_id or "",
    }
    try:
        resp = await http.put(f"{req.target_url}/blobs/{blob_id}", content=data, headers=headers, timeout=PUSH_TIMEOUT_S)
    except httpx.HTTPError as exc:
        return JSONResponse({"error": "target-unreachable", "detail": f"{type(exc).__name__} {exc}"}, status_code=502)
    if resp.status_code not in (200, 201):
        return JSONResponse(
            {"error": "target-rejected", "status": resp.status_code, "detail": resp.text[:300]}, status_code=502
        )
    log.info("Blob %s copiado a %s (%d B)", blob_id, req.target_url, len(data))
    return JSONResponse({"from": NODE_ID, "to": req.target_url, "blob_id": blob_id, "size": len(data)})


@app.delete("/blobs/{blob_id}", status_code=204)
async def delete_blob(blob_id: str) -> Response:
    if store.delete(blob_id):
        log.info("Blob %s eliminado", blob_id)
    _usage_cache["at"] = 0.0
    return Response(status_code=204)
