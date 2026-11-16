"""API pública (plano de aplicación): PUT / GET / HEAD / DELETE / LIST.

Rutas estilo S3 con bucket explícito y atajos con el bucket "default":

    PUT    /buckets/{bucket}/objects/{key}      PUT    /objects/{key}
    GET    /buckets/{bucket}/objects/{key}      GET    /objects/{key}
    HEAD   /buckets/{bucket}/objects/{key}      HEAD   /objects/{key}
    DELETE /buckets/{bucket}/objects/{key}      DELETE /objects/{key}
    GET    /buckets/{bucket}/objects?prefix=    GET    /objects?prefix=
    GET    /buckets
"""
from __future__ import annotations

import mimetypes
import time
from email.utils import format_datetime

from fastapi import APIRouter, Depends, Request, Response

from .errors import ApiError
from .ids import etag_for
from .responses import PrettyJSONResponse
from .services import Services, get_services

router = APIRouter(tags=["objects"])

DEFAULT_BUCKET = "default"
META_PREFIX = "x-meta-"


def _object_headers(obj: dict, replicas: list[dict] | None = None) -> dict[str, str]:
    headers = {
        "ETag": etag_for(obj["checksum"]),
        "Last-Modified": format_datetime(obj["updated_at"], usegmt=True),
        "X-Object-Id": obj["id"],
        "X-Object-Version": str(obj["version"]),
        "X-Checksum-SHA256": obj["checksum"],
    }
    for name, value in (obj.get("user_metadata") or {}).items():
        headers[f"X-Meta-{name}"] = value
    if replicas is not None:
        headers["X-Replicas"] = ",".join(f"{r['node_id']}:{r['status']}" for r in replicas)
    return headers


def _content_type(request: Request, key: str) -> str:
    declared = request.headers.get("content-type", "").strip()
    # curl --data-binary envía form-urlencoded por defecto: en ese caso se infiere por la extensión
    if not declared or declared.startswith("application/x-www-form-urlencoded"):
        return mimetypes.guess_type(key)[0] or "application/octet-stream"
    return declared


def _track(s: Services, op: str, bucket: str, key: str | None, status: int, started: float, **info) -> None:
    """Deja constancia de la petición en el feed en vivo (lo anima la interfaz /ui)."""
    s.feed.push("request", op=op, bucket=bucket, key=key, status=status,
                ms=round((time.perf_counter() - started) * 1000, 1), **info)


async def _read_body(request: Request, limit: int) -> bytes:
    length = request.headers.get("content-length")
    if length and int(length) > limit:
        raise ApiError(413, f"Objeto demasiado grande (máximo {limit // (1024 * 1024)} MB)")
    chunks: list[bytes] = []
    size = 0
    async for chunk in request.stream():
        size += len(chunk)
        if size > limit:
            raise ApiError(413, f"Objeto demasiado grande (máximo {limit // (1024 * 1024)} MB)")
        chunks.append(chunk)
    return b"".join(chunks)


# ---------------------------------------------------------------------- LIST
@router.get("/buckets")
async def list_buckets(s: Services = Depends(get_services)) -> dict:
    return {"buckets": await s.meta.list_buckets()}


@router.get("/objects")
@router.get("/buckets/{bucket}/objects")
async def list_objects(
    bucket: str = DEFAULT_BUCKET,
    prefix: str = "",
    after: str | None = None,
    limit: int = 100,
    s: Services = Depends(get_services),
) -> dict:
    started = time.perf_counter()
    result = await s.objects.list(bucket, prefix, after, limit)
    _track(s, "LIST", bucket, prefix or None, 200, started, count=result["count"])
    return result


# ---------------------------------------------------------------------- HEAD
# (declarado antes que GET para que HEAD no se resuelva con el handler de GET)
@router.head("/objects/{key:path}")
@router.head("/buckets/{bucket}/objects/{key:path}")
async def head_object(key: str, bucket: str = DEFAULT_BUCKET, s: Services = Depends(get_services)) -> Response:
    started = time.perf_counter()
    obj, replicas = await s.objects.head(bucket, key)
    _track(s, "HEAD", bucket, key, 200, started, object_id=obj["id"], version=obj["version"],
           replicas=[r["node_id"] for r in replicas if r["status"] == "OK"])
    headers = _object_headers(obj, replicas)
    headers["Content-Length"] = str(obj["size"])
    return Response(status_code=200, headers=headers, media_type=obj["content_type"])


# ----------------------------------------------------------------------- GET
@router.get("/objects/{key:path}")
@router.get("/buckets/{bucket}/objects/{key:path}")
async def get_object(
    request: Request, key: str, bucket: str = DEFAULT_BUCKET, s: Services = Depends(get_services)
) -> Response:
    if not key:  # "/objects/" se comporta como LIST
        return await list_objects(bucket=bucket, s=s)  # type: ignore[return-value]
    started = time.perf_counter()
    result = await s.objects.get(bucket, key, if_none_match=request.headers.get("if-none-match"))
    headers = _object_headers(result.obj)
    _track(s, "GET", bucket, key, 304 if result.data is None else 200, started, object_id=result.obj["id"],
           version=result.obj["version"], size=result.obj["size"], served_by=result.served_by,
           attempts=result.attempts)
    if result.data is None:
        return Response(status_code=304, headers=headers)
    headers["X-Served-By"] = result.served_by or ""
    headers["X-Read-Attempts"] = ",".join(result.attempts)
    return Response(content=result.data, media_type=result.obj["content_type"], headers=headers)


# ----------------------------------------------------------------------- PUT
@router.put("/objects/{key:path}")
@router.put("/buckets/{bucket}/objects/{key:path}")
async def put_object(
    request: Request, key: str, bucket: str = DEFAULT_BUCKET, s: Services = Depends(get_services)
) -> Response:
    started = time.perf_counter()
    data = await _read_body(request, s.settings.max_object_bytes)
    user_metadata = {
        name[len(META_PREFIX):]: value
        for name, value in request.headers.items()
        if name.lower().startswith(META_PREFIX)
    }
    result = await s.objects.put(
        bucket,
        key,
        data,
        _content_type(request, key),
        user_metadata,
        if_match=request.headers.get("if-match"),
        if_none_match=request.headers.get("if-none-match"),
    )
    obj = result["object"]
    stored = result["stored_on"]
    body = {
        "id": obj["id"],
        "bucket": obj["bucket"],
        "key": obj["key"],
        "version": obj["version"],
        "size": obj["size"],
        "content_type": obj["content_type"],
        "checksum_sha256": obj["checksum"],
        "etag": etag_for(obj["checksum"]),
        "replicas": stored,
        "desired_placement": result["desired"],
        "durability": (
            f"{len(stored)} copias en nodos distintos, cada una con checksum verificado y fsync;"
            " metadatos confirmados en PostgreSQL"
        ),
    }
    if result["failures"]:
        body["write_failures"] = result["failures"]
    _track(s, "PUT", bucket, key, 201 if result["created"] else 200, started, object_id=obj["id"],
           version=obj["version"], size=obj["size"], replicas=stored, desired=result["desired"],
           failures=result["failures"])
    return PrettyJSONResponse(
        body,
        status_code=201 if result["created"] else 200,
        headers={"ETag": etag_for(obj["checksum"]), "X-Object-Id": obj["id"]},
    )


# -------------------------------------------------------------------- DELETE
@router.delete("/objects/{key:path}")
@router.delete("/buckets/{bucket}/objects/{key:path}")
async def delete_object(key: str, bucket: str = DEFAULT_BUCKET, s: Services = Depends(get_services)) -> Response:
    started = time.perf_counter()
    deleted = await s.objects.delete(bucket, key)
    _track(s, "DELETE", bucket, key, 204, started, deleted=deleted is not None,
           object_id=deleted["id"] if deleted else None,
           queued=deleted["queued_deletions"] if deleted else [])
    # DELETE es idempotente (como en S3): 204 exista o no el objeto.
    return Response(status_code=204, headers={"X-Deleted": "true" if deleted else "false"})
