"""Cliente HTTP del plano de datos (gateway -> Storage Nodes)."""
from __future__ import annotations

import httpx

from .config import Settings
from .membership import NodeState


class StorageError(Exception):
    """Error hablando con un Storage Node.

    kind = "network": no hubo respuesta (nodo caído, timeout, DNS...)
    kind = "http":    el nodo respondió con un código de error (status)
    """

    def __init__(self, message: str, *, kind: str = "http", status: int | None = None, body: dict | None = None):
        super().__init__(message)
        self.kind = kind
        self.status = status
        self.body = body or {}


def _json(resp: httpx.Response) -> dict:
    try:
        data = resp.json()
        return data if isinstance(data, dict) else {"data": data}
    except ValueError:
        return {"detail": resp.text[:200]}


class StorageClient:
    def __init__(self, settings: Settings):
        self._settings = settings
        self._http = httpx.AsyncClient(timeout=settings.storage_timeout_s)

    async def close(self) -> None:
        await self._http.aclose()

    async def _request(self, method: str, url: str, *, timeout: float | None = None, **kwargs) -> httpx.Response:
        try:
            return await self._http.request(
                method, url, timeout=timeout or self._settings.storage_timeout_s, **kwargs
            )
        except httpx.HTTPError as exc:
            raise StorageError(f"{method} {url}: {type(exc).__name__} {exc}".strip(), kind="network") from exc

    @staticmethod
    def _fail(resp: httpx.Response, what: str) -> StorageError:
        body = _json(resp)
        return StorageError(f"{what}: HTTP {resp.status_code} {body}", status=resp.status_code, body=body)

    # ------------------------------------------------------------------ API
    async def health(self, node: NodeState) -> dict:
        resp = await self._request("GET", f"{node.url}/health", timeout=self._settings.health_timeout_s)
        if resp.status_code != 200:
            raise self._fail(resp, f"health {node.node_id}")
        return _json(resp)

    async def put_blob(self, node: NodeState, blob_id: str, data: bytes, checksum: str, object_id: str) -> None:
        resp = await self._request(
            "PUT",
            f"{node.url}/blobs/{blob_id}",
            content=data,
            headers={
                "Content-Type": "application/octet-stream",
                "X-Checksum-SHA256": checksum,
                "X-Object-Id": object_id,
            },
        )
        if resp.status_code not in (200, 201):
            raise self._fail(resp, f"PUT blob {blob_id} en {node.node_id}")

    async def get_blob(self, node: NodeState, blob_id: str) -> bytes | None:
        resp = await self._request("GET", f"{node.url}/blobs/{blob_id}")
        if resp.status_code == 404:
            return None
        if resp.status_code != 200:
            raise self._fail(resp, f"GET blob {blob_id} en {node.node_id}")
        return resp.content

    async def delete_blob(self, node: NodeState, blob_id: str) -> None:
        resp = await self._request("DELETE", f"{node.url}/blobs/{blob_id}")
        if resp.status_code not in (200, 204, 404):
            raise self._fail(resp, f"DELETE blob {blob_id} en {node.node_id}")

    async def list_blobs(self, node: NodeState) -> list[dict]:
        resp = await self._request("GET", f"{node.url}/blobs")
        if resp.status_code != 200:
            raise self._fail(resp, f"inventario de {node.node_id}")
        return resp.json()["blobs"]

    async def digest(self, node: NodeState, blob_id: str) -> dict | None:
        """El nodo recalcula el SHA-256 leyendo el disco (scrub profundo)."""
        resp = await self._request("GET", f"{node.url}/blobs/{blob_id}/digest")
        if resp.status_code == 404:
            return None
        if resp.status_code != 200:
            raise self._fail(resp, f"digest {blob_id} en {node.node_id}")
        return resp.json()

    async def push_blob(
        self, source: NodeState, blob_id: str, target: NodeState, checksum: str, object_id: str
    ) -> dict:
        """Copia nodo -> nodo: el origen verifica su copia y la envía al destino,
        que vuelve a verificar el checksum antes de aceptarla."""
        resp = await self._request(
            "POST",
            f"{source.url}/blobs/{blob_id}/push",
            json={"target_url": target.url, "expected_sha256": checksum, "object_id": object_id},
            timeout=self._settings.storage_timeout_s * 2,
        )
        if resp.status_code != 200:
            raise self._fail(resp, f"push {blob_id} {source.node_id}->{target.node_id}")
        return _json(resp)
