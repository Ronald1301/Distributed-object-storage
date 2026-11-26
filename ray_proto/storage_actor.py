"""Actor de almacenamiento persistente para el prototipo Ray.

Cada instancia usa un directorio propio en disco. El actor no conserva blobs en
memoria: al reiniciarse, ``BlobStore`` vuelve a abrir los mismos datos y
sidecars confirmados.
"""
from __future__ import annotations

import asyncio
import importlib.util
import os
import sys
from collections.abc import AsyncIterator
from pathlib import Path

STORAGE_NODE_NAMESPACE = "p16-storage-v1"
DEFAULT_MAX_RESTARTS = 1


def _load_blob_store():
    """Carga el BlobStore nativo aunque ``storage-node`` tenga guion en el nombre."""
    module_name = "_p16_native_blob_store"
    if module_name in sys.modules:
        return sys.modules[module_name]

    source = Path(__file__).resolve().parents[1] / "storage-node" / "app" / "blob_store.py"
    spec = importlib.util.spec_from_file_location(module_name, source)
    if spec is None or spec.loader is None:
        raise ImportError(f"no se pudo cargar BlobStore desde {source}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


_blob_store = _load_blob_store()
BlobStore = _blob_store.BlobStore
ChecksumMismatch = _blob_store.ChecksumMismatch
InvalidBlobId = _blob_store.InvalidBlobId


class SidecarMissingError(RuntimeError):
    """El blob existe, pero no tiene metadatos suficientes para validarlo."""


class _StorageNode:
    """Implementación sin estado en memoria; cada escritura confirmada vive en disco."""

    def __init__(self, root: str | Path):
        self.store = BlobStore(str(root))
        self.store.init()

    def put(
        self,
        blob_id: str,
        data: bytes,
        expected_sha256: str,
        object_id: str | None = None,
    ) -> dict[str, str | int]:
        """Persiste un blob y confirma sólo después de completar fsync y rename."""
        if not isinstance(data, bytes):
            raise TypeError("data debe ser bytes")
        # Validar antes de abrir archivos temporales o construir rutas.
        self.store.path(blob_id)

        async def chunks() -> AsyncIterator[bytes]:
            yield data

        sha256, size = asyncio.run(
            self.store.write(blob_id, chunks(), expected_sha256, object_id)
        )
        return {"blob_id": blob_id, "sha256": sha256, "size": size}

    def get(self, blob_id: str) -> bytes | None:
        """Lee y verifica el checksum del sidecar antes de devolver bytes."""
        info = self.store.info(blob_id)
        if info is None:
            return None
        expected = info["recorded_sha256"]
        if not isinstance(expected, str):
            raise SidecarMissingError(f"falta un checksum válido para {blob_id}")

        data = self.store.read(blob_id)
        if data is None:  # El archivo pudo desaparecer entre info() y read().
            return None
        import hashlib

        actual = hashlib.sha256(data).hexdigest()
        if actual != expected:
            raise ChecksumMismatch(expected, actual)
        return data

    def delete(self, blob_id: str) -> bool:
        return self.store.delete(blob_id)

    def inventory(self) -> list[dict]:
        return self.store.inventory()

    def digest(self, blob_id: str) -> dict | None:
        """Calcula el hash actual y también exige que coincida con el sidecar."""
        data = self.get(blob_id)
        if data is None:
            return None
        import hashlib

        return {"sha256": hashlib.sha256(data).hexdigest(), "size": len(data)}

    def process_id(self) -> int:
        """PID actual; útil para observar y probar reinicios del actor."""
        return os.getpid()


try:
    import ray
except ImportError:  # Permite ejecutar las pruebas del almacenamiento sin instalar Ray.
    ray = None

if ray is None:
    # La clase de implementación sigue siendo testeable con unittest sin Ray instalado.
    StorageNode = _StorageNode
else:
    StorageNode = ray.remote(_StorageNode)


def create_named_storage_node(
    root: str | Path,
    *,
    name: str,
    namespace: str = STORAGE_NODE_NAMESPACE,
    max_restarts: int = DEFAULT_MAX_RESTARTS,
):
    """Crea o recupera un actor de almacenamiento persistente y descubrible.

    El nombre es único dentro del namespace. El actor es detached para que
    sobreviva al driver que lo creó y Ray puede reiniciarlo; ``_StorageNode``
    vuelve a abrir el mismo directorio al ejecutar su constructor.
    """
    if ray is None:
        raise RuntimeError("Ray es necesario para crear un actor StorageNode")
    if not name.strip():
        raise ValueError("name no puede estar vacío")
    if not namespace.strip():
        raise ValueError("namespace no puede estar vacío")
    if not isinstance(max_restarts, int) or isinstance(max_restarts, bool) or max_restarts < 1:
        raise ValueError("max_restarts debe ser un entero mayor que cero")

    return StorageNode.options(
        name=name,
        namespace=namespace,
        lifetime="detached",
        max_restarts=max_restarts,
        get_if_exists=True,
    ).remote(str(root))


def get_named_storage_node(name: str, *, namespace: str = STORAGE_NODE_NAMESPACE):
    """Devuelve el actor nombrado dentro de su namespace de Ray."""
    if ray is None:
        raise RuntimeError("Ray es necesario para descubrir un actor StorageNode")
    return ray.get_actor(name, namespace=namespace)
