"""Almacenamiento local de blobs inmutables en el sistema de ficheros.

Estructura en el volumen del nodo:

    /data/blobs/<blob_id>            bytes del objeto
    /data/blobs/<blob_id>.meta.json  sidecar: checksum, tamaño, object_id, fecha
    /data/tmp/                       escrituras en curso

Escritura crash-safe: se escribe en tmp/, se verifica el SHA-256, se hace
fsync, y se publica con rename atómico (+ fsync del directorio en sistemas que
permiten abrir directorios con la API de Python). Windows no permite ese fsync
de directorio; allí se conserva el fsync del archivo y el rename atómico.
Un fallo a mitad nunca deja un blob parcial visible: sólo restos en tmp/, que
se limpian al arrancar.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import time
import uuid
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from pathlib import Path

BLOB_ID_RE = re.compile(r"^[0-9A-HJKMNP-TV-Z]{26}$")  # ULID (evita path traversal)
CHUNK = 1024 * 1024


class InvalidBlobId(ValueError):
    pass


class ChecksumMismatch(Exception):
    def __init__(self, expected: str, actual: str):
        super().__init__(f"checksum esperado {expected}, recibido {actual}")
        self.expected = expected
        self.actual = actual


def _fsync_dir(path: Path) -> None:
    # os.open no admite directorios en Windows. Los archivos temporales ya se
    # fsync antes del rename, así que se mantiene la publicación atómica allí.
    if os.name == "nt":
        return
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class BlobStore:
    def __init__(self, root: str):
        self.root = Path(root)
        self.blobs = self.root / "blobs"
        self.tmp = self.root / "tmp"

    def init(self) -> int:
        self.blobs.mkdir(parents=True, exist_ok=True)
        self.tmp.mkdir(parents=True, exist_ok=True)
        leftovers = 0
        for entry in self.tmp.iterdir():  # restos de escrituras interrumpidas por un crash
            entry.unlink(missing_ok=True)
            leftovers += 1
        return leftovers

    def path(self, blob_id: str) -> Path:
        if not BLOB_ID_RE.match(blob_id):
            raise InvalidBlobId(blob_id)
        return self.blobs / blob_id

    def _meta_path(self, blob_id: str) -> Path:
        return self.blobs / f"{blob_id}.meta.json"

    async def write(
        self, blob_id: str, chunks: AsyncIterator[bytes], expected_sha256: str, object_id: str | None
    ) -> tuple[str, int]:
        final = self.path(blob_id)
        tmp = self.tmp / f"{blob_id}.{uuid.uuid4().hex}"
        digest = hashlib.sha256()
        size = 0
        try:
            # Escritura local por bloques para conservar memoria acotada.
            with open(tmp, "wb") as fh:  # noqa: ASYNC230
                async for chunk in chunks:
                    digest.update(chunk)
                    fh.write(chunk)
                    size += len(chunk)
                fh.flush()
                os.fsync(fh.fileno())
            actual = digest.hexdigest()
            if actual != expected_sha256:
                raise ChecksumMismatch(expected_sha256, actual)
            meta_tmp = tmp.parent / f"{tmp.name}.meta"
            # El sidecar se publica junto con el blob mediante rename atómico.
            with open(meta_tmp, "w") as fh:  # noqa: ASYNC230
                json.dump(
                    {
                        "blob_id": blob_id,
                        "object_id": object_id,
                        "sha256": actual,
                        "size": size,
                        # datetime.UTC no existe en la versión mínima Python 3.10.
                        "stored_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),  # noqa: UP017
                    },
                    fh,
                )
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, final)  # publicación atómica (sobrescribe una copia corrupta)
            os.replace(meta_tmp, self._meta_path(blob_id))
            _fsync_dir(self.blobs)
            return actual, size
        finally:
            tmp.unlink(missing_ok=True)
            (tmp.parent / f"{tmp.name}.meta").unlink(missing_ok=True)

    def info(self, blob_id: str) -> dict | None:
        path = self.path(blob_id)
        if not path.is_file():
            return None
        meta: dict = {}
        try:
            meta = json.loads(self._meta_path(blob_id).read_text())
        except (OSError, ValueError):
            pass
        return {"path": path, "size": path.stat().st_size, "recorded_sha256": meta.get("sha256")}

    def read(self, blob_id: str) -> bytes | None:
        path = self.path(blob_id)
        try:
            return path.read_bytes()
        except FileNotFoundError:
            return None

    def digest(self, blob_id: str) -> dict | None:
        """Recalcula el SHA-256 leyendo el disco (scrub)."""
        path = self.path(blob_id)
        digest = hashlib.sha256()
        size = 0
        try:
            with open(path, "rb") as fh:
                while chunk := fh.read(CHUNK):
                    digest.update(chunk)
                    size += len(chunk)
        except FileNotFoundError:
            return None
        return {"sha256": digest.hexdigest(), "size": size}

    def delete(self, blob_id: str) -> bool:
        path = self.path(blob_id)
        existed = path.exists()
        path.unlink(missing_ok=True)
        self._meta_path(blob_id).unlink(missing_ok=True)
        return existed

    def inventory(self) -> list[dict]:
        now = time.time()
        items = []
        for entry in self.blobs.iterdir():
            if not BLOB_ID_RE.match(entry.name):
                continue  # sidecars .meta.json
            try:
                st = entry.stat()
            except FileNotFoundError:
                continue
            items.append({"blob_id": entry.name, "size": st.st_size, "age_s": round(now - st.st_mtime, 1)})
        items.sort(key=lambda b: b["blob_id"])
        return items
