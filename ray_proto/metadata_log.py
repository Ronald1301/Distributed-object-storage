"""Durabilidad del MetadataService: registro de escritura anticipada (WAL) + snapshot.

Estructura en disco (un directorio propio por instancia del actor)::

    <root>/wal.log         una línea por mutación confirmada, en orden de ``seq``
    <root>/snapshot.json   estado completo hasta ``last_seq`` (escritura atómica)
    <root>/tmp/            ficheros temporales del snapshot y del WAL nuevo

Formato de cada línea (también el del snapshot): ``<crc32 en hex, 8 car.> <json>\\n``.

Reglas:

* Una mutación sólo se confirma al llamante después de que su registro está en el
  WAL con ``fsync`` (mismo criterio que la escritura de blobs: nada se confirma
  antes de estar en disco).
* El snapshot se escribe en ``tmp/``, se hace ``fsync``, se publica con
  ``os.replace`` y se hace ``fsync`` del directorio. Después se sustituye el WAL por
  uno vacío. Si el proceso muere entre ambos pasos, la recuperación descarta los
  registros con ``seq <= last_seq`` y no aplica nada dos veces.
* Una última línea incompleta o con CRC inválido es una escritura que murió a
  mitad: nunca se confirmó, así que se trunca. Una línea inválida **seguida** de
  líneas válidas no puede venir de un corte (cada línea se sincroniza antes de
  escribir la siguiente): es corrupción y la recuperación falla cerrada.
"""
from __future__ import annotations

import json
import os
import zlib
from pathlib import Path
from typing import Any

from ray_proto.storage_actor import _blob_store as _native_blob_store

FORMAT_VERSION = 1


class CorruptLogError(RuntimeError):
    """El WAL o el snapshot no se pueden interpretar sin perder datos confirmados."""


class LogUnavailableError(RuntimeError):
    """Una escritura anterior falló y no se pudo deshacer: hace falta reiniciar."""


def _fsync_dir(path: Path) -> None:
    # Se resuelve en cada llamada para que los tests puedan sustituirla en Windows,
    # igual que en ray_proto/tests/test_storage_actor.py.
    _native_blob_store._fsync_dir(path)


def encode_line(payload: Any) -> bytes:
    body = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return b"%08x " % zlib.crc32(body) + body + b"\n"


def decode_line(line: bytes) -> Any:
    """Devuelve el contenido de una línea completa o lanza ``ValueError``."""
    if not line.endswith(b"\n") or len(line) < 10 or line[8:9] != b" ":
        raise ValueError("línea incompleta")
    body = line[9:-1]
    if int(line[:8], 16) != zlib.crc32(body):
        raise ValueError("CRC32 no coincide")
    return json.loads(body.decode("utf-8"))


class MetadataLog:
    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.wal_path = self.root / "wal.log"
        self.snapshot_path = self.root / "snapshot.json"
        self.tmp = self.root / "tmp"
        self._fh = None
        self._broken = False

    # ------------------------------------------------------------ recuperación
    def recover(self) -> tuple[dict | None, list[dict], dict]:
        """Lee snapshot y WAL. Devuelve ``(snapshot, registros_a_reaplicar, info)``.

        Debe llamarse una vez, antes de ``open()``. Trunca la cola rota del WAL."""
        self.root.mkdir(parents=True, exist_ok=True)
        self.tmp.mkdir(exist_ok=True)
        for entry in self.tmp.iterdir():  # restos de un snapshot interrumpido
            entry.unlink(missing_ok=True)

        snapshot = self._read_snapshot()
        last_seq = snapshot["last_seq"] if snapshot else 0
        records, skipped, discarded = self._read_wal(last_seq)
        info = {
            "snapshot_seq": last_seq if snapshot else None,
            "replayed_records": len(records),
            "skipped_records": skipped,
            "discarded_tail_bytes": discarded,
        }
        return snapshot, records, info

    def _read_snapshot(self) -> dict | None:
        if not self.snapshot_path.exists():
            return None
        try:
            snapshot = decode_line(self.snapshot_path.read_bytes())
        except ValueError as exc:
            raise CorruptLogError(f"snapshot ilegible ({exc}): {self.snapshot_path}") from exc
        if snapshot.get("format") != FORMAT_VERSION:
            raise CorruptLogError(f"formato de snapshot desconocido: {snapshot.get('format')!r}")
        return snapshot

    def _read_wal(self, last_seq: int) -> tuple[list[dict], int, int]:
        if not self.wal_path.exists():
            return [], 0, 0
        lines = self.wal_path.read_bytes().splitlines(keepends=True)
        records: list[dict] = []
        skipped = 0
        offset = 0
        expected = last_seq + 1
        for index, line in enumerate(lines):
            try:
                record = decode_line(line)
            except ValueError as exc:
                if any(self._is_valid(rest) for rest in lines[index + 1:]):
                    raise CorruptLogError(
                        f"registro {index + 1} del WAL corrupto ({exc}) con registros válidos detrás"
                    ) from exc
                discarded = sum(len(rest) for rest in lines[index:])
                self._truncate(offset)
                return records, skipped, discarded
            seq = record.get("seq")
            if not isinstance(seq, int):
                raise CorruptLogError(f"registro {index + 1} del WAL sin seq")
            if seq <= last_seq:
                skipped += 1  # ya incluido en el snapshot
            elif seq != expected:
                raise CorruptLogError(f"hueco en el WAL: se esperaba seq {expected}, hay {seq}")
            else:
                records.append(record)
                expected += 1
            offset += len(line)
        return records, skipped, 0

    @staticmethod
    def _is_valid(line: bytes) -> bool:
        try:
            decode_line(line)
        except ValueError:
            return False
        return True

    def _truncate(self, size: int) -> None:
        with open(self.wal_path, "r+b") as fh:
            fh.truncate(size)
            fh.flush()
            os.fsync(fh.fileno())

    # --------------------------------------------------------------- escritura
    def open(self) -> None:
        # Handle de larga duración (se cierra en close()/snapshot): no cabe en un with.
        self._fh = open(self.wal_path, "ab")  # noqa: SIM115
        _fsync_dir(self.root)  # el fichero del WAL existe de forma duradera

    def close(self) -> None:
        if self._fh is not None:
            self._fh.close()
            self._fh = None

    def append(self, record: dict) -> dict:
        """Escribe y sincroniza un registro. Devuelve el registro tal como quedó en
        disco (tras pasar por JSON), que es lo que se debe aplicar en memoria."""
        if self._broken:
            raise LogUnavailableError("el WAL quedó en un estado dudoso; reinicie el actor")
        if self._fh is None:
            raise RuntimeError("MetadataLog.open() no se ha llamado")
        line = encode_line(record)  # puede fallar (p. ej. TypeError) sin tocar el disco
        start = self._fh.tell()
        try:
            self._fh.write(line)
            self._fh.flush()
            os.fsync(self._fh.fileno())
        except BaseException:
            self._rollback(start)
            raise
        return decode_line(line)

    def _rollback(self, size: int) -> None:
        """Deja el WAL como estaba antes del registro fallido, o lo bloquea."""
        try:
            self._fh.truncate(size)
            self._fh.seek(size)
            self._fh.flush()
            os.fsync(self._fh.fileno())
        except BaseException:  # noqa: BLE001 - cualquier fallo deja el WAL dudoso: se bloquea
            self._broken = True

    # ---------------------------------------------------------------- snapshot
    def write_snapshot(self, state: dict, last_seq: int) -> None:
        """Publica el snapshot de forma atómica y empieza un WAL vacío."""
        payload = {"format": FORMAT_VERSION, "last_seq": last_seq, "state": state}
        tmp = self.tmp / "snapshot.json"
        with open(tmp, "wb") as fh:
            fh.write(encode_line(payload))
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, self.snapshot_path)
        _fsync_dir(self.root)
        # A partir de aquí el WAL antiguo es redundante (seq <= last_seq): si no se
        # consigue sustituir, se sigue escribiendo en él y la recuperación lo filtra.
        self.close()
        try:
            new_wal = self.tmp / "wal.log"
            with open(new_wal, "wb") as fh:
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(new_wal, self.wal_path)
            _fsync_dir(self.root)
        finally:
            try:
                self.open()
            except BaseException:
                self._broken = True
                raise
