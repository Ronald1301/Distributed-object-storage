"""Feed en memoria de la actividad reciente, para la interfaz visual (/ui).

Cada petición de un cliente (PUT, GET, HEAD, DELETE, LIST) y cada evento del
clúster (caídas, reparaciones, corrupción...) se añade aquí con un número de
secuencia. La interfaz pide periódicamente lo nuevo (`/cluster/feed?after=N`)
y lo anima. No se persiste: es sólo para observar el sistema en vivo.
"""
from __future__ import annotations

import uuid
from collections import deque
from datetime import datetime, timezone
from typing import Any


class LiveFeed:
    def __init__(self, size: int = 2000):
        self.boot_id = uuid.uuid4().hex[:8]  # cambia si el gateway se reinicia
        self._items: deque[dict] = deque(maxlen=size)
        self._seq = 0

    @property
    def seq(self) -> int:
        return self._seq

    def push(self, kind: str, **data: Any) -> None:
        self._seq += 1
        self._items.append(
            {"seq": self._seq, "ts": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
             "kind": kind, **data}
        )

    def since(self, after: int, limit: int = 300) -> list[dict]:
        if after < 0:  # el cliente sólo quiere sincronizarse
            return []
        if after > self._seq:  # el gateway se reinició: se reenvía lo que haya
            after = 0
        return [item for item in self._items if item["seq"] > after][:limit]
