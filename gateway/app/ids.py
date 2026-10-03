"""Identificadores e integridad.

- ULID: identificador estable de 128 bits (48 bits de tiempo + 80 aleatorios),
  codificado en 26 caracteres Crockford base32. Se genera sin coordinación
  (cada gateway puede crearlos) y es ordenable por fecha de creación.
- SHA-256: checksum de integridad extremo a extremo de cada objeto.
"""
from __future__ import annotations

import hashlib
import os
import re
import time

_CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
ULID_RE = re.compile(r"^[0-9A-HJKMNP-TV-Z]{26}$")


def new_ulid(now_ms: int | None = None) -> str:
    ts = int(time.time() * 1000) if now_ms is None else now_ms
    time_part = ""
    for _ in range(10):  # 48 bits de tiempo -> 10 caracteres
        time_part = _CROCKFORD[ts % 32] + time_part
        ts //= 32
    # 256 es múltiplo de 32, por lo que b % 32 es uniforme
    rand_part = "".join(_CROCKFORD[b % 32] for b in os.urandom(16))
    return time_part + rand_part


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def etag_for(checksum: str) -> str:
    return f'"{checksum}"'


def etag_matches(header_value: str, checksum: str | None) -> bool:
    """Evalúa If-Match / If-None-Match (lista de ETags o '*')."""
    if checksum is None:
        return False
    for candidate in header_value.split(","):
        candidate = candidate.strip()
        if candidate.startswith("W/"):
            candidate = candidate[2:]
        if candidate == "*" or candidate.strip('"') == checksum:
            return True
    return False
