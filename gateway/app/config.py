"""Configuración del gateway, leída de variables de entorno.

Todos los parámetros que afectan al contrato del sistema (factor de
replicación, ACKs de escritura, tiempos del detector de fallos...) se
declaran aquí para que sean visibles y ajustables desde docker-compose.yml.
"""
from __future__ import annotations

import os
from dataclasses import asdict, dataclass


def _int(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    return int(raw) if raw else default


def _float(name: str, default: float) -> float:
    raw = os.environ.get(name, "").strip()
    return float(raw) if raw else default


@dataclass(frozen=True)
class Settings:
    database_url: str
    # --- Redundancia y durabilidad ---
    replication_factor: int  # RF: copias que el sistema intenta mantener
    write_acks: int  # W: copias confirmadas (fsync) necesarias para responder a un PUT
    max_object_bytes: int
    # --- Plano de datos ---
    storage_timeout_s: float  # timeout de cada operación contra un Storage Node
    # --- Detector de fallos ---
    health_interval_s: float  # periodo de sondeo /health
    health_timeout_s: float
    suspect_after: int  # fallos consecutivos para pasar a SUSPECT
    down_after: int  # fallos consecutivos para pasar a DOWN
    repair_delay_s: float  # tiempo en DOWN antes de re-replicar sus datos
    # --- Reconciliación / reparación ---
    reconcile_interval_s: float
    scrub_interval_s: float
    orphan_grace_s: float  # antigüedad mínima de un blob huérfano antes de borrarlo
    delete_grace_s: float  # retardo del borrado físico (lecturas en curso de versiones antiguas)
    max_copies_per_cycle: int  # límite de copias por ciclo (throttling del backfill)

    def public(self) -> dict:
        data = asdict(self)
        data.pop("database_url")
        return data


def load_settings() -> Settings:
    rf = _int("REPLICATION_FACTOR", 2)
    acks = _int("WRITE_ACKS", rf)
    if rf < 1:
        raise ValueError("REPLICATION_FACTOR debe ser >= 1")
    if not 1 <= acks <= rf:
        raise ValueError("WRITE_ACKS debe estar entre 1 y REPLICATION_FACTOR")
    return Settings(
        database_url=os.environ.get("DATABASE_URL", "postgresql://p16:p16@localhost:5432/p16"),
        replication_factor=rf,
        write_acks=acks,
        max_object_bytes=_int("MAX_OBJECT_SIZE_MB", 64) * 1024 * 1024,
        storage_timeout_s=_float("STORAGE_TIMEOUT_SECONDS", 5),
        health_interval_s=_float("HEALTH_INTERVAL_SECONDS", 2),
        health_timeout_s=_float("HEALTH_TIMEOUT_SECONDS", 1),
        suspect_after=_int("SUSPECT_AFTER", 1),
        down_after=_int("DOWN_AFTER", 3),
        repair_delay_s=_float("REPAIR_DELAY_SECONDS", 30),
        reconcile_interval_s=_float("RECONCILE_INTERVAL_SECONDS", 10),
        scrub_interval_s=_float("SCRUB_INTERVAL_SECONDS", 300),
        orphan_grace_s=_float("ORPHAN_GRACE_SECONDS", 60),
        delete_grace_s=_float("DELETE_GRACE_SECONDS", 5),
        max_copies_per_cycle=_int("MAX_COPIES_PER_CYCLE", 200),
    )
