"""Contenedor de servicios compartidos (inyección de dependencias sencilla)."""
from __future__ import annotations

from dataclasses import dataclass

from fastapi import Request

from .config import Settings
from .db import Database
from .events import EventLog
from .health_monitor import HealthMonitor
from .live import LiveFeed
from .membership import Membership
from .metadata import MetadataRepository
from .objects_service import ObjectService
from .reconciler import Reconciler
from .storage_client import StorageClient


@dataclass
class Services:
    settings: Settings
    db: Database
    events: EventLog
    membership: Membership
    storage: StorageClient
    meta: MetadataRepository
    reconciler: Reconciler
    objects: ObjectService
    monitor: HealthMonitor
    feed: LiveFeed


def get_services(request: Request) -> Services:
    return request.app.state.services
