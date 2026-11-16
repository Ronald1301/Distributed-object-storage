"""Errores de la API con código HTTP y cuerpo JSON explicativo."""
from __future__ import annotations

from typing import Any


class ApiError(Exception):
    def __init__(self, status: int, message: str, **extra: Any):
        super().__init__(message)
        self.status = status
        self.message = message
        self.extra = extra

    def body(self) -> dict:
        return {"error": self.message, **self.extra}
