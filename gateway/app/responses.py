"""Respuesta JSON indentada (legible directamente con curl en la demo)."""
from __future__ import annotations

import json
from typing import Any

from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse


class PrettyJSONResponse(JSONResponse):
    def render(self, content: Any) -> bytes:
        return json.dumps(jsonable_encoder(content), ensure_ascii=False, indent=2).encode("utf-8") + b"\n"
