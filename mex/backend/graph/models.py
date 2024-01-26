from typing import Any

from pydantic import BaseModel


class GraphResult(BaseModel):
    """Model for graph query results."""

    data: list[dict[str, Any]] = []
