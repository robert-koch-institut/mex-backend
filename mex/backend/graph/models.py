from typing import Any

from pydantic import BaseModel as PydanticBaseModel


class GraphResult(PydanticBaseModel):
    """Model for graph query results."""

    data: list[dict[str, Any]] = []
