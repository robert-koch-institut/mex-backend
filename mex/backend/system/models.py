from pydantic import BaseModel


class SystemStatus(BaseModel):
    """Model for system status responses."""

    status: str
    version: str


class GraphStatus(BaseModel):
    """Model for graph status responses."""

    status: str
