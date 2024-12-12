from pydantic import BaseModel


class SystemStatus(BaseModel):
    """Model for system status responses."""

    status: str
