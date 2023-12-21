from enum import Enum

from pydantic import SecretStr

from mex.common.models import BaseModel


class AccessLevel(Enum):
    """Enum of access level."""

    READ = "read"
    WRITE = "write"


class APIKey(SecretStr):
    """An API Key used for authenticating and authorizing a client."""

    def __repr__(self) -> str:
        """Return a secure representation of this key."""
        return f"APIKey('{self}')"


class APIKeyDatabase(BaseModel):
    """A lookup from access level to list of allowed APIKeys."""

    read: list[APIKey] = []
    write: list[APIKey] = []


class APIUserDatabase(BaseModel):
    """Database containing usernames and passwords for backend API."""

    read: dict[str, APIKey] = {}
    write: dict[str, APIKey] = {}
