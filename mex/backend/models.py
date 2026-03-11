from typing import Annotated

from pydantic import Field

from mex.backend.types import APIKey, APIUserPassword, ReferenceFieldName
from mex.common.models import BaseModel
from mex.common.types import Identifier


class APIKeyDatabase(BaseModel):
    """A lookup from access level to list of allowed APIKeys."""

    read: list[APIKey] = []
    write: list[APIKey] = []


class APIUserDatabase(BaseModel):
    """Database containing usernames and passwords for backend API."""

    read: dict[str, APIUserPassword] = {}
    write: dict[str, APIUserPassword] = {}


class ReferenceFieldFilter(BaseModel):
    """Reference field filter definition."""

    field: ReferenceFieldName
    identifiers: Annotated[list[Identifier], Field(min_length=1, max_length=100)]
