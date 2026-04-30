from typing import Annotated, TypedDict

from pydantic import BaseModel, Field

from mex.backend.types import (
    APIKey,
    APIUserPassword,
    ReferenceFieldName,
)
from mex.common.types import Identifier


class APIKeyDatabase(BaseModel):
    """A lookup from access level to list of allowed APIKeys."""

    read: list[APIKey] = []
    write: list[APIKey] = []


class APIUserDatabase(BaseModel):
    """Database containing usernames and passwords for backend API."""

    read: dict[str, APIUserPassword] = {}
    write: dict[str, APIUserPassword] = {}


class ReferenceFilter(BaseModel):
    """Reference filter definition with a field and list of identifiers."""

    field: ReferenceFieldName
    identifiers: Annotated[list[Identifier | None], Field(min_length=1, max_length=100)]


class RawReferenceFilter(TypedDict):
    """Reference filter in raw dictionary form to be used as cypher parameter.

    The MEX editor primary source identifier and None values are replaced with
    a sentinel so the cypher query can match nodes without that relationship.
    """

    field: str
    identifiers: list[str]
