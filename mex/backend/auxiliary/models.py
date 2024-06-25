from typing import Generic, TypeVar

from pydantic import BaseModel

T = TypeVar("T")


class PagedResponseSchema(BaseModel, Generic[T]):
    """Response schema for any paged API."""

    total: int
    offset: int
    limit: int
    results: list[T]
