from typing import Generic, TypeVar

from pydantic import BaseModel

ResponseItemT = TypeVar("ResponseItemT", bound=BaseModel)


class PagedAuxiliaryResponse(BaseModel, Generic[ResponseItemT]):
    """Response model for any paged aux API."""

    items: list[ResponseItemT]
    total: int
