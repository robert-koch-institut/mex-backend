from typing import Generic, TypeVar

from mex.common.models import BaseModel, ExtractedData

ExtractedItemT = TypeVar("ExtractedItemT", bound=ExtractedData)


class AuxiliarySearch(BaseModel, Generic[ExtractedItemT]):
    """Result of searching for extracted items in auxiliary sources."""

    items: list[ExtractedItemT]
    total: int
