from typing import Generic, TypeVar

from mex.common.models import AnyExtractedModel, BaseModel

ExtractedModelT = TypeVar("ExtractedModelT", bound=AnyExtractedModel)


class AuxiliarySearch(BaseModel, Generic[ExtractedModelT]):
    """Result of searching for extracted items in auxiliary sources."""

    items: list[ExtractedModelT]
    total: int
