from enum import Enum
from typing import TYPE_CHECKING, Union

from pydantic import Field

from mex.backend.types import DynamicStrEnum
from mex.common.models import (
    EXTRACTED_MODEL_CLASSES_BY_NAME,
    BaseExtractedData,
    BaseModel,
)


class ExtractedType(Enum, metaclass=DynamicStrEnum):
    """Enumeration of possible types for extracted items."""

    __names__ = list(EXTRACTED_MODEL_CLASSES_BY_NAME)


if TYPE_CHECKING:  # pragma: no cover
    AnyExtractedModel = BaseExtractedData
else:
    AnyExtractedModel = Union[*EXTRACTED_MODEL_CLASSES_BY_NAME.values()]


class ExtractedItemSearchResponse(BaseModel):
    """Response body for the extracted item search endpoint."""

    total: int
    items: list[AnyExtractedModel] = Field(discriminator="entityType")
