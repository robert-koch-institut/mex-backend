from enum import Enum, EnumMeta, _EnumDict
from typing import TYPE_CHECKING, Union

from pydantic import Field

from mex.common.models import (
    EXTRACTED_MODEL_CLASSES_BY_NAME,
    BaseExtractedData,
    BaseModel,
)
from mex.common.transform import dromedary_to_snake


class ExtractedTypeMeta(EnumMeta):
    """Meta class to dynamically populate the entity type enumeration."""

    def __new__(
        cls: type["ExtractedTypeMeta"], name: str, bases: tuple[type], dct: _EnumDict
    ) -> "ExtractedTypeMeta":
        """Create a new entity type enum by adding an entry for each model."""
        for entity_type in EXTRACTED_MODEL_CLASSES_BY_NAME:
            dct[dromedary_to_snake(entity_type).upper()] = entity_type
        return super().__new__(cls, name, bases, dct)


class ExtractedType(Enum, metaclass=ExtractedTypeMeta):
    """Enumeration of possible types for extracted items."""


if TYPE_CHECKING:  # pragma: no cover
    AnyExtractedModel = BaseExtractedData
else:
    AnyExtractedModel = Union[*EXTRACTED_MODEL_CLASSES_BY_NAME.values()]


class ExtractedItemSearchResponse(BaseModel):
    """Response body for the extracted item search endpoint."""

    total: int
    items: list[AnyExtractedModel] = Field(discriminator="entityType")
