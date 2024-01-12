from enum import Enum, EnumMeta, _EnumDict
from typing import TYPE_CHECKING, Union

from pydantic import Field

from mex.common.models import MERGED_MODEL_CLASSES_BY_NAME, MergedItem
from mex.common.models.base import BaseModel
from mex.common.transform import dromedary_to_snake


class MergedTypeMeta(EnumMeta):
    """Meta class to dynamically populate the entity type enumeration."""

    def __new__(
        cls: type["MergedTypeMeta"], name: str, bases: tuple[type], dct: _EnumDict
    ) -> "MergedTypeMeta":
        """Create a new entity type enum by adding an entry for each model."""
        for entity_type in MERGED_MODEL_CLASSES_BY_NAME:
            dct[dromedary_to_snake(entity_type).upper()] = entity_type
        return super().__new__(cls, name, bases, dct)


class MergedType(Enum, metaclass=MergedTypeMeta):
    """Enumeration of possible types for merged items."""


if TYPE_CHECKING:  # pragma: no cover
    AnyMergedModel = MergedItem
else:
    AnyMergedModel = Union[*MERGED_MODEL_CLASSES_BY_NAME.values()]


class MergedItemSearchResponse(BaseModel):
    """Response body for the merged item search endpoint."""

    total: int
    items: list[AnyMergedModel] = Field(discriminator="entityType")
