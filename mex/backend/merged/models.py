from enum import Enum
from typing import TYPE_CHECKING, Union

from pydantic import Field

from mex.backend.types import DynamicStrEnum
from mex.common.models import MERGED_MODEL_CLASSES_BY_NAME, MergedItem
from mex.common.models.base import BaseModel


class UnprefixedType(Enum, metaclass=DynamicStrEnum):
    """Enumeration of possible unprefixed types for merged items."""

    __names__ = list(m.removeprefix("Merged") for m in MERGED_MODEL_CLASSES_BY_NAME)


class MergedType(Enum, metaclass=DynamicStrEnum):
    """Enumeration of possible types for merged items."""

    __names__ = list(MERGED_MODEL_CLASSES_BY_NAME)


if TYPE_CHECKING:  # pragma: no cover
    AnyMergedModel = MergedItem
else:
    AnyMergedModel = Union[*MERGED_MODEL_CLASSES_BY_NAME.values()]


class MergedItemSearchResponse(BaseModel):
    """Response body for the merged item search endpoint."""

    total: int
    items: list[AnyMergedModel] = Field(discriminator="entityType")
