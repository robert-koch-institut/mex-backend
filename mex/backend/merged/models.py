from enum import Enum, EnumMeta, _EnumDict
from typing import TYPE_CHECKING, Iterator, Literal, Union, cast

from pydantic import Field, create_model

from mex.common.models import BASE_MODEL_CLASSES, MergedItem
from mex.common.models.base import BaseModel
from mex.common.transform import dromedary_to_snake
from mex.common.types import Identifier


def _collect_merged_model_classes(
    base_models: list[type[BaseModel]],
) -> Iterator[tuple[str, type[MergedItem]]]:
    """Create merged model classes with type for the given MEx models."""
    for model in base_models:
        name = model.__name__.replace("Base", "Merged")
        merged_model = create_model(
            name,
            __base__=(model,),
            identifier=(Identifier, ...),
            entityType=(Literal[name], Field(name, alias="$type", frozen=True)),
        )
        yield name, cast(type[MergedItem], merged_model)


MERGED_MODEL_CLASSES_BY_NAME: dict[str, type[MergedItem]] = dict(
    _collect_merged_model_classes(BASE_MODEL_CLASSES)
)


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
