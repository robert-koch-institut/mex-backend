from enum import Enum, EnumMeta, _EnumDict
from typing import TYPE_CHECKING, Generator, Union, cast

from pydantic import create_model

from mex.backend.models import TypeSerializer
from mex.common.models import BASE_MODEL_CLASSES, BaseExtractedData, BaseModel
from mex.common.transform import dromedary_to_snake
from mex.common.types import Identifier, PrimarySourceID


def _collect_extracted_model_classes(
    base_models: list[type[BaseModel]],
) -> Generator[tuple[str, type[BaseExtractedData]], None, None]:
    """Create extracted model classes with type for the given MEx models."""
    for model in base_models:
        # to satisfy current frontend, rename ExtractedThing -> Thing
        name = model.__name__.removeprefix("Base")
        extracted_model = create_model(
            name,
            # pydantic 2 will have a simpler solution here
            # it can simply use `__base__=(model, BaseExtractedData, TypeSerializer)`
            __base__=(model, TypeSerializer),
            identifier=(Identifier, ...),
            hadPrimarySource=(PrimarySourceID, ...),
            identifierInPrimarySource=(str, ...),
        )
        yield name, cast(type[BaseExtractedData], extracted_model)


# mx-1387 stopgap: because we do not yet have a backend-powered identity provider,
#                  we need to re-create the extracted models without automatic
#                  identifier and stableTargetId assignment
EXTRACTED_MODEL_CLASSES_BY_NAME: dict[str, type[BaseExtractedData]] = dict(
    _collect_extracted_model_classes(BASE_MODEL_CLASSES)
)


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
    items: list[AnyExtractedModel]
