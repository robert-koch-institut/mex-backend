from types import UnionType
from typing import Union, get_args, get_origin

from pydantic.fields import ModelField

from mex.backend.extracted.models import EXTRACTED_MODEL_CLASSES_BY_NAME
from mex.common.types import Identifier, Text


def is_reference_field(field: ModelField) -> bool:
    """Return whether the given field contains a stable target id."""
    if get_origin(field.type_) in (Union, UnionType):
        field_types = get_args(field.type_)
    else:
        field_types = (field.type_,)
    return any(issubclass(t, Identifier) for t in field_types)


def is_text_field(field: ModelField) -> bool:
    """Return whether the given field is holding text objects."""
    if get_origin(field.type_) in (Union, UnionType):
        field_types = get_args(field.type_)
    else:
        field_types = (field.type_,)
    return any(issubclass(t, Text) for t in field_types)


REFERENCE_FIELDS_BY_CLASS_NAME = {
    name: {
        field.name
        for field in cls.__fields__.values()
        if field.name
        not in (
            "identifier",
            "stableTargetId",
        )
        and is_reference_field(field)
    }
    for name, cls in EXTRACTED_MODEL_CLASSES_BY_NAME.items()
}

TEXT_FIELDS_BY_CLASS_NAME = {
    name: {
        f"{field.name}_value"
        for field in cls.__fields__.values()
        if is_text_field(field)
    }
    for name, cls in EXTRACTED_MODEL_CLASSES_BY_NAME.items()
}
