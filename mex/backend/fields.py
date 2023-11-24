from types import UnionType
from typing import Annotated, Any, Generator, Union, get_args, get_origin

from pydantic.fields import FieldInfo

from mex.backend.extracted.models import EXTRACTED_MODEL_CLASSES_BY_NAME
from mex.common.types import Identifier, Text


def _get_inner_types(annotation: Any) -> Generator[type, None, None]:
    """Yield all inner types from Unions, lists and annotations."""
    if get_origin(annotation) == Annotated:
        yield from _get_inner_types(get_args(annotation)[0])
    elif get_origin(annotation) in (Union, UnionType, list):
        for arg in get_args(annotation):
            yield from _get_inner_types(arg)
    elif annotation is None:
        yield type(None)
    else:
        yield annotation


def is_reference_field(field: FieldInfo) -> bool:
    """Return whether the given field contains a stable target id."""
    return any(issubclass(t, Identifier) for t in _get_inner_types(field.annotation))


def is_text_field(field: FieldInfo) -> bool:
    """Return whether the given field is holding text objects."""
    return any(issubclass(t, Text) for t in _get_inner_types(field.annotation))


REFERENCE_FIELDS_BY_CLASS_NAME = {
    name: {
        field_name
        for field_name, field_info in cls.model_fields.items()
        if field_name
        not in (
            "identifier",
            "stableTargetId",
        )
        and is_reference_field(field_info)
    }
    for name, cls in EXTRACTED_MODEL_CLASSES_BY_NAME.items()
}

TEXT_FIELDS_BY_CLASS_NAME = {
    name: {
        f"{field_name}_value"
        for field_name, field_info in cls.model_fields.items()
        if is_text_field(field_info)
    }
    for name, cls in EXTRACTED_MODEL_CLASSES_BY_NAME.items()
}
