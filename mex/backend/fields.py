from collections.abc import Generator
from types import UnionType
from typing import (
    Annotated,
    Any,
    Callable,
    Literal,
    Mapping,
    Union,
    get_args,
    get_origin,
)

from pydantic import BaseModel
from pydantic.fields import FieldInfo

from mex.common.models import EXTRACTED_MODEL_CLASSES_BY_NAME
from mex.common.types import Identifier, Link, Text


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


def _has_true_subclass_type(field: FieldInfo, type_: type) -> bool:
    """Return whether a field is annotated as a true subclass of the given type.

    A "true" subclass is defined as not being identical to the provided `type_` itself.
    """
    return all(
        isinstance(t, type) and issubclass(t, type_) and t is not type_
        for t in _get_inner_types(field.annotation)
    )


def _has_exact_type(field: FieldInfo, type_: type) -> bool:
    """Return whether a field is annotated as exactly the given type."""
    return all(
        isinstance(t, type) and t is type_ for t in _get_inner_types(field.annotation)
    )


def _group_fields_by_class_name(
    model_classes_by_name: Mapping[str, type[BaseModel]],
    predicate: Callable[[FieldInfo], bool],
) -> dict[str, list[str]]:
    """Group the field names by model class and filter them by the given predicate."""
    return {
        name: sorted(
            {
                field_name
                for field_name, field_info in cls.model_fields.items()
                if predicate(field_info)
            }
        )
        for name, cls in model_classes_by_name.items()
    }


FROZEN_FIELDS_BY_CLASS_NAME = _group_fields_by_class_name(
    EXTRACTED_MODEL_CLASSES_BY_NAME, lambda field_info: field_info.frozen is True
)

LITERAL_FIELDS_BY_CLASS_NAME = _group_fields_by_class_name(
    EXTRACTED_MODEL_CLASSES_BY_NAME,
    lambda field_info: isinstance(field_info.annotation, type(Literal["entityType"])),
)

REFERENCE_FIELDS_BY_CLASS_NAME = _group_fields_by_class_name(
    EXTRACTED_MODEL_CLASSES_BY_NAME,
    # true subclasses only because we want to ignore literal `Identifier` typed
    # fields like `identifier`
    lambda field_info: _has_true_subclass_type(field_info, Identifier),
)

TEXT_FIELDS_BY_CLASS_NAME = _group_fields_by_class_name(
    EXTRACTED_MODEL_CLASSES_BY_NAME,
    lambda field_info: _has_exact_type(field_info, Text),
)

STRING_FIELDS_BY_CLASS_NAME = _group_fields_by_class_name(
    EXTRACTED_MODEL_CLASSES_BY_NAME, lambda field_info: _has_exact_type(field_info, str)
)

SEARCHABLE_FIELDS = sorted(
    {
        field_name
        for field_names in STRING_FIELDS_BY_CLASS_NAME.values()
        for field_name in field_names
    }
)

SEARCHABLE_CLASSES = sorted(
    {name for name, field_names in STRING_FIELDS_BY_CLASS_NAME.items() if field_names}
)

# Model fields that store link objects and are typed as `Link` or lists thereof.
# Link fields are stored as nested mappings in JSON form but are modelled as their
# own nodes when written to the graph. They also need special treatment when querying.
LINK_FIELDS_BY_CLASS_NAME = _group_fields_by_class_name(
    EXTRACTED_MODEL_CLASSES_BY_NAME,
    lambda field_info: _has_exact_type(field_info, Link),
)

MUTABLE_FIELDS_BY_CLASS_NAME = {
    name: sorted(
        {
            field_name
            for field_name in cls.model_fields
            if field_name
            not in (
                *FROZEN_FIELDS_BY_CLASS_NAME[name],
                *REFERENCE_FIELDS_BY_CLASS_NAME[name],
                *TEXT_FIELDS_BY_CLASS_NAME[name],
                *LINK_FIELDS_BY_CLASS_NAME[name],
            )
        }
    )
    for name, cls in EXTRACTED_MODEL_CLASSES_BY_NAME.items()
}
