from types import NoneType, UnionType
from typing import (
    Annotated,
    Any,
    Callable,
    Generator,
    Mapping,
    Union,
    get_args,
    get_origin,
)

from pydantic import BaseModel
from pydantic.fields import FieldInfo

from mex.backend.types import LiteralStringType
from mex.common.models import EXTRACTED_MODEL_CLASSES_BY_NAME
from mex.common.types import Identifier, Link, Text


def _get_inner_types(annotation: Any) -> Generator[type, None, None]:
    """Yield all inner types from unions, lists and optional annotations."""
    if get_origin(annotation) == Annotated:
        yield from _get_inner_types(get_args(annotation)[0])
    elif get_origin(annotation) in (Union, UnionType, list):
        for arg in get_args(annotation):
            yield from _get_inner_types(arg)
    elif annotation not in (None, NoneType):
        yield annotation


def _has_true_subclass_type(field: FieldInfo, type_: type) -> bool:
    """Return whether a field is annotated as a true subclass of the given type.

    A "true" subclass is defined as not being identical to the provided `type_` itself.
    Optional annotations and unions with `None` are allowed as long as at least one
    non-`NoneType` type is present in the annotation.
    """
    if inner_types := list(_get_inner_types(field.annotation)):
        return all(
            isinstance(t, type) and issubclass(t, type_) and t is not type_
            for t in inner_types
        )
    return False


def _has_exact_type(field: FieldInfo, type_: type) -> bool:
    """Return whether a field is annotated as exactly the given type.

    Lists and unions with `NoneType` are allowed and only the non-`NoneType` annotation
    is considered.
    """
    if inner_types := list(_get_inner_types(field.annotation)):
        return all(isinstance(t, type) and t is type_ for t in inner_types)
    return False


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


# fields that are immutable and can only be set once
FROZEN_FIELDS_BY_CLASS_NAME = _group_fields_by_class_name(
    EXTRACTED_MODEL_CLASSES_BY_NAME, lambda field_info: field_info.frozen is True
)

# static fields that are set once on class-level to a literal type
LITERAL_FIELDS_BY_CLASS_NAME = _group_fields_by_class_name(
    EXTRACTED_MODEL_CLASSES_BY_NAME,
    lambda field_info: isinstance(field_info.annotation, LiteralStringType),
)

# fields typed as subclasses of `Identifier` containing references to merged items
REFERENCE_FIELDS_BY_CLASS_NAME = _group_fields_by_class_name(
    EXTRACTED_MODEL_CLASSES_BY_NAME,
    lambda field_info: _has_true_subclass_type(field_info, Identifier),
)

# nested fields that contain `Text` objects
TEXT_FIELDS_BY_CLASS_NAME = _group_fields_by_class_name(
    EXTRACTED_MODEL_CLASSES_BY_NAME,
    lambda field_info: _has_exact_type(field_info, Text),
)

# nested fields that contain `Link` objects
LINK_FIELDS_BY_CLASS_NAME = _group_fields_by_class_name(
    EXTRACTED_MODEL_CLASSES_BY_NAME,
    lambda field_info: _has_exact_type(field_info, Link),
)

# fields annotated as `str` type
STRING_FIELDS_BY_CLASS_NAME = _group_fields_by_class_name(
    EXTRACTED_MODEL_CLASSES_BY_NAME, lambda field_info: _has_exact_type(field_info, str)
)

# fields that should be indexed as searchable fields
SEARCHABLE_FIELDS = sorted(
    {
        field_name
        for field_names in STRING_FIELDS_BY_CLASS_NAME.values()
        for field_name in field_names
    }
)

# classes that have fields that should be searchable
SEARCHABLE_CLASSES = sorted(
    {name for name, field_names in STRING_FIELDS_BY_CLASS_NAME.items() if field_names}
)

# fields with changeable values that are not nested objects or merged item references
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

# fields with values that should be set once but are neither literal nor references
FINAL_FIELDS_BY_CLASS_NAME = {
    name: sorted(
        {
            field_name
            for field_name in cls.model_fields
            if field_name in FROZEN_FIELDS_BY_CLASS_NAME[name]
            and field_name
            not in (
                *LITERAL_FIELDS_BY_CLASS_NAME[name],
                *REFERENCE_FIELDS_BY_CLASS_NAME[name],
            )
        }
    )
    for name, cls in EXTRACTED_MODEL_CLASSES_BY_NAME.items()
}
