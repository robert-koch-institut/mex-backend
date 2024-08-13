from collections.abc import Callable, Generator, Mapping
from types import NoneType, UnionType
from typing import Annotated, Any, Union, get_args, get_origin

from mex.common.models import (
    ADDITIVE_MODEL_CLASSES_BY_NAME,
    EXTRACTED_MODEL_CLASSES_BY_NAME,
    MERGED_MODEL_CLASSES_BY_NAME,
    PREVENTIVE_MODEL_CLASSES_BY_NAME,
    SUBTRACTIVE_MODEL_CLASSES_BY_NAME,
    BaseModel,
    GenericFieldInfo,
)
from mex.common.types import MERGED_IDENTIFIER_CLASSES, Link, LiteralStringType, Text


def _get_inner_types(annotation: Any) -> Generator[type, None, None]:
    """Yield all inner types from unions, lists and type annotations (except NoneType).

    Args:
        annotation: A valid python type annotation

    Returns:
        A generator for all (non-NoneType) types found in the annotation
    """
    if get_origin(annotation) == Annotated:
        yield from _get_inner_types(get_args(annotation)[0])
    elif get_origin(annotation) in (Union, UnionType, list):
        for arg in get_args(annotation):
            yield from _get_inner_types(arg)
    elif annotation not in (None, NoneType):
        yield annotation


def _contains_only_types(field: GenericFieldInfo, *types: type) -> bool:
    """Return whether a `field` is annotated as one of the given `types`.

    Unions, lists and type annotations are checked for their inner types and only the
    non-`NoneType` types are considered for the type-check.

    Args:
        field: A `GenericFieldInfo` instance
        types: Types to look for in the field's annotation

    Returns:
        Whether the field contains any of the given types
    """
    if inner_types := list(_get_inner_types(field.annotation)):
        return all(inner_type in types for inner_type in inner_types)
    return False


def _group_fields_by_class_name(
    model_classes_by_name: Mapping[str, type[BaseModel]],
    predicate: Callable[[GenericFieldInfo], bool],
) -> dict[str, list[str]]:
    """Group the field names by model class and filter them by the given predicate.

    Args:
        model_classes_by_name: Map from class names to model classes
        predicate: Function to filter the fields of the classes by

    Returns:
        Dictionary mapping class names to a list of field names filtered by `predicate`
    """
    return {
        name: sorted(
            {
                field_name
                for field_name, field_info in cls.get_all_fields().items()
                if predicate(field_info)
            }
        )
        for name, cls in model_classes_by_name.items()
    }


# all models classes
ALL_MODEL_CLASSES_BY_NAME = {
    **ADDITIVE_MODEL_CLASSES_BY_NAME,
    **EXTRACTED_MODEL_CLASSES_BY_NAME,
    **MERGED_MODEL_CLASSES_BY_NAME,
    **PREVENTIVE_MODEL_CLASSES_BY_NAME,
    **SUBTRACTIVE_MODEL_CLASSES_BY_NAME,
}

# fields that are immutable and can only be set once
FROZEN_FIELDS_BY_CLASS_NAME = _group_fields_by_class_name(
    ALL_MODEL_CLASSES_BY_NAME,
    lambda field_info: field_info.frozen is True,
)

# static fields that are set once on class-level to a literal type
LITERAL_FIELDS_BY_CLASS_NAME = _group_fields_by_class_name(
    ALL_MODEL_CLASSES_BY_NAME,
    lambda field_info: isinstance(field_info.annotation, LiteralStringType),
)

# fields typed as merged identifiers containing references to merged items
REFERENCE_FIELDS_BY_CLASS_NAME = _group_fields_by_class_name(
    ALL_MODEL_CLASSES_BY_NAME,
    lambda field_info: _contains_only_types(field_info, *MERGED_IDENTIFIER_CLASSES),
)

# nested fields that contain `Text` objects
TEXT_FIELDS_BY_CLASS_NAME = _group_fields_by_class_name(
    ALL_MODEL_CLASSES_BY_NAME,
    lambda field_info: _contains_only_types(field_info, Text),
)

# nested fields that contain `Link` objects
LINK_FIELDS_BY_CLASS_NAME = _group_fields_by_class_name(
    ALL_MODEL_CLASSES_BY_NAME,
    lambda field_info: _contains_only_types(field_info, Link),
)

# fields annotated as `str` type
STRING_FIELDS_BY_CLASS_NAME = _group_fields_by_class_name(
    ALL_MODEL_CLASSES_BY_NAME,
    lambda field_info: _contains_only_types(field_info, str),
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
            for field_name in cls.get_all_fields()
            if field_name
            not in (
                *FROZEN_FIELDS_BY_CLASS_NAME[name],
                *REFERENCE_FIELDS_BY_CLASS_NAME[name],
                *TEXT_FIELDS_BY_CLASS_NAME[name],
                *LINK_FIELDS_BY_CLASS_NAME[name],
            )
        }
    )
    for name, cls in ALL_MODEL_CLASSES_BY_NAME.items()
}

# fields with values that should be set once but are neither literal nor references
FINAL_FIELDS_BY_CLASS_NAME = {
    name: sorted(
        {
            field_name
            for field_name in cls.get_all_fields()
            if field_name in FROZEN_FIELDS_BY_CLASS_NAME[name]
            and field_name
            not in (
                *LITERAL_FIELDS_BY_CLASS_NAME[name],
                *REFERENCE_FIELDS_BY_CLASS_NAME[name],
            )
        }
    )
    for name, cls in ALL_MODEL_CLASSES_BY_NAME.items()
}
