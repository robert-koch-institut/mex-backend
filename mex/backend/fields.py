from collections.abc import Generator
from types import UnionType
from typing import Annotated, Any, Union, get_args, get_origin

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


def _has_subclass_type(field: FieldInfo, type_: type) -> bool:
    """Return whether a field is annotated as a subclass of the given type."""
    return any(
        isinstance(t, type) and issubclass(t, type_)
        for t in _get_inner_types(field.annotation)
    )


def _has_exact_type(field: FieldInfo, type_: type) -> bool:
    """Return whether a field is annotated as exactly the given type."""
    return any(
        isinstance(t, type) and t is type_ for t in _get_inner_types(field.annotation)
    )


# Model fields that link one entity to another by storing the other's stableTargetId.
# Reference fields are typed as subclasses of `Identifier` or lists thereof.
REFERENCE_FIELDS_BY_CLASS_NAME = {
    name: tuple(
        sorted(
            {
                field_name
                for field_name, field_info in cls.model_fields.items()
                if field_name
                not in (
                    "identifier",
                    #  "stableTargetId",
                )
                and _has_subclass_type(field_info, Identifier)
            }
        )
    )
    for name, cls in EXTRACTED_MODEL_CLASSES_BY_NAME.items()
}

# Model fields that store text objects and are typed as `Text` or lists thereof.
# Text fields are stored as nested mappings in JSON form but are modelled as their
# own nodes when written to the graph. They also need special treatment when querying.
TEXT_FIELDS_BY_CLASS_NAME = {
    name: tuple(
        sorted(
            {
                field_name
                for field_name, field_info in cls.model_fields.items()
                if _has_exact_type(field_info, Text)
            }
        )
    )
    for name, cls in EXTRACTED_MODEL_CLASSES_BY_NAME.items()
}

# XXX add Text support
SEARCH_FIELDS_BY_CLASS_NAME = {
    name: tuple(
        sorted(
            {
                field_name
                for field_name, field_info in cls.model_fields.items()
                if _has_exact_type(field_info, str)
            }
        )
    )
    for name, cls in EXTRACTED_MODEL_CLASSES_BY_NAME.items()
}

# Model fields that store link objects and are typed as `Link` or lists thereof.
# Link fields are stored as nested mappings in JSON form but are modelled as their
# own nodes when written to the graph. They also need special treatment when querying.
LINK_FIELDS_BY_CLASS_NAME = {
    name: tuple(
        sorted(
            {
                field_name
                for field_name, field_info in cls.model_fields.items()
                if _has_exact_type(field_info, Link)
            }
        )
    )
    for name, cls in EXTRACTED_MODEL_CLASSES_BY_NAME.items()
}

# Model fields that are searchable via full text queries and should be indexed
# by neo4j for lucene-backed searches. These fields need to be of type `str`.
SEARCHABLE_FIELDS_BY_CLASS_NAME = {
    name: tuple(
        sorted(
            {
                field_name
                for field_name, field_info in cls.model_fields.items()
                if _has_exact_type(field_info, str)
            }
        )
    )
    for name, cls in EXTRACTED_MODEL_CLASSES_BY_NAME.items()
}
