from itertools import chain

from mex.backend.utils import contains_any_types
from mex.common.fields import (
    ALL_MODEL_CLASSES_BY_NAME,
    ALL_TYPES_BY_FIELDS_BY_CLASS_NAMES,
    EMAIL_FIELDS_BY_CLASS_NAME,
    REFERENCE_FIELDS_BY_CLASS_NAME,
    STRING_FIELDS_BY_CLASS_NAME,
)
from mex.common.types import MERGED_IDENTIFIER_CLASSES, NESTED_MODEL_CLASSES_BY_NAME
from mex.common.utils import get_all_fields

# fields that should be indexed as searchable fields
SEARCHABLE_FIELDS = sorted(
    {
        field_name
        for field_names in chain(
            STRING_FIELDS_BY_CLASS_NAME.values(),
            EMAIL_FIELDS_BY_CLASS_NAME.values(),  # stopgap MX-1766
        )
        for field_name in field_names
    }
)

# classes that have fields that should be searchable
SEARCHABLE_CLASSES = sorted(
    {
        class_name
        for class_name, field_names in chain(
            STRING_FIELDS_BY_CLASS_NAME.items(),
            EMAIL_FIELDS_BY_CLASS_NAME.items(),  # stopgap MX-1766
        )
        if field_names
    }
)

# allowed nested types grouped by fields
NESTED_ENTITY_TYPES_BY_FIELD_BY_CLASS_NAME = {
    class_name: {
        field_name: sorted(
            nested_name
            for nested_name, nested_class in NESTED_MODEL_CLASSES_BY_NAME.items()
            if nested_class in field_types
        )
        for field_name, field_types in types_by_fields.items()
    }
    for class_name, types_by_fields in ALL_TYPES_BY_FIELDS_BY_CLASS_NAMES.items()
}

# all nested types grouped by class name
NESTED_ENTITY_TYPES_BY_CLASS_NAME = {
    class_name: sorted(set(chain(*types_by_field.values())))
    for class_name, types_by_field in (
        NESTED_ENTITY_TYPES_BY_FIELD_BY_CLASS_NAME.items()
    )
}

# allowed entity types grouped for reference fields
REFERENCED_ENTITY_TYPES_BY_FIELD_BY_CLASS_NAME = {
    class_name: {
        field_name: sorted(
            identifier_class.__name__.removesuffix("Identifier")
            for identifier_class in MERGED_IDENTIFIER_CLASSES
            if contains_any_types(
                get_all_fields(ALL_MODEL_CLASSES_BY_NAME[class_name])[field_name],
                identifier_class,
            )
        )
        for field_name in field_names
    }
    for class_name, field_names in REFERENCE_FIELDS_BY_CLASS_NAME.items()
}

# all referenced entity types grouped by class name
REFERENCED_ENTITY_TYPES_BY_CLASS_NAME = {
    class_name: sorted(set(chain(*types_by_field.values())))
    for class_name, types_by_field in (
        REFERENCED_ENTITY_TYPES_BY_FIELD_BY_CLASS_NAME.items()
    )
}

# unique set of all fields from any class that contain references
ALL_REFERENCE_FIELD_NAMES = {
    field_name
    for class_name in REFERENCED_ENTITY_TYPES_BY_FIELD_BY_CLASS_NAME.values()
    for field_name in class_name
}
