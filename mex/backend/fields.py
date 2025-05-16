from itertools import chain

from mex.common.fields import (
    ALL_MODEL_CLASSES_BY_NAME,
    EMAIL_FIELDS_BY_CLASS_NAME,
    REFERENCE_FIELDS_BY_CLASS_NAME,
    STRING_FIELDS_BY_CLASS_NAME,
)
from mex.common.types import MERGED_IDENTIFIER_CLASSES
from mex.common.utils import contains_only_types, get_all_fields

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

# allowed entity types grouped for reference fields
REFERENCED_ENTITY_TYPES_BY_FIELD_BY_CLASS_NAME = {
    class_name: {
        field_name: sorted(
            identifier_class.__name__.removesuffix("Identifier")
            for identifier_class in MERGED_IDENTIFIER_CLASSES
            if contains_only_types(
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
    class_name: sorted(chain(*types_by_field.values()))
    for class_name, types_by_field in REFERENCED_ENTITY_TYPES_BY_FIELD_BY_CLASS_NAME.items()
}
