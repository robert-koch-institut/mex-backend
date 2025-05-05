from itertools import chain

from mex.common.fields import EMAIL_FIELDS_BY_CLASS_NAME, STRING_FIELDS_BY_CLASS_NAME
from mex.common.types import NESTED_MODEL_CLASSES, NESTED_MODEL_CLASSES_BY_NAME
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
        name
        for name, field_names in chain(
            STRING_FIELDS_BY_CLASS_NAME.items(),
            EMAIL_FIELDS_BY_CLASS_NAME.items(),  # stopgap MX-1766
        )
        if field_names
    }
)

NESTED_TYPE_FIELDS = sorted(
    {
        field_name
        for model_class in NESTED_MODEL_CLASSES
        for field_name in get_all_fields(model_class)
    }
)
NESTED_TYPE_CLASSES = sorted(
    NESTED_MODEL_CLASSES_BY_NAME,
)
