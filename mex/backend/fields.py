from mex.common.fields import STRING_FIELDS_BY_CLASS_NAME

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
