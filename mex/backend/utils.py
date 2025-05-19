from mex.common.utils import GenericFieldInfo, get_inner_types


def contains_any_types(field: GenericFieldInfo, *types: type) -> bool:
    """Return whether a `field` is annotated as any of the given `types`.

    Unions, lists and type annotations are checked for their inner types and only the
    non-`NoneType` types are considered for the type-check.

    Args:
        field: A `GenericFieldInfo` instance
        types: Types to look for in the field's annotation

    Returns:
        Whether the field contains any of the given types
    """
    # TODO(ND): move this into mex-common for completeness sake
    if inner_types := list(get_inner_types(field.annotation, include_none=False)):
        return any(inner_type in types for inner_type in inner_types)
    return False
