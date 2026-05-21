from typing import TYPE_CHECKING

from fastapi import HTTPException
from starlette import status

from mex.backend.models import ReferenceFilter

if TYPE_CHECKING:
    from collections.abc import Sequence

    from mex.backend.types import ReferenceFieldName
    from mex.common.types import Identifier


def build_reference_filters(
    reference_field: ReferenceFieldName | None,
    referenced_identifier: Sequence[Identifier] | None,
) -> list[ReferenceFilter]:
    """Convert deprecated reference parameters to ReferenceFilter objects.

    Args:
        reference_field: Deprecated query parameter for reference field name
        referenced_identifier: Deprecated query parameter for identifiers

    Raises:
        HTTPException: If only one of the two parameters is provided

    Returns:
        list with a single ReferenceFilter, or empty list if neither parameter provided
    """
    if not referenced_identifier and not reference_field:
        return []
    if referenced_identifier and reference_field:
        return [
            ReferenceFilter(field=reference_field, identifiers=referenced_identifier)
        ]
    msg = "Must provide referencedIdentifier AND referenceField or neither."
    raise HTTPException(status.HTTP_400_BAD_REQUEST, msg)
