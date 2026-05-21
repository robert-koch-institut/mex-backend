from typing import TYPE_CHECKING

from fastapi import HTTPException
from starlette import status

from mex.backend.models import ReferenceFilter
from mex.backend.types import ReferenceFieldName

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Sequence

    from mex.common.types import Identifier


def build_reference_filters(
    reference_field: ReferenceFieldName | None = None,
    referenced_identifier: Sequence[Identifier] | None = None,
    stable_target_id: Identifier | None = None,
) -> list[ReferenceFilter]:
    """Convert deprecated reference parameters to ReferenceFilter objects.

    Args:
        reference_field: Deprecated query parameter for reference field name
        referenced_identifier: Deprecated query parameter for identifiers
        stable_target_id: Deprecated query parameter for stableTargetId

    Raises:
        HTTPException: If only one of the two parameters is provided

    Returns:
        list with a single ReferenceFilter, or empty list if neither parameter provided
    """
    reference_filters: list[ReferenceFilter] = []
    if any([reference_field, referenced_identifier]):
        if referenced_identifier and reference_field:
            reference_filters.append(
                ReferenceFilter(
                    field=reference_field, identifiers=referenced_identifier
                )
            )
        else:
            msg = "Must provide referencedIdentifier AND referenceField or neither."
            raise HTTPException(status.HTTP_400_BAD_REQUEST, msg)
    if stable_target_id:
        reference_filters.append(
            ReferenceFilter(
                field=ReferenceFieldName("stableTargetId"),
                identifiers=[stable_target_id],
            )
        )
    return reference_filters
