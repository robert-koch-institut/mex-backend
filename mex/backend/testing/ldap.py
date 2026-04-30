from typing import Annotated

from fastapi import APIRouter, Depends, Query

from mex.backend.testing.security import has_read_access_mocked
from mex.common.models import (
    MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
    ExtractedContactPoint,
    ExtractedPerson,
    PaginatedItemsContainer,
)

DEFAULT_LDAP_QUERY = "mex@rki.de"

router = APIRouter()


@router.get("/ldap", tags=["auxiliary"], dependencies=[Depends(has_read_access_mocked)])
def search_persons_or_contact_points_in_ldap(
    q: Annotated[str, Query(max_length=1000)] = DEFAULT_LDAP_QUERY,
    limit: Annotated[int, Query(ge=1, le=100)] = 10,
) -> PaginatedItemsContainer[ExtractedPerson | ExtractedContactPoint]:
    """Search for person or contact points in LDAP and return mocked data for testing.

    Args:
        q: The name of the person or contact point
        limit: The maximum number of results to return

    Returns:
        Paginated list of ExtractedPersons and ExtractedContactPoints
    """
    items = [
        ExtractedPerson(
            hadPrimarySource=MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
            identifierInPrimarySource=f"{q}1",
            fullName=[f"{q}1"],
            email=["mex1@rki.com"],
        ),
        ExtractedPerson(
            hadPrimarySource=MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
            identifierInPrimarySource=f"{q}2",
            fullName=[f"{q}2"],
            email=["mex2@rki.com"],
        ),
        ExtractedContactPoint(
            hadPrimarySource=MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
            identifierInPrimarySource=f"{q}3",
            email=["mex3@rki.com"],
        ),
    ]
    limited_items = items[:limit]

    return PaginatedItemsContainer[ExtractedPerson | ExtractedContactPoint](
        items=limited_items,
        total=len(limited_items),
    )
