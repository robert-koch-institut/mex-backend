from collections import deque
from typing import Annotated

from fastapi import APIRouter, Depends, Query

from mex.backend.graph.connector import GraphConnector
from mex.backend.merged.helpers import merge_search_result_item
from mex.backend.testing.security import has_write_access_ldap
from mex.common.models import (
    MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
    ExtractedContactPoint,
    ExtractedPerson,
    MergedPerson,
    PaginatedItemsContainer,
)
from mex.common.types import Email, Validation

DEFAULT_LDAP_QUERY = "mex@rki.de"

router = APIRouter()


@router.post("/merged-person-from-login")
def get_merged_person_from_login(
    username: Annotated[str, Depends(has_write_access_ldap)],
) -> MergedPerson:
    """Return the merged person from the ldap information and verify the login."""
    person = ExtractedPerson(
        hadPrimarySource=MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
        identifierInPrimarySource=username,
        fullName=[username],
        email=[Email(f"{username}@rki.com")],
        orcidId=["https://orcid.org/1234-5678-9012-345"],
    )
    connector = GraphConnector.get()
    deque(connector.ingest_items([person]))

    result = connector.fetch_merged_items(
        query_string=username,
        identifier=None,
        entity_type=["MergedPerson"],
        reference_field=None,
        referenced_identifiers=None,
        skip=0,
        limit=1,
    )
    return merge_search_result_item(result["items"][0], Validation.STRICT)  # type: ignore [return-value]


@router.get("/ldap", tags=["auxiliary"])
def search_persons_or_contact_points_in_ldap(
    q: Annotated[str, Query(max_length=1000)] = DEFAULT_LDAP_QUERY,
    limit: Annotated[int, Query(ge=1, le=100)] = 10,
    _: Annotated[str | None, Depends(has_write_access_ldap)] = None,
) -> PaginatedItemsContainer[ExtractedPerson | ExtractedContactPoint]:
    """Search for person or contact points in LDAP.

    Args:
        q: The name of the person or contact point
        limit: The maximum number of results to return

    Returns:
        Paginated list of ExtractedPersons and ExtractedContactPoints
    """
    items = [
        ExtractedPerson(
            hadPrimarySource=MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
            identifierInPrimarySource="mex",
            fullName=[f"{q}"],
            email=[Email("mex@rki.com")],
        ),
        ExtractedPerson(
            hadPrimarySource=MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
            identifierInPrimarySource="mex2",
            fullName=["mex2"],
            email=[Email("mex2@rki.com")],
        ),
        ExtractedContactPoint(
            hadPrimarySource=MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
            identifierInPrimarySource="mex3",
            email=[Email("mex3@rki.com")],
        ),
    ]
    limited_items = items[:limit]

    return PaginatedItemsContainer[ExtractedPerson | ExtractedContactPoint](
        items=limited_items,
        total=len(limited_items),
    )
