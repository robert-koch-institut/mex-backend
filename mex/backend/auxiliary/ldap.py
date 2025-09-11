from typing import Annotated

from fastapi import APIRouter, Query

from mex.backend.auxiliary.organigram import extracted_organizational_units
from mex.backend.auxiliary.primary_source import extracted_primary_source_ldap
from mex.common.ldap.connector import LDAPConnector
from mex.common.ldap.transform import (
    transform_any_ldap_actor_to_extracted_persons_or_contact_points,
)
from mex.common.models import (
    ExtractedContactPoint,
    ExtractedPerson,
    PaginatedItemsContainer,
)

DEFAULT_LDAP_QUERY = "mex@rki.de"

router = APIRouter()


@router.get("/ldap", tags=["auxiliary"])
def search_persons_or_contact_points_in_ldap(
    q: Annotated[str, Query(max_length=1000)] = DEFAULT_LDAP_QUERY,
    limit: Annotated[int, Query(ge=1, le=100)] = 10,
) -> PaginatedItemsContainer[ExtractedPerson | ExtractedContactPoint]:
    """Search for person or contact points in LDAP.

    Args:
        q: The name of the person or contact point
        limit: The maximum number of results to return

    Returns:
        Paginated list of ExtractedPersons and ExtractedContactPoints
    """
    connector = LDAPConnector.get()
    ldap_actors = connector.get_persons_or_functional_accounts(query=q, limit=limit)
    extracted_persons_or_contact_points = (
        transform_any_ldap_actor_to_extracted_persons_or_contact_points(
            ldap_actors,
            extracted_organizational_units(),
            extracted_primary_source_ldap(),
        )
    )
    return PaginatedItemsContainer[ExtractedPerson | ExtractedContactPoint](
        items=extracted_persons_or_contact_points,
        total=len(extracted_persons_or_contact_points),
    )
