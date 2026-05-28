from typing import Annotated, Final

from fastapi import APIRouter, Depends, Query

from mex.backend.auxiliary.organigram import extracted_organizational_units
from mex.backend.auxiliary.primary_source import extracted_primary_source_ldap
from mex.backend.auxiliary.wikidata import extracted_organization_rki
from mex.backend.security import has_read_access
from mex.common.ldap.connector import LDAPConnector
from mex.common.ldap.transform import (
    transform_any_ldap_actor_to_extracted_persons_or_contact_points,
)
from mex.common.models import (
    ExtractedContactPoint,
    ExtractedPerson,
    PaginatedItemsContainer,
)

DEFAULT_LDAP_QUERY: Final = "mex@rki.de"

router = APIRouter()


@router.get("/ldap", tags=["auxiliary"], dependencies=[Depends(has_read_access)])
def search_persons_or_contact_points_in_ldap(
    q: Annotated[str, Query(max_length=1000)] = DEFAULT_LDAP_QUERY,
    offset: Annotated[int, Query(ge=0, le=10e10)] = 0,
    limit: Annotated[int, Query(ge=1, le=100)] = 10,
) -> PaginatedItemsContainer[ExtractedPerson | ExtractedContactPoint]:
    """Search for person or contact points in LDAP.

    Args:
        q: The name of the person or contact point
        offset: The number of results to skip before return
        limit: The maximum number of results to return

    Returns:
        Paginated list of ExtractedPersons and ExtractedContactPoints
    """
    connector = LDAPConnector.get()

    ldap_actors = connector.get_persons_or_functional_accounts(
        query=q, limit=limit, offset=offset
    )
    extracted_persons_or_contact_points = (
        transform_any_ldap_actor_to_extracted_persons_or_contact_points(
            ldap_actors.items,
            extracted_organizational_units(),
            extracted_primary_source_ldap().stableTargetId,
            extracted_organization_rki(),
        )
    )
    return PaginatedItemsContainer[ExtractedPerson | ExtractedContactPoint](
        items=extracted_persons_or_contact_points,
        total=ldap_actors.total,
    )
