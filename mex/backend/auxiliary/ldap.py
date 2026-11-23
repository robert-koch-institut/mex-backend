from functools import partial
from typing import Annotated

from fastapi import APIRouter, Body, Depends, Query

from mex.backend.auxiliary.constants import (
    DEFAULT_LDAP_QUERY,
    LDAP_PRIMARY_SOURCE_NAME,
    RKI_WIKIDATA_ID,
)
from mex.backend.auxiliary.helpers import (
    cached_organization,
    cached_organizational_units,
    cached_primary_source,
    fetch_ldap_page,
)
from mex.backend.auxiliary.models import LDAPPersonLookup
from mex.backend.security import has_read_access
from mex.common.ldap.connector import LDAPConnector
from mex.common.ldap.transform import (
    transform_any_ldap_actor_to_extracted_persons_or_contact_points,
    transform_ldap_person_to_extracted_person,
)
from mex.common.models import (
    ExtractedContactPoint,
    ExtractedPerson,
    PaginatedItemsContainer,
)

router = APIRouter()


@router.get("/ldap", tags=["auxiliary"], dependencies=[Depends(has_read_access)])
def search_persons_or_contact_points_in_ldap(
    q: Annotated[str, Query(max_length=1000)] = DEFAULT_LDAP_QUERY,
    offset: Annotated[int, Query(ge=0, le=10e10)] = 0,
    limit: Annotated[int, Query(ge=1, le=100)] = 10,
) -> PaginatedItemsContainer[ExtractedPerson | ExtractedContactPoint]:
    """Search for person or contact points in LDAP.

    This is a free-text search across LDAP persons and functional accounts. For
    structured lookups by exact attribute values, use `POST /ldap/_search` instead.

    Args:
        q: The name of the person or contact point
        offset: The number of results to skip before return
        limit: The maximum number of results to return

    Returns:
        Paginated list of ExtractedPersons and ExtractedContactPoints
    """
    connector = LDAPConnector.get()
    ldap_actors = fetch_ldap_page(
        partial(connector.get_persons_or_functional_accounts, query=q),
        limit=limit,
        offset=offset,
    )
    extracted_persons_or_contact_points = (
        transform_any_ldap_actor_to_extracted_persons_or_contact_points(
            ldap_actors.items,
            cached_organizational_units(),
            cached_primary_source(LDAP_PRIMARY_SOURCE_NAME).stableTargetId,
            cached_organization(RKI_WIKIDATA_ID).stableTargetId,
        )
    )
    return PaginatedItemsContainer[ExtractedPerson | ExtractedContactPoint](
        items=extracted_persons_or_contact_points,
        total=ldap_actors.total,
    )


@router.post(
    "/ldap/_search", tags=["auxiliary"], dependencies=[Depends(has_read_access)]
)
def search_in_ldap(
    lookup: Annotated[LDAPPersonLookup, Body()],
) -> PaginatedItemsContainer[ExtractedPerson | ExtractedContactPoint]:
    """Look up LDAP persons by exact attribute values.

    The lookup criteria are sent in the request body, so that they don't end up in
    access logs. Unspecified attributes default to `"*"`, and attribute values may
    contain `*` wildcards. For a free-text search, see `GET /ldap`.

    Args:
        lookup: Person lookup with entity type, attribute filters and pagination

    Returns:
        Paginated list of ExtractedPersons
    """
    connector = LDAPConnector.get()
    ldap_persons = fetch_ldap_page(
        partial(
            connector.get_persons,
            display_name=lookup.displayName,
            employee_id=lookup.employeeId,
            given_name=lookup.givenName,
            mail=lookup.mail,
            object_guid=lookup.objectGuid,
            sam_account_name=lookup.samAccountName,
            surname=lookup.surname,
        ),
        limit=lookup.limit,
        offset=lookup.skip,
    )
    units_by_identifier_in_primary_source = {
        unit.identifierInPrimarySource: unit for unit in cached_organizational_units()
    }
    extracted_persons = [
        transform_ldap_person_to_extracted_person(
            person,
            cached_primary_source(LDAP_PRIMARY_SOURCE_NAME).stableTargetId,
            units_by_identifier_in_primary_source,
            cached_organization(RKI_WIKIDATA_ID).stableTargetId,
        )
        for person in ldap_persons.items
    ]
    return PaginatedItemsContainer[ExtractedPerson | ExtractedContactPoint](
        items=extracted_persons,
        total=ldap_persons.total,
    )
