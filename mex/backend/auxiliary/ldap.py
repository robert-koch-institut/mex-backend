from typing import Annotated

from fastapi import APIRouter, Depends, Query

from mex.backend.auxiliary.constants import (
    DEFAULT_LDAP_QUERY,
    LDAP_PRIMARY_SOURCE_NAME,
    RKI_WIKIDATA_ID,
)
from mex.backend.auxiliary.helpers import (
    cached_organization,
    cached_organizational_units,
    cached_primary_source,
)
from mex.backend.ldap.connector import LDAPConnector
from mex.backend.security import has_read_access
from mex.common.ldap.transform import (
    transform_any_ldap_actor_to_extracted_persons_or_contact_points,
    transform_ldap_functional_account_to_extracted_contact_point,
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

    This is a free-text fuzzy search across both persons and functional accounts. For
    structured, exact-match lookups of a single entity type,
    see `GET /ldap/persons` and `GET /ldap/functional-accounts` instead.

    Args:
        q: The name of the person or contact point
        offset: The number of results to skip before return
        limit: The maximum number of results to return

    Returns:
        Paginated list of ExtractedPersons and ExtractedContactPoints
    """
    connector = LDAPConnector.get()
    ldap_actors = connector.get_persons_or_functional_accounts(
        query=q,
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


@router.get(
    "/ldap/persons", tags=["extractors"], dependencies=[Depends(has_read_access)]
)
def search_persons_in_ldap(  # noqa: PLR0913, PLR0917
    display_name: Annotated[str, Query()] = "*",
    employee_id: Annotated[str, Query()] = "*",
    given_name: Annotated[str, Query()] = "*",
    mail: Annotated[str, Query()] = "*",
    object_guid: Annotated[str, Query()] = "*",
    sam_account_name: Annotated[str, Query()] = "*",
    surname: Annotated[str, Query()] = "*",
    offset: Annotated[int, Query(ge=0, le=10e10)] = 0,
    limit: Annotated[int, Query(ge=1, le=100)] = 10,
) -> PaginatedItemsContainer[ExtractedPerson]:
    """Look up LDAP persons by exact-match structured attributes.

    This is a structured, exact-match lookup for a single attribute set, to
    deterministically resolve a specific LDAP person. For
    free-text fuzzy search across persons and functional accounts combined, see
    `GET /ldap` instead.

    Args:
        display_name: Display name of the person
        employee_id: Employee identifier
        given_name: Given name of the person
        mail: Email address
        object_guid: Internal LDAP identifier
        sam_account_name: Account name
        surname: Surname of the person
        offset: The number of results to skip before return
        limit: The maximum number of results to return

    Returns:
        Paginated list of ExtractedPersons
    """
    connector = LDAPConnector.get()
    ldap_persons = connector.get_persons(
        display_name=display_name,
        employee_id=employee_id,
        given_name=given_name,
        mail=mail,
        object_guid=object_guid,
        sam_account_name=sam_account_name,
        surname=surname,
        offset=offset,
        limit=limit,
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
    return PaginatedItemsContainer[ExtractedPerson](
        items=extracted_persons,
        total=ldap_persons.total,
    )


@router.get(
    "/ldap/functional-accounts",
    tags=["extractors"],
    dependencies=[Depends(has_read_access)],
)
def search_functional_accounts_in_ldap(
    mail: Annotated[str, Query()] = "*",
    offset: Annotated[int, Query(ge=0, le=10e10)] = 0,
    limit: Annotated[int, Query(ge=1, le=100)] = 10,
) -> PaginatedItemsContainer[ExtractedContactPoint]:
    """Look up LDAP functional accounts by exact-match mail address.

    This is a structured, exact-match lookup to
    deterministically resolve a specific LDAP functional account. For free-text
    fuzzy search across persons and functional accounts combined, see `GET /ldap`
    instead.

    Args:
        mail: Email address of the functional account
        offset: The number of results to skip before return
        limit: The maximum number of results to return

    Returns:
        Paginated list of ExtractedContactPoints
    """
    connector = LDAPConnector.get()
    ldap_functional_accounts = connector.get_functional_accounts(
        mail=mail,
        offset=offset,
        limit=limit,
    )
    extracted_contact_points = [
        transform_ldap_functional_account_to_extracted_contact_point(
            functional_account,
            cached_primary_source(LDAP_PRIMARY_SOURCE_NAME).stableTargetId,
        )
        for functional_account in ldap_functional_accounts.items
    ]
    return PaginatedItemsContainer[ExtractedContactPoint](
        items=extracted_contact_points,
        total=ldap_functional_accounts.total,
    )
