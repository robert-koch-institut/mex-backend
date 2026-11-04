from typing import Annotated, cast

from fastapi import APIRouter, Depends, HTTPException, Query
from starlette import status

from mex.backend.auxiliary.constants import LDAP_PRIMARY_SOURCE_NAME, RKI_WIKIDATA_ID
from mex.backend.auxiliary.helpers import (
    cached_organization,
    cached_organizational_units,
    cached_primary_source,
)
from mex.backend.ldap.connector import LDAPConnector
from mex.backend.merged.main import get_merged_item
from mex.backend.security import has_read_access, is_ldap_authenticated
from mex.common.identity import get_provider
from mex.common.ldap.transform import (
    transform_ldap_functional_account_to_extracted_contact_point,
    transform_ldap_person_to_extracted_person,
)
from mex.common.models import (
    ExtractedContactPoint,
    ExtractedPerson,
    MergedPerson,
    PaginatedItemsContainer,
)
from mex.common.types import MergedPrimarySourceIdentifier

router = APIRouter()


@router.post("/merged-person-from-login", tags=["editor"])
def get_merged_person_from_login(
    username: Annotated[str, Depends(is_ldap_authenticated)],
) -> MergedPerson:
    """Return the merged person from the ldap information and verify the login."""
    ldap_connector = LDAPConnector.get()
    ldap_person = ldap_connector.get_person(sam_account_name=username)
    provider = get_provider()
    primary_source_identities = cached_primary_source("ldap")
    identities = provider.fetch(
        identifier_in_primary_source=str(ldap_person.objectGUID),
        had_primary_source=MergedPrimarySourceIdentifier(
            primary_source_identities.stableTargetId
        ),
    )
    if not identities:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User is not authorized for MEx.",
        )
    return cast("MergedPerson", get_merged_item(identities[0].stableTargetId))


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
