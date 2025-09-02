from base64 import b64encode
from collections.abc import Sequence
from typing import Annotated

from fastapi import APIRouter, Query
from fastapi.exceptions import HTTPException
from pydantic import BaseModel
from requests import HTTPError
from starlette import status

from mex.backend.graph.exceptions import NoResultFoundError
from mex.backend.merged.helpers import (
    get_merged_item_from_graph,
    has_write_access_ldap,
    search_merged_items_in_graph,
)
from mex.backend.types import MergedType, ReferenceFieldName
from mex.common.identity import get_provider
from mex.common.ldap.connector import LDAPConnector
from mex.common.logging import logger
from mex.common.models import (
    MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
    AnyMergedModel,
    MergedPerson,
    PaginatedItemsContainer,
)
from mex.common.types import Identifier, Validation

router = APIRouter()


@router.get("/merged-item", tags=["editor"])
def search_merged_items(  # noqa: PLR0913
    q: Annotated[str, Query(max_length=100)] = "",
    identifier: Annotated[Identifier | None, Query()] = None,
    entityType: Annotated[Sequence[MergedType], Query(max_length=len(MergedType))] = [],
    hadPrimarySource: Annotated[
        Sequence[Identifier] | None, Query(deprecated=True)
    ] = None,
    referencedIdentifier: Annotated[Sequence[Identifier] | None, Query()] = None,
    referenceField: Annotated[ReferenceFieldName | None, Query()] = None,
    skip: Annotated[int, Query(ge=0, le=10e10)] = 0,
    limit: Annotated[int, Query(ge=1, le=100)] = 10,
) -> PaginatedItemsContainer[AnyMergedModel]:
    """Search for merged items by query text or by type and identifier."""
    if hadPrimarySource:
        referencedIdentifier = hadPrimarySource  # noqa: N806
        referenceField = ReferenceFieldName("hadPrimarySource")  # noqa: N806
    if bool(referencedIdentifier) != bool(referenceField):
        msg = "Must provide referencedIdentifier AND referenceField or neither."
        raise HTTPException(status.HTTP_400_BAD_REQUEST, msg)
    return search_merged_items_in_graph(
        query_string=q,
        identifier=identifier,
        entity_type=[str(t.value) for t in entityType or MergedType],
        referenced_identifiers=[str(s) for s in referencedIdentifier]
        if referencedIdentifier
        else None,
        reference_field=str(referenceField.value) if referenceField else None,
        skip=skip,
        limit=limit,
        validation=Validation.IGNORE,
    )


@router.get("/merged-item/{identifier}", tags=["editor"])
def get_merged_item(identifier: Identifier) -> AnyMergedModel:
    """Return one merged item for the given `identifier`."""
    try:
        return get_merged_item_from_graph(identifier)
    except NoResultFoundError as error:
        raise HTTPException(status.HTTP_404_NOT_FOUND) from error


class User(BaseModel):
    """Info on the currently logged-in user."""

    name: str
    authorization: str
    write_access: bool


@router.post("/merged-person-from-login", tags=["editor"])
def get_merged_person_from_login(
    username: str, password: str
) -> tuple[MergedPerson, User] | None:
    """Return the merged person from the ldap login information."""
    write_access = has_write_access_ldap(username, password)
    if write_access:
        encoded = b64encode(f"{username}:{password}".encode("ascii"))
        user_ldap = User(
            name=username,
            authorization=f"Basic {encoded.decode('ascii')}",
            write_access=write_access,
        )
        ldap_connector = LDAPConnector.get()
        ldap_person = ldap_connector.get_person(sAMAccountName=user_ldap.name)
        provider = get_provider()
        primary_source_identities = provider.fetch(
            had_primary_source=MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
            identifier_in_primary_source="ldap",
        )
        try:
            identities = provider.fetch(
                identifier_in_primary_source=str(ldap_person.objectGUID),
                had_primary_source=primary_source_identities[0].stableTargetId,  # type: ignore  [arg-type]
            )
        except HTTPError as exc:
            logger.error(f"Error fetching identities: {exc}")
            return None
        else:
            if len(identities) > 0:
                person = get_merged_item(identities[0].stableTargetId)
                if person.entityType == "MergedPerson":
                    return (person, user_ldap)
    return None
