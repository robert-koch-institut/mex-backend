from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException
from requests import HTTPError
from starlette import status

from mex.backend.merged.main import get_merged_item
from mex.backend.security import has_write_access_ldap
from mex.common.identity import get_provider
from mex.common.ldap.connector import LDAPConnector
from mex.common.logging import logger
from mex.common.models import MEX_PRIMARY_SOURCE_STABLE_TARGET_ID, MergedPerson
from mex.common.types import MergedPrimarySourceIdentifier

router = APIRouter()


@router.post("/merged-person-from-login")
def get_merged_person_from_login(
    username: Annotated[str, Depends(has_write_access_ldap)],
) -> MergedPerson | None:
    """Return the merged person from the ldap information and verify the login."""
    if not username:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid LDAP credentials.",
            headers={"WWW-Authenticate": "Basic"},
        )
    ldap_connector = LDAPConnector.get()
    ldap_person = ldap_connector.get_person(sAMAccountName=username)
    provider = get_provider()
    primary_source_identities = provider.fetch(
        had_primary_source=MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
        identifier_in_primary_source="ldap",
    )
    try:
        identities = provider.fetch(
            identifier_in_primary_source=str(ldap_person.objectGUID),
            had_primary_source=MergedPrimarySourceIdentifier(
                primary_source_identities[0].stableTargetId
            ),
        )
    except HTTPError as exc:
        if (
            exc.response is not None
            and exc.response.status_code == status.HTTP_401_UNAUTHORIZED
        ):
            logger.error(f"Error fetching identities: {exc}")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Forbidden while fetching identities.",
            ) from exc
        raise
    if not identities:
        return None
    try:
        merged_person = get_merged_item(identities[0].stableTargetId)
    except HTTPError as exc:
        if (
            exc.response is not None
            and exc.response.status_code == status.HTTP_403_FORBIDDEN
        ):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Forbidden while fetching merged person.",
            ) from exc
        raise
    if merged_person.entityType == "MergedPerson":
        return merged_person
    return None
