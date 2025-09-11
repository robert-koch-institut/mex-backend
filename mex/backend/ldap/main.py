from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException
from starlette import status

from mex.backend.auxiliary.primary_source import extracted_primary_source_ldap
from mex.backend.merged.main import get_merged_item
from mex.backend.security import has_write_access_ldap
from mex.common.identity import get_provider
from mex.common.ldap.connector import LDAPConnector
from mex.common.models import MergedPerson
from mex.common.types import MergedPrimarySourceIdentifier

router = APIRouter()


@router.post("/merged-person-from-login")
def get_merged_person_from_login(
    username: Annotated[str, Depends(has_write_access_ldap)],
) -> MergedPerson | None:
    """Return the merged person from the ldap information and verify the login."""
    ldap_connector = LDAPConnector.get()
    ldap_person = ldap_connector.get_person(sAMAccountName=username)
    provider = get_provider()
    primary_source_identities = extracted_primary_source_ldap()
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
    return get_merged_item(identities[0].stableTargetId)  # type: ignore [return-value]
