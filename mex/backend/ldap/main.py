from typing import Annotated, cast

from fastapi import APIRouter, Depends, HTTPException
from starlette import status

from mex.backend.auxiliary.primary_source import extracted_primary_source_ldap
from mex.backend.preview.main import search_preview_items
from mex.backend.security import has_write_access_ldap
from mex.common.identity import get_provider
from mex.common.ldap.connector import LDAPConnector
from mex.common.models import PreviewPerson
from mex.common.types import MergedPrimarySourceIdentifier

router = APIRouter()


@router.post("/preview-person-from-login", tags=["editor"])
def get_preview_person_from_login(
    username: Annotated[str, Depends(has_write_access_ldap)],
) -> PreviewPerson:
    """Return the preview person from the ldap information and verify the login."""
    ldap_connector = LDAPConnector.get()
    ldap_person = ldap_connector.get_person(sam_account_name=username)
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
    return cast(
        "PreviewPerson",
        search_preview_items(identifier=identities[0].stableTargetId).items[0],
    )
