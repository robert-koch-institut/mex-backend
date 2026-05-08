from typing import TYPE_CHECKING, Annotated

from fastapi import Depends
from ldap3.utils.dn import escape_rdn

from mex.backend.security import HTTP_BASIC_AUTH

if TYPE_CHECKING:
    from fastapi.security import HTTPBasicCredentials


def is_ldap_authenticated_mocked(
    credentials: Annotated[HTTPBasicCredentials, Depends(HTTP_BASIC_AUTH)],
) -> str:
    """Mocked function to authenticate against LDAP.

    Args:
        credentials: username and password
    """
    return escape_rdn(credentials.username.split("@")[0])
