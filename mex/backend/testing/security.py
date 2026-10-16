from typing import TYPE_CHECKING, Annotated

import ldap
from fastapi import Depends

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
    return ldap.dn.escape_dn_chars(credentials.username.split("@")[0])
