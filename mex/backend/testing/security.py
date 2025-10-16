from typing import Annotated

from fastapi import Depends
from fastapi.security import APIKeyHeader, HTTPBasic, HTTPBasicCredentials
from ldap3.utils.dn import escape_rdn

from mex.backend.security import check_header_for_authorization_method

X_API_KEY = APIKeyHeader(name="X-API-Key", auto_error=False)
HTTP_BASIC_AUTH = HTTPBasic(auto_error=False)


def has_write_access_ldap_mocked(
    credentials: Annotated[HTTPBasicCredentials, Depends(HTTP_BASIC_AUTH)],
) -> str:
    """Mocked function to verify if provided credentials have LDAP write access.

    Args:
        credentials: username and password
    """
    check_header_for_authorization_method(credentials=credentials)

    return escape_rdn(credentials.username.split("@")[0])
