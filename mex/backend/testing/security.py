from typing import Annotated

from fastapi import Depends, Header, HTTPException
from fastapi.security import APIKeyHeader, HTTPBasic, HTTPBasicCredentials
from ldap3.utils.dn import escape_rdn
from starlette import status

X_API_KEY = APIKeyHeader(name="X-API-Key", auto_error=False)
HTTP_BASIC_AUTH = HTTPBasic(auto_error=False)


def _check_header_for_authorization_method(
    api_key: Annotated[str | None, Depends(X_API_KEY)] = None,
    credentials: Annotated[
        HTTPBasicCredentials | None, Depends(HTTP_BASIC_AUTH)
    ] = None,
    user_agent: Annotated[str, Header(include_in_schema=False)] = "n/a",
) -> None:
    """Check authorization header for API key or credentials.

    Raises:
        HTTPException if both API key and credentials or none of them are in header.

    Args:
        api_key: the API key
        credentials: username and password
        user_agent: user-agent (in case of a web browser starts with "Mozilla/")
    """
    if not api_key and not credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing authentication header X-API-Key or credentials.",
            headers=(
                {"WWW-Authenticate": "Basic"}
                if user_agent.startswith("Mozilla/")
                else None
            ),
        )
    if api_key and credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authenticate with X-API-Key or credentials, not both.",
            headers=(
                {"WWW-Authenticate": "Basic"}
                if user_agent.startswith("Mozilla/")
                else None
            ),
        )


def has_write_access_ldap(
    credentials: Annotated[HTTPBasicCredentials, Depends(HTTP_BASIC_AUTH)],
) -> str:
    """Verify if provided credentials have LDAP write access.

    Args:
        credentials: username and password
    """
    _check_header_for_authorization_method(credentials=credentials)

    return escape_rdn(credentials.username.split("@")[0])
