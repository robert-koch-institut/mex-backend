from secrets import compare_digest
from typing import Annotated

from fastapi import Depends, Header, HTTPException
from fastapi.security import APIKeyHeader, HTTPBasic, HTTPBasicCredentials
from starlette import status

from mex.backend.settings import BackendSettings
from mex.backend.types import APIKey

X_API_KEY = APIKeyHeader(name="X-API-Key", auto_error=False)
X_API_CREDENTIALS = HTTPBasic(auto_error=False)


def _check_header_for_authorization_method(
    api_key: Annotated[str | None, Depends(X_API_KEY)] = None,
    credentials: Annotated[
        HTTPBasicCredentials | None, Depends(X_API_CREDENTIALS)
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
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Authenticate with X-API-Key or credentials, not both.",
            headers=(
                {"WWW-Authenticate": "Basic"}
                if user_agent.startswith("Mozilla/")
                else None
            ),
        )


def has_write_access(
    api_key: Annotated[str | None, Depends(X_API_KEY)] = None,
    credentials: Annotated[
        HTTPBasicCredentials | None, Depends(X_API_CREDENTIALS)
    ] = None,
    user_agent: Annotated[str, Header(include_in_schema=False)] = "n/a",
) -> None:
    """Verify if provided api key or credentials have write access.

    Raises:
        HTTPException if no header or provided APIKey/credentials have no write access.

    Args:
        api_key: the API key
        credentials: username and password
        user_agent: user-agent (in case of a web browser starts with "Mozilla/")

    Settings:
        check credentials in backend_user_database or backend_api_key_database
    """
    _check_header_for_authorization_method(api_key, credentials, user_agent)

    settings = BackendSettings.get()
    can_write = False
    if api_key:
        api_key_database = settings.backend_api_key_database
        can_write = APIKey(api_key) in api_key_database.write
    elif credentials:
        api_write_user_db = settings.backend_user_database.write
        user, pw = credentials.username, credentials.password.encode("utf-8")
        if api_write_user := api_write_user_db.get(user):
            can_write = compare_digest(
                pw, api_write_user.get_secret_value().encode("utf-8")
            )
    if not can_write:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Unauthorized {'API Key' if api_key else 'credentials'}.",
            headers=(
                {"WWW-Authenticate": "Basic"}
                if user_agent.startswith("Mozilla/")
                else None
            ),
        )


def has_read_access(
    api_key: Annotated[str | None, Depends(X_API_KEY)] = None,
    credentials: Annotated[
        HTTPBasicCredentials | None,
        Depends(X_API_CREDENTIALS),
    ] = None,
    user_agent: Annotated[str, Header(include_in_schema=False)] = "n/a",
) -> None:
    """Verify if api key or credentials have read access or write access.

    Raises:
        HTTPException if no header or provided APIKey/credentials have no read access.

    Args:
        api_key: the API key
        credentials: username and password
        user_agent: user-agent (in case of a web browser starts with "Mozilla/")

    Settings:
        check credentials in backend_user_database or backend_api_key_database
    """
    _check_header_for_authorization_method(api_key, credentials, user_agent)

    try:
        has_write_access(api_key, credentials)  # read access implied by write access
        can_write = True
    except HTTPException:
        can_write = False

    settings = BackendSettings.get()
    can_read = False
    if api_key:
        api_key_database = settings.backend_api_key_database
        can_read = APIKey(api_key) in api_key_database.read
    elif credentials:
        api_read_user_db = settings.backend_user_database.read
        user, pw = credentials.username, credentials.password.encode("utf-8")
        if api_read_user := api_read_user_db.get(user):
            can_read = compare_digest(
                pw, api_read_user.get_secret_value().encode("utf-8")
            )
    can_read = can_read or can_write
    if not can_read:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Unauthorized {'API Key' if api_key else 'credentials'}.",
            headers=(
                {"WWW-Authenticate": "Basic"}
                if user_agent.startswith("Mozilla/")
                else None
            ),
        )
