from enum import Enum
from typing import Annotated

from fastapi import Depends, HTTPException
from fastapi.security import APIKeyHeader, HTTPBasic, HTTPBasicCredentials
from starlette import status

from mex.backend.settings import BackendSettings
from mex.backend.types import APIKey

X_API_KEY = APIKeyHeader(name="X-API-Key", auto_error=False)
X_API_CREDENTIALS = HTTPBasic(auto_error=False)


class AccessMode(Enum):
    """Requested Access Mode."""

    READ = "read"
    WRITE = "write"


def verify_user_access(
    mode: AccessMode,
    creds: Annotated[HTTPBasicCredentials | None, Depends(X_API_CREDENTIALS)] = None,
) -> bool:
    """Verify credentials of a user for read or write access.

    Args:
        mode: requested access mode, either read or write
        creds: username and password

    Settings:
        backend_user_database: checked for username and password

    Returns:
        True if user has access regarding requested mode, False otherwise.
    """
    if not creds:
        return False
    settings = BackendSettings.get()
    read_db = settings.backend_user_database.read
    write_db = settings.backend_user_database.write

    match mode:
        case AccessMode.READ:
            if creds.username in read_db:
                db = read_db
            elif creds.username in write_db:  # read access is implied by write
                db = write_db
            else:  # user unknown
                return False
            return creds.password == db[creds.username].get_secret_value()
        case AccessMode.WRITE:
            return (creds.username in write_db) and (
                creds.password == write_db[creds.username].get_secret_value()
            )
    return False


def has_write_access(
    api_key: Annotated[str | None, Depends(X_API_KEY)] = None,
    credentials: Annotated[
        HTTPBasicCredentials | None, Depends(X_API_CREDENTIALS)
    ] = None,
) -> None:
    """Verify if provided api key or credentials have write access.

    Raises:
        HTTPException if no header or provided APIKey/credentials have no write access.

    Args:
        api_key: the API key
        credentials: username and password

    Settings:
        check credentials in backend_user_database or backend_api_key_database
    """
    if not api_key and not credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing authentication header X-API-Key or credentials.",
        )

    settings = BackendSettings.get()
    if api_key:
        api_key_database = settings.backend_api_key_database
        can_write = APIKey(api_key) in api_key_database.write
    else:
        can_write = verify_user_access(AccessMode.WRITE, credentials)
    if not can_write:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Unauthorized {'API Key' if api_key else 'credentials'}.",
        )


def has_read_access(
    api_key: Annotated[str | None, Depends(X_API_KEY)] = None,
    credentials: Annotated[
        HTTPBasicCredentials | None, Depends(X_API_CREDENTIALS)
    ] = None,
) -> None:
    """Verify if api key or credentials have read access or write access.

    Raises:
        HTTPException if no header or provided APIKey/credentials have no read access.

    Args:
        api_key: the API key
        credentials: username and password

    Settings:
        check credentials in backend_user_database or backend_api_key_database
    """
    if not api_key and not credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing authentication header X-API-Key or credentials.",
        )

    settings = BackendSettings.get()
    api_key_database = settings.backend_api_key_database

    if api_key:
        can_read = (APIKey(api_key) in api_key_database.write) or (
            APIKey(api_key) in api_key_database.read
        )
    else:
        can_read = verify_user_access(AccessMode.READ, credentials)
    if not can_read:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Unauthorized {'API Key' if api_key else 'credentials'}.",
        )
