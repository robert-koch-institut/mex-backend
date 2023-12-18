from typing import Annotated

from fastapi import Depends, HTTPException
from fastapi.security import APIKeyHeader
from starlette import status

from mex.backend.settings import BackendSettings
from mex.backend.types import APIKey

X_API_KEY = APIKeyHeader(name="X-API-Key", auto_error=False)


def has_write_access(api_key: Annotated[str | None, Depends(X_API_KEY)]) -> None:
    """Verify if api key has write access.

    Raises:
        HTTPException if no header is provided or APIKey does not have write access.

    Args:
        api_key: the API key

    Settings:
        backend_user_database: checked for presence of api_key
    """
    if not api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing authentication header X-API-Key.",
        )

    settings = BackendSettings.get()
    user_database = settings.backend_user_database
    can_write = APIKey(api_key) in user_database.write
    if not can_write:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Unauthorized API Key.",
        )


def has_read_access(api_key: Annotated[str | None, Depends(X_API_KEY)]) -> None:
    """Verify if api key has read access or read access implied by write access.

    Raises:
        HTTPException if no header is provided or APIKey does not have read access.

    Args:
        api_key: the API key

    Settings:
        backend_user_database: checked for presence of api_key
    """
    if not api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing authentication header X-API-Key.",
        )

    settings = BackendSettings.get()
    user_database = settings.backend_user_database
    try:
        has_write_access(api_key)
        can_write = True
    except HTTPException:
        can_write = False
    can_read = can_write or (APIKey(api_key) in user_database.read)

    if not can_read:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Unauthorized API Key.",
        )
