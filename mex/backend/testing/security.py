from typing import Annotated

from fastapi import Depends, HTTPException
from fastapi.security import APIKeyHeader, HTTPAuthorizationCredentials, HTTPBearer
from starlette import status

from mex.backend.settings import BackendSettings
from mex.backend.types import APIKey

X_API_KEY = APIKeyHeader(name="X-API-Key", auto_error=False)
HTTP_BEARER = HTTPBearer(auto_error=False)


def has_read_access_mocked(
    api_key: Annotated[str | None, Depends(X_API_KEY)] = None,
    credentials: Annotated[
        HTTPAuthorizationCredentials | None, Depends(HTTP_BEARER)
    ] = None,
) -> None:
    """Mocked read access — validates API keys normally, accepts any Bearer token."""
    if api_key:
        db = BackendSettings.get().backend_api_key_database
        if APIKey(api_key) not in db.read and APIKey(api_key) not in db.write:
            raise HTTPException(status_code=403, detail="Unauthorized API Key.")
    elif credentials:
        pass  # Accept any Bearer token without JWT verification
    else:
        raise HTTPException(status_code=401, detail="Missing credentials.")


def has_write_access_mocked(
    api_key: Annotated[str | None, Depends(X_API_KEY)] = None,
    credentials: Annotated[
        HTTPAuthorizationCredentials | None, Depends(HTTP_BEARER)
    ] = None,
) -> None:
    """Mocked write access — validates API keys normally, accepts any Bearer token."""
    if api_key:
        if APIKey(api_key) not in BackendSettings.get().backend_api_key_database.write:
            raise HTTPException(status_code=403, detail="Unauthorized API Key.")
    elif credentials:
        pass  # Accept any Bearer token without JWT verification
    else:
        raise HTTPException(status_code=401, detail="Missing credentials.")


def has_write_access_oidc_mocked(
    credentials: Annotated[
        HTTPAuthorizationCredentials | None, Depends(HTTP_BEARER)
    ] = None,
) -> str:
    """Mocked OIDC write access — returns Bearer token value as username.

    Raises:
        HTTPException 401 if no Bearer header is present.

    Args:
        credentials: HTTP Bearer credentials

    Returns:
        The raw Bearer token string, used as the username in tests
    """
    if not credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing Bearer token.",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return credentials.credentials
