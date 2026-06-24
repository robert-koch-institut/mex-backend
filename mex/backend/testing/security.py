from functools import lru_cache
from typing import Annotated

from fastapi import Depends, HTTPException, Request
from fastapi.security import APIKeyHeader, OAuth2AuthorizationCodeBearer
from starlette import status

from mex.backend.settings import BackendSettings
from mex.backend.types import APIKey

X_API_KEY = APIKeyHeader(name="X-API-Key", auto_error=False)


@lru_cache(maxsize=1)
def _build_oauth2_scheme_mocked(issuer_url: str) -> OAuth2AuthorizationCodeBearer:
    """Build (and memoise) the mocked OAuth2 scheme for the given issuer URL."""
    return OAuth2AuthorizationCodeBearer(
        authorizationUrl=f"{issuer_url}/auth",
        tokenUrl="/v0/oauth/token",
        auto_error=False,
    )


async def oauth2_scheme_mocked(request: Request) -> str | None:
    """Extract the Bearer token from the request via the mocked OAuth2 scheme."""
    issuer_url = str(BackendSettings.get().oidc_issuer_url).rstrip("/")
    return await _build_oauth2_scheme_mocked(issuer_url)(request)


def has_read_access_mocked(
    api_key: Annotated[str | None, Depends(X_API_KEY)] = None,
    token: Annotated[str | None, Depends(oauth2_scheme_mocked)] = None,
) -> None:
    """Mocked read access - validates API keys normally, accepts any Bearer token."""
    if api_key:
        db = BackendSettings.get().backend_api_key_database
        if APIKey(api_key) not in db.read and APIKey(api_key) not in db.write:
            raise HTTPException(status_code=403, detail="Unauthorized API Key.")
    elif token:
        pass  # Accept any Bearer token without JWT verification
    else:
        raise HTTPException(status_code=401, detail="Missing credentials.")


def has_write_access_mocked(
    api_key: Annotated[str | None, Depends(X_API_KEY)] = None,
    token: Annotated[str | None, Depends(oauth2_scheme_mocked)] = None,
) -> None:
    """Mocked write access - validates API keys normally, accepts any Bearer token."""
    if api_key:
        if APIKey(api_key) not in BackendSettings.get().backend_api_key_database.write:
            raise HTTPException(status_code=403, detail="Unauthorized API Key.")
    elif token:
        pass  # Accept any Bearer token without JWT verification
    else:
        raise HTTPException(status_code=401, detail="Missing credentials.")


def has_oidc_access_mocked(
    token: Annotated[str | None, Depends(oauth2_scheme_mocked)] = None,
) -> str:
    """Mocked OIDC access - returns Bearer token value as username.

    Raises:
        HTTPException 401 if no Bearer header is present.

    Args:
        token: Bearer token string

    Returns:
        The raw Bearer token string, used as the username in tests
    """
    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing Bearer token.",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return token
