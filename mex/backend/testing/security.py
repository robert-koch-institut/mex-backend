from typing import Annotated

from fastapi import Depends, HTTPException
from starlette import status

from mex.backend.security import X_API_KEY, check_api_key, oauth2_scheme


def _check_credentials_mocked(
    api_key: str | None, token: str | None, *, write: bool
) -> None:
    """Validate API keys normally, accept any Bearer token without verification."""
    if api_key:
        check_api_key(api_key, write=write)
    elif not token:
        raise HTTPException(status_code=401, detail="Missing credentials.")


def has_read_access_mocked(
    api_key: Annotated[str | None, Depends(X_API_KEY)] = None,
    token: Annotated[str | None, Depends(oauth2_scheme)] = None,
) -> None:
    """Mocked read access - validates API keys normally, accepts any Bearer token."""
    _check_credentials_mocked(api_key, token, write=False)


def has_write_access_mocked(
    api_key: Annotated[str | None, Depends(X_API_KEY)] = None,
    token: Annotated[str | None, Depends(oauth2_scheme)] = None,
) -> None:
    """Mocked write access - validates API keys normally, accepts any Bearer token."""
    _check_credentials_mocked(api_key, token, write=True)


def has_oidc_access_mocked(
    token: Annotated[str | None, Depends(oauth2_scheme)] = None,
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
