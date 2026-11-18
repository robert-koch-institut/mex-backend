from functools import lru_cache
from typing import Annotated, Any

import jwt
from fastapi import Depends, HTTPException, Request
from fastapi.security import APIKeyHeader, OAuth2AuthorizationCodeBearer
from jwt import PyJWKClient
from pydantic import ValidationError
from starlette import status

from mex.backend.settings import BackendSettings
from mex.backend.types import APIKey, OIDCClaims
from mex.common.logging import logger

X_API_KEY = APIKeyHeader(
    name="X-API-Key",
    auto_error=False,
)
SWAGGER_UI_INIT_OAUTH = {
    "clientId": "mex-backend",
    "scopes": "openid profile groups email",
    "usePkceWithAuthorizationCodeGrant": True,
}


@lru_cache(maxsize=1)
def _get_jwks_client(jwks_uri: str) -> PyJWKClient:
    """Return a process-wide JWKS client for the given URI.

    The client is memoised so its internal signing-key cache (TTL set via
    ``lifespan``) survives across requests. ``maxsize=1`` ensures that if the
    issuer URL changes (e.g. settings reload in tests), the stale client is
    evicted instead of leaking. ``lru_cache`` is thread-safe, so no explicit
    lock is needed.

    Args:
        jwks_uri: Absolute URL of the OIDC provider's JWKS endpoint.

    Returns:
        A ``PyJWKClient`` pointing at ``jwks_uri``.
    """
    return PyJWKClient(jwks_uri, lifespan=3600)


def _verify_jwt(token: str) -> OIDCClaims:
    """Verify JWT signature and return parsed claims.

    Raises:
        HTTPException 503 if JWKS fetch or key lookup fails (dex down).
        HTTPException 401 if token is expired, has invalid signature, wrong aud/iss,
            or claims do not match the expected schema.

    Args:
        token: raw JWT string

    Returns:
        Parsed and validated OIDC claims
    """
    try:
        issuer = str(BackendSettings.get().oidc_issuer_url)
        client = _get_jwks_client(f"{issuer}/keys")
        signing_key = client.get_signing_key_from_jwt(token)
    except Exception as e:
        logger.error("JWKS fetch/key lookup failed: %s", e)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Authentication service unavailable.",
        ) from e
    settings = BackendSettings.get()
    try:
        decoded: dict[str, Any] = jwt.decode(
            token,
            signing_key,
            algorithms=settings.oidc_algorithms,
            audience=settings.oidc_client_id,
            issuer=str(settings.oidc_issuer_url),
        )
    except jwt.ExpiredSignatureError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token expired.",
        ) from e
    except jwt.InvalidTokenError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token.",
        ) from e
    else:
        try:
            return OIDCClaims.model_validate(decoded)
        except ValidationError as e:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid token claims.",
            ) from e


@lru_cache(maxsize=1)
def _build_oauth2_scheme(issuer_url: str) -> OAuth2AuthorizationCodeBearer:
    """Build (and memoise) the OAuth2 scheme for the given issuer URL.

    ``maxsize=1`` evicts the stale scheme if the issuer URL changes (e.g. on a
    settings reload in tests), so the scheme always reflects current settings
    without re-reading them at import time.
    """
    return OAuth2AuthorizationCodeBearer(
        authorizationUrl=f"{issuer_url}/auth",
        tokenUrl=f"{issuer_url}/token",
        scopes={
            "openid": "OpenID Connect",
            "profile": "User profile",
            "groups": "Group membership",
            "email": "Email address",
        },
        auto_error=False,
    )


async def oauth2_scheme(request: Request) -> str | None:
    """Extract the Bearer token from the request via the OAuth2 scheme.

    Settings are read lazily here (not at import) so test fixtures can configure
    the issuer URL before the first request. The scheme must be invoked against
    the ``Request`` to return the raw token string; injecting the scheme object
    itself as a dependency would yield the object instead of the token.
    """
    issuer_url = str(BackendSettings.get().oidc_issuer_url).rstrip("/")
    return await _build_oauth2_scheme(issuer_url)(request)


def check_api_key(api_key: str, *, write: bool) -> None:
    """Verify that the API key grants read (or, if `write`, write) access.

    Write keys implicitly grant read access.

    Raises:
        HTTPException 403 if the key is not allowed.

    Args:
        api_key: the API key
        write: whether write access is required
    """
    db = BackendSettings.get().backend_api_key_database
    allowed = db.write if write else [*db.read, *db.write]
    if APIKey(api_key) not in allowed:
        raise HTTPException(status_code=403, detail="Unauthorized API Key.")


def check_token(token: str, *, write: bool) -> OIDCClaims:
    """Verify the JWT and that its groups grant read (or, if `write`, write) access.

    Write group members implicitly have read access.

    Raises:
        HTTPException 401/503 if the token cannot be verified,
            403 if group membership is insufficient.

    Args:
        token: Bearer JWT string
        write: whether write access is required

    Returns:
        Parsed and validated OIDC claims
    """
    claims = _verify_jwt(token)
    db = BackendSettings.get().oidc_groups_database
    allowed = db.write if write else [*db.read, *db.write]
    if not set(claims.groups) & set(allowed):
        level = "write" if write else "read"
        raise HTTPException(
            status_code=403, detail=f"No {level}-level group membership."
        )
    return claims


def _check_credentials(api_key: str | None, token: str | None, *, write: bool) -> None:
    """Accept exactly one of X-API-Key or Bearer JWT with sufficient access."""
    if api_key and token:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Provide either X-API-Key or Bearer token, not both.",
        )
    if api_key:
        check_api_key(api_key, write=write)
    elif token:
        check_token(token, write=write)
    else:
        raise HTTPException(status_code=401, detail="Missing credentials.")


def has_read_access(
    api_key: Annotated[str | None, Depends(X_API_KEY)] = None,
    token: Annotated[str | None, Depends(oauth2_scheme)] = None,
) -> None:
    """Accept X-API-Key OR Bearer JWT with read or write access.

    Raises:
        HTTPException if credentials are missing or insufficient.

    Args:
        api_key: the API key
        token: Bearer JWT string
    """
    _check_credentials(api_key, token, write=False)


def has_write_access(
    api_key: Annotated[str | None, Depends(X_API_KEY)] = None,
    token: Annotated[str | None, Depends(oauth2_scheme)] = None,
) -> None:
    """Accept X-API-Key OR Bearer JWT with write access.

    Raises:
        HTTPException if credentials are missing or insufficient.

    Args:
        api_key: the API key
        token: Bearer JWT string
    """
    _check_credentials(api_key, token, write=True)


def has_oidc_access(
    api_key: Annotated[str | None, Depends(X_API_KEY)] = None,
    token: Annotated[str | None, Depends(oauth2_scheme)] = None,
) -> str:
    """Accept Bearer JWT with read or write group membership; return preferred_username.

    Used by the user endpoint to identify the authenticated user.

    Raises:
        HTTPException if an API key is provided, Bearer token is missing,
            invalid, or lacks group membership.

    Args:
        api_key: the API key (rejected - OIDC-only endpoint)
        token: Bearer JWT string

    Returns:
        The preferred_username claim from the JWT
    """
    if api_key:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="This endpoint requires a Bearer token, not an API key.",
        )
    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing Bearer token.",
            headers={"WWW-Authenticate": "Bearer"},
        )
    claims = check_token(token, write=False)
    if not claims.preferred_username:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token missing 'preferred_username' claim.",
        )
    return claims.preferred_username
