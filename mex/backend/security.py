import threading
from typing import Annotated, Any

import jwt
from fastapi import Depends, HTTPException
from fastapi.security import APIKeyHeader, OAuth2AuthorizationCodeBearer
from jwt import PyJWKClient
from pydantic import ValidationError
from starlette import status

from mex.backend.settings import BackendSettings
from mex.backend.types import APIKey, OIDCClaims
from mex.common.logging import logger

X_API_KEY = APIKeyHeader(name="X-API-Key", auto_error=False)
OAUTH2_SCHEME = OAuth2AuthorizationCodeBearer(
    authorizationUrl="http://localhost:5556/dex/auth",
    tokenUrl="/v0/oauth/token",
    scopes={
        "openid": "OpenID Connect",
        "profile": "User profile",
        "groups": "Group membership",
        "email": "Email address",
    },
    auto_error=False,
)

_jwks_client: PyJWKClient | None = None
_jwks_lock = threading.Lock()


def _get_jwks_client() -> PyJWKClient:
    global _jwks_client  # noqa: PLW0603
    issuer = str(BackendSettings.get().oidc_issuer_url)
    jwks_uri = f"{issuer}/keys"
    with _jwks_lock:
        if _jwks_client is None or _jwks_client.uri != jwks_uri:
            _jwks_client = PyJWKClient(jwks_uri, lifespan=3600)
    return _jwks_client


def _verify_jwt(token: str) -> OIDCClaims:
    """Verify JWT signature and return parsed claims.

    Raises:
        HTTPException 503 if JWKS fetch or key lookup fails (Dex down).
        HTTPException 401 if token is expired, has invalid signature, wrong aud/iss,
            or claims do not match the expected schema.

    Args:
        token: raw JWT string

    Returns:
        Parsed and validated OIDC claims
    """
    try:
        client = _get_jwks_client()
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


def has_read_access(
    api_key: Annotated[str | None, Depends(X_API_KEY)] = None,
    token: Annotated[str | None, Depends(OAUTH2_SCHEME)] = None,
) -> None:
    """Accept X-API-Key OR Bearer JWT with read or write group membership.

    Write group members implicitly have read access.

    Raises:
        HTTPException if credentials are missing or insufficient.

    Args:
        api_key: the API key
        token: Bearer JWT string
    """
    if api_key and token:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Provide either X-API-Key or Bearer token, not both.",
        )
    if api_key:
        api_key_db = BackendSettings.get().backend_api_key_database
        if (
            APIKey(api_key) not in api_key_db.read
            and APIKey(api_key) not in api_key_db.write
        ):
            raise HTTPException(status_code=403, detail="Unauthorized API Key.")
    elif token:
        claims = _verify_jwt(token)
        logger.info(
            "OIDC claims: groups=%s, preferred_username=%s",
            claims.groups,
            claims.preferred_username,
        )
        oidc_db = BackendSettings.get().oidc_groups_database
        if not set(claims.groups) & (set(oidc_db.read) | set(oidc_db.write)):
            raise HTTPException(
                status_code=403, detail="No read-level group membership."
            )
    else:
        raise HTTPException(status_code=401, detail="Missing credentials.")


def has_write_access(
    api_key: Annotated[str | None, Depends(X_API_KEY)] = None,
    token: Annotated[str | None, Depends(OAUTH2_SCHEME)] = None,
) -> None:
    """Accept X-API-Key OR Bearer JWT with write group membership.

    Raises:
        HTTPException if credentials are missing or insufficient.

    Args:
        api_key: the API key
        token: Bearer JWT string
    """
    if api_key and token:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Provide either X-API-Key or Bearer token, not both.",
        )
    if api_key:
        if APIKey(api_key) not in BackendSettings.get().backend_api_key_database.write:
            raise HTTPException(status_code=403, detail="Unauthorized API Key.")
    elif token:
        claims = _verify_jwt(token)
        db = BackendSettings.get().oidc_groups_database
        if not set(claims.groups) & set(db.write):
            raise HTTPException(
                status_code=403, detail="No write-level group membership."
            )
    else:
        raise HTTPException(status_code=401, detail="Missing credentials.")


def has_oidc_access(
    api_key: Annotated[str | None, Depends(X_API_KEY)] = None,
    token: Annotated[str | None, Depends(OAUTH2_SCHEME)] = None,
) -> str:
    """Accept Bearer JWT with read or write group membership; return preferred_username.

    Used by the user endpoint to identify the authenticated user.

    Raises:
        HTTPException if both credentials are provided, Bearer token is missing,
            invalid, or lacks group membership.

    Args:
        api_key: the API key (rejected — OIDC-only endpoint)
        token: Bearer JWT string

    Returns:
        The preferred_username claim from the JWT
    """
    if api_key and token:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Provide either X-API-Key or Bearer token, not both.",
        )
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
    claims = _verify_jwt(token)
    oidc_db = BackendSettings.get().oidc_groups_database
    if not set(claims.groups) & (set(oidc_db.read) | set(oidc_db.write)):
        raise HTTPException(status_code=403, detail="No read-level group membership.")
    if not claims.preferred_username:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token missing 'preferred_username' claim.",
        )
    return claims.preferred_username
