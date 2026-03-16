from typing import Annotated
from urllib.parse import urlsplit

from fastapi import Depends, HTTPException
from fastapi.security import APIKeyHeader, HTTPBasic, HTTPBasicCredentials
from ldap3 import AUTO_BIND_NO_TLS, Connection, Server
from ldap3.core.exceptions import LDAPBindError
from ldap3.utils.dn import escape_rdn
from starlette import status

from mex.backend.settings import BackendSettings
from mex.backend.types import APIKey
from mex.common.logging import logger

X_API_KEY = APIKeyHeader(name="X-API-Key", auto_error=False)
HTTP_BASIC_AUTH = HTTPBasic(auto_error=False)


def check_header_for_authorization_method(
    credentials: Annotated[
        HTTPBasicCredentials | None, Depends(HTTP_BASIC_AUTH)
    ] = None,
) -> None:
    """Check authorization header for credentials.

    Raises:
        HTTPException if credentials are missing from header.

    Args:
        credentials: username and password
    """
    if not credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing credentials.",
            headers={"WWW-Authenticate": "Basic"},
        )


def has_write_access(
    api_key: Annotated[str | None, Depends(X_API_KEY)] = None,
) -> None:
    """Verify if provided api key has write access.

    Raises:
        HTTPException if no header or provided APIKey has no write access.

    Args:
        api_key: the API key
    """
    if not api_key:
        raise HTTPException(status_code=401, detail="Missing X-API-Key header.")
    if APIKey(api_key) not in BackendSettings.get().backend_api_key_database.write:
        raise HTTPException(status_code=403, detail="Unauthorized API Key.")


def has_read_access(
    api_key: Annotated[str | None, Depends(X_API_KEY)] = None,
) -> None:
    """Verify if api key has read access or write access.

    Raises:
        HTTPException if no header or provided APIKey has no read access.

    Args:
        api_key: the API key
    """
    if not api_key:
        raise HTTPException(status_code=401, detail="Missing X-API-Key header.")
    db = BackendSettings.get().backend_api_key_database
    if APIKey(api_key) not in db.read and APIKey(api_key) not in db.write:
        raise HTTPException(status_code=403, detail="Unauthorized API Key.")


def has_write_access_ldap(
    credentials: Annotated[HTTPBasicCredentials, Depends(HTTP_BASIC_AUTH)],
) -> str:
    """Verify if provided credentials have LDAP write access.

    Raises:
        HTTPException if credentials have no LDAP write access or are missing.

    Args:
        credentials: username and password
    """
    check_header_for_authorization_method(credentials=credentials)
    settings = BackendSettings.get()
    url = urlsplit(settings.ldap_url.get_secret_value())
    host = str(url.hostname)
    port = int(url.port) if url.port else None
    server = Server(host, port, use_ssl=True)
    username = escape_rdn(credentials.username.split("@")[0])
    try:
        with Connection(
            server,
            user=f"{username}@rki.local",
            password=credentials.password,
            auto_bind=AUTO_BIND_NO_TLS,
            read_only=True,
        ) as connection:
            availability = connection.server.check_availability()
            if availability is True:
                return credentials.username
            logger.error(f"LDAP server not available: {availability}")
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="LDAP server not available.",
                headers=({"WWW-Authenticate": "Basic"}),
            )
    except LDAPBindError as e:
        logger.error(f"LDAP bind error: {e}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="LDAP bind failed.",
            headers=({"WWW-Authenticate": "Basic"}),
        ) from e
