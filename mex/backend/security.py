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

X_API_KEY = APIKeyHeader(name="X-API-Key")
HTTP_BASIC_AUTH = HTTPBasic()


def has_write_access(
    api_key: Annotated[str, Depends(X_API_KEY)],
) -> None:
    """Verify if provided api key has write access.

    Raises:
        HTTPException if APIKey has no write access.

    Args:
        api_key: the API key
    """
    if APIKey(api_key) not in BackendSettings.get().backend_api_key_database.write:
        raise HTTPException(status_code=403, detail="Unauthorized API Key.")


def has_read_access(
    api_key: Annotated[str, Depends(X_API_KEY)],
) -> None:
    """Verify if api key has read access or write access.

    Raises:
        HTTPException if APIKey has no read access.

    Args:
        api_key: the API key
    """
    db = BackendSettings.get().backend_api_key_database
    can_read = APIKey(api_key) in db.read
    can_write = APIKey(api_key) in db.write
    if not (can_read or can_write):
        raise HTTPException(status_code=403, detail="Unauthorized API Key.")


def is_ldap_authenticated(
    credentials: Annotated[HTTPBasicCredentials, Depends(HTTP_BASIC_AUTH)],
) -> str:
    """Authenticate against LDAP.

    Raises:
        HTTPException if credentials have no LDAP write access.

    Args:
        credentials: username and password

    Returns:
        username
    """
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
