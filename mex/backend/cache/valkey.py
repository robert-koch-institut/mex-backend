from typing import Final, cast

from valkey import Valkey
from valkey.exceptions import AuthenticationError, AuthorizationError, ValkeyError
from valkey.exceptions import ConnectionError as ValkeyConnectionError
from valkey.exceptions import TimeoutError as ValkeyTimeoutError

from mex.backend.cache.base import BaseCacheConnector
from mex.backend.settings import BackendSettings
from mex.common.logging import logger
from mex.common.models import VersionStatus

DASHBOARD_METRICS: Final[dict[str, str]] = {
    "connected_clients": "connected_clients",  # no suffix means gauge
    "evicted_keys": "evicted_keys_total",
    "keyspace_hits": "keyspace_hits_total",  # _total suffix signals counter
    "keyspace_misses": "keyspace_misses_total",
    "uptime_in_seconds": "uptime_in_seconds",
    "used_memory": "used_memory_bytes",  # _bytes signals correct unit
}


class ValkeyCacheConnector(BaseCacheConnector):
    """Cache connector based on a valkey server."""

    def __init__(self) -> None:
        """Create a new valkey client with the configured url."""
        settings = BackendSettings.get()
        self._client = Valkey.from_url(settings.valkey_url.get_secret_value())

    def _get(self, key: str) -> str | None:
        """Retrieve the value for the given key, or None if not found."""
        return cast("str | None", self._client.get(key))

    def _set(self, key: str, value: str) -> None:
        """Store a key-value pair in the cache."""
        self._client.set(key, value)

    def _delete(self, key: str) -> None:
        """Delete the key-value pair with the given key."""
        self._client.delete(key)

    def _info(self) -> dict[str, int | str]:
        """Return the subset of valkey server stats that we track as metrics."""
        info = cast("dict[str, int | str]", self._client.info())
        return {
            "dbsize": cast("int", self._client.dbsize()),
            **{
                DASHBOARD_METRICS[k]: v
                for k, v in info.items()
                if k in DASHBOARD_METRICS
            },
        }

    def _flush(self) -> None:
        """Flush all keys from the cache."""
        self._client.flushdb()

    def get_status(self) -> VersionStatus:
        """Get the status and version of the valkey server.

        Returns:
            VersionStatus with status "ok" and the valkey server version, status
            "offline" when the configured valkey server cannot be reached, or status
            "error" when the cache is misconfigured or answers with an error
        """
        try:
            info = cast("dict[str, int | str]", self._client.info())
        except AuthenticationError, AuthorizationError:
            logger.exception("error authenticating with the valkey cache")
            return VersionStatus(status="error", version="unknown")
        except ValkeyConnectionError, ValkeyTimeoutError:
            logger.exception("valkey cache is unreachable")
            return VersionStatus(status="offline", version="unknown")
        except ValkeyError:
            logger.exception("error checking the valkey cache status")
            return VersionStatus(status="error", version="unknown")
        version = info.get("valkey_version")
        return VersionStatus(
            status="ok", version=str(version) if version else "unknown"
        )

    def close(self) -> None:
        """Close the valkey connection."""
        self._client.close()  # type: ignore[no-untyped-call]
