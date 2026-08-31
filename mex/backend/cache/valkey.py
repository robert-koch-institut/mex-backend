from typing import Final, cast

from valkey import Valkey

from mex.backend.cache.base import BaseCacheConnector
from mex.backend.settings import BackendSettings

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

    def close(self) -> None:
        """Close the valkey connection."""
        self._client.close()  # type: ignore[no-untyped-call]
