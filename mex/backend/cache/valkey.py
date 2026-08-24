from typing import cast

from valkey import Valkey

from mex.backend.cache.base import BaseCacheConnector
from mex.backend.settings import BackendSettings


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
        """Return information and statistics about the cache."""
        return cast("dict[str, int | str]", self._client.info())

    def _clear(self) -> None:
        """Clear all keys from the cache."""
        self._client.flushdb()

    def close(self) -> None:
        """Close the valkey connection."""
        self._client.close()  # type: ignore[no-untyped-call]
