import json
from typing import Any, Protocol, cast

from pydantic import BaseModel
from redis import Redis

from mex.backend.settings import BackendSettings
from mex.common.connector import BaseConnector
from mex.common.transform import MExEncoder


class CacheProto(Protocol):
    """Protocol for unified cache interface."""

    def get(self, key: str) -> str | None: ...  # noqa: D102

    def set(self, key: str, value: str) -> None: ...  # noqa: D102

    def info(self) -> dict[str, int | str]: ...  # noqa: D102

    def flushdb(self) -> None: ...  # noqa: D102

    def close(self) -> None: ...  # noqa: D102


class LocalCache(dict[str, str]):
    """Fallback key/value store based on a local dict."""

    def set(self, key: str, value: str) -> None:
        """Store the given value for the given key."""
        self[key] = value

    def info(self) -> dict[str, int | str]:
        """Return basic info for local cache usage."""
        return {"local_cache_size": len(self)}

    def flushdb(self) -> None:
        """Clear the local cache."""
        self.clear()

    def close(self) -> None:
        """Close the local cache."""


class CacheConnector(BaseConnector):
    """Connector to handle getting and setting cache values.

    Depending on whether `redis_url` is configured, this cache connector
    will use either a redis server or a local dictionary cache.
    """

    def __init__(self) -> None:
        """Create a new cache connector instance."""
        settings = BackendSettings.get()
        if settings.redis_url:
            self._cache: CacheProto = Redis.from_url(
                settings.redis_url.get_secret_value()
            )
        else:
            self._cache = LocalCache()

    def get_value(self, key: str) -> dict[str, Any] | None:
        """Retrieve and deserialize a value from the cache.

        Args:
            key: Cache key to retrieve the value for

        Returns:
            Dictionary if key exists, None otherwise
        """
        if value := self._cache.get(key):
            return cast("dict[str, Any]", json.loads(value))
        return None

    def set_value(self, key: str, model: BaseModel) -> None:
        """Store a Pydantic model in the cache as JSON under the given key.

        Args:
            key: Cache key to store the value under
            model: Pydantic model to serialize and cache
        """
        self._cache.set(key, json.dumps(model, cls=MExEncoder))

    def metrics(self) -> dict[str, int]:
        """Generate metrics about the underlying cache."""
        return {k: v for k, v in self._cache.info().items() if isinstance(v, int)}

    def flush(self) -> None:
        """Flush the cache by clearing all stored data.

        This operation only executes when debug mode is enabled in settings.
        """
        settings = BackendSettings.get()
        if settings.debug is True:
            self._cache.flushdb()

    def close(self) -> None:
        """Close the connector's underlying sockets."""
        self._cache.close()
