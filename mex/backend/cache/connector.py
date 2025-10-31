import json
from typing import Any, cast

from pydantic import BaseModel, SecretStr
from valkey import Redis

from mex.backend.settings import BackendSettings
from mex.common.connector import BaseConnector
from mex.common.transform import MExEncoder


class RedisCache:
    """Wrapper around redis client for a unified cache interface."""

    def __init__(self, url: SecretStr) -> None:
        """Create a new redis client with the given url."""
        self._client = Redis.from_url(url.get_secret_value())

    def get(self, key: str) -> str | None:
        """Retrieve value for the given key, or None if not found."""
        return cast("str | None", self._client.get(key))

    def set(self, key: str, value: str) -> None:
        """Store a key-value pair in the cache."""
        self._client.set(key, value)

    def info(self) -> dict[str, int | str]:
        """Return Redis server information and statistics."""
        return cast("dict[str, int | str]", self._client.info())

    def flushdb(self) -> None:
        """Clear all keys from the current database."""
        self._client.flushdb()

    def close(self) -> None:
        """Close the Redis connection."""
        self._client.close()  # type: ignore[no-untyped-call]


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
            self._cache: LocalCache | RedisCache = RedisCache(settings.redis_url)
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
