import json
from typing import Any, Protocol, cast

from pydantic import BaseModel
from redis import Redis

from mex.backend.settings import BackendSettings
from mex.common.connector import BaseConnector
from mex.common.transform import MExEncoder


class CacheProto(Protocol):
    """Protocol for implementing a local redis fallback."""

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
    """Connector to handle getting and setting cache values in redis."""

    def __init__(self) -> None:
        """Create a new connector instance."""
        settings = BackendSettings.get()
        if settings.redis_url:
            self._cache: CacheProto = Redis.from_url(settings.redis_url)
        else:
            self._cache = LocalCache()

    def get_value(self, key: str) -> dict[str, Any] | None:
        """Return the value for the given key."""
        if value := self._cache.get(key):
            return cast("dict[str, Any]", json.loads(value))
        return None

    def set_value(self, key: str, model: BaseModel) -> None:
        """Return the value for the given key."""
        self._cache.set(key, json.dumps(model, cls=MExEncoder))

    def metrics(self) -> dict[str, int]:
        """Generate metrics about redis."""
        return {k: v for k, v in self._cache.info().items() if isinstance(v, int)}

    def flush(self) -> None:
        """Flush the cache (only in debug mode)."""
        settings = BackendSettings.get()
        if settings.debug is True:
            self._cache.flushdb()

    def close(self) -> None:
        """Close the connector's underlying sockets."""
        self._cache.close()
