from mex.backend.cache.base import BaseCacheConnector


class MemoryCacheConnector(BaseCacheConnector):
    """Cache connector based on an in-memory dictionary."""

    def __init__(self) -> None:
        """Create a new in-memory cache."""
        self._database: dict[str, str] = {}

    def _get(self, key: str) -> str | None:
        """Retrieve the value for the given key, or None if not found."""
        return self._database.get(key)

    def _set(self, key: str, value: str) -> None:
        """Store a key-value pair in the cache."""
        self._database[key] = value

    def _delete(self, key: str) -> None:
        """Delete the key-value pair with the given key."""
        del self._database[key]

    def _info(self) -> dict[str, int | str]:
        """Return information and statistics about the cache."""
        return {"memory_cache_size": len(self._database)}

    def _flush(self) -> None:
        """Flush all keys from the cache."""
        self._database.clear()

    def close(self) -> None:
        """Trash the in-memory cache."""
        self._database.clear()
