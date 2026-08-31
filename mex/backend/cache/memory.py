from mex.backend.cache.base import BaseCacheConnector
from mex.common.models import VersionStatus


class MemoryCacheConnector(BaseCacheConnector):
    """Cache connector based on an in-memory dictionary."""

    def __init__(self) -> None:
        """Create a new in-memory cache."""
        self._database: dict[str, str] = {}
        self._keyspace_hits = 0
        self._keyspace_misses = 0

    def _get(self, key: str) -> str | None:
        """Retrieve the value for the given key, or None if not found."""
        if (value := self._database.get(key)) is None:
            self._keyspace_misses += 1
            return None
        self._keyspace_hits += 1
        return value

    def _set(self, key: str, value: str) -> None:
        """Store a key-value pair in the cache."""
        self._database[key] = value

    def _delete(self, key: str) -> None:
        """Delete the key-value pair with the given key, if it exists."""
        self._database.pop(key, None)

    def _info(self) -> dict[str, int | str]:
        """Return the cache stats that we track as metrics."""
        return {
            "dbsize": len(self._database),
            "keyspace_hits_total": self._keyspace_hits,
            "keyspace_misses_total": self._keyspace_misses,
        }

    def _flush(self) -> None:
        """Flush all keys from the cache and reset the keyspace stats."""
        self._database.clear()
        self._keyspace_hits = 0
        self._keyspace_misses = 0

    def get_status(self) -> VersionStatus:
        """Get the status and version of the in-memory cache.

        Returns:
            VersionStatus with status "local", because an in-memory cache is not
            backed by a server that could report a version
        """
        return VersionStatus(status="local", version="unknown")

    def close(self) -> None:
        """Trash the in-memory cache."""
        self._database.clear()
