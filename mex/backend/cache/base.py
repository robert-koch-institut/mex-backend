import json
from abc import abstractmethod
from typing import TYPE_CHECKING, Any, cast

from mex.backend.settings import BackendSettings
from mex.common.connector import BaseConnector
from mex.common.transform import MExEncoder

if TYPE_CHECKING:
    from pydantic import BaseModel

    from mex.common.models import VersionStatus


class BaseCacheConnector(BaseConnector):
    """Base class for cache connectors that handle key-value storage."""

    @abstractmethod
    def _get(self, key: str) -> str | None:
        """Retrieve the value for the given key, or None if not found."""
        ...

    @abstractmethod
    def _set(self, key: str, value: str) -> None:
        """Store a key-value pair in the cache."""
        ...

    @abstractmethod
    def _delete(self, key: str) -> None:
        """Delete the key-value pair with the given key."""
        ...

    @abstractmethod
    def _info(self) -> dict[str, int | str]:
        """Return information and statistics about the cache."""
        ...

    @abstractmethod
    def _flush(self) -> None:
        """Flush all keys from the cache."""
        ...

    @abstractmethod
    def get_status(self) -> VersionStatus:
        """Get the status and version of the underlying cache."""
        ...

    def get_value(self, key: str) -> dict[str, Any] | None:
        """Retrieve and deserialize the value for the given key."""
        if value := self._get(key):
            return cast("dict[str, Any]", json.loads(value))
        return None

    def set_value(self, key: str, model: BaseModel) -> None:
        """Store a pydantic model in the cache as JSON under the given key."""
        self._set(key, json.dumps(model, cls=MExEncoder))

    def delete_value(self, key: str) -> None:
        """Delete the value with the given key from the cache."""
        self._delete(key)

    def metrics(self) -> dict[str, int]:
        """Generate metrics about the cache."""
        return {k: v for k, v in self._info().items() if isinstance(v, int)}

    def flush(self) -> None:
        """Flush all stored data, but only when debug mode is enabled."""
        if BackendSettings.get().debug is True:
            self._flush()
