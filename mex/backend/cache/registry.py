from typing import TYPE_CHECKING, Final

from mex.backend.settings import BackendSettings

if TYPE_CHECKING:  # pragma: no cover
    from mex.backend.cache.base import BaseCacheConnector
    from mex.backend.types import CacheConnectorType

_CONNECTOR_REGISTRY: Final[dict[CacheConnectorType, type[BaseCacheConnector]]] = {}


def register_cache_connector(
    key: CacheConnectorType, connector_cls: type[BaseCacheConnector]
) -> None:
    """Register an implementation of a cache connector to a settings key.

    Args:
        key: Possible value of `BackendSettings.cache_connector`
        connector_cls: Implementation of a cache connector

    Raises:
        RuntimeError: When the `key` is already registered
    """
    if key in _CONNECTOR_REGISTRY:
        msg = f"Already registered cache connector: {key}"
        raise RuntimeError(msg)
    _CONNECTOR_REGISTRY[key] = connector_cls


def get_cache_connector() -> BaseCacheConnector:
    """Get an instance of the cache connector as configured by `cache_connector`.

    Raises:
        RuntimeError: When the configured connector is not registered

    Returns:
        An instance of a subclass of `BaseCacheConnector`
    """
    settings = BackendSettings.get()
    if settings.cache_connector in _CONNECTOR_REGISTRY:
        connector_cls = _CONNECTOR_REGISTRY[settings.cache_connector]
        return connector_cls.get()
    msg = f"Cache connector not implemented: {settings.cache_connector}"
    raise RuntimeError(msg)
