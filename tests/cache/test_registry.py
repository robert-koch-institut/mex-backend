from typing import TYPE_CHECKING

import pytest
from pytest import MonkeyPatch

from mex.backend.cache import (
    MemoryCacheConnector,
    ValkeyCacheConnector,
    get_cache_connector,
    register_cache_connector,
)
from mex.backend.cache.registry import _CONNECTOR_REGISTRY
from mex.backend.types import CacheConnectorType

if TYPE_CHECKING:
    from mex.backend.settings import BackendSettings


def test_get_memory_cache_connector(settings: BackendSettings) -> None:
    assert settings.cache_connector == CacheConnectorType.MEMORY
    assert isinstance(get_cache_connector(), MemoryCacheConnector)


@pytest.mark.integration
def test_get_valkey_cache_connector(settings: BackendSettings) -> None:
    assert settings.cache_connector == CacheConnectorType.VALKEY
    assert isinstance(get_cache_connector(), ValkeyCacheConnector)


def test_get_cache_connector_error_on_unregistered(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.delitem(_CONNECTOR_REGISTRY, CacheConnectorType.MEMORY)

    with pytest.raises(RuntimeError, match="Cache connector not implemented"):
        get_cache_connector()


def test_register_cache_connector_error_on_duplicate() -> None:
    with pytest.raises(RuntimeError, match="Already registered cache connector"):
        register_cache_connector(CacheConnectorType.VALKEY, ValkeyCacheConnector)
