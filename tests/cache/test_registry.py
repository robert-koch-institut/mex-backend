from typing import TYPE_CHECKING

import pytest
from pytest import MonkeyPatch

from mex.backend.cache.memory import MemoryCacheConnector
from mex.backend.cache.registry import (
    _CONNECTOR_REGISTRY,
    get_cache_connector,
    register_cache_connector,
)
from mex.backend.cache.valkey import ValkeyCacheConnector
from mex.backend.types import CacheConnectorType

if TYPE_CHECKING:  # pragma: no cover
    from mex.backend.settings import BackendSettings


def test_get_cache_connector(settings: BackendSettings) -> None:
    assert settings.cache_connector == CacheConnectorType.MEMORY
    assert isinstance(get_cache_connector(), MemoryCacheConnector)


def test_get_cache_connector_error_on_unregistered(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.delitem(_CONNECTOR_REGISTRY, CacheConnectorType.MEMORY)

    with pytest.raises(RuntimeError, match="Cache connector not implemented"):
        get_cache_connector()


def test_register_cache_connector_error_on_duplicate() -> None:
    with pytest.raises(RuntimeError, match="Already registered cache connector"):
        register_cache_connector(CacheConnectorType.VALKEY, ValkeyCacheConnector)
