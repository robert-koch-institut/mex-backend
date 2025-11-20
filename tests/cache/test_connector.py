import json
from typing import Any
from unittest.mock import Mock

import pytest
from pydantic import BaseModel
from pytest import MonkeyPatch
from valkey import Valkey
from valkey.exceptions import ConnectionError as ValkeyConnectionError
from valkey.exceptions import ValkeyError

from mex.backend.cache.connector import CacheConnector, LocalCache, ValkeyCache
from mex.backend.settings import BackendSettings
from mex.common.transform import MExEncoder


class DummyModel(BaseModel):
    """Dummy model for testing cache operations."""

    name: str
    value: int


@pytest.fixture
def sample_model() -> DummyModel:
    """Return a sample model for testing."""
    return DummyModel(name="test", value=42)


@pytest.fixture
def local_cache() -> LocalCache:
    """Return a LocalCache instance."""
    return LocalCache()


@pytest.fixture
def mocked_valkey() -> Mock:
    """Return a mocked Valkey instance."""
    mock_valkey = Mock(spec=Valkey)
    mock_valkey.get.return_value = None
    mock_valkey.set.return_value = None
    mock_valkey.info.return_value = {"used_memory": 1024, "keyspace_hits": 100}
    mock_valkey.flushdb.return_value = None
    mock_valkey.close.return_value = None
    return mock_valkey


def test_local_cache_initialization(local_cache: LocalCache) -> None:
    assert len(local_cache) == 0


def test_local_cache_set_and_get(local_cache: LocalCache) -> None:
    local_cache.set("key1", "value1")
    local_cache.set("key2", "value2")

    assert local_cache.get("key1") == "value1"
    assert local_cache.get("key2") == "value2"
    assert local_cache.get("nonexistent") is None


def test_local_cache_info(local_cache: LocalCache) -> None:
    info = local_cache.info()
    assert info == {"local_cache_size": 0}

    local_cache.set("key1", "value1")
    local_cache.set("key2", "value2")

    info = local_cache.info()
    assert info == {"local_cache_size": 2}


def test_local_cache_flushdb(local_cache: LocalCache) -> None:
    local_cache.set("key1", "value1")
    local_cache.set("key2", "value2")
    assert len(local_cache) == 2

    local_cache.flushdb()
    assert len(local_cache) == 0


def test_local_cache_close(local_cache: LocalCache) -> None:
    local_cache.close()


def test_cache_connector_with_valkey_url(
    monkeypatch: MonkeyPatch, mocked_valkey: Mock
) -> None:
    settings = BackendSettings.get()
    monkeypatch.setattr(settings, "valkey_url", "valkey://localhost:6379")
    monkeypatch.setattr(Valkey, "from_url", lambda url: mocked_valkey)

    connector = CacheConnector()
    assert isinstance(connector._cache, ValkeyCache)
    assert connector._cache._client is mocked_valkey


def test_cache_connector_without_valkey_url(monkeypatch: MonkeyPatch) -> None:
    settings = BackendSettings.get()
    monkeypatch.setattr(settings, "valkey_url", None)

    connector = CacheConnector()
    assert isinstance(connector._cache, LocalCache)
    assert len(connector._cache) == 0


def test_get_value_existing_key(
    monkeypatch: MonkeyPatch, sample_model: DummyModel
) -> None:
    mock_cache = Mock()
    mock_cache.get.return_value = json.dumps(sample_model.model_dump(), cls=MExEncoder)

    connector = CacheConnector()
    monkeypatch.setattr(connector, "_cache", mock_cache)

    result = connector.get_value("test_key")
    expected = sample_model.model_dump()

    assert result == expected
    mock_cache.get.assert_called_once_with("test_key")


def test_get_value_nonexistent_key(monkeypatch: MonkeyPatch) -> None:
    mock_cache = Mock()
    mock_cache.get.return_value = None

    connector = CacheConnector()
    monkeypatch.setattr(connector, "_cache", mock_cache)

    result = connector.get_value("nonexistent_key")
    assert result is None
    mock_cache.get.assert_called_once_with("nonexistent_key")


def test_get_value_invalid_json(monkeypatch: MonkeyPatch) -> None:
    mock_cache = Mock()
    mock_cache.get.return_value = "invalid json"

    connector = CacheConnector()
    monkeypatch.setattr(connector, "_cache", mock_cache)

    with pytest.raises(json.JSONDecodeError):
        connector.get_value("test_key")


def test_set_value(monkeypatch: MonkeyPatch, sample_model: DummyModel) -> None:
    mock_cache = Mock()

    connector = CacheConnector()
    monkeypatch.setattr(connector, "_cache", mock_cache)

    connector.set_value("test_key", sample_model)

    mock_cache.set.assert_called_once()
    call_args = mock_cache.set.call_args
    assert call_args[0][0] == "test_key"

    serialized_value = json.loads(call_args[0][1])
    assert serialized_value == sample_model.model_dump()


def test_metrics_with_integer_values(monkeypatch: MonkeyPatch) -> None:
    mock_cache = Mock()
    mock_cache.info.return_value = {
        "used_memory": 1024,
        "keyspace_hits": 100,
        "keyspace_misses": 50,
        "connected_clients": 5,
        "version": "7.0.0",
        "valkey_mode": "standalone",
    }

    connector = CacheConnector()
    monkeypatch.setattr(connector, "_cache", mock_cache)

    metrics = connector.metrics()
    expected = {
        "used_memory": 1024,
        "keyspace_hits": 100,
        "keyspace_misses": 50,
        "connected_clients": 5,
    }

    assert metrics == expected
    mock_cache.info.assert_called_once()


def test_metrics_with_local_cache(monkeypatch: MonkeyPatch) -> None:
    local_cache = LocalCache()
    local_cache.set("key1", "value1")
    local_cache.set("key2", "value2")

    connector = CacheConnector()
    monkeypatch.setattr(connector, "_cache", local_cache)

    metrics = connector.metrics()
    assert metrics == {"local_cache_size": 2}


def test_flush_in_debug_mode(monkeypatch: MonkeyPatch) -> None:
    settings = BackendSettings.get()
    monkeypatch.setattr(settings, "debug", True)

    mock_cache = Mock()
    connector = CacheConnector()
    monkeypatch.setattr(connector, "_cache", mock_cache)

    connector.flush()
    mock_cache.flushdb.assert_called_once()


def test_flush_not_in_debug_mode(monkeypatch: MonkeyPatch) -> None:
    settings = BackendSettings.get()
    monkeypatch.setattr(settings, "debug", False)

    mock_cache = Mock()
    connector = CacheConnector()
    monkeypatch.setattr(connector, "_cache", mock_cache)

    connector.flush()
    mock_cache.flushdb.assert_not_called()


def test_close(monkeypatch: MonkeyPatch) -> None:
    mock_cache = Mock()
    connector = CacheConnector()
    monkeypatch.setattr(connector, "_cache", mock_cache)

    connector.close()
    mock_cache.close.assert_called_once()


def test_valkey_connection_error_fallback(monkeypatch: MonkeyPatch) -> None:
    settings = BackendSettings.get()
    monkeypatch.setattr(settings, "valkey_url", "valkey://localhost:6379")

    def mock_from_url(_url: str) -> Mock:
        mock_valkey = Mock()
        mock_valkey.get.side_effect = ValkeyConnectionError("Connection failed")
        return mock_valkey

    monkeypatch.setattr(Valkey, "from_url", mock_from_url)

    connector = CacheConnector()

    with pytest.raises(ValkeyConnectionError):
        connector.get_value("test_key")


def test_valkey_operation_errors(
    monkeypatch: MonkeyPatch, sample_model: DummyModel
) -> None:
    mock_valkey = Mock()
    mock_valkey.get.side_effect = ValkeyError("Valkey operation failed")
    mock_valkey.set.side_effect = ValkeyError("Valkey operation failed")

    monkeypatch.setattr(Valkey, "from_url", lambda url: mock_valkey)
    settings = BackendSettings.get()
    monkeypatch.setattr(settings, "valkey_url", "valkey://localhost:6379")

    connector = CacheConnector()

    with pytest.raises(ValkeyError):
        connector.get_value("test_key")

    with pytest.raises(ValkeyError):
        connector.set_value("test_key", sample_model)


def test_roundtrip_with_local_cache(
    monkeypatch: MonkeyPatch, sample_model: DummyModel
) -> None:
    settings = BackendSettings.get()
    monkeypatch.setattr(settings, "valkey_url", None)

    connector = CacheConnector()

    assert connector.get_value("test_key") is None

    connector.set_value("test_key", sample_model)
    result = connector.get_value("test_key")

    assert result == sample_model.model_dump()


def test_complex_model_serialization(monkeypatch: MonkeyPatch) -> None:
    class ComplexModel(BaseModel):
        name: str
        items: list[dict[str, Any]]
        metadata: dict[str, str]

    complex_model = ComplexModel(
        name="complex",
        items=[{"id": 1, "value": "test"}, {"id": 2, "value": "test2"}],
        metadata={"version": "1.0", "author": "test"},
    )

    settings = BackendSettings.get()
    monkeypatch.setattr(settings, "valkey_url", None)

    connector = CacheConnector()
    connector.set_value("complex_key", complex_model)
    result = connector.get_value("complex_key")

    assert result == complex_model.model_dump()


def test_cache_isolation_after_flush(monkeypatch: MonkeyPatch) -> None:
    settings = BackendSettings.get()
    monkeypatch.setattr(settings, "valkey_url", None)
    monkeypatch.setattr(settings, "debug", True)

    connector = CacheConnector()
    sample_model = DummyModel(name="test", value=42)

    connector.set_value("test_key", sample_model)
    assert connector.get_value("test_key") is not None

    connector.flush()

    assert connector.get_value("test_key") is None


def test_metrics_consistency(monkeypatch: MonkeyPatch) -> None:
    settings = BackendSettings.get()
    monkeypatch.setattr(settings, "valkey_url", None)

    connector = CacheConnector()
    sample_model = DummyModel(name="test", value=42)

    initial_metrics = connector.metrics()
    assert initial_metrics["local_cache_size"] == 0

    connector.set_value("key1", sample_model)
    connector.set_value("key2", sample_model)

    updated_metrics = connector.metrics()
    assert updated_metrics["local_cache_size"] == 2
