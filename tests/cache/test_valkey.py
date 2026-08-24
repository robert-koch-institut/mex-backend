from unittest.mock import Mock

import pytest
from pydantic import BaseModel
from pytest import MonkeyPatch
from valkey import Valkey
from valkey.exceptions import ValkeyError

from mex.backend.cache.valkey import ValkeyCacheConnector


class DummyModel(BaseModel):
    name: str
    value: int


@pytest.fixture
def mocked_client(monkeypatch: MonkeyPatch) -> Mock:
    client = Mock(spec=Valkey)
    client.get.return_value = None
    monkeypatch.setattr(Valkey, "from_url", lambda _url: client)
    return client


def test_init_connects_to_configured_url(monkeypatch: MonkeyPatch) -> None:
    urls: list[str] = []

    def from_url(url: str) -> Mock:
        urls.append(url)
        return Mock(spec=Valkey)

    monkeypatch.setattr(Valkey, "from_url", from_url)

    ValkeyCacheConnector()

    assert urls == ["valkey://localhost:6379"]


def test_get_value(mocked_client: Mock) -> None:
    mocked_client.get.return_value = '{"name": "test", "value": 42}'
    connector = ValkeyCacheConnector()

    assert connector.get_value("test_key") == {"name": "test", "value": 42}
    mocked_client.get.assert_called_once_with("test_key")


def test_get_value_nonexistent_key(mocked_client: Mock) -> None:
    connector = ValkeyCacheConnector()

    assert connector.get_value("test_key") is None
    mocked_client.get.assert_called_once_with("test_key")


def test_set_value(mocked_client: Mock) -> None:
    connector = ValkeyCacheConnector()

    connector.set_value("test_key", DummyModel(name="test", value=42))

    mocked_client.set.assert_called_once_with(
        "test_key", '{"name": "test", "value": 42}'
    )


def test_delete_value(mocked_client: Mock) -> None:
    connector = ValkeyCacheConnector()

    connector.delete_value("test_key")

    mocked_client.delete.assert_called_once_with("test_key")


def test_metrics_filters_non_integers(mocked_client: Mock) -> None:
    mocked_client.info.return_value = {
        "used_memory": 1024,
        "keyspace_hits": 100,
        "version": "7.0.0",
    }
    connector = ValkeyCacheConnector()

    assert connector.metrics() == {"used_memory": 1024, "keyspace_hits": 100}


def test_close(mocked_client: Mock) -> None:
    connector = ValkeyCacheConnector()

    connector.close()

    mocked_client.close.assert_called_once()


def test_errors_are_propagated(mocked_client: Mock) -> None:
    mocked_client.get.side_effect = ValkeyError("Valkey operation failed")
    mocked_client.set.side_effect = ValkeyError("Valkey operation failed")
    connector = ValkeyCacheConnector()

    with pytest.raises(ValkeyError):
        connector.get_value("test_key")

    with pytest.raises(ValkeyError):
        connector.set_value("test_key", DummyModel(name="test", value=42))
