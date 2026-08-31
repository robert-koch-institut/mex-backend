import json
from typing import TYPE_CHECKING

import pytest
from pydantic import BaseModel
from pytest import MonkeyPatch

from mex.backend.cache.memory import MemoryCacheConnector
from mex.common.models import VersionStatus

if TYPE_CHECKING:
    from mex.backend.settings import BackendSettings


class DummyModel(BaseModel):
    name: str
    value: int


@pytest.fixture
def connector() -> MemoryCacheConnector:
    return MemoryCacheConnector()


def test_roundtrip(connector: MemoryCacheConnector) -> None:
    model = DummyModel(name="test", value=42)

    assert connector.get_value("test_key") is None

    connector.set_value("test_key", model)
    assert connector.get_value("test_key") == model.model_dump()

    connector.delete_value("test_key")
    assert connector.get_value("test_key") is None


def test_delete_value_missing_key(connector: MemoryCacheConnector) -> None:
    connector.delete_value("missing_key")

    assert connector.get_value("missing_key") is None


def test_get_value_invalid_json(connector: MemoryCacheConnector) -> None:
    connector._set("test_key", "invalid json")

    with pytest.raises(json.JSONDecodeError):
        connector.get_value("test_key")


def test_metrics(connector: MemoryCacheConnector) -> None:
    assert connector.metrics() == {
        "dbsize": 0,
        "keyspace_hits_total": 0,
        "keyspace_misses_total": 0,
    }

    connector.set_value("key1", DummyModel(name="test", value=42))
    connector.set_value("key2", DummyModel(name="test", value=42))
    connector.get_value("key1")
    connector.get_value("key2")
    connector.get_value("missing_key")

    assert connector.metrics() == {
        "dbsize": 2,
        "keyspace_hits_total": 2,
        "keyspace_misses_total": 1,
    }


def test_flush_in_debug_mode(
    connector: MemoryCacheConnector, monkeypatch: MonkeyPatch, settings: BackendSettings
) -> None:
    monkeypatch.setattr(settings, "debug", True)
    connector.set_value("test_key", DummyModel(name="test", value=42))
    connector.get_value("test_key")

    connector.flush()

    assert connector.metrics() == {
        "dbsize": 0,
        "keyspace_hits_total": 0,
        "keyspace_misses_total": 0,
    }
    assert connector.get_value("test_key") is None


def test_flush_not_in_debug_mode(
    connector: MemoryCacheConnector, monkeypatch: MonkeyPatch, settings: BackendSettings
) -> None:
    monkeypatch.setattr(settings, "debug", False)
    connector.set_value("test_key", DummyModel(name="test", value=42))
    connector.get_value("test_key")

    connector.flush()

    assert connector.metrics() == {
        "dbsize": 1,
        "keyspace_hits_total": 1,
        "keyspace_misses_total": 0,
    }
    assert connector.get_value("test_key") is not None


def test_get_status(connector: MemoryCacheConnector) -> None:
    assert connector.get_status() == VersionStatus(status="local", version="unknown")


def test_close(connector: MemoryCacheConnector) -> None:
    connector.set_value("test_key", DummyModel(name="test", value=42))

    connector.close()

    assert connector.get_value("test_key") is None
