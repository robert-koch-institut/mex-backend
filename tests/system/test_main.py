from typing import TYPE_CHECKING
from unittest.mock import Mock

import pytest
from neo4j.exceptions import ServiceUnavailable
from starlette import status
from valkey import Valkey
from valkey.exceptions import ConnectionError as ValkeyConnectionError

from mex.backend.cache import (
    MemoryCacheConnector,
    ValkeyCacheConnector,
    get_cache_connector,
)
from mex.backend.types import CacheConnectorType
from mex.common.models import VersionStatus
from mex.common.testing import Joker

if TYPE_CHECKING:
    from fastapi.testclient import TestClient
    from pytest import MonkeyPatch

    from mex.backend.settings import BackendSettings
    from tests.conftest import MockedGraph


@pytest.fixture
def mocked_valkey_client(monkeypatch: MonkeyPatch, settings: BackendSettings) -> Mock:
    """Configure the valkey cache connector and point it at a mocked client."""
    client = Mock(spec=Valkey)
    monkeypatch.setattr(settings, "cache_connector", CacheConnectorType.VALKEY)
    monkeypatch.setattr(Valkey, "from_url", lambda _url: client)
    return client


def test_openapi(client: TestClient) -> None:
    response = client.get("/openapi.json")
    assert response.status_code == status.HTTP_200_OK, response.text
    response_json = response.json()
    assert "/v0/extracted-item/{identifier}" in response_json["paths"]
    assert "PreventiveOrganizationalUnit" in response_json["components"]["schemas"]


def test_health_check(client: TestClient) -> None:
    response = client.get("/v0/_system/check")
    assert response.status_code == status.HTTP_200_OK, response.text
    assert response.json() == {"status": "ok", "version": Joker()}


def test_check_neo4j_status(client: TestClient, mocked_graph: MockedGraph) -> None:
    mocked_graph.return_value = [{"version": "2026.07.1"}]

    response = client.get("/v0/_system/neo4j")

    assert response.status_code == status.HTTP_200_OK, response.text
    assert response.json() == {"status": "ok", "version": "2026.07.1"}


def test_check_neo4j_status_unreachable(
    client: TestClient, mocked_graph: MockedGraph
) -> None:
    mocked_graph.run.side_effect = ServiceUnavailable("cannot connect to neo4j")

    response = client.get("/v0/_system/neo4j")

    assert response.status_code == status.HTTP_200_OK, response.text
    assert response.json() == {"status": "offline", "version": "unknown"}


@pytest.mark.integration
def test_check_neo4j_status_integration(client: TestClient) -> None:
    response = client.get("/v0/_system/neo4j")

    assert response.status_code == status.HTTP_200_OK, response.text
    assert response.json() == {"status": "ok", "version": Joker()}


def test_check_valkey_status_with_memory_cache(
    client: TestClient, monkeypatch: MonkeyPatch, settings: BackendSettings
) -> None:
    monkeypatch.setattr(settings, "cache_connector", CacheConnectorType.MEMORY)

    response = client.get("/v0/_system/valkey")

    assert response.status_code == status.HTTP_200_OK, response.text
    assert response.json() == {"status": "ok", "version": Joker()}


def test_check_valkey_status(client: TestClient, mocked_valkey_client: Mock) -> None:
    mocked_valkey_client.info.return_value = {"valkey_version": "9.1.1"}

    response = client.get("/v0/_system/valkey")

    assert response.status_code == status.HTTP_200_OK, response.text
    assert response.json() == {"status": "ok", "version": "9.1.1"}


def test_check_valkey_status_without_version(
    client: TestClient, mocked_valkey_client: Mock
) -> None:
    mocked_valkey_client.info.return_value = {"used_memory": 1024}

    response = client.get("/v0/_system/valkey")

    assert response.status_code == status.HTTP_200_OK, response.text
    assert response.json() == {"status": "ok", "version": "unknown"}


def test_check_valkey_status_unreachable(
    client: TestClient, mocked_valkey_client: Mock
) -> None:
    mocked_valkey_client.info.side_effect = ValkeyConnectionError(
        "cannot connect to valkey"
    )

    response = client.get("/v0/_system/valkey")

    assert response.status_code == status.HTTP_200_OK, response.text
    assert response.json() == {"status": "offline", "version": "unknown"}


@pytest.mark.integration
def test_check_valkey_status_integration(client: TestClient) -> None:
    response = client.get("/v0/_system/valkey")

    assert response.status_code == status.HTTP_200_OK, response.text
    assert response.json() == {"status": "ok", "version": Joker()}


def parse_metrics(text: str) -> dict[str, tuple[str, int]]:
    metric_types = {}
    metrics = {}
    for line in text.splitlines():
        if line.startswith("# TYPE "):
            _, _, name, metric_type = line.split(" ")
            metric_types[name] = metric_type
        elif line:
            name, value = line.split(" ")
            metrics[name] = (metric_types[name], int(value))
    return metrics


@pytest.mark.integration
def test_prometheus_metrics_for_valkey_cache(client: TestClient) -> None:
    connector = get_cache_connector()
    assert isinstance(connector, ValkeyCacheConnector)
    connector.set_value("test_key", VersionStatus(status="ok", version="1"))
    assert connector.get_value("test_key") is not None
    assert connector.get_value("missing_key") is None

    response = client.get("/v0/_system/metrics")

    assert response.status_code == status.HTTP_200_OK, response.text
    assert parse_metrics(response.text) == {
        "valkey_cache_connector_dbsize": ("gauge", 1),
        "valkey_cache_connector_connected_clients": ("gauge", Joker()),
        "valkey_cache_connector_evicted_keys_total": ("counter", 0),
        "valkey_cache_connector_keyspace_hits_total": ("counter", Joker()),
        "valkey_cache_connector_keyspace_misses_total": ("counter", Joker()),
        "valkey_cache_connector_uptime_in_seconds": ("gauge", Joker()),
        "valkey_cache_connector_used_memory_bytes": ("gauge", Joker()),
    }


@pytest.mark.integration
def test_prometheus_metrics_for_memory_cache(
    client: TestClient, monkeypatch: MonkeyPatch, settings: BackendSettings
) -> None:
    monkeypatch.setattr(settings, "cache_connector", CacheConnectorType.MEMORY)
    connector = get_cache_connector()
    assert isinstance(connector, MemoryCacheConnector)
    connector.set_value("test_key", VersionStatus(status="ok", version="1"))
    assert connector.get_value("test_key") is not None
    assert connector.get_value("missing_key") is None

    response = client.get("/v0/_system/metrics")

    assert response.status_code == status.HTTP_200_OK, response.text
    assert parse_metrics(response.text) == {
        "memory_cache_connector_dbsize": ("gauge", 1),
        "memory_cache_connector_keyspace_hits_total": ("counter", 1),
        "memory_cache_connector_keyspace_misses_total": ("counter", 1),
    }
