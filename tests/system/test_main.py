from typing import TYPE_CHECKING
from unittest.mock import Mock

import pytest
from neo4j.exceptions import ServiceUnavailable
from pydantic import SecretStr
from pytest import MonkeyPatch
from starlette import status
from valkey import Valkey
from valkey.exceptions import ConnectionError as ValkeyConnectionError

from mex.common.testing import Joker

if TYPE_CHECKING:
    from fastapi.testclient import TestClient

    from mex.backend.settings import BackendSettings
    from tests.conftest import MockedGraph

# the valkey server published by `compose.yaml` for integration testing
VALKEY_TEST_URL = SecretStr("valkey://localhost:6379")


@pytest.fixture
def mocked_valkey_client(monkeypatch: MonkeyPatch, settings: BackendSettings) -> Mock:
    """Point the cache connector at a mocked valkey client."""
    client = Mock(spec=Valkey)
    monkeypatch.setattr(settings, "valkey_url", VALKEY_TEST_URL)
    monkeypatch.setattr(Valkey, "from_url", lambda _: client)
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


@pytest.mark.usefixtures("mocked_valkey")
def test_check_neo4j_status(client: TestClient, mocked_graph: MockedGraph) -> None:
    mocked_graph.return_value = [{"version": "2026.07.1"}]

    response = client.get("/v0/_system/neo4j")

    assert response.status_code == status.HTTP_200_OK, response.text
    assert response.json() == {"status": "ok", "version": "2026.07.1"}


@pytest.mark.usefixtures("mocked_valkey")
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


def test_check_valkey_status_local(
    client: TestClient, monkeypatch: MonkeyPatch, settings: BackendSettings
) -> None:
    monkeypatch.setattr(settings, "valkey_url", None)

    response = client.get("/v0/_system/valkey")

    assert response.status_code == status.HTTP_200_OK, response.text
    assert response.json() == {"status": "local", "version": "unknown"}


def test_check_valkey_status(client: TestClient, mocked_valkey_client: Mock) -> None:
    mocked_valkey_client.info.return_value = {
        "valkey_version": "9.1.1",
        "redis_version": "7.2.4",
    }

    response = client.get("/v0/_system/valkey")

    assert response.status_code == status.HTTP_200_OK, response.text
    assert response.json() == {"status": "ok", "version": "9.1.1"}


def test_check_valkey_status_falls_back_to_redis_version(
    client: TestClient, mocked_valkey_client: Mock
) -> None:
    mocked_valkey_client.info.return_value = {"redis_version": "7.2.4"}

    response = client.get("/v0/_system/valkey")

    assert response.status_code == status.HTTP_200_OK, response.text
    assert response.json() == {"status": "ok", "version": "7.2.4"}


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
def test_check_valkey_status_integration(
    client: TestClient, monkeypatch: MonkeyPatch, settings: BackendSettings
) -> None:
    monkeypatch.setattr(settings, "valkey_url", VALKEY_TEST_URL)

    response = client.get("/v0/_system/valkey")

    assert response.status_code == status.HTTP_200_OK, response.text
    assert response.json() == {"status": "ok", "version": Joker()}
