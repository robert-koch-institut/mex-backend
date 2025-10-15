import pytest
from fastapi.testclient import TestClient
from pytest import MonkeyPatch
from starlette import status

from mex.backend.settings import BackendSettings
from mex.common.testing import Joker


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


@pytest.mark.integration
def test_flush_graph_database_unauthorized(client: TestClient) -> None:
    response = client.delete("/v0/_system/graph")
    assert response.status_code == status.HTTP_401_UNAUTHORIZED, response.text
    assert response.json() == {
        "detail": "Missing authentication header X-API-Key or credentials."
    }


@pytest.mark.integration
def test_flush_graph_database_forbidden(
    client_with_api_key_read_permission: TestClient,
) -> None:
    response = client_with_api_key_read_permission.delete("/v0/_system/graph")
    assert response.status_code == status.HTTP_403_FORBIDDEN, response.text
    assert response.json() == {"detail": "Unauthorized API Key."}


def test_flush_graph_database_refused(
    client_with_api_key_write_permission: TestClient,
) -> None:
    response = client_with_api_key_write_permission.delete("/v0/_system/graph")
    assert response.status_code == status.HTTP_403_FORBIDDEN, response.text
    assert response.json() == {"detail": "refusing to flush the database"}


@pytest.mark.integration
def test_flush_graph_database(
    client_with_api_key_write_permission: TestClient,
    monkeypatch: MonkeyPatch,
) -> None:
    settings = BackendSettings.get()
    monkeypatch.setattr(settings, "debug", True)
    response = client_with_api_key_write_permission.delete("/v0/_system/graph")
    assert response.status_code == status.HTTP_200_OK, response.text
    assert response.json() == {"status": "ok"}
